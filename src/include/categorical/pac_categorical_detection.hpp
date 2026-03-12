//
// PAC Categorical Pattern Detection
//
// Read-only analysis of logical plans to find categorical query patterns.
// A categorical pattern is a comparison (in a filter, join, or projection)
// where one or both sides trace back to a PAC aggregate through the plan.
//
// This is the detection phase only — no plan mutation happens here.
// See pac_categorical_rewriter.hpp/.cpp for the rewriting phase.
//
// Created by ila on 1/23/26.
//

#ifndef PAC_CATEGORICAL_DETECTION_HPP
#define PAC_CATEGORICAL_DETECTION_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types.hpp"
#include "query_processing/pac_plan_traversal.hpp"

namespace duckdb {

// ============================================================================
// Structs — detection inputs and outputs
// ============================================================================

// Metadata about a PAC aggregate produced by the bitslice compiler.
// Keyed by aggregate_index in PacAggregateInfoMap.
struct PacAggregateInfo {
	idx_t aggregate_index;      // agg->aggregate_index
	idx_t group_index;          // agg->group_index
	ColumnBinding hash_binding; // the hash column binding used for this aggregate's children[0]
};
// Map from aggregate_index → PacAggregateInfo
using PacAggregateInfoMap = unordered_map<idx_t, PacAggregateInfo>;

// Information about a single PAC aggregate binding found in an expression
struct PacBindingInfo {
	ColumnBinding binding;
	idx_t index;                  // Position in the list (0-based, for list_zip field access)
	ColumnBinding source_binding; // The aggregate-level binding that defines this PAC aggregate
	PacBindingInfo() : index(0) {
	}
};

// Information about a detected categorical pattern
struct CategoricalPatternInfo {
	// The parent operator containing the expression (Filter, Join, or Projection)
	LogicalOperator *parent_op;
	// Index of the expression in the parent's expressions list (or conditions list for joins)
	idx_t expr_index;
	// Pre-collected PAC bindings from detection
	vector<PacBindingInfo> pac_bindings;
	// Hash binding from outer PAC aggregate (resolved to filter level)
	ColumnBinding outer_pac_hash;
	// True if an outer PAC aggregate was found above this pattern
	bool has_outer_pac_hash = false;

	CategoricalPatternInfo() : parent_op(nullptr), expr_index(0) {
	}
};

// ============================================================================
// Shared inline helpers — used by both detection and rewriting
// ============================================================================

// Check if name matches pac_noised_<aggr> (noised scalar aggregates).
// Default: matches pac_noised_sum, pac_noised_count, pac_noised_min, pac_noised_max.
// With prefix/suffix overrides: matches other PAC aggregate families (counters/list = pac_<aggr>, bare).
static inline bool IsPacAggregate(const string &pattern, const string &suffix = "",
                                  const string &prefix = "pac_noised_") {
	const string name = StringUtil::Lower(pattern);
	for (auto &aggr_name : {"sum", "count", "min", "max", "avg"}) {
		if (name == prefix + aggr_name + suffix) {
			return true;
		}
	}
	return false;
}

// pac_sum, pac_count, pac_min, pac_max (counters and list variants share this name)
static inline bool IsPacCountersAggregate(const string &name) {
	return IsPacAggregate(name, "", "pac_");
}

// pac_sum, pac_count, pac_min, pac_max (same name for list variant)
static inline bool IsPacListAggregate(const string &name) {
	return IsPacAggregate(name, "", "pac_");
}

static inline bool IsAnyPacAggregate(const string &name) {
	return IsPacAggregate(name) || IsPacCountersAggregate(name);
}

// pac_noised_sum → pac_sum (counters/list variant name)
string inline GetCountersVariant(const string &aggregate_name) {
	if (IsPacCountersAggregate(aggregate_name)) {
		return aggregate_name;
	}
	for (auto &aggr : {"sum", "count", "min", "max", "avg"}) {
		if (aggregate_name == string("pac_noised_") + aggr) {
			return string("pac_") + aggr;
		}
	}
	return "";
}

// pac_sum → pac_noised_sum
static inline string GetBasePacAggregateName(const string &name) {
	for (auto &aggr : {"sum", "count", "min", "max", "avg"}) {
		if (name == string("pac_") + aggr) {
			return string("pac_noised_") + aggr;
		}
	}
	return name;
}

// Any variant → pac_sum (list aggregate name, same as counters name now)
static inline string GetListAggregateVariant(const string &name) {
	for (auto &aggr : {"sum", "count", "min", "max", "avg"}) {
		// Match bare names: sum → pac_sum
		if (name == string(aggr)) {
			return string("pac_") + aggr;
		}
		// Match noised names: pac_noised_sum → pac_sum
		if (name == string("pac_noised_") + aggr) {
			return string("pac_") + aggr;
		}
		// Match already-correct names: pac_sum → pac_sum
		if (name == string("pac_") + aggr) {
			return string("pac_") + aggr;
		}
	}
	return "";
}

// Check if a type is numerical (can be used with pac_noised)
inline bool IsNumericalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

// Hash a column binding to a unique 64-bit value for use in maps/sets
static inline uint64_t HashBinding(const ColumnBinding &binding) {
	return (uint64_t(binding.table_index) << 32) | binding.column_index;
}

// Rebind an aggregate function by name (catalog lookup + FunctionBinder).
// Used by both top-k pushdown (_counters variants) and categorical rewriting.
static inline unique_ptr<Expression> RebindAggregate(ClientContext &context, const string &func_name,
                                                     vector<unique_ptr<Expression>> children, bool is_distinct) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &func_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, func_name);
	vector<LogicalType> arg_types;
	for (auto &child : children) {
		arg_types.push_back(child->return_type);
	}
	ErrorData error;
	FunctionBinder function_binder(context);
	auto best_function = function_binder.BindFunction(func_name, func_entry.functions, arg_types, error);
	if (!best_function.IsValid()) {
		return nullptr;
	}
	AggregateFunction func = func_entry.functions.GetFunctionByOffset(best_function.GetIndex());
	return function_binder.BindAggregateFunction(func, std::move(children), nullptr,
	                                             is_distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT);
}

// Walk through cast expressions to find the underlying expression.
// Returns the innermost non-cast expression.
static inline Expression *StripCasts(Expression *expr) {
	while (expr->type == ExpressionType::OPERATOR_CAST) {
		expr = expr->Cast<BoundCastExpression>().child.get();
	}
	return expr;
}

// Check if an expression is already wrapped in a categorical rewrite terminal function
// This includes pac_noised, pac_filter, list_transform, and list_zip
// These functions indicate that the expression has already been processed by categorical rewriting
static inline bool IsAlreadyWrappedInPacNoised(Expression *expr) {
	if (!expr) {
		return false;
	}
	if (expr->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = expr->Cast<BoundFunctionExpression>();
		// Check for all categorical rewrite terminal functions
		if (StringUtil::StartsWith(func.function.name, "pac_noised") ||
		    StringUtil::StartsWith(func.function.name, "pac_filter") ||
		    StringUtil::StartsWith(func.function.name, "pac_select") || func.function.name == "pac_coalesce" ||
		    func.function.name == "list_transform" || func.function.name == "list_zip") {
			return true;
		}
	}
	// Check for CAST(terminal_function(...))
	if (expr->type == ExpressionType::OPERATOR_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		return IsAlreadyWrappedInPacNoised(cast.child.get());
	}
	return false;
}

// Check if a CASE expression is DuckDB's scalar subquery wrapper pattern:
// CASE WHEN count>1 THEN error(...) ELSE value END
static inline bool IsScalarSubqueryWrapper(const BoundCaseExpression &case_expr) {
	if (case_expr.case_checks.size() != 1 || !case_expr.else_expr) {
		return false;
	}
	auto &check = case_expr.case_checks[0];
	if (check.then_expr && check.then_expr->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = check.then_expr->Cast<BoundFunctionExpression>();
		return func.function.name == "error";
	}
	return false;
}

// ============================================================================
// Detection-only inline helpers
// ============================================================================

// Search an operator subtree for PAC aggregates
// Returns the first PAC aggregate name found, empty string otherwise
static inline string FindPacAggregateInOperator(LogicalOperator *op) {
	// Check if this is an aggregate operator with PAC aggregates
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		for (auto &agg_expr : agg.expressions) {
			if (agg_expr->type == ExpressionType::BOUND_AGGREGATE) {
				auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
				if (IsAnyPacAggregate(bound_agg.function.name)) {
					return GetBasePacAggregateName(bound_agg.function.name);
				}
			}
		}
	}
	for (auto &child : op->children) {
		string result = FindPacAggregateInOperator(child.get());
		if (!result.empty()) {
			return result;
		}
	}
	return "";
}

// Recognize DuckDB's scalar subquery wrapper pattern
//
// Pattern  (uncorrelated): Projection -> Aggregate(first) -> Projection
//   Projection (CASE error if count > 1, else first(value))
//   └── Aggregate (first(#0), count_star())
//       └── Projection (#0)
//           └── [actual scalar subquery result]
static inline LogicalOperator *RecognizeDuckDBScalarWrapper(LogicalOperator *op) {
	auto &outer_proj = op->Cast<LogicalProjection>();
	if (outer_proj.expressions.empty()) {
		return nullptr;
	}
	auto &expr = outer_proj.expressions[0];
	if (expr->type != ExpressionType::CASE_EXPR) {
		return nullptr;
	}
	if (!IsScalarSubqueryWrapper(expr->Cast<BoundCaseExpression>())) {
		return nullptr;
	}
	if (outer_proj.children.empty()) {
		return nullptr;
	}
	auto *child = outer_proj.children[0].get();
	if (child->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}
	auto &agg = child->Cast<LogicalAggregate>();
	if (agg.expressions.empty()) {
		return nullptr;
	}
	auto &agg_expr = agg.expressions[0];
	if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
		return nullptr;
	}
	auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
	if (StringUtil::Lower(bound_agg.function.name) != "first") {
		return nullptr;
	}
	if (agg.children.empty()) {
		return nullptr;
	}
	auto *inner_proj_op = agg.children[0].get();
	if (inner_proj_op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return nullptr;
	}
	auto &inner_proj = inner_proj_op->Cast<LogicalProjection>();
	return inner_proj.children.empty() ? nullptr : inner_proj.children[0].get();
}

// ============================================================================
// Detection function declarations
// ============================================================================

// Recursively search the plan for categorical patterns (plan-aware version).
// Detects filter/join/projection expressions containing PAC aggregates.
void DetectCategoricalPatterns(LogicalOperator *op, LogicalOperator *plan_root,
                               vector<CategoricalPatternInfo> &patterns, bool inside_aggregate,
                               const PacAggregateInfoMap &pac_agg_info, ColumnBinding outer_pac_hash = ColumnBinding());

// Find all PAC aggregate bindings in an expression (traces through plan).
vector<PacBindingInfo> FindAllPacBindingsInExpression(Expression *expr, LogicalOperator *plan_root);

} // namespace duckdb

#endif // PAC_CATEGORICAL_DETECTION_HPP
