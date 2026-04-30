//
// PAC Derived Table Rewriter
//
// Handles read and write paths for derived_pu tables (materialized views of PU data).
// Derived_pu tables store PAC counter lists (LIST<FLOAT>, 64 elements) instead of scalar values.
//
// Write path (ConvertDerivedPuToCounters):
//   Converts pac_noised_* aggregates to pac_* counter variants in INSERT/CTAS plans,
//   so the table stores raw counter lists instead of finalized noised scalars.
//
// Read path (InjectPacFinalizeForDerivedPu):
//   Rewrites comparisons on counter columns to pac_filter_<cmp>(scalar, counters)
//   calls, which evaluate all 64 subsamples via majority voting. Non-comparison
//   contexts (projections, ORDER BY) are wrapped with pac_finalize() instead.
//   Runs as a post-optimizer rule (after DuckDB's built-in optimizers).
//
// Key challenges:
//   - DuckDB's optimizer reorders columns and inserts intermediate projections,
//     so we use op->types (resolved output) for binding identification rather than
//     position-based matching against the original GET.
//   - DuckDB's filter pushdown may push comparisons (including BETWEEN) into
//     GET.table_filters before our post-optimizer runs. RewriteCounterFiltersFromGets
//     pulls these out and converts them to pac_filter_<cmp> calls.
//   - AGGREGATE operators (e.g. first() in scalar subqueries) need raw FLOAT[] input,
//     so pac_finalize injection is suppressed inside aggregate subtrees.
//

#include "query_processing/pac_derived_rewriter.hpp"

#include "aggregates/pac_aggregate.hpp"
#include "categorical/pac_categorical_detection.hpp"
#include "metadata/privacy_metadata_manager.hpp"
#include "privacy_debug.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// --- Helpers ---

static LogicalType CounterListType() {
	return LogicalType::LIST(PacFloatLogicalType());
}

// Returns true if the expression tree contains a pac_finalize call.
static bool ExpressionContainsPacFinalize(Expression &e) {
	if (e.type == ExpressionType::BOUND_FUNCTION && e.Cast<BoundFunctionExpression>().function.name == "pac_finalize") {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(e, [&](Expression &child) {
		if (!found && ExpressionContainsPacFinalize(child)) {
			found = true;
		}
	});
	return found;
}

// ============================================================================
// Write path: convert pac_noised_* → pac_* counter variants for derived_pu DML
// ============================================================================

static void ConvertAggregatesRecursive(OptimizerExtensionInput &input, LogicalOperator *op,
                                       unordered_set<uint64_t> &converted) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		ConvertAggregatesRecursive(input, child.get(), converted);
	}
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}

	auto &agg = op->Cast<LogicalAggregate>();
	auto list_type = CounterListType();

	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}

		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		if (!IsPacAggregate(bound_agg.function.name)) {
			continue;
		}

		string counters_name = GetCountersVariant(bound_agg.function.name);
		if (counters_name.empty()) {
			continue;
		}

		vector<unique_ptr<Expression>> children;
		for (auto &child_expr : bound_agg.children) {
			children.push_back(child_expr->Copy());
		}

		auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
		if (new_aggr) {
			PRIVACY_DEBUG_PRINT("[PAC DERIVED WRITE] Rebound " + bound_agg.function.name + " → " +
			                    new_aggr->Cast<BoundAggregateExpression>().function.name);
			agg.expressions[i] = std::move(new_aggr);
		} else {
			bound_agg.function.name = counters_name;
			bound_agg.function.return_type = list_type;
			expr->return_type = list_type;
		}

		idx_t types_index = agg.groups.size() + i;
		if (types_index < agg.types.size()) {
			agg.types[types_index] = list_type;
		}

		converted.insert(HashBinding(ColumnBinding(agg.aggregate_index, i)));
	}
}

static void FixStaleColumnRefTypes(LogicalOperator *op, const unordered_set<uint64_t> &converted) {
	if (!op) {
		return;
	}
	auto list_type = CounterListType();
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
		std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = e->Cast<BoundColumnRefExpression>();
				if (converted.count(HashBinding(col_ref.binding))) {
					col_ref.return_type = list_type;
				}
			}
			ExpressionIterator::EnumerateChildren(*e, Fix);
		};
		Fix(*expr_ptr);
	});
	for (auto &child : op->children) {
		FixStaleColumnRefTypes(child.get(), converted);
	}
}

static void StripRedundantCasts(LogicalOperator *op) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		StripRedundantCasts(child.get());
	}
	std::function<void(unique_ptr<Expression> &)> Strip = [&](unique_ptr<Expression> &e) {
		ExpressionIterator::EnumerateChildren(*e, Strip);
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.child->return_type == cast.return_type) {
				e = std::move(cast.child);
			}
		}
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { Strip(*expr_ptr); });
}

void ConvertDerivedPuToCounters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	unordered_set<uint64_t> converted;
	ConvertAggregatesRecursive(input, plan.get(), converted);
	if (!converted.empty()) {
		FixStaleColumnRefTypes(plan.get(), converted);
		StripRedundantCasts(plan.get());
	}
}

// ============================================================================
// Read path: inject pac_finalize for SELECT on derived_pu tables
// ============================================================================

// Find all GET operators that scan derived_pu tables with counter columns.
static void FindDerivedPuGets(LogicalOperator *op, unordered_set<idx_t> &derived_table_indices) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		FindDerivedPuGets(child.get(), derived_table_indices);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	auto entry = get.GetTable();
	if (!entry) {
		return;
	}
	auto &mgr = PrivacyMetadataManager::Get();
	auto *meta = mgr.GetTableMetadata(entry->name);
	if (!meta || !meta->derived_pu) {
		return;
	}
	auto list_type = CounterListType();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		if (get.returned_types[i] == list_type) {
			derived_table_indices.insert(get.table_index);
			return;
		}
	}
}

// Returns true if the expression is (or contains at its root) a counter column ref.
static bool IsCounterColumnRef(Expression &e, const LogicalType &list_type) {
	if (e.type == ExpressionType::BOUND_COLUMN_REF && e.return_type == list_type) {
		return true;
	}
	// Also catch CAST(counter_col → scalar) inserted by the binder
	if (e.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = e.Cast<BoundCastExpression>();
		return IsCounterColumnRef(*cast.child, list_type);
	}
	return false;
}

// Extract the raw counter column ref (stripping binder-inserted casts).
static unique_ptr<Expression> ExtractCounterRef(unique_ptr<Expression> e, const LogicalType &list_type) {
	if (e->type == ExpressionType::BOUND_COLUMN_REF && e->return_type == list_type) {
		return e;
	}
	if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = e->Cast<BoundCastExpression>();
		return ExtractCounterRef(std::move(cast.child), list_type);
	}
	return e;
}

// Map ExpressionType comparison to pac_filter_<cmp> function name.
// When the counter is on the left side, we flip the comparison direction.
static string GetPacFilterFuncName(ExpressionType cmp_type, bool counter_on_left) {
	if (counter_on_left) {
		// Flip: counter > scalar becomes pac_filter_lt(scalar, counter)
		switch (cmp_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			return "pac_filter_lt";
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return "pac_filter_lte";
		case ExpressionType::COMPARE_LESSTHAN:
			return "pac_filter_gt";
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return "pac_filter_gte";
		case ExpressionType::COMPARE_EQUAL:
			return "pac_filter_eq";
		case ExpressionType::COMPARE_NOTEQUAL:
			return "pac_filter_neq";
		default:
			return "";
		}
	} else {
		switch (cmp_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			return "pac_filter_gt";
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return "pac_filter_gte";
		case ExpressionType::COMPARE_LESSTHAN:
			return "pac_filter_lt";
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return "pac_filter_lte";
		case ExpressionType::COMPARE_EQUAL:
			return "pac_filter_eq";
		case ExpressionType::COMPARE_NOTEQUAL:
			return "pac_filter_neq";
		default:
			return "";
		}
	}
}

// Ensure the scalar side is cast to PAC_FLOAT for pac_filter_<cmp>.
// Strips binder-inserted CAST(scalar→FLOAT[]) and FLOAT[] constants back to scalars.
static unique_ptr<Expression> CastScalarToPacFloat(unique_ptr<Expression> scalar, const LogicalType &pac_float_type,
                                                   const LogicalType &list_type) {
	if (scalar->return_type == pac_float_type) {
		return scalar;
	}
	// Strip any binder-inserted CAST to FLOAT[] (scalar→LIST<FLOAT>)
	if (scalar->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = scalar->Cast<BoundCastExpression>();
		if (cast.return_type == list_type && cast.child->return_type != list_type) {
			scalar = std::move(cast.child);
		}
	}
	// Also unwrap FLOAT[] constants back to scalar
	if (scalar->type == ExpressionType::VALUE_CONSTANT) {
		auto &constant = scalar->Cast<BoundConstantExpression>();
		if (constant.return_type == list_type && !constant.value.IsNull()) {
			auto &list_children = ListValue::GetChildren(constant.value);
			if (!list_children.empty()) {
				auto scalar_val = list_children[0].DefaultCastAs(pac_float_type);
				return make_uniq<BoundConstantExpression>(std::move(scalar_val));
			}
		}
	}
	if (scalar->return_type != pac_float_type) {
		return BoundCastExpression::AddDefaultCastToType(std::move(scalar), pac_float_type);
	}
	return scalar;
}

// Rewrite a comparison expression on a counter column to a pac_filter_<cmp> call.
// Returns the rewritten expression, or nullptr if the comparison doesn't involve a counter column.
static unique_ptr<Expression> RewriteComparisonToFilterCmp(OptimizerExtensionInput &input,
                                                           BoundComparisonExpression &cmp, const LogicalType &list_type,
                                                           const LogicalType &finalized_type) {
	bool left_is_counter = IsCounterColumnRef(*cmp.left, list_type);
	bool right_is_counter = IsCounterColumnRef(*cmp.right, list_type);
	if (!left_is_counter && !right_is_counter) {
		return nullptr;
	}
	// Both sides are counters — can't rewrite
	if (left_is_counter && right_is_counter) {
		return nullptr;
	}
	bool counter_on_left = left_is_counter;
	string func_name = GetPacFilterFuncName(cmp.type, counter_on_left);
	if (func_name.empty()) {
		return nullptr;
	}
	auto counter_ref = ExtractCounterRef(std::move(counter_on_left ? cmp.left : cmp.right), list_type);
	auto scalar_expr =
	    CastScalarToPacFloat(std::move(counter_on_left ? cmp.right : cmp.left), finalized_type, list_type);
	PRIVACY_DEBUG_PRINT("[PAC DERIVED READ] Rewriting comparison to " + func_name);
	return input.optimizer.BindScalarFunction(func_name, std::move(scalar_expr), std::move(counter_ref));
}

// Rewrite a BETWEEN expression on a counter column to pac_filter_<cmp> AND conjunction.
// total BETWEEN low AND high → pac_filter_lte(low, counter) AND pac_filter_gte(high, counter)
static unique_ptr<Expression>
RewriteBetweenToFilterCmp(OptimizerExtensionInput &input, unique_ptr<Expression> counter_ref1,
                          unique_ptr<Expression> counter_ref2, unique_ptr<Expression> lower,
                          unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive,
                          const LogicalType &finalized_type, const LogicalType &list_type) {
	auto lower_scalar = CastScalarToPacFloat(std::move(lower), finalized_type, list_type);
	auto upper_scalar = CastScalarToPacFloat(std::move(upper), finalized_type, list_type);
	// counter >= low → low <= counter → pac_filter_lte(low, counter)
	// counter <= high → high >= counter → pac_filter_gte(high, counter)
	string lower_func = lower_inclusive ? "pac_filter_lte" : "pac_filter_lt";
	string upper_func = upper_inclusive ? "pac_filter_gte" : "pac_filter_gt";
	auto lower_call = input.optimizer.BindScalarFunction(lower_func, std::move(lower_scalar), std::move(counter_ref1));
	auto upper_call = input.optimizer.BindScalarFunction(upper_func, std::move(upper_scalar), std::move(counter_ref2));
	PRIVACY_DEBUG_PRINT("[PAC DERIVED READ] Rewriting BETWEEN to " + lower_func + " AND " + upper_func);
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower_call),
	                                             std::move(upper_call));
}

// Wrap counter column refs with pac_finalize, bottom-up through the plan.
//
// Uses op->types (resolved after DuckDB's optimizer) to identify counter columns
// because the optimizer may reorder columns, making position-based matching unreliable.
//
// Comparisons involving counter columns are rewritten to pac_filter_<cmp>(scalar, counters)
// which evaluates all 64 subsamples using majority voting. This works for scan-level
// filters too because pac_filter_<cmp> returns BOOLEAN.
//
// Non-comparison contexts (projections, ORDER BY, etc.) still use pac_finalize wrapping.
//
// Suppresses wrapping inside AGGREGATE subtrees — aggregates (e.g. first() in scalar
// subqueries, SUM() over counter columns) need raw FLOAT[] input. pac_finalize is
// applied above the aggregate via projection wrapping or the registered implicit cast.
static void WrapCounterRefsWithFinalize(OptimizerExtensionInput &input, LogicalOperator *op,
                                        const unordered_set<idx_t> &derived_table_indices,
                                        unordered_set<uint64_t> &finalized_bindings, bool suppress = false) {
	if (!op) {
		return;
	}
	// Suppress wrapping inside AGGREGATE subtrees (e.g. first() in scalar subqueries needs
	// raw FLOAT[] input).
	bool child_suppress = suppress || (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	for (auto &child : op->children) {
		WrapCounterRefsWithFinalize(input, child.get(), derived_table_indices, finalized_bindings, child_suppress);
	}

	auto list_type = CounterListType();
	auto finalized_type = PacFloatLogicalType();

	// Skip GET nodes and aggregate/suppressed subtrees — nothing to wrap
	if (op->type == LogicalOperatorType::LOGICAL_GET || suppress ||
	    op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}

	// Fix stale column refs that were finalized by a child operator
	std::function<void(unique_ptr<Expression> &)> FixExpr = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (finalized_bindings.count(HashBinding(col_ref.binding))) {
				col_ref.return_type = finalized_type;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, FixExpr);
		if (e->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = e->Cast<BoundBetweenExpression>();
			FixExpr(between.input);
			FixExpr(between.lower);
			FixExpr(between.upper);
		}
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { FixExpr(*expr_ptr); });

	// Rewrite comparisons on counter columns to pac_filter_<cmp> calls,
	// and wrap remaining counter column refs with pac_finalize.
	std::function<void(unique_ptr<Expression> &)> WrapExpr = [&](unique_ptr<Expression> &e) {
		// Skip already-wrapped expressions
		if (e->type == ExpressionType::BOUND_FUNCTION) {
			auto &func_name = e->Cast<BoundFunctionExpression>().function.name;
			if (func_name == "pac_finalize" || StringUtil::StartsWith(func_name, "pac_filter_")) {
				return;
			}
		}
		// Skip binder-inserted CAST(FLOAT[] → scalar) — the registered cast handles finalization
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.child->return_type == list_type && cast.return_type != list_type) {
				return;
			}
		}

		// Rewrite BoundComparisonExpression where one side is a counter column
		if (e->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &cmp = e->Cast<BoundComparisonExpression>();
			// Recurse into the scalar side first (it may contain nested expressions)
			bool left_is_counter = IsCounterColumnRef(*cmp.left, list_type);
			bool right_is_counter = IsCounterColumnRef(*cmp.right, list_type);
			if (left_is_counter && !right_is_counter) {
				WrapExpr(cmp.right);
			} else if (right_is_counter && !left_is_counter) {
				WrapExpr(cmp.left);
			}
			auto rewritten = RewriteComparisonToFilterCmp(input, cmp, list_type, finalized_type);
			if (rewritten) {
				e = std::move(rewritten);
				return;
			}
			// Both sides are counters or no counter — fall through to pac_finalize wrapping
		}

		// Rewrite BoundBetweenExpression where input is a counter column
		if (e->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = e->Cast<BoundBetweenExpression>();
			if (IsCounterColumnRef(*between.input, list_type)) {
				WrapExpr(between.lower);
				WrapExpr(between.upper);
				auto counter_ref1 = ExtractCounterRef(between.input->Copy(), list_type);
				auto counter_ref2 = ExtractCounterRef(std::move(between.input), list_type);
				e = RewriteBetweenToFilterCmp(input, std::move(counter_ref1), std::move(counter_ref2),
				                              std::move(between.lower), std::move(between.upper),
				                              between.lower_inclusive, between.upper_inclusive, finalized_type,
				                              list_type);
				return;
			}
			// Non-counter BETWEEN: recurse into fields manually (not visited by EnumerateChildren)
			WrapExpr(between.input);
			WrapExpr(between.lower);
			WrapExpr(between.upper);
		}

		ExpressionIterator::EnumerateChildren(*e, WrapExpr);

		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (col_ref.return_type == list_type) {
				PRIVACY_DEBUG_PRINT("[PAC DERIVED READ] Wrapping binding (" +
				                    std::to_string(col_ref.binding.table_index) + "," +
				                    std::to_string(col_ref.binding.column_index) + ") with pac_finalize");
				e = input.optimizer.BindScalarFunction("pac_finalize", std::move(e));
			}
		}
		// Retarget CAST(scalar→FLOAT[]) to CAST(scalar→FLOAT), strip no-op casts
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.return_type == list_type && cast.child->return_type != list_type) {
				e = BoundCastExpression::AddDefaultCastToType(std::move(cast.child), finalized_type);
			} else if (cast.child->return_type == cast.return_type) {
				e = std::move(cast.child);
			}
		}
		// Convert FLOAT[] constants back to scalar
		if (e->type == ExpressionType::VALUE_CONSTANT) {
			auto &constant = e->Cast<BoundConstantExpression>();
			if (constant.return_type == list_type && !constant.value.IsNull()) {
				auto &list_children = ListValue::GetChildren(constant.value);
				if (!list_children.empty()) {
					auto scalar_val = list_children[0].DefaultCastAs(finalized_type);
					e = make_uniq<BoundConstantExpression>(std::move(scalar_val));
				}
			}
		}
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { WrapExpr(*expr_ptr); });

	// Update operator output types after wrapping
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (i < proj.types.size()) {
				proj.types[i] = proj.expressions[i]->return_type;
			}
			// Only mark as finalized if the expression actually produces a counter type.
			// Expressions that CONTAIN pac_finalize in sub-expressions (e.g. CASE WHEN pac_finalize(x) > 100 ...)
			// may return a different type (e.g. VARCHAR) and should NOT be marked finalized.
			bool is_counter_output =
			    (proj.expressions[i]->return_type == finalized_type) ||
			    (proj.expressions[i]->return_type == list_type && ExpressionContainsPacFinalize(*proj.expressions[i]));
			if (is_counter_output) {
				finalized_bindings.insert(HashBinding(ColumnBinding(proj.table_index, i)));
				if (i < proj.types.size() && proj.types[i] == list_type) {
					proj.types[i] = finalized_type;
				}
				if (proj.expressions[i]->return_type == list_type) {
					proj.expressions[i]->return_type = finalized_type;
				}
			}
		}
	} else {
		// For pass-through operators (DISTINCT, ORDER BY, etc.): sync types from child
		for (idx_t i = 0; i < op->types.size(); i++) {
			if (op->types[i] == list_type) {
				for (auto &child : op->children) {
					if (i < child->types.size() && child->types[i] == finalized_type) {
						op->types[i] = finalized_type;
						auto out_bindings = op->GetColumnBindings();
						if (i < out_bindings.size()) {
							finalized_bindings.insert(HashBinding(out_bindings[i]));
						}
					}
				}
			}
		}
	}
}

// Rewrite scan-level table_filters on counter columns into pac_filter_<cmp> expressions.
// DuckDB's filter pushdown may push comparisons (including BETWEEN) into GET.table_filters
// before our post-optimizer runs. The scan executor doesn't support LIST types, so we pull
// these filters out and convert them to pac_filter_<cmp> calls in a FILTER operator.
static void RewriteCounterFiltersFromGets(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op,
                                          const unordered_set<idx_t> &derived_table_indices) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		RewriteCounterFiltersFromGets(input, child, derived_table_indices);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	if (!derived_table_indices.count(get.table_index)) {
		return;
	}
	auto list_type = CounterListType();
	auto finalized_type = PacFloatLogicalType();
	vector<idx_t> to_pull;
	for (auto &entry : get.table_filters.filters) {
		auto col_idx = entry.first;
		if (col_idx < get.returned_types.size() && get.returned_types[col_idx] == list_type) {
			to_pull.push_back(col_idx);
		}
	}
	if (to_pull.empty()) {
		return;
	}
	auto out_bindings = op->GetColumnBindings();
	auto &col_ids = get.GetColumnIds();
	vector<unique_ptr<Expression>> filter_exprs;
	for (auto filter_col_idx : to_pull) {
		auto &filter = get.table_filters.filters[filter_col_idx];
		auto col_type = get.returned_types[filter_col_idx];
		// Find the output binding for this column
		ColumnBinding binding;
		bool found = false;
		for (idx_t i = 0; i < col_ids.size() && i < out_bindings.size(); i++) {
			if (col_ids[i].GetPrimaryIndex() == filter_col_idx) {
				binding = out_bindings[i];
				found = true;
				break;
			}
		}
		if (!found) {
			get.table_filters.filters.erase(filter_col_idx);
			continue;
		}
		auto col_ref = make_uniq<BoundColumnRefExpression>(col_type, binding);
		auto expr = filter->ToExpression(*col_ref);
		// Rewrite the expression to pac_filter_<cmp> calls
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = expr->Cast<BoundBetweenExpression>();
			auto counter1 = make_uniq<BoundColumnRefExpression>(col_type, binding);
			auto counter2 = make_uniq<BoundColumnRefExpression>(col_type, binding);
			filter_exprs.push_back(RewriteBetweenToFilterCmp(
			    input, std::move(counter1), std::move(counter2), std::move(between.lower), std::move(between.upper),
			    between.lower_inclusive, between.upper_inclusive, finalized_type, list_type));
		} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &cmp = expr->Cast<BoundComparisonExpression>();
			auto rewritten = RewriteComparisonToFilterCmp(input, cmp, list_type, finalized_type);
			filter_exprs.push_back(rewritten ? std::move(rewritten) : std::move(expr));
		} else {
			filter_exprs.push_back(std::move(expr));
		}
		get.table_filters.filters.erase(filter_col_idx);
		PRIVACY_DEBUG_PRINT("[PAC DERIVED READ] Rewrote table_filter to pac_filter_<cmp> for counter column " +
		                    std::to_string(filter_col_idx));
	}
	auto filter_op = make_uniq<LogicalFilter>();
	for (auto &expr : filter_exprs) {
		filter_op->expressions.push_back(std::move(expr));
	}
	filter_op->children.push_back(std::move(op));
	filter_op->ResolveOperatorTypes();
	op = std::move(filter_op);
}

// --- Public API ---

bool HasDerivedPuCounterGets(LogicalOperator *plan) {
	unordered_set<idx_t> indices;
	FindDerivedPuGets(plan, indices);
	return !indices.empty();
}

void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	unordered_set<idx_t> derived_table_indices;
	FindDerivedPuGets(plan.get(), derived_table_indices);
	if (derived_table_indices.empty()) {
		return;
	}
	PRIVACY_DEBUG_PRINT("[PAC DERIVED READ] Injecting pac_finalize for " +
	                    std::to_string(derived_table_indices.size()) + " derived_pu table(s)");
	// Rewrite scan-level filters on counter columns to pac_filter_<cmp> calls.
	// DuckDB's filter pushdown may push comparisons (including BETWEEN) into table_filters
	// before our post-optimizer runs. The scan executor doesn't support LIST types.
	RewriteCounterFiltersFromGets(input, plan, derived_table_indices);
	unordered_set<uint64_t> finalized_bindings;
	WrapCounterRefsWithFinalize(input, plan.get(), derived_table_indices, finalized_bindings);
	plan->ResolveOperatorTypes();
}

} // namespace duckdb
