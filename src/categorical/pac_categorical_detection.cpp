//
// PAC Categorical Pattern Detection - Implementation
//
// See pac_categorical_detection.hpp for design documentation.
//
// Created by ila on 1/23/26.
//
#include "categorical/pac_categorical_detection.hpp"

namespace duckdb {

static string FindPacAggregateInExpression(Expression *expr, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding = nullptr); // forward declaration

// Trace a column binding through the plan to find if it comes from a PAC aggregate
// Returns the PAC aggregate name if found (base name without _counters), empty string otherwise
static string TracePacAggregateFromBinding(const ColumnBinding &binding, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding = nullptr) {
	auto *source_op = FindOperatorByTableIndex(plan_root, binding.table_index);
	if (!source_op) {
		return "";
	}
	if (source_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto *unwrapped = RecognizeDuckDBScalarWrapper(source_op);
		if (unwrapped) { // Scalar subquery wrapper: trace through inner projection's expression
			auto &agg_child = source_op->Cast<LogicalProjection>().children[0]->Cast<LogicalAggregate>();
			auto &inner_proj = agg_child.children[0]->Cast<LogicalProjection>();
			if (!inner_proj.expressions.empty()) {
				return FindPacAggregateInExpression(inner_proj.expressions[0].get(), plan_root, out_source_binding);
			}
			return "";
		}
		auto &proj = source_op->Cast<LogicalProjection>();
		if (binding.column_index < proj.expressions.size()) {
			if (IsAlreadyWrappedInPacNoised(proj.expressions[binding.column_index].get())) {
				return "";
			}
			return FindPacAggregateInExpression(proj.expressions[binding.column_index].get(), plan_root,
			                                    out_source_binding);
		}
	} else if (source_op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggr = source_op->Cast<LogicalAggregate>();
		if (binding.table_index == aggr.aggregate_index) {
			if (binding.column_index < aggr.expressions.size()) {
				auto &agg_expr = aggr.expressions[binding.column_index];
				if (agg_expr->type == ExpressionType::BOUND_AGGREGATE) {
					auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
					if (IsAnyPacAggregate(bound_agg.function.name)) {
						if (out_source_binding) {
							*out_source_binding = binding;
						}
						return GetBasePacAggregateName(bound_agg.function.name);
					}
				}
			}
		}
	}
	return "";
}

// Helper to collect ALL distinct PAC aggregate bindings in an expression
// Returns the bindings in the order they were discovered
static void CollectPacBindingsInExpression(Expression *expr, LogicalOperator *root, vector<PacBindingInfo> &bindings,
                                           unordered_map<uint64_t, idx_t> &binding_hash_to_index) {
	// Check if this is a column reference that traces back to a PAC aggregate
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		ColumnBinding source_binding;
		string pac_name = TracePacAggregateFromBinding(col_ref.binding, root, &source_binding);
		if (!pac_name.empty()) {
			uint64_t binding_hash = HashBinding(col_ref.binding);
			if (binding_hash_to_index.find(binding_hash) == binding_hash_to_index.end()) {
				PacBindingInfo info;
				info.binding = col_ref.binding;
				info.index = bindings.size();
				info.source_binding = source_binding;
				binding_hash_to_index[binding_hash] = info.index;
				bindings.push_back(info);
			}
		}
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		CollectPacBindingsInExpression(&child, root, bindings, binding_hash_to_index);
	});
}

// Find all PAC aggregate bindings in an expression
vector<PacBindingInfo> FindAllPacBindingsInExpression(Expression *expr, LogicalOperator *plan_root) {
	vector<PacBindingInfo> bindings;
	unordered_map<uint64_t, idx_t> binding_hash_to_index;
	CollectPacBindingsInExpression(expr, plan_root, bindings, binding_hash_to_index);
	return bindings;
}

// Recursively search for PAC aggregate in an expression tree, with plan context for tracing column refs
// Returns the base aggregate name (without _counters suffix)
static string FindPacAggregateInExpression(Expression *expr, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding) {
	// examine specific expression types where the PAC aggregate could be in
	if (expr->type == ExpressionType::BOUND_AGGREGATE) { // Base case: direct PAC aggregate
		auto &agg_expr = expr->Cast<BoundAggregateExpression>();
		if (IsAnyPacAggregate(agg_expr.function.name)) {
			return GetBasePacAggregateName(agg_expr.function.name);
		}
	} else if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		return TracePacAggregateFromBinding(col_ref.binding, plan_root, out_source_binding);
	} else if (expr->type == ExpressionType::SUBQUERY) {
		auto &subquery_expr = expr->Cast<BoundSubqueryExpression>();
		for (auto &child : subquery_expr.children) {
			string result = FindPacAggregateInExpression(child.get(), plan_root);
			if (!result.empty()) {
				return result; // Found PAC aggregate in the subquery's children (for IN, ANY, ALL operators)
			}
		}
		if (subquery_expr.subquery_type == SubqueryType::SCALAR) {
			string result = FindPacAggregateInOperator(plan_root);
			if (!result.empty()) {
				return result; // found PAC aggregate in scalar subquery
			}
		}
		return "";
	}
	// Generic traversal for all other expression types (comparisons, operators, casts,
	// functions, constants, CASE, BETWEEN, conjunctions, window functions, etc.)
	string result;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (result.empty()) {
			result = FindPacAggregateInExpression(&child, plan_root, out_source_binding);
		}
	});
	return result;
}

// Check if a binding in a FILTER traces to the filter's own child aggregate (HAVING clause).
// HAVING predicates should use scalar pac_noised comparison, not categorical pac_filter,
// because pac_filter's majority-vote decision can disagree with the pac_noised aggregate
// values that appear alongside it in the output.
static bool IsHavingBinding(LogicalOperator *filter_op, const ColumnBinding &binding) {
	if (!filter_op || filter_op->children.empty()) {
		return false;
	}
	// Walk through projections to find the child aggregate
	LogicalOperator *child = filter_op->children[0].get();
	while (child && child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (child->children.empty()) {
			break;
		}
		child = child->children[0].get();
	}
	if (child && child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = child->Cast<LogicalAggregate>();
		return binding.table_index == agg.aggregate_index || binding.table_index == agg.group_index;
	}
	return false;
}

// Recursively search the plan for categorical patterns (plan-aware version)
// Now detects ANY filter expression containing a PAC aggregate, not just comparisons
// outer_pac_hash: if an outer PAC aggregate was found above, this is the hash binding resolved to the current level
void DetectCategoricalPatterns(LogicalOperator *op, LogicalOperator *plan_root,
                               vector<CategoricalPatternInfo> &patterns, bool inside_aggregate,
                               const PacAggregateInfoMap &pac_agg_info, ColumnBinding outer_pac_hash) {
	// Track if we're entering an aggregate
	bool now_inside_aggregate = inside_aggregate || (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);

	// Track outer PAC hash for children: updated when we find a PAC aggregate or pass through a projection
	ColumnBinding hash_for_children = outer_pac_hash;

	// When entering a PAC aggregate, extract the hash binding.
	// Fast path: use pre-computed info from the bitslice compiler if available.
	// Fallback: extract from children[0] of the first PAC aggregate expression.
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		auto it = pac_agg_info.find(agg.aggregate_index);
		if (it != pac_agg_info.end()) {
			hash_for_children = it->second.hash_binding;
		} else {
			for (auto &agg_expr : agg.expressions) {
				if (agg_expr->type == ExpressionType::BOUND_AGGREGATE) {
					auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
					if (IsPacAggregate(bound_agg.function.name) && !bound_agg.children.empty() &&
					    bound_agg.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
						hash_for_children = bound_agg.children[0]->Cast<BoundColumnRefExpression>().binding;
						break;
					}
				}
			}
		}
	}

	// Resolve hash binding through projections (follow col_ref chains)
	if (hash_for_children.table_index != DConstants::INVALID_INDEX &&
	    op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		if (hash_for_children.table_index == proj.table_index &&
		    hash_for_children.column_index < proj.expressions.size()) {
			auto &expr = proj.expressions[hash_for_children.column_index];
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				hash_for_children = expr->Cast<BoundColumnRefExpression>().binding;
			}
		}
	}

	// Check filter expressions - detect ANY boolean expression containing a PAC aggregate
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		for (idx_t i = 0; i < filter.expressions.size(); i++) {
			auto &filter_expr = filter.expressions[i];
			// Find ALL PAC aggregate bindings in this expression (not just single)
			auto pac_bindings = FindAllPacBindingsInExpression(filter_expr.get(), plan_root);
			if (pac_bindings.empty()) {
				continue;
			}
			// Skip HAVING predicates: if ALL bindings trace to the filter's own child
			// aggregate, this is a HAVING clause. HAVING should use scalar pac_noised
			// comparison, not categorical pac_filter, because pac_filter's majority-vote
			// can disagree with pac_noised values shown in the same row.
			bool all_having = true;
			for (auto &bi : pac_bindings) {
				if (!IsHavingBinding(op, bi.binding)) {
					all_having = false;
					break;
				}
			}
			if (all_having) {
				continue;
			}
			{
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.pac_bindings = std::move(pac_bindings);
				info.outer_pac_hash = hash_for_children;
				info.has_outer_pac_hash = (hash_for_children.table_index != DConstants::INVALID_INDEX);
				patterns.push_back(info);
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	           op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		for (idx_t i = 0; i < join.conditions.size(); i++) {
			auto &cond = join.conditions[i];
			// Check both sides; any PAC aggregate in a join condition is categorical (not HAVING).
			for (auto *side : {cond.left.get(), cond.right.get()}) {
				string pac_name = FindPacAggregateInExpression(side, plan_root);
				if (pac_name.empty()) {
					continue;
				}
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.outer_pac_hash = hash_for_children;
				info.has_outer_pac_hash = (hash_for_children.table_index != DConstants::INVALID_INDEX);
				patterns.push_back(info);
				break; // one match per condition is enough
			}
		}
	} else if (!inside_aggregate && patterns.empty() && op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// Check projection expressions for arithmetic involving multiple PAC aggregates
		// This handles cases like Q08: sum(CASE...)/sum(volume) in SELECT list
		// NOTE: Only check projections if we haven't already found filter/join patterns,
		// because those patterns will handle the projections via RewriteProjectionsWithCounters.
		// We only want standalone projection patterns (no filter/join categorical patterns).
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			auto &expr = proj.expressions[i];
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				continue; // Skip simple column references - they don't need rewriting
			}
			// Find ALL PAC aggregate bindings in this expression
			auto pac_bindings = FindAllPacBindingsInExpression(expr.get(), plan_root);
			if (pac_bindings.empty()) {
				continue;
			}
			// Check if this expression has arithmetic with PAC aggregates
			// - Multiple PAC bindings (e.g., pac_sum(...) / pac_sum(...))
			// - Or single PAC binding with arithmetic (e.g., pac_sum(...) * 0.5)
			bool is_arithmetic_with_pac = false;
			if (pac_bindings.size() >= 2) { // Multiple PAC aggregates - definitely needs lambda rewrite
				is_arithmetic_with_pac = true;
			} else if (pac_bindings.size() == 1) {
				// Single aggregate - check if it's in an arithmetic expression (not just a column ref or simple cast)
				if (expr->type != ExpressionType::BOUND_COLUMN_REF && expr->type != ExpressionType::OPERATOR_CAST) {
					is_arithmetic_with_pac = true;
				} else if (expr->type == ExpressionType::OPERATOR_CAST) {
					// Check if cast contains arithmetic
					auto &cast_expr = expr->Cast<BoundCastExpression>();
					if (cast_expr.child->type != ExpressionType::BOUND_COLUMN_REF) {
						is_arithmetic_with_pac = true;
					}
				}
			}
			if (is_arithmetic_with_pac) {
				if (!IsNumericalType(expr->return_type)) {
					continue; // Only create pattern if result is numerical (pac_noised only works on numbers)
				}
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.pac_bindings = std::move(pac_bindings);
				patterns.push_back(std::move(info));
			}
		}
	}
	for (auto &child : op->children) {
		DetectCategoricalPatterns(child.get(), plan_root, patterns, now_inside_aggregate, pac_agg_info,
		                          hash_for_children);
	}
}

} // namespace duckdb
