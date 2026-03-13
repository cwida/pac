//
// PAC Categorical Query Rewriter - Implementation
//
// See pac_categorical_rewriter.hpp for design documentation.
//
// Created by ila on 1/23/26.
//
#include "categorical/pac_categorical_lambdas.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "utils/pac_helpers.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

// Find counter bindings in an expression using the known counter_bindings set (no plan tracing).
// Used during the rewrite phase where counter_bindings tracks all bindings rewritten to counters.
vector<PacBindingInfo> FindCounterBindings(Expression *expr,
                                           const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	vector<PacBindingInfo> result;
	unordered_map<uint64_t, idx_t> seen;
	std::function<void(Expression *)> Collect = [&](Expression *e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			uint64_t h = HashBinding(col_ref.binding);
			if (counter_bindings.count(h) && !seen.count(h)) {
				PacBindingInfo info;
				info.binding = col_ref.binding;
				info.index = result.size();
				seen[h] = info.index;
				result.push_back(info);
			}
		}
		ExpressionIterator::EnumerateChildren(*e, [&](Expression &child) { Collect(&child); });
	};
	Collect(expr);
	return result;
}

// Register a binding as a counter binding, optionally tracing to its origin.
static void AddCounterBinding(unordered_map<uint64_t, ColumnBinding> &counter_bindings, const ColumnBinding &binding,
                              const ColumnBinding &origin = ColumnBinding()) {
	counter_bindings[HashBinding(binding)] = (origin.table_index != DConstants::INVALID_INDEX) ? origin : binding;
}

// For a projection expression, trace back through counter_bindings to find the origin binding.
// If the expression is a simple col_ref to a known counter, returns its tracked origin; otherwise returns the binding
// itself.
static ColumnBinding TraceCounterOrigin(const unique_ptr<Expression> &expr,
                                        const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		auto it = counter_bindings.find(HashBinding(col_ref.binding));
		if (it != counter_bindings.end()) {
			return it->second;
		}
	}
	return ColumnBinding(); // invalid — AddCounterBinding will use the binding itself
}

// Check if an expression references any counter binding.
bool ReferencesCounterBinding(Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		return counter_bindings.count(HashBinding(expr->Cast<BoundColumnRefExpression>().binding)) > 0;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (!found) {
			found = ReferencesCounterBinding(&child, counter_bindings);
		}
	});
	return found;
}

// with pac_*_list variants that aggregate element-wise
// Try to rebind an aggregate expression at index i to its _list variant.
// Returns true if the aggregate was successfully rebound.
// The aggregate's value child is extracted (dropping the hash for PAC aggregates),
// its type set to LIST<FLOAT>, and the aggregate rebound to the _list variant.
static bool TryRebindToListVariant(LogicalAggregate &agg, idx_t i, ClientContext &context) {
	auto &agg_expr = agg.expressions[i];
	if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
		return false;
	}
	auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
	if (bound_agg.children.empty()) {
		return false;
	}
	// PAC/PAC_counters aggregates have (hash, value); standard aggregates have (value).
	// For _list variants we only need the value child.
	bool is_pac_style = (IsPacAggregate(bound_agg.function.name) || IsPacCountersAggregate(bound_agg.function.name)) &&
	                    bound_agg.children.size() > 1;
	idx_t value_child_idx = is_pac_style ? 1 : 0;
	string base_name = GetBasePacAggregateName(bound_agg.function.name); // strips _counters if present
	string list_name = GetListAggregateVariant(base_name);
	if (list_name.empty()) {
		// Generic aggregate (e.g., first()) over counter input — rebind with same name
		if (bound_agg.children[value_child_idx]->return_type == LogicalType::LIST(PacFloatLogicalType())) {
			list_name = bound_agg.function.name;
		} else {
			return false;
		}
	}
	vector<unique_ptr<Expression>> children;
	auto child_copy = bound_agg.children[value_child_idx]->Copy();
	children.push_back(std::move(child_copy));
	auto new_aggr = RebindAggregate(context, list_name, std::move(children), bound_agg.IsDistinct());
	if (!new_aggr) {
		return false;
	}
	agg.expressions[i] = std::move(new_aggr);
	idx_t types_index = agg.groups.size() + i;
	if (types_index < agg.types.size()) {
		agg.types[types_index] = LogicalType::LIST(PacFloatLogicalType());
	}
	return true;
}

// Replace aggregates whose input references a counter binding with _list variants.
// Called during RewriteBottomUp so projections above get properly list_transform'd.
// Adds output bindings of converted aggregates to counter_bindings.
static void ReplaceAggregatesOverCounters(LogicalOperator *op, ClientContext &context,
                                          unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = op->Cast<LogicalAggregate>();
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &agg_expr = agg.expressions[i];
		if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
		if (bound_agg.children.empty()) {
			continue;
		}
		idx_t value_child_idx = IsPacAggregate(bound_agg.function.name) && bound_agg.children.size() > 1 ? 1 : 0;
		if (!ReferencesCounterBinding(bound_agg.children[value_child_idx].get(), counter_bindings)) {
			continue;
		}
		if (TryRebindToListVariant(agg, i, context)) {
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
		}
	}
}

// Minimal processing for filter-pattern projection slots:
// update counter col_ref types but don't wrap with any terminal.
// The parent filter/join will expand these expressions and generate list_transform + pac_filter.
static void PrepareFilterPatternSlot(LogicalProjection &proj, idx_t i,
                                     const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	auto &expr = proj.expressions[i];
	auto pac_bindings = FindCounterBindings(expr.get(), counter_bindings);
	if (pac_bindings.empty()) {
		return;
	}
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	// col_ref types are already fixed by the universal type fix above.
	// Update projection output type
	if (i < proj.types.size()) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			proj.types[i] = list_type;
		} else if (expr->type == ExpressionType::BOUND_FUNCTION) {
			proj.types[i] = expr->return_type;
		}
	}
}

// Unified expression rewrite: build list_transform over PAC counters and wrap.
// pac_bindings: all PAC aggregate bindings in the expression
// expr: the expression to transform (boolean for filter, numeric for projection)
// wrap_kind: determines terminal function and type parameters
// target_type: for PAC_NOISED, cast result to this type (ignored for PAC_FILTER/PAC_SELECT)
// pac_hash: for PAC_SELECT, the hash binding to pass to pac_select/pac_select_<cmp>
static unique_ptr<Expression>
RewriteExpressionWithCounters(OptimizerExtensionInput &input, const vector<PacBindingInfo> &pac_bindings,
                              Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                              PacWrapKind wrap_kind, const LogicalType &target_type = PacFloatLogicalType(),
                              ColumnBinding pac_hash = ColumnBinding()) {
	if (pac_bindings.empty()) {
		return nullptr;
	}
	// Try optimized pac_{filter,select}_<cmp> for filter/select comparisons (single binding, simple comparison).
	// TryRewriteFilterComparison always returns the (possibly simplified) expression when it can
	// do any work. Check if the result is a pac_{filter,select}_<cmp> call (full optimization succeeded)
	// or a simplified comparison (partial — pass to lambda path which benefits from the rewriting).
	unique_ptr<Expression> simplified_expr;
	if (wrap_kind == PacWrapKind::PAC_FILTER || wrap_kind == PacWrapKind::PAC_SELECT) {
		auto cmp_result = TryRewriteFilterComparison(input, pac_bindings, expr, counter_bindings, wrap_kind, pac_hash);
		if (cmp_result) {
			if (cmp_result->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &func = cmp_result->Cast<BoundFunctionExpression>();
				if (func.function.name.rfind("pac_filter_", 0) == 0 ||
				    func.function.name.rfind("pac_select_", 0) == 0) {
					return cmp_result; // Full optimization to pac_{filter,select}_<cmp>
				}
			}
			simplified_expr = std::move(cmp_result); // Partial simplification — use for the lambda path
		}
	}
	Expression *expr_for_lambda = simplified_expr ? simplified_expr.get() : expr;

	LogicalType result_element_type =
	    (wrap_kind == PacWrapKind::PAC_NOISED) ? PacFloatLogicalType() : LogicalType::BOOLEAN;
	auto list_expr =
	    BuildCategoricalLambdas(input, pac_bindings, expr_for_lambda, counter_bindings, result_element_type);
	if (list_expr) {
		if (wrap_kind == PacWrapKind::PAC_NOISED) {
			auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(list_expr));
			if (target_type != PacFloatLogicalType()) {
				noised = BoundCastExpression::AddDefaultCastToType(std::move(noised), target_type);
			}
			return noised;
		} else if (wrap_kind == PacWrapKind::PAC_SELECT) {
			auto hash_ref = make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, pac_hash);
			return input.optimizer.BindScalarFunction("pac_select", std::move(hash_ref), std::move(list_expr));
		} else {
			return input.optimizer.BindScalarFunction("pac_filter", std::move(list_expr));
		}
	}
	return nullptr;
}

// Replace column bindings in-place throughout an expression tree
static void ReplaceBindingInExpression(Expression &expr, const ColumnBinding &old_binding,
                                       const ColumnBinding &new_binding) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.binding == old_binding) {
			col_ref.binding = new_binding;
		}
	} else {
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { ReplaceBindingInExpression(child, old_binding, new_binding); });
	}
}

// Expand projection references in a filter/join expression.
// For each col_ref to a projection slot with counter arithmetic:
// - create counter pass-through slots in the projection
// - substitute the col_ref with the arithmetic rebased to pass-through bindings
// This lets the filter fold arithmetic + comparison into one list_transform lambda.
static void ExpandProjectionRefsInExpression(unique_ptr<Expression> &expr, LogicalOperator *plan_root,
                                             unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (!counter_bindings.count(HashBinding(col_ref.binding))) {
			return; // Not a counter binding — nothing to expand
		}
		auto *source_op = FindOperatorByTableIndex(plan_root, col_ref.binding.table_index);
		if (!source_op || source_op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return;
		}
		auto &proj = source_op->Cast<LogicalProjection>();
		idx_t slot = col_ref.binding.column_index;
		if (slot >= proj.expressions.size()) {
			return;
		}
		// Only expand slots with multi-binding counter arithmetic that needs decomposition.
		// Skip: simple col_refs (already pass-throughs) and already-wrapped terminals.
		auto &proj_expr = proj.expressions[slot];
		if (proj_expr->type == ExpressionType::BOUND_COLUMN_REF) {
			return;
		}
		if (IsAlreadyWrappedInPacNoised(proj_expr.get())) {
			return;
		}
		auto pac_bindings = FindCounterBindings(proj_expr.get(), counter_bindings);
		if (pac_bindings.empty()) {
			return;
		}
		auto list_type = LogicalType::LIST(PacFloatLogicalType());
		// Clone the projection expression for rebasing
		auto expanded = proj_expr->Copy();
		// Create pass-through for first binding (reuse this slot)
		proj.expressions[slot] = make_uniq<BoundColumnRefExpression>("pac_var", list_type, pac_bindings[0].binding);
		if (slot < proj.types.size()) {
			proj.types[slot] = list_type;
		}
		ReplaceBindingInExpression(*expanded, pac_bindings[0].binding, ColumnBinding(proj.table_index, slot));
		// Extra pass-throughs for additional bindings
		for (idx_t j = 1; j < pac_bindings.size(); j++) {
			idx_t extra_slot = proj.expressions.size();
			proj.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>("pac_var", list_type, pac_bindings[j].binding));
			proj.types.push_back(list_type);
			auto extra_cb = ColumnBinding(proj.table_index, extra_slot);
			auto src_it = counter_bindings.find(HashBinding(pac_bindings[j].binding));
			AddCounterBinding(counter_bindings, extra_cb,
			                  (src_it != counter_bindings.end()) ? src_it->second : pac_bindings[j].binding);
			ReplaceBindingInExpression(*expanded, pac_bindings[j].binding, extra_cb);
		}
		expr = std::move(expanded);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ExpandProjectionRefsInExpression(child, plan_root, counter_bindings);
	});
}

// Wrap counter column references with pac_noised.
// After converting pac_noised_sum → pac_sum (LIST<FLOAT> output),
// expressions may still reference the aggregate with the original type (e.g. DECIMAL).
// This wraps those references with pac_noised() to convert back to scalar, then casts to original type.
static void WrapCounterRefsWithNoised(unique_ptr<Expression> &expr,
                                      const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                      OptimizerExtensionInput &input) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (counter_bindings.count(HashBinding(col_ref.binding))) {
			auto original_type = col_ref.return_type;
			unique_ptr<Expression> noised = input.optimizer.BindScalarFunction("pac_noised", expr->Copy());
			if (original_type != PacFloatLogicalType()) {
				noised = BoundCastExpression::AddDefaultCastToType(std::move(noised), original_type);
			}
			expr = std::move(noised);
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { WrapCounterRefsWithNoised(child, counter_bindings, input); });
}

// Rewrite a single projection expression: update col_ref types, build list_transform + pac_noised terminal.
// Only called for non-filter-pattern projections (filter patterns are handled by the parent filter/join).
// Returns true if the slot remains a counter binding (non-terminal pass-through).
static bool RewriteProjectionExpression(OptimizerExtensionInput &input, LogicalProjection &proj, idx_t i,
                                        LogicalOperator *plan_root, bool is_terminal,
                                        const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	auto &expr = proj.expressions[i];
	if (IsAlreadyWrappedInPacNoised(expr.get())) {
		return false;
	}
	auto pac_bindings = FindCounterBindings(expr.get(), counter_bindings);
	if (pac_bindings.empty()) {
		return false;
	}
	// Simple column reference to a counter list.
	// After the universal col_ref type fix, the col_ref type is LIST<FLOAT>.
	// proj.types[i] still holds the original scalar type (e.g. HUGEINT).
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto original_type = (i < proj.types.size()) ? proj.types[i] : expr->return_type;
		if (is_terminal) {
			unique_ptr<Expression> result = input.optimizer.BindScalarFunction("pac_noised", expr->Copy());
			if (original_type != PacFloatLogicalType()) {
				result = BoundCastExpression::AddDefaultCastToType(std::move(result), original_type);
			}
			proj.expressions[i] = std::move(result);
			proj.types[i] = original_type;
			return false; // terminal — scalar output
		} else if (i < proj.types.size()) {
			proj.types[i] = LogicalType::LIST(PacFloatLogicalType());
		}
		return true; // non-terminal pass-through — still a counter binding
	}
	if (!IsNumericalType(expr->return_type)) {
		WrapCounterRefsWithNoised(expr, counter_bindings, input);
		return false;
	}
	// Arithmetic over PAC aggregates: build list_transform + pac_noised terminal.
	Expression *expr_to_clone = expr.get();
	if (expr->type == ExpressionType::OPERATOR_CAST &&
	    expr->Cast<BoundCastExpression>().return_type != PacFloatLogicalType()) {
		expr_to_clone = expr->Cast<BoundCastExpression>().child.get();
	}
	auto result = RewriteExpressionWithCounters(input, pac_bindings, expr_to_clone, counter_bindings,
	                                            PacWrapKind::PAC_NOISED, expr->return_type);
	if (result) {
		proj.expressions[i] = std::move(result);
		proj.types[i] = expr->return_type;
	}
	return false;
}

// Insert a pass-through projection below filter_child_slot that replaces the hash column
// with pac_select_expr. Returns the new hash binding in the projection's output.
// Updates all bindings above via ColumnBindingReplacer.
static ColumnBinding InsertPacSelectProjection(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                               unique_ptr<LogicalOperator> &filter_child_slot,
                                               const ColumnBinding &hash_binding,
                                               unique_ptr<Expression> pac_select_expr) {
	filter_child_slot->ResolveOperatorTypes();
	auto old_bindings = filter_child_slot->GetColumnBindings();
	idx_t num_cols = old_bindings.size();

	idx_t proj_table_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;

	// Find which position the hash binding occupies
	idx_t hash_position = num_cols;
	for (idx_t i = 0; i < num_cols; i++) {
		if (old_bindings[i] == hash_binding) {
			hash_position = i;
			break;
		}
	}
	for (idx_t i = 0; i < num_cols; i++) {
		if (i == hash_position) {
			proj_expressions.push_back(nullptr); // placeholder — filled below
		} else {
			proj_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(filter_child_slot->types[i], old_bindings[i]));
		}
	}
	if (hash_position < num_cols) {
		proj_expressions[hash_position] = std::move(pac_select_expr);
	} else {
		// Hash binding was not in direct output — append it
		proj_expressions.push_back(std::move(pac_select_expr));
	}

	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(filter_child_slot));
	projection->ResolveOperatorTypes();

	LogicalOperator *proj_ptr = projection.get();
	filter_child_slot = std::move(projection);

	// Remap old bindings → new projection bindings
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < num_cols; i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], ColumnBinding(proj_table_index, i));
	}
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);

	return ColumnBinding(proj_table_index, hash_position);
}

// Single bottom-up rewrite pass.
// Processes children first, then current operator. Handles:
// - AGGREGATE: convert pac_noised_sum → pac_sum (counters), then aggregate-over-counters → pac_sum (list)
// - PROJECTION: update simple col_ref types, build list_transform + pac_noised for arithmetic
// - FILTER (in rewrite_map): build list_transform + pac_filter
// - JOIN (in rewrite_map): rewrite conditions (two-list → CROSS_PRODUCT+FILTER, single-list → double-lambda)
static void RewriteBottomUp(unique_ptr<LogicalOperator> &op_ptr, OptimizerExtensionInput &input,
                            unique_ptr<LogicalOperator> &plan,
                            const unordered_map<LogicalOperator *, unordered_set<idx_t>> &pattern_lookup,
                            vector<CategoricalPatternInfo> &patterns, unordered_set<uint64_t> &replaced_mark_bindings,
                            unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                            bool inside_cte_definition = false) {
	auto *op = op_ptr.get();
	for (idx_t ci = 0; ci < op->children.size(); ci++) { // Recurse into children first (bottom-up)
		// Child 0 of MATERIALIZED_CTE is the CTE definition — its output types must remain
		// stable (numeric, not LIST) because CTE_SCAN consumers may re-aggregate the results.
		bool child_in_cte =
		    inside_cte_definition || (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && ci == 0);
		RewriteBottomUp(op->children[ci], input, plan, pattern_lookup, patterns, replaced_mark_bindings,
		                counter_bindings, child_in_cte);
	}
	// After processing children, check if this FILTER references a mark column
	// from a MARK_JOIN that was replaced by CROSS_PRODUCT + FILTER(pac_filter_eq).
	// The pac_filter_eq below already handles the filtering, so this FILTER is
	// now redundant with a dangling mark column reference. Remove it.
	if (op->type == LogicalOperatorType::LOGICAL_FILTER && !replaced_mark_bindings.empty()) {
		auto &filter = op->Cast<LogicalFilter>();
		bool has_dangling_mark = false;
		for (auto &fexpr : filter.expressions) {
			if (fexpr->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = fexpr->Cast<BoundColumnRefExpression>();
				if (replaced_mark_bindings.count(HashBinding(ref.binding))) {
					has_dangling_mark = true;
					break;
				}
			}
		}
		if (has_dangling_mark && !filter.children.empty()) {
			op_ptr = std::move(filter.children[0]);
			return;
		}
	}
	LogicalOperator *plan_root = plan.get();
	// Universal col_ref type fix: after children have been processed (and populated counter_bindings),
	// fix stale col_ref types in the current operator's expressions. When PAC aggregates are converted
	// to counters, output types change (e.g. DECIMAL → LIST<FLOAT>). Expressions in all operators
	// may reference the old types. Fix them before operator-specific semantic work.
	// Skip universal type fix for ordering/limit operators — they need scalar types.
	// The terminal projection below them handles pac_noised wrapping.
	bool skip_type_fix =
	    (op->type == LogicalOperatorType::LOGICAL_ORDER_BY || op->type == LogicalOperatorType::LOGICAL_TOP_N ||
	     op->type == LogicalOperatorType::LOGICAL_LIMIT);
	if (!inside_cte_definition && !counter_bindings.empty() && !skip_type_fix) {
		auto list_type = LogicalType::LIST(PacFloatLogicalType());
		LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
			std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
				if (e->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &col_ref = e->Cast<BoundColumnRefExpression>();
					if (counter_bindings.count(HashBinding(col_ref.binding))) {
						col_ref.return_type = list_type;
					}
				}
				ExpressionIterator::EnumerateChildren(*e, Fix);
			};
			Fix(*expr_ptr);
		});
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && !inside_cte_definition) {
		// === AGGREGATE: convert PAC aggregates to _counters, then check aggregates-over-counters ===
		auto &agg = op->Cast<LogicalAggregate>();

		// First: replace standard/PAC aggregates whose input references counter bindings
		// with _list variants. Must happen before _counters conversion below so that
		// the projection rewrite (later in this pass) sees _list outputs.
		ReplaceAggregatesOverCounters(op, input.context, counter_bindings);

		// Convert PAC aggregates to _counters variants — only the specific
		// expressions that are sources of categorical patterns.
		for (idx_t i = 0; i < agg.expressions.size(); i++) {
			auto &agg_expr = agg.expressions[i];
			if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
				continue;
			}
			auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
			if (!IsPacAggregate(bound_agg.function.name)) {
				continue;
			}
			// Check if this specific aggregate expression is a source of any pattern
			ColumnBinding this_binding(agg.aggregate_index, i);
			bool convert = false;
			for (auto &p : patterns) {
				for (auto &bi : p.pac_bindings) {
					if (bi.source_binding == this_binding) {
						convert = true;
						break;
					}
				}
				if (convert) {
					break;
				}
			}
			if (!convert) {
				continue;
			}
			string counters_name = GetCountersVariant(bound_agg.function.name);
			vector<unique_ptr<Expression>> children;
			for (auto &child_expr : bound_agg.children) {
				children.push_back(child_expr->Copy());
			}
			auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
			if (!new_aggr) { // Fallback: just rename the function in place
				bound_agg.function.name = counters_name;
				bound_agg.function.return_type = LogicalType::LIST(PacFloatLogicalType());
				agg_expr->return_type = LogicalType::LIST(PacFloatLogicalType());
			} else {
				agg.expressions[i] = std::move(new_aggr);
			}
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
			idx_t types_index = agg.groups.size() + i;
			if (types_index < agg.types.size()) {
				agg.types[types_index] = LogicalType::LIST(PacFloatLogicalType());
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	           !inside_cte_definition) { // === PROJECTION: rewrite PAC expressions ===
		auto &proj = op->Cast<LogicalProjection>();
		// Check if this projection's output is consumed by a filter/join pattern.
		// If so, skip terminal wrapping — the parent filter/join handles the rewrite.
		bool is_filter_pattern = false;
		for (auto &p : patterns) {
			if (p.parent_op && pattern_lookup.count(p.parent_op)) {
				for (auto &bi : p.pac_bindings) {
					if (bi.binding.table_index == proj.table_index) {
						is_filter_pattern = true;
						break;
					}
				}
			}
			if (is_filter_pattern) {
				break;
			}
		}
		if (is_filter_pattern) {
			// Filter-pattern projection: minimal processing only (type updates, no terminal wrapping).
			// The parent filter/join will expand these expressions and generate list_transform + pac_filter.
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				PrepareFilterPatternSlot(proj, i, counter_bindings);
				// All filter-pattern slots with counter content remain counter bindings
				auto slot_bindings = FindCounterBindings(proj.expressions[i].get(), counter_bindings);
				if (!slot_bindings.empty()) {
					auto proj_cb = ColumnBinding(proj.table_index, i);
					AddCounterBinding(counter_bindings, proj_cb,
					                  TraceCounterOrigin(proj.expressions[i], counter_bindings));
				}
			}
		} else {
			// Normal projection: full PAC rewriting with pac_noised terminal.
			bool is_terminal = (op == plan_root);
			if (!is_terminal) {
				auto *root = plan_root;
				while (root &&
				       (root->type == LogicalOperatorType::LOGICAL_ORDER_BY ||
				        root->type == LogicalOperatorType::LOGICAL_TOP_N ||
				        root->type == LogicalOperatorType::LOGICAL_LIMIT) &&
				       !root->children.empty()) {
					root = root->children[0].get();
				}
				is_terminal = (root == op);
			}
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				if (RewriteProjectionExpression(input, proj, i, plan_root, is_terminal, counter_bindings)) {
					auto proj_cb = ColumnBinding(proj.table_index, i);
					AddCounterBinding(counter_bindings, proj_cb,
					                  TraceCounterOrigin(proj.expressions[i], counter_bindings));
				}
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_FILTER &&
	           !inside_cte_definition) { // === FILTER: rewrite expressions with pac_filter ===
		auto it = pattern_lookup.find(op);
		if (it != pattern_lookup.end()) {
			auto &filter = op->Cast<LogicalFilter>();
			for (auto expr_idx : it->second) {
				if (expr_idx >= filter.expressions.size()) {
					continue;
				}
				auto &filter_expr = filter.expressions[expr_idx];
				// Expand projection arithmetic into the filter expression so arithmetic + comparison
				// are folded into one list_transform lambda. Handles both single and multi-binding.
				ExpandProjectionRefsInExpression(filter_expr, plan_root, counter_bindings);
				auto pac_bindings = FindCounterBindings(filter_expr.get(), counter_bindings);
				if (pac_bindings.empty()) {
					continue;
				}
				// Determine wrap kind: PAC_SELECT if outer pac aggregate exists and setting enabled.
				// HAVING filters are excluded during detection, so any FILTER pattern here
				// is a genuine categorical filter (WHERE or subquery predicate).
				PacWrapKind wrap_kind = PacWrapKind::PAC_FILTER;
				ColumnBinding pattern_hash;
				for (auto &p : patterns) {
					if (p.parent_op == op && p.has_outer_pac_hash) {
						pattern_hash = p.outer_pac_hash;
						if (GetBooleanSetting(input.context, "pac_select", true)) {
							wrap_kind = PacWrapKind::PAC_SELECT;
						}
						break;
					}
				}
				auto result = RewriteExpressionWithCounters(input, pac_bindings, filter_expr.get(), counter_bindings,
				                                            wrap_kind, PacFloatLogicalType(), pattern_hash);
				if (result) {
					if (wrap_kind == PacWrapKind::PAC_SELECT) {
						// Insert projection below filter replacing hash with pac_select result
						auto new_hash =
						    InsertPacSelectProjection(input, plan, op->children[0], pattern_hash, std::move(result));
						// Replace filter condition with pac_hash <> 0 for majority-vote decision
						filter.expressions[expr_idx] = make_uniq<BoundComparisonExpression>(
						    ExpressionType::COMPARE_NOTEQUAL,
						    make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, new_hash),
						    make_uniq<BoundConstantExpression>(Value::UBIGINT(0)));
					} else {
						filter.expressions[expr_idx] = std::move(result);
					}
				}
			}
		}
	} else if ((op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	            op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
	           !inside_cte_definition) { // === JOIN: rewrite comparison conditions ===
		auto it = pattern_lookup.find(op);
		if (it == pattern_lookup.end()) {
			return;
		}
		// Determine wrap kind for join patterns (same logic as FILTER above)
		PacWrapKind join_wrap_kind = PacWrapKind::PAC_FILTER;
		ColumnBinding join_pattern_hash;
		for (auto &p : patterns) {
			if (p.parent_op == op && p.has_outer_pac_hash) {
				join_pattern_hash = p.outer_pac_hash;
				if (GetBooleanSetting(input.context, "pac_select", true)) {
					join_wrap_kind = PacWrapKind::PAC_SELECT;
				}
				break;
			}
		}
		auto &join = op->Cast<LogicalComparisonJoin>();
		for (auto expr_idx : it->second) {
			if (expr_idx >= join.conditions.size()) {
				continue;
			}
			auto &cond = join.conditions[expr_idx];
			// Expand projection arithmetic into join conditions
			ExpandProjectionRefsInExpression(cond.left, plan_root, counter_bindings);
			ExpandProjectionRefsInExpression(cond.right, plan_root, counter_bindings);
			auto left_bindings = FindCounterBindings(cond.left.get(), counter_bindings);
			auto right_bindings = FindCounterBindings(cond.right.get(), counter_bindings);
			bool left_is_list = !left_bindings.empty();
			bool right_is_list = !right_bindings.empty();
			if (!left_is_list && !right_is_list) {
				continue;
			}
			// Collect PAC bindings (deduplicated)
			vector<PacBindingInfo> all_bindings;
			unordered_set<uint64_t> seen_hashes;
			for (auto &b : left_bindings) {
				uint64_t h = HashBinding(b.binding);
				if (seen_hashes.insert(h).second) {
					all_bindings.push_back(b);
				}
			}
			for (auto &b : right_bindings) {
				uint64_t h = HashBinding(b.binding);
				if (seen_hashes.insert(h).second) {
					all_bindings.push_back(b);
				}
			}
			for (idx_t j = 0; j < all_bindings.size(); j++) {
				all_bindings[j].index = j;
			}
			auto comparison =
			    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
			auto pac_expr = RewriteExpressionWithCounters(input, all_bindings, comparison.get(), counter_bindings,
			                                              join_wrap_kind, PacFloatLogicalType(), join_pattern_hash);
			if (pac_expr) {
				auto cross_product =
				    LogicalCrossProduct::Create(std::move(join.children[0]), std::move(join.children[1]));
				auto filter_op = make_uniq<LogicalFilter>();

				if (join_wrap_kind == PacWrapKind::PAC_SELECT) {
					// Insert projection below filter replacing hash with pac_select result
					auto new_hash =
					    InsertPacSelectProjection(input, plan, cross_product, join_pattern_hash, std::move(pac_expr));
					filter_op->expressions.push_back(make_uniq<BoundComparisonExpression>(
					    ExpressionType::COMPARE_NOTEQUAL,
					    make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, new_hash),
					    make_uniq<BoundConstantExpression>(Value::UBIGINT(0))));
				} else {
					filter_op->expressions.push_back(std::move(pac_expr));
				}
				filter_op->children.push_back(std::move(cross_product));

				for (auto &p : patterns) {
					if (p.parent_op == op) {
						p.parent_op = nullptr;
					}
				}
				// Record the mark column binding so the parent FILTER
				// (which references it) can be cleaned up.
				if (join.join_type == JoinType::MARK) {
					replaced_mark_bindings.insert(HashBinding(ColumnBinding(join.mark_index, 0)));
				}
				op_ptr = std::move(filter_op);
				break;
			}
		}
	}
}

// Convert aggregates whose value child references a counter binding to _list variants.
// Catches cases ReplaceAggregatesOverCounters misses (DELIM_JOIN paths where tracing
// through the plan fails but counter_bindings has the correct information).
static void ConvertPacAggregatesToListVariants(LogicalOperator &plan, ClientContext &context,
                                               unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	for (auto &child : plan.children) {
		ConvertPacAggregatesToListVariants(*child, context, counter_bindings);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = plan.Cast<LogicalAggregate>();
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		if (agg.expressions[i]->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}
		if (counter_bindings.count(HashBinding(ColumnBinding(agg.aggregate_index, i)))) {
			continue;
		}
		auto &bound_agg = agg.expressions[i]->Cast<BoundAggregateExpression>();
		if (bound_agg.children.empty() || IsPacListAggregate(bound_agg.function.name)) {
			continue;
		}
		idx_t value_idx =
		    (IsPacAggregate(bound_agg.function.name) || IsPacCountersAggregate(bound_agg.function.name)) &&
		            bound_agg.children.size() > 1
		        ? 1
		        : 0;
		if (!ReferencesCounterBinding(bound_agg.children[value_idx].get(), counter_bindings)) {
			continue;
		}
		if (TryRebindToListVariant(agg, i, context)) {
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
		}
	}
}

void RewriteCategoricalQuery(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                             const PacAggregateInfoMap &pac_agg_info) {
	// Detect categorical patterns
	vector<CategoricalPatternInfo> patterns;
	DetectCategoricalPatterns(plan.get(), plan.get(), patterns, false, pac_agg_info);
	if (patterns.empty()) {
		return;
	}
	// Build lightweight lookup: operator → set of expression indices
	unordered_map<LogicalOperator *, unordered_set<idx_t>> pattern_lookup;
	for (auto &p : patterns) {
		if (p.parent_op) {
			pattern_lookup[p.parent_op].insert(p.expr_index);
		}
	}
	// Bottom-up rewrite pass
	// - Aggregates: pac_noised_sum → pac_sum (counters), then aggregate-over-counters → pac_sum (list)
	// - Projections: update col_ref types, build list_transform + pac_noised/pass-through
	// - Filters: build list_transform + pac_filter
	// - Joins: rewrite conditions (two-list → CROSS_PRODUCT+FILTER, single-list → double-lambda)
	unordered_set<uint64_t> replaced_mark_bindings;
	unordered_map<uint64_t, ColumnBinding> counter_bindings;
	RewriteBottomUp(plan, input, plan, pattern_lookup, patterns, replaced_mark_bindings, counter_bindings);
	// Fix up aggregates that now consume LIST (from inner _counters) to _list variants.
	ConvertPacAggregatesToListVariants(*plan, input.context, counter_bindings);
	plan->ResolveOperatorTypes();
}

} // namespace duckdb
