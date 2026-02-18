//
// PAC Top-K Pushdown Rewriter
//
// Post-optimization rule that rewrites TopN over PAC aggregates for better utility.
// See pac_topk_rewriter.hpp for design documentation.
//

#include "query_processing/pac_topk_rewriter.hpp"
#include "pac_debug.hpp"
#include "utils/pac_helpers.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "categorical/pac_categorical_rewriter.hpp"

#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

namespace duckdb {

// ============================================================================
// pac_mean(LIST<PAC_FLOAT>) -> PAC_FLOAT
// Computes the mean of non-NULL elements in a list of 64 counters.
// Used for ordering in TopN: gives the true aggregate value before noise.
// ============================================================================
static void PacMeanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &list_vec = args.data[0];
	idx_t count = args.size();

	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);

	auto result_data = FlatVector::GetData<PAC_FLOAT>(result);
	auto &result_validity = FlatVector::Validity(result);

	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_values = UnifiedVectorFormat::GetData<PAC_FLOAT>(child_data);

	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_idx = list_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto &entry = list_entries[list_idx];

		if (entry.length == 0) {
			result_validity.SetInvalid(i);
			continue;
		}

		// Compute mean of non-NULL elements
		PAC_FLOAT sum = 0;
		idx_t valid_count = 0;
		for (idx_t j = 0; j < entry.length; j++) {
			auto child_idx = child_data.sel->get_index(entry.offset + j);
			if (child_data.validity.RowIsValid(child_idx)) {
				sum += child_values[child_idx];
				valid_count++;
			}
		}

		if (valid_count == 0) {
			result_validity.SetInvalid(i);
		} else {
			result_data[i] = sum / static_cast<PAC_FLOAT>(valid_count);
		}
	}
}

void RegisterPacMeanFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	ScalarFunction pac_mean("pac_mean", {list_type}, PacFloatLogicalType(), PacMeanFunction);
	loader.RegisterFunction(pac_mean);
}

// ============================================================================
// Top-K Rewrite Logic
// ============================================================================

// Information about a PAC aggregate expression that was rewritten to _counters
struct PacTopKAggInfo {
	idx_t agg_index;           // Index in LogicalAggregate::expressions
	string original_name;      // e.g., "pac_sum"
	LogicalType original_type; // Return type before converting to LIST
	ColumnBinding agg_binding; // Binding in the aggregate's output (aggregate_index, agg_index)
};

// Find the LogicalAggregate below the TopN (possibly through projections).
// Returns nullptr if the structure is not suitable for rewriting.
struct TopKContext {
	LogicalTopN *topn = nullptr;
	LogicalAggregate *aggregate = nullptr;
	// Whether there are projections between TopN and Aggregate
	bool has_intermediate_projection = false;
	// Chain of projections from outermost (closest to TopN) to innermost (closest to Aggregate)
	vector<LogicalProjection *> intermediate_projections;
};

static TopKContext FindTopKContext(const unique_ptr<LogicalOperator> &plan) {
	TopKContext ctx;

	if (plan->type != LogicalOperatorType::LOGICAL_TOP_N) {
		return ctx;
	}
	ctx.topn = &plan->Cast<LogicalTopN>();

	if (plan->children.empty()) {
		return ctx;
	}

	auto *child = plan->children[0].get();

	// Case 1: TopN directly above Aggregate
	if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		ctx.aggregate = &child->Cast<LogicalAggregate>();
		return ctx;
	}

	// Case 2: TopN -> Projection(s) -> Aggregate
	// Walk through any number of intermediate projections
	auto *current = child;
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION && !current->children.empty()) {
		ctx.intermediate_projections.push_back(&current->Cast<LogicalProjection>());
		current = current->children[0].get();
	}

	if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		ctx.has_intermediate_projection = !ctx.intermediate_projections.empty();
		ctx.aggregate = &current->Cast<LogicalAggregate>();
		return ctx;
	}

	// Not a supported pattern - reset
	ctx.intermediate_projections.clear();
	return ctx;
}

// Check if a TopN order expression references a PAC aggregate binding,
// tracing through any number of intermediate projections.
static bool OrderExprReferencesPacAgg(Expression &expr, const vector<PacTopKAggInfo> &pac_aggs,
                                      const vector<LogicalProjection *> &projections, idx_t proj_depth) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		// If there are more projections to trace through, resolve the column ref
		if (proj_depth < projections.size() && col_ref.binding.table_index == projections[proj_depth]->table_index) {
			auto col_idx = col_ref.binding.column_index;
			if (col_idx < projections[proj_depth]->expressions.size()) {
				auto &proj_expr = projections[proj_depth]->expressions[col_idx];
				return OrderExprReferencesPacAgg(*proj_expr, pac_aggs, projections, proj_depth + 1);
			}
		}
		// Direct reference to aggregate (or past all projections)
		for (auto &info : pac_aggs) {
			if (col_ref.binding == info.agg_binding) {
				return true;
			}
		}
	}
	// Check through casts
	if (expr.type == ExpressionType::OPERATOR_CAST) {
		auto &cast = expr.Cast<BoundCastExpression>();
		return OrderExprReferencesPacAgg(*cast.child, pac_aggs, projections, proj_depth);
	}
	return false;
}

// Find all PAC aggregate expressions in the aggregate node
static vector<PacTopKAggInfo> FindPacAggregates(LogicalAggregate &agg) {
	vector<PacTopKAggInfo> result;
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		if (IsPacAggregate(bound_agg.function.name)) {
			PacTopKAggInfo info;
			info.agg_index = i;
			info.original_name = bound_agg.function.name;
			info.original_type = expr->return_type;
			info.agg_binding = ColumnBinding(agg.aggregate_index, i);
			result.push_back(std::move(info));
		}
	}
	return result;
}

// Trace a column binding through a chain of projections to find the underlying
// aggregate-level binding. Only follows simple BoundColumnRefExpression chains —
// returns false if a projection expression is a function call (e.g.
// __internal_decompress_string), since those transform the value and can't be
// treated as a simple passthrough. This is fine because PAC aggregate columns
// (numeric types) are always simple column refs, while function wrappers only
// appear on VARCHAR group columns.
static bool TraceBindingThroughProjections(const ColumnBinding &binding, const vector<LogicalProjection *> &projections,
                                           idx_t depth, ColumnBinding &out_binding) {
	if (depth >= projections.size()) {
		out_binding = binding;
		return true;
	}
	auto *proj = projections[depth];
	if (binding.table_index != proj->table_index) {
		out_binding = binding;
		return true;
	}
	auto col_idx = binding.column_index;
	if (col_idx >= proj->expressions.size()) {
		return false;
	}
	auto &proj_expr = proj->expressions[col_idx];
	if (proj_expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &inner_ref = proj_expr->Cast<BoundColumnRefExpression>();
	return TraceBindingThroughProjections(inner_ref.binding, projections, depth + 1, out_binding);
}

// Remap column bindings in an operator's expressions after a child was rewritten.
// When a TopN under a Join gets wrapped in NoisedProj, the Join's column references
// still point to the old TopN output bindings. This function updates them to match
// the new output bindings.
static void RemapBindingsInOperator(LogicalOperator &op, const vector<ColumnBinding> &old_bindings,
                                    const vector<ColumnBinding> &new_bindings) {
	if (old_bindings.size() != new_bindings.size()) {
		return;
	}
	// Build remap table (only for bindings that actually changed)
	unordered_map<uint64_t, ColumnBinding> remap;
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		if (old_bindings[i] != new_bindings[i]) {
			remap[HashBinding(old_bindings[i])] = new_bindings[i];
		}
	}
	if (remap.empty()) {
		return;
	}

	std::function<void(unique_ptr<Expression> &)> Rewrite = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			auto it = remap.find(HashBinding(col_ref.binding));
			if (it != remap.end()) {
				col_ref.binding = it->second;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, Rewrite);
	};

	// Rewrite the operator's main expression list (covers Projection, Filter, etc.)
	for (auto &expr : op.expressions) {
		Rewrite(expr);
	}

	// Handle join conditions (stored separately from expressions)
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			Rewrite(cond.left);
			Rewrite(cond.right);
		}
	}

	// Handle TopN order expressions (stored separately from expressions)
	if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op.Cast<LogicalTopN>();
		for (auto &order : topn.orders) {
			Rewrite(order.expression);
		}
	}

	op.ResolveOperatorTypes();
}

// The main rewrite function
void PACTopKRule::PACTopKOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	// Check the setting
	bool pushdown_enabled = GetBooleanSetting(input.context, "pac_pushdown_topk", true);
	if (!pushdown_enabled) {
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: pac_pushdown_topk is disabled, skipping");
#endif
		return;
	}

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Visiting node type=" + std::to_string(static_cast<int>(plan->type)));
#endif

	// Recurse into children first (bottom-up) so TopN nodes buried under
	// joins, projections, etc. are also rewritten. After rewriting a child
	// (wrapping its TopN in NoisedProj), the child's output bindings change
	// because NoisedProj has a new table_index. We snapshot bindings before
	// and after recursion; if they differ, we remap this node's expressions
	// to reference the new bindings. This propagates up naturally: each
	// parent sees its child's updated GetColumnBindings() result.
	for (idx_t ci = 0; ci < plan->children.size(); ci++) {
		auto &child = plan->children[ci];
		auto old_bindings = child->GetColumnBindings();
		PACTopKOptimizeFunction(input, child);
		auto new_bindings = child->GetColumnBindings();
		if (old_bindings.size() == new_bindings.size()) {
			RemapBindingsInOperator(*plan, old_bindings, new_bindings);
		}
	}

	// Find TopN context at this node
	TopKContext ctx = FindTopKContext(plan);
	if (!ctx.topn || !ctx.aggregate) {
#if PAC_DEBUG
		if (!ctx.topn) {
			PAC_DEBUG_PRINT("PACTopKRule: No TopN found at this node, skipping");
		} else {
			PAC_DEBUG_PRINT("PACTopKRule: TopN found but no aggregate child, skipping");
		}
#endif
		return;
	}

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Found TopN (limit=" + std::to_string(ctx.topn->limit) +
	                ") above aggregate (groups=" + std::to_string(ctx.aggregate->groups.size()) +
	                ", aggs=" + std::to_string(ctx.aggregate->expressions.size()) + ")");
	if (ctx.has_intermediate_projection) {
		PAC_DEBUG_PRINT("PACTopKRule: " + std::to_string(ctx.intermediate_projections.size()) +
		                " intermediate projection(s) present");
	}
#endif

	// Find PAC aggregates
	vector<PacTopKAggInfo> pac_aggs = FindPacAggregates(*ctx.aggregate);
	if (pac_aggs.empty()) {
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: No PAC aggregates found in aggregate node, skipping");
#endif
		return;
	}

#if PAC_DEBUG
	for (auto &info : pac_aggs) {
		PAC_DEBUG_PRINT("PACTopKRule: Found PAC aggregate: " + info.original_name + " at index " +
		                std::to_string(info.agg_index) + " binding=[" + std::to_string(info.agg_binding.table_index) +
		                "." + std::to_string(info.agg_binding.column_index) + "]");
	}
#endif

	// Check that at least one TopN order expression references a PAC aggregate
	bool has_pac_order = false;
	for (auto &order : ctx.topn->orders) {
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: Checking order expression: " + order.expression->ToString());
#endif
		if (OrderExprReferencesPacAgg(*order.expression, pac_aggs, ctx.intermediate_projections, 0)) {
			has_pac_order = true;
			break;
		}
	}
	if (!has_pac_order) {
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: No TopN order expression references a PAC aggregate, skipping");
#endif
		return;
	}

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Detected TopN over PAC aggregate, rewriting for top-k pushdown");
	PAC_DEBUG_PRINT("PACTopKRule: Found " + std::to_string(pac_aggs.size()) + " PAC aggregates to rewrite");
#endif

	// Save original TopN orders for the ORDER BY we'll add on top after rewriting.
	// After TopN selects the right top-k groups by true means, and NoisedProj applies
	// noise, we need to re-sort the results so the output is ordered by noised values.
	vector<BoundOrderByNode> saved_orders;
	for (auto &order : ctx.topn->orders) {
		saved_orders.push_back(order.Copy());
	}
	// Maps original column bindings to NoisedProj bindings (populated by PATH A or B)
	unordered_map<uint64_t, ColumnBinding> order_remap;

	auto &binder = input.optimizer.binder;
	auto &agg = *ctx.aggregate;
	auto list_type = LogicalType::LIST(PacFloatLogicalType());

	// ==========================================================================
	// Step 1: Convert pac_* aggregates to pac_*_counters
	// ==========================================================================
#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Step 1 - Converting PAC aggregates to _counters variants");
#endif
	for (auto &info : pac_aggs) {
		auto &expr = agg.expressions[info.agg_index];
		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		vector<unique_ptr<Expression>> children;
		for (auto &child_expr : bound_agg.children) {
			children.push_back(child_expr->Copy());
		}
		auto new_aggr = RebindAggregate(input.context, GetCountersVariant(bound_agg.function.name), std::move(children),
		                                bound_agg.IsDistinct());
		if (new_aggr) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("PACTopKRule:   Rebound " + info.original_name + " -> " +
			                GetCountersVariant(info.original_name));
#endif
			agg.expressions[info.agg_index] = std::move(new_aggr);
		} else {
			// Fallback: rename in place
#if PAC_DEBUG
			PAC_DEBUG_PRINT("PACTopKRule:   Fallback rename " + info.original_name + " -> " +
			                GetCountersVariant(bound_agg.function.name));
#endif
			bound_agg.function.name = GetCountersVariant(bound_agg.function.name);
			bound_agg.function.return_type = list_type;
			expr->return_type = list_type;
		}
		// Update types in the aggregate
		idx_t types_index = agg.groups.size() + info.agg_index;
		if (types_index < agg.types.size()) {
			agg.types[types_index] = list_type;
		}
	}

	// ==========================================================================
	// Step 2: Build the "mean projection" between Aggregate and TopN.
	//         This projection passes through all existing columns and adds
	//         pac_mean(counters) columns for ordering.
	// ==========================================================================
#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Step 2 - Building mean projection");
#endif
	idx_t mean_proj_idx = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> mean_proj_exprs;

	// Get all column bindings from the aggregate
	auto agg_bindings = agg.GetColumnBindings();

	// Pass through all existing aggregate output columns
	for (idx_t i = 0; i < agg_bindings.size(); i++) {
		auto &binding = agg_bindings[i];
		auto col_ref = make_uniq<BoundColumnRefExpression>(agg.types[i], binding);
		mean_proj_exprs.push_back(std::move(col_ref));
	}

	// Map from pac aggregate binding -> index of the pac_mean column in the mean projection
	unordered_map<uint64_t, idx_t> pac_mean_column_map;

	// Add pac_mean(counters) for each PAC aggregate
	for (auto &info : pac_aggs) {
		idx_t mean_col_idx = mean_proj_exprs.size();
		auto counters_ref = make_uniq<BoundColumnRefExpression>(list_type, info.agg_binding);
		auto mean_expr = input.optimizer.BindScalarFunction("pac_mean", std::move(counters_ref));
		mean_proj_exprs.push_back(std::move(mean_expr));
		pac_mean_column_map[HashBinding(info.agg_binding)] = mean_col_idx;
	}

	auto mean_proj = make_uniq<LogicalProjection>(mean_proj_idx, std::move(mean_proj_exprs));

	if (ctx.has_intermediate_projection) {
		// ==================================================================
		// PATH A: Intermediate projections exist (e.g. string decompress).
		// Keep them in place. Insert MeanProj below them, add pac_mean
		// passthrough columns, rewrite only PAC ORDER BY refs.
		//
		// Before: TopN -> Proj_outer -> ... -> Proj_inner -> Aggregate
		// After:  NoisedProj -> TopN -> Proj_outer -> ... -> Proj_inner -> MeanProj -> Aggregate
		// ==================================================================

		auto *outermost = ctx.intermediate_projections[0];
		auto *innermost = ctx.intermediate_projections.back();

		// ------------------------------------------------------------------
		// Step A1: Pre-analysis on UNMODIFIED projections.
		// We trace ORDER BY expressions and outermost projection columns
		// through the projection chain to identify which ones reference PAC
		// aggregates. This must happen before Step A2 modifies bindings.
		// TraceBindingThroughProjections will succeed for PAC agg columns
		// (simple ColRef chains) but fail for decompress wrappers (which
		// are group columns, not PAC aggregates) — and that's correct.
		// ------------------------------------------------------------------

		// Map: ORDER BY index -> pac_aggs vector index (for expressions that reference PAC aggs)
		unordered_map<idx_t, idx_t> order_pac_refs;
		for (idx_t oi = 0; oi < ctx.topn->orders.size(); oi++) {
			auto *stripped = StripCasts(ctx.topn->orders[oi].expression.get());
			auto *col_ref = stripped->type == ExpressionType::BOUND_COLUMN_REF
			                    ? &stripped->Cast<BoundColumnRefExpression>()
			                    : nullptr;
			if (!col_ref) {
				continue;
			}
			ColumnBinding resolved;
			if (TraceBindingThroughProjections(col_ref->binding, ctx.intermediate_projections, 0, resolved)) {
				for (idx_t j = 0; j < pac_aggs.size(); j++) {
					if (resolved == pac_aggs[j].agg_binding) {
						order_pac_refs[oi] = j;
						break;
					}
				}
			}
		}

		// Map: outermost projection column index -> pac_aggs index (for PAC aggregate columns)
		unordered_map<idx_t, idx_t> outermost_pac_col_map;
		for (idx_t i = 0; i < outermost->expressions.size(); i++) {
			if (outermost->expressions[i]->type != ExpressionType::BOUND_COLUMN_REF) {
				continue; // function calls (e.g. decompress) are non-PAC group columns
			}
			auto &ref = outermost->expressions[i]->Cast<BoundColumnRefExpression>();
			ColumnBinding resolved;
			if (TraceBindingThroughProjections(ref.binding, ctx.intermediate_projections, 1, resolved)) {
				for (idx_t j = 0; j < pac_aggs.size(); j++) {
					if (resolved == pac_aggs[j].agg_binding) {
						outermost_pac_col_map[i] = j;
						break;
					}
				}
			}
		}

		idx_t original_outermost_col_count = outermost->expressions.size();

		// ------------------------------------------------------------------
		// Step A2: Insert MeanProj between innermost projection and aggregate.
		// The innermost projection previously referenced the aggregate directly;
		// now it references MeanProj (which passes through all aggregate columns
		// at the same positions, plus pac_mean columns at the end).
		// ------------------------------------------------------------------
		mean_proj->children.push_back(std::move(innermost->children[0]));
		mean_proj->ResolveOperatorTypes();
		innermost->children[0] = std::move(mean_proj);

		// Rewrite all ColRefs in innermost projection: aggregate bindings -> MeanProj bindings.
		// Also update return_type since PAC columns changed from e.g. BIGINT to LIST<FLOAT>.
		std::function<void(unique_ptr<Expression> &)> UpdateRefs = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = e->Cast<BoundColumnRefExpression>();
				for (idx_t i = 0; i < agg_bindings.size(); i++) {
					if (col_ref.binding == agg_bindings[i]) {
						col_ref.binding = ColumnBinding(mean_proj_idx, i);
						col_ref.return_type = agg.types[i];
						break;
					}
				}
			}
			ExpressionIterator::EnumerateChildren(*e, UpdateRefs);
		};
		for (auto &proj_expr : innermost->expressions) {
			UpdateRefs(proj_expr);
		}

		// ------------------------------------------------------------------
		// Step A3: Add pac_mean passthrough columns to each intermediate
		// projection (bottom-up: innermost first). Each projection gets a
		// new column that references the pac_mean column from the projection
		// below it, forming a chain: MeanProj -> innermost -> ... -> outermost.
		// ------------------------------------------------------------------
		vector<vector<idx_t>> pac_mean_col_indices(ctx.intermediate_projections.size());

		for (int pi = static_cast<int>(ctx.intermediate_projections.size()) - 1; pi >= 0; pi--) {
			auto *proj = ctx.intermediate_projections[pi];
			for (idx_t j = 0; j < pac_aggs.size(); j++) {
				idx_t new_col_idx = proj->expressions.size();
				pac_mean_col_indices[pi].push_back(new_col_idx);

				if (pi == static_cast<int>(ctx.intermediate_projections.size()) - 1) {
					// Innermost: reference MeanProj's pac_mean column directly
					idx_t mean_col = pac_mean_column_map[HashBinding(pac_aggs[j].agg_binding)];
					proj->expressions.push_back(make_uniq<BoundColumnRefExpression>(
					    PacFloatLogicalType(), ColumnBinding(mean_proj_idx, mean_col)));
				} else {
					// Reference the projection below's pac_mean passthrough column
					auto *below = ctx.intermediate_projections[pi + 1];
					idx_t below_col = pac_mean_col_indices[pi + 1][j];
					proj->expressions.push_back(make_uniq<BoundColumnRefExpression>(
					    PacFloatLogicalType(), ColumnBinding(below->table_index, below_col)));
				}
			}
		}

		// Resolve types bottom-up. After Step 1 changed PAC aggregates to _counters
		// (BIGINT -> LIST<FLOAT>), ColRef return_types in outer projections are stale.
		// Fix them by matching against the child operator's resolved column types.
		for (int pi = static_cast<int>(ctx.intermediate_projections.size()) - 1; pi >= 0; pi--) {
			auto *proj = ctx.intermediate_projections[pi];
			auto child_bindings = proj->children[0]->GetColumnBindings();
			auto &child_types = proj->children[0]->types;
			// Update ColRef return_types based on the child operator's resolved types
			std::function<void(unique_ptr<Expression> &)> FixTypes = [&](unique_ptr<Expression> &e) {
				if (e->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &col_ref = e->Cast<BoundColumnRefExpression>();
					for (idx_t ci = 0; ci < child_bindings.size(); ci++) {
						if (col_ref.binding == child_bindings[ci] && ci < child_types.size()) {
							col_ref.return_type = child_types[ci];
							break;
						}
					}
				}
				ExpressionIterator::EnumerateChildren(*e, FixTypes);
			};
			for (auto &proj_expr : proj->expressions) {
				FixTypes(proj_expr);
			}
			proj->ResolveOperatorTypes();
		}

		// ------------------------------------------------------------------
		// Step A4: Rewrite TopN ORDER BY — only PAC aggregate references
		// are rewritten to point to the pac_mean passthrough column in the
		// outermost projection. Non-PAC refs (e.g. o_orderpriority through
		// __internal_decompress_string) are left unchanged since the
		// intermediate projections are preserved in the plan.
		// ------------------------------------------------------------------
		for (auto &kv : order_pac_refs) {
			auto *stripped = StripCasts(ctx.topn->orders[kv.first].expression.get());
			auto *col_ref = stripped->type == ExpressionType::BOUND_COLUMN_REF
			                    ? &stripped->Cast<BoundColumnRefExpression>()
			                    : nullptr;
			if (col_ref) {
				col_ref->binding = ColumnBinding(outermost->table_index, pac_mean_col_indices[0][kv.second]);
				col_ref->return_type = PacFloatLogicalType();
			}
		}

		// Resolve types for TopN
		plan->ResolveOperatorTypes();

		// ------------------------------------------------------------------
		// Step A5: Build NoisedProj above TopN. Only covers the ORIGINAL
		// columns of the outermost projection (not the pac_mean passthrough
		// columns, which were just for sorting). PAC aggregate columns get
		// pac_noised() applied then cast back to the original return type.
		// Non-PAC columns are passed through unchanged.
		// ------------------------------------------------------------------
		idx_t noised_proj_idx = binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> noised_proj_exprs;

		for (idx_t i = 0; i < original_outermost_col_count; i++) {
			auto it_pac = outermost_pac_col_map.find(i);
			if (it_pac != outermost_pac_col_map.end()) {
				// PAC aggregate column: apply pac_noised to counter LIST, cast back to original type
				auto counters_ref =
				    make_uniq<BoundColumnRefExpression>(list_type, ColumnBinding(outermost->table_index, i));
				auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(counters_ref));
				auto &orig_type = pac_aggs[it_pac->second].original_type;
				if (orig_type != noised->return_type) {
					noised = BoundCastExpression::AddCastToType(input.context, std::move(noised), orig_type);
				}
				noised_proj_exprs.push_back(std::move(noised));
			} else {
				// Non-PAC column: passthrough (preserves decompress result, etc.)
				auto ref =
				    make_uniq<BoundColumnRefExpression>(outermost->types[i], ColumnBinding(outermost->table_index, i));
				noised_proj_exprs.push_back(std::move(ref));
			}
		}

		auto noised_proj = make_uniq<LogicalProjection>(noised_proj_idx, std::move(noised_proj_exprs));

		// Build order remap: original outermost projection columns -> NoisedProj columns
		for (idx_t i = 0; i < original_outermost_col_count; i++) {
			order_remap[HashBinding(ColumnBinding(outermost->table_index, i))] = ColumnBinding(noised_proj_idx, i);
		}

		// ------------------------------------------------------------------
		// Step A6: Reassemble the plan tree.
		// Before: plan(TopN) -> Proj_outer -> ... -> Proj_inner -> MeanProj -> Agg
		// After:  OrderBy -> NoisedProj -> TopN -> Proj_outer -> ... -> Proj_inner -> MeanProj -> Agg
		// ------------------------------------------------------------------
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: Step A6 - Reassembling plan tree (with intermediate projections)");
		PAC_DEBUG_PRINT("PACTopKRule:   mean_proj table_index=" + std::to_string(mean_proj_idx));
		PAC_DEBUG_PRINT("PACTopKRule:   noised_proj table_index=" + std::to_string(noised_proj_idx));
#endif

		noised_proj->children.push_back(std::move(plan));
		noised_proj->ResolveOperatorTypes();
		plan = std::move(noised_proj);

	} else {
		// ==================================================================
		// PATH B: No intermediate projections — TopN directly above Aggregate.
		// Original approach: MeanProj between Aggregate and TopN,
		// NoisedProj above TopN, all refs point to MeanProj.
		//
		// Before: TopN -> Aggregate
		// After:  NoisedProj -> TopN -> MeanProj -> Aggregate
		// ==================================================================

		// Step B1: Rewrite TopN ORDER BY to reference MeanProj
		std::function<void(unique_ptr<Expression> &)> RewriteOrderExpr = [&](unique_ptr<Expression> &expr) {
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expr->Cast<BoundColumnRefExpression>();
				auto it = pac_mean_column_map.find(HashBinding(col_ref.binding));
				if (it != pac_mean_column_map.end()) {
					col_ref.binding = ColumnBinding(mean_proj_idx, it->second);
					col_ref.return_type = PacFloatLogicalType();
				} else {
					for (idx_t i = 0; i < agg_bindings.size(); i++) {
						if (agg_bindings[i] == col_ref.binding) {
							col_ref.binding = ColumnBinding(mean_proj_idx, i);
							break;
						}
					}
				}
				return;
			}
			ExpressionIterator::EnumerateChildren(*expr,
			                                      [&](unique_ptr<Expression> &child) { RewriteOrderExpr(child); });
		};

		for (auto &order : ctx.topn->orders) {
			RewriteOrderExpr(order.expression);
		}

		// Step B2: Build NoisedProj
		idx_t noised_proj_idx = binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> noised_proj_exprs;

		for (idx_t i = 0; i < agg_bindings.size(); i++) {
			auto &binding = agg_bindings[i];
			auto it = pac_mean_column_map.find(HashBinding(binding));
			if (it != pac_mean_column_map.end()) {
				auto counters_ref = make_uniq<BoundColumnRefExpression>(list_type, ColumnBinding(mean_proj_idx, i));
				auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(counters_ref));
				// Cast back to the original aggregate return type (e.g. BIGINT for count)
				for (auto &info : pac_aggs) {
					if (info.agg_binding == binding && info.original_type != noised->return_type) {
						noised =
						    BoundCastExpression::AddCastToType(input.context, std::move(noised), info.original_type);
						break;
					}
				}
				noised_proj_exprs.push_back(std::move(noised));
			} else {
				auto ref = make_uniq<BoundColumnRefExpression>(agg.types[i], ColumnBinding(mean_proj_idx, i));
				noised_proj_exprs.push_back(std::move(ref));
			}
		}

		auto noised_proj = make_uniq<LogicalProjection>(noised_proj_idx, std::move(noised_proj_exprs));

		// Build order remap: original aggregate bindings -> NoisedProj columns
		for (idx_t i = 0; i < agg_bindings.size(); i++) {
			order_remap[HashBinding(agg_bindings[i])] = ColumnBinding(noised_proj_idx, i);
		}

		// Step B3: Reassemble
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PACTopKRule: Step B3 - Reassembling plan tree (no intermediate projections)");
		PAC_DEBUG_PRINT("PACTopKRule:   mean_proj table_index=" + std::to_string(mean_proj_idx));
		PAC_DEBUG_PRINT("PACTopKRule:   noised_proj table_index=" + std::to_string(noised_proj_idx));
#endif

		mean_proj->children.push_back(std::move(plan->children[0])); // aggregate
		mean_proj->ResolveOperatorTypes();
		plan->children[0] = std::move(mean_proj);
		plan->ResolveOperatorTypes();

		noised_proj->children.push_back(std::move(plan));
		noised_proj->ResolveOperatorTypes();
		plan = std::move(noised_proj);
	}

	// ==========================================================================
	// Final Step: Add ORDER BY on top to re-sort by noised values.
	// TopN selected the right top-k groups using true means, but after
	// NoisedProj applies noise the output values no longer match the sort order.
	// Re-sorting the small top-k result set by noised values is cheap.
	// ==========================================================================
	vector<BoundOrderByNode> final_orders;
	for (auto &saved : saved_orders) {
		auto expr = saved.expression->Copy();
		std::function<void(unique_ptr<Expression> &)> RemapToNoised = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = e->Cast<BoundColumnRefExpression>();
				auto it = order_remap.find(HashBinding(ref.binding));
				if (it != order_remap.end()) {
					ref.binding = it->second;
				}
			}
			ExpressionIterator::EnumerateChildren(*e, RemapToNoised);
		};
		RemapToNoised(expr);
		final_orders.push_back(BoundOrderByNode(saved.type, saved.null_order, std::move(expr)));
	}
	auto order_by = make_uniq<LogicalOrder>(std::move(final_orders));
	order_by->children.push_back(std::move(plan));
	order_by->ResolveOperatorTypes();
	plan = std::move(order_by);

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Successfully rewrote plan for top-k pushdown");
	PAC_DEBUG_PRINT(plan->ToString());
#endif
}

} // namespace duckdb
