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
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"

#include <cmath>
#include <algorithm>

namespace duckdb {

// ============================================================================
// pac_mean(LIST<PAC_FLOAT>) -> PAC_FLOAT
// Computes the mean of non-NULL elements in a list of 64 counters.
// Kept for backward compatibility / debugging. Not used in plan rewriting.
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
// pac_unnoised(LIST<PAC_FLOAT>) -> PAC_FLOAT
// Extracts counter[J] where J = query_hash % 64, using the same world
// selection as PacNoisySampleFrom64Counters. Kept for debugging / manual use.
// ============================================================================

struct PacUnnoisedBindData : public FunctionData {
	idx_t world_idx; // J = query_hash % 64
	explicit PacUnnoisedBindData(idx_t world_idx) : world_idx(world_idx) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<PacUnnoisedBindData>(world_idx);
	}
	bool Equals(const FunctionData &other) const override {
		return world_idx == other.Cast<PacUnnoisedBindData>().world_idx;
	}
};

static unique_ptr<FunctionData> PacUnnoisedBind(ClientContext &ctx, ScalarFunction &,
                                                vector<unique_ptr<Expression>> &) {
	// Compute world index J using the same formula as PacBindData
	uint64_t seed = 42;
	Value pac_seed_val;
	if (ctx.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull()) {
		seed = static_cast<uint64_t>(pac_seed_val.GetValue<int64_t>());
	}
	double mi = GetPacMiFromSetting(ctx);
	if (mi != 0.0) {
		seed ^= PAC_MAGIC_HASH * static_cast<uint64_t>(ctx.ActiveTransaction().GetActiveQuery());
	}
	uint64_t query_hash = (seed * PAC_MAGIC_HASH) ^ PAC_MAGIC_HASH;
	return make_uniq<PacUnnoisedBindData>(query_hash % 64);
}

static void PacUnnoisedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_info = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<PacUnnoisedBindData>();
	idx_t J = bind_info.world_idx;

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

		PAC_FLOAT val = 0;
		if (J < entry.length) {
			auto child_idx = child_data.sel->get_index(entry.offset + J);
			if (child_data.validity.RowIsValid(child_idx)) {
				val = child_values[child_idx];
			}
		}
		result_data[i] = val;
	}
}

void RegisterPacUnnoisedFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	ScalarFunction pac_unnoised("pac_unnoised", {list_type}, PacFloatLogicalType(), PacUnnoisedFunction,
	                            PacUnnoisedBind);
	loader.RegisterFunction(pac_unnoised);
}

// ============================================================================
// pac_topk_superset — Custom window aggregate for single-world top-K ranking
//
// Selects one world J (= query_hash % 64, consistent with PacBindData /
// PacNoisySampleFrom64Counters) and marks the top-K groups by counter[J].
// Only those K groups are kept (via a downstream filter), noised, and
// re-ranked by the final TopN.
//
// This is implemented as a custom DuckDB window aggregate:
//   window_init: for each of the 64 worlds, independently find the top-K groups
//                by that world's counter value, then union all 64 per-world top-K
//                sets to form the superset (marks membership in a boolean vector)
//   window:      returns the precomputed boolean for each row
// ============================================================================

// Bind data: stores K (the per-world TopN limit) and the ranking direction
struct PacTopKSupersetBindData : public FunctionData {
	idx_t k;
	bool ascending; // true if ORDER BY ASC (per-world ranking picks smallest K), false for DESC (largest K)
	PacTopKSupersetBindData(idx_t k, bool ascending) : k(k), ascending(ascending) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<PacTopKSupersetBindData>(k, ascending);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<PacTopKSupersetBindData>();
		return k == o.k && ascending == o.ascending;
	}
};

// Aggregate state: used for both global (window_init) and local (window) purposes
struct PacTopKSupersetState {
	// Global: precomputed superset membership (one per row in partition)
	vector<bool> in_superset;
	// Local: sequential counter to map per-batch rid to global row index
	idx_t total_processed = 0;
};

static idx_t PacTopKSupersetStateSize(const AggregateFunction &) {
	return sizeof(PacTopKSupersetState);
}

static void PacTopKSupersetInit(const AggregateFunction &, data_ptr_t state) {
	new (state) PacTopKSupersetState();
}

static void PacTopKSupersetDestroy(Vector &state_vec, AggregateInputData &, idx_t count) {
	auto states = FlatVector::GetData<data_ptr_t>(state_vec);
	for (idx_t i = 0; i < count; i++) {
		reinterpret_cast<PacTopKSupersetState *>(states[i])->~PacTopKSupersetState();
	}
}

// window_init: for each of the 64 worlds, independently find the top-K groups
// by that world's counter value, then take the union of all 64 per-world top-K
// sets as the superset. This ensures the superset is determined by ALL worlds
// (not just the secret world), preserving PAC privacy for the output key set.
static void PacTopKSupersetWindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                      data_ptr_t g_state) {
	auto &state = *reinterpret_cast<PacTopKSupersetState *>(g_state);
	auto &bind_data = aggr_input_data.bind_data->Cast<PacTopKSupersetBindData>();
	idx_t K = bind_data.k;
	bool ascending = bind_data.ascending;
	auto count = partition.count;

	state.in_superset.resize(count, false);
	if (count == 0 || K == 0 || !partition.inputs) {
		return;
	}

	// If K >= count, all groups are in the superset (no filtering needed)
	if (K >= count) {
		std::fill(state.in_superset.begin(), state.in_superset.end(), true);
		return;
	}

	// Step 1: Collect all 64 counter values for every row.
	// Flat layout: all_counters[row * 64 + world] = counter value
	vector<PAC_FLOAT> all_counters(count * 64, PAC_FLOAT(0));

	auto *inputs = partition.inputs;
	auto counters_col_idx = partition.column_ids[0]; // first argument = counters list

	DataChunk chunk;
	inputs->InitializeScanChunk(chunk);
	ColumnDataScanState scan_state;
	inputs->InitializeScan(scan_state);

	idx_t row_offset = 0;
	while (inputs->Scan(scan_state, chunk)) {
		auto &list_vec = chunk.data[counters_col_idx];
		idx_t chunk_size = chunk.size();

		UnifiedVectorFormat list_data;
		list_vec.ToUnifiedFormat(chunk_size, list_data);
		auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

		auto &child_vec = ListVector::GetEntry(list_vec);
		UnifiedVectorFormat child_data;
		child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
		auto child_values = UnifiedVectorFormat::GetData<PAC_FLOAT>(child_data);

		for (idx_t i = 0; i < chunk_size; i++) {
			auto list_idx = list_data.sel->get_index(i);

			if (list_data.validity.RowIsValid(list_idx)) {
				auto &entry = list_entries[list_idx];
				idx_t num_counters = std::min(entry.length, idx_t(64));
				for (idx_t j = 0; j < num_counters; j++) {
					auto child_idx = child_data.sel->get_index(entry.offset + j);
					if (child_data.validity.RowIsValid(child_idx)) {
						all_counters[row_offset * 64 + j] = child_values[child_idx];
					}
				}
			}
			row_offset++;
		}
	}

	// Step 2: For each world w (0..63), find the top-K groups by counter[w]
	// and add them to the union superset.
	vector<pair<idx_t, PAC_FLOAT>> world_values(count);

	for (idx_t w = 0; w < 64; w++) {
		// Extract (row_idx, counter[w]) for all rows
		for (idx_t r = 0; r < count; r++) {
			world_values[r] = {r, all_counters[r * 64 + w]};
		}

		// Partial sort: nth_element places the K-th element at position K-1,
		// with all elements before it being the top-K (in unspecified order).
		if (ascending) {
			std::nth_element(world_values.begin(), world_values.begin() + static_cast<ptrdiff_t>(K),
			                 world_values.end(),
			                 [](const pair<idx_t, PAC_FLOAT> &a, const pair<idx_t, PAC_FLOAT> &b) { return a.second < b.second; });
		} else {
			std::nth_element(world_values.begin(), world_values.begin() + static_cast<ptrdiff_t>(K),
			                 world_values.end(),
			                 [](const pair<idx_t, PAC_FLOAT> &a, const pair<idx_t, PAC_FLOAT> &b) { return a.second > b.second; });
		}

		// Mark the top-K rows for this world in the union
		for (idx_t i = 0; i < K; i++) {
			state.in_superset[world_values[i].first] = true;
		}
	}

#if PAC_DEBUG
	idx_t superset_size = 0;
	for (idx_t i = 0; i < count; i++) {
		if (state.in_superset[i]) superset_size++;
	}
	PAC_DEBUG_PRINT("pac_topk_superset: K=" + std::to_string(K) +
	                " ascending=" + std::to_string(ascending) +
	                " total_groups=" + std::to_string(count) +
	                " superset_size=" + std::to_string(superset_size));
#endif
}

// window: return the precomputed superset membership for each row
static void PacTopKSupersetWindow(AggregateInputData &, const WindowPartitionInput &, const_data_ptr_t g_state,
                                  data_ptr_t l_state, const SubFrames &, Vector &result, idx_t rid) {
	auto &gstate = *reinterpret_cast<const PacTopKSupersetState *>(g_state);
	auto &lstate = *reinterpret_cast<PacTopKSupersetState *>(l_state);

	idx_t global_row = lstate.total_processed;
	lstate.total_processed++;

	auto result_data = FlatVector::GetData<bool>(result);
	result_data[rid] = (global_row < gstate.in_superset.size()) ? gstate.in_superset[global_row] : false;
}

void RegisterPacTopKSupersetFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	// Window-only aggregate: only window_init and window callbacks are used
	AggregateFunction pac_topk_superset("pac_topk_superset", {list_type}, LogicalType::BOOLEAN,
	                                    PacTopKSupersetStateSize, PacTopKSupersetInit,
	                                    nullptr, // update (unused — window-only)
	                                    nullptr, // combine (unused — window-only)
	                                    nullptr, // finalize (unused — window-only)
	                                    FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                                    nullptr, // simple_update
	                                    nullptr, // bind
	                                    PacTopKSupersetDestroy,
	                                    nullptr, // statistics
	                                    PacTopKSupersetWindow);
	pac_topk_superset.window_init = PacTopKSupersetWindowInit;
	loader.RegisterFunction(AggregateFunctionSet(pac_topk_superset));
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
	//
	// IMPORTANT: For materialized CTEs, skip the CTE definition child (children[0]).
	// TopK rewrites aggregate types (e.g., pac_sum → pac_sum_counters), which changes
	// the CTE's output type. Any CTE_SCAN referencing this CTE would then receive
	// FLOAT[] instead of the expected scalar type, causing execution crashes.
	// Only recurse into the consumer child (children[1]).
	bool is_cte = plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE;
	for (idx_t ci = 0; ci < plan->children.size(); ci++) {
		if (is_cte && ci == 0) {
			continue; // skip CTE definition
		}
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

	auto &binder = input.optimizer.binder;
	auto &agg = *ctx.aggregate;
	auto list_type = LogicalType::LIST(PacFloatLogicalType());

	// ==========================================================================
	// Step 0: Capture the hash child binding from a PAC aggregate.
	//         Must happen before Step 1 converts aggregates to _counters.
	// ==========================================================================
	ColumnBinding hash_child_binding;
	bool found_hash = false;
	for (auto &info : pac_aggs) {
		auto &expr = agg.expressions[info.agg_index];
		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		if (!bound_agg.children.empty() && bound_agg.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
			hash_child_binding = bound_agg.children[0]->Cast<BoundColumnRefExpression>().binding;
			found_hash = true;
			break;
		}
	}

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
	// Step 1b: Add pac_keyhash aggregate (if not already present from
	//          the categorical rewriter). Must happen BEFORE computing
	//          agg_bindings so keyhash is included in passthrough.
	// ==========================================================================
	ColumnBinding keyhash_agg_binding;
	bool has_keyhash = false;

	// Check if pac_keyhash already exists
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		if (agg.expressions[i]->type == ExpressionType::BOUND_AGGREGATE) {
			auto &ba = agg.expressions[i]->Cast<BoundAggregateExpression>();
			if (ba.function.name == "pac_keyhash") {
				keyhash_agg_binding = ColumnBinding(agg.aggregate_index, agg.groups.size() + i);
				has_keyhash = true;
				break;
			}
		}
	}

	// If not found, add it
	if (!has_keyhash && found_hash) {
		vector<unique_ptr<Expression>> keyhash_children;
		keyhash_children.push_back(
		    make_uniq<BoundColumnRefExpression>("pac_hash", LogicalType::UBIGINT, hash_child_binding));
		auto keyhash_aggr = RebindAggregate(input.context, "pac_keyhash", std::move(keyhash_children), false);
		if (keyhash_aggr) {
			idx_t keyhash_idx = agg.expressions.size();
			agg.expressions.push_back(std::move(keyhash_aggr));
			agg.types.push_back(LogicalType::UBIGINT);
			keyhash_agg_binding = ColumnBinding(agg.aggregate_index, agg.groups.size() + keyhash_idx);
			has_keyhash = true;
		}
	}

	// Get aggregate output bindings (groups + aggregates including keyhash)
	auto agg_bindings = agg.GetColumnBindings();

	// Track keyhash column index in agg_bindings
	idx_t keyhash_agg_bindings_idx = DConstants::INVALID_INDEX;
	if (has_keyhash) {
		for (idx_t i = 0; i < agg_bindings.size(); i++) {
			if (agg_bindings[i] == keyhash_agg_binding) {
				keyhash_agg_bindings_idx = i;
				break;
			}
		}
	}

	// ==========================================================================
	// Step 2: Union-of-all-worlds top-K ranking via window function.
	//
	// For each world w (0..63), independently rank all groups by counter[w]
	// and select the top-K. The union of all 64 per-world top-K sets forms
	// the superset. This is privacy-safe because the superset is determined
	// by ALL worlds (public), not any single secret world. Only superset
	// members are noised and considered for the final top-K selection.
	//
	// Plan structure (both PATH A and PATH B):
	//   NoisedProj(pac_noised(counters, keyhash) for output values only)
	//     → FinalTopN(K, ORDER BY pac_mean DESC)
	//       → RankProj(pac_mean(counters) for ranking, passthrough counters+keyhash)
	//         → Filter(superset_flag = TRUE)
	//           → Window(pac_topk_superset(counters) OVER () as superset_flag)
	//             → [IntermediateProjs?] → Aggregate(_counters + keyhash)
	// ==========================================================================
#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Step 2 - Building per-world ranking pipeline");
#endif

	// Determine which PAC aggregate the ORDER BY references (for ranking)
	// and capture the sort direction for per-world ranking.
	idx_t ranking_agg_idx = 0;
	bool ranking_ascending = false; // default to descending (most top-k queries use DESC)
	for (idx_t oi = 0; oi < ctx.topn->orders.size(); oi++) {
		if (OrderExprReferencesPacAgg(*ctx.topn->orders[oi].expression, pac_aggs, ctx.intermediate_projections, 0)) {
			ranking_ascending = (ctx.topn->orders[oi].type == OrderType::ASCENDING);
			// Find which PAC agg this order references
			for (idx_t j = 0; j < pac_aggs.size(); j++) {
				auto *stripped = StripCasts(ctx.topn->orders[oi].expression.get());
				if (stripped->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &ref = stripped->Cast<BoundColumnRefExpression>();
					ColumnBinding resolved;
					if (ctx.has_intermediate_projection) {
						TraceBindingThroughProjections(ref.binding, ctx.intermediate_projections, 0, resolved);
					} else {
						resolved = ref.binding;
					}
					if (resolved == pac_aggs[j].agg_binding) {
						ranking_agg_idx = j;
						break;
					}
				}
			}
			break;
		}
	}

	// ---- Fix intermediate projection types if PATH A ----
	// After Step 1 changed PAC aggregates to _counters (BIGINT -> LIST<FLOAT>),
	// ColRef return_types in intermediate projections are stale. Fix them.
	if (ctx.has_intermediate_projection) {
		auto *innermost = ctx.intermediate_projections.back();

		// Fix ColRef return_types in innermost projection (references aggregate directly)
		std::function<void(unique_ptr<Expression> &)> FixAggTypes = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = e->Cast<BoundColumnRefExpression>();
				for (idx_t i = 0; i < agg_bindings.size(); i++) {
					if (col_ref.binding == agg_bindings[i] && i < agg.types.size()) {
						col_ref.return_type = agg.types[i];
						break;
					}
				}
			}
			ExpressionIterator::EnumerateChildren(*e, FixAggTypes);
		};
		for (auto &proj_expr : innermost->expressions) {
			FixAggTypes(proj_expr);
		}

		// Resolve types bottom-up through projection chain
		for (int pi = static_cast<int>(ctx.intermediate_projections.size()) - 1; pi >= 0; pi--) {
			auto *proj = ctx.intermediate_projections[pi];
			if (pi < static_cast<int>(ctx.intermediate_projections.size()) - 1) {
				auto child_bindings = proj->children[0]->GetColumnBindings();
				auto &child_types = proj->children[0]->types;
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
			}
			proj->ResolveOperatorTypes();
		}

		// Wrap LIST-typed column refs inside compound expressions with pac_noised()
		// and cast to original type. We must save original col ref types before FixTypes
		// overwrites them (e.g., DOUBLE for pac_avg), so we can cast pac_noised output
		// (FLOAT) back to the type the parent expression was bound for.
		// Build a map: binding hash → original return type, from all col refs in all
		// intermediate projections (captured before FixTypes/FixAggTypes above).
		// Since FixTypes already ran, we reconstruct original types from pac_aggs.
		unordered_map<uint64_t, LogicalType> pac_orig_type_map;
		for (auto &info : pac_aggs) {
			pac_orig_type_map[HashBinding(info.agg_binding)] = info.original_type;
		}

		for (idx_t pi = 0; pi < ctx.intermediate_projections.size(); pi++) {
			auto *proj = ctx.intermediate_projections[pi];
			auto MakeKeyhashRef = [&]() -> unique_ptr<Expression> {
				if (!has_keyhash) {
					return make_uniq<BoundConstantExpression>(Value::UBIGINT(~uint64_t(0)));
				}
				// Find keyhash in the child operator's bindings
				auto child_bindings = proj->children[0]->GetColumnBindings();
				auto &child_types = proj->children[0]->types;
				for (idx_t ci = 0; ci < child_bindings.size(); ci++) {
					if (ci < child_types.size() && child_types[ci] == LogicalType::UBIGINT) {
						// Check if this is the keyhash column by tracing
						ColumnBinding resolved = child_bindings[ci];
						if (resolved == keyhash_agg_binding) {
							return make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, child_bindings[ci]);
						}
					}
				}
				return make_uniq<BoundConstantExpression>(Value::UBIGINT(~uint64_t(0)));
			};
			// Resolve the original type for a col ref by tracing its binding through
			// projections back to the aggregate and looking up in pac_orig_type_map.
			auto ResolveOrigType = [&](const ColumnBinding &binding) -> LogicalType {
				// Find which projection this binding belongs to, then trace to aggregate
				for (idx_t start = 0; start < ctx.intermediate_projections.size(); start++) {
					if (binding.table_index == ctx.intermediate_projections[start]->table_index) {
						ColumnBinding resolved;
						if (TraceBindingThroughProjections(binding, ctx.intermediate_projections, start, resolved)) {
							auto it = pac_orig_type_map.find(HashBinding(resolved));
							if (it != pac_orig_type_map.end()) {
								return it->second;
							}
						}
						break;
					}
				}
				// Direct match (col ref directly references aggregate output)
				auto it = pac_orig_type_map.find(HashBinding(binding));
				if (it != pac_orig_type_map.end()) {
					return it->second;
				}
				return PacFloatLogicalType();
			};
			for (idx_t i = 0; i < proj->expressions.size(); i++) {
				auto &expr = proj->expressions[i];
				if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
					continue;
				}
				std::function<void(unique_ptr<Expression> &)> WrapListRefs = [&](unique_ptr<Expression> &e) {
					if (e->type == ExpressionType::BOUND_COLUMN_REF && e->return_type.id() == LogicalTypeId::LIST) {
						auto &ref = e->Cast<BoundColumnRefExpression>();
						auto orig_type = ResolveOrigType(ref.binding);
						e = input.optimizer.BindScalarFunction("pac_noised", std::move(e), MakeKeyhashRef());
						// Cast pac_noised (returns FLOAT) to original type if different,
						// so parent expressions (e.g., GreaterThan<double,double>) get the expected type.
						if (orig_type != PacFloatLogicalType()) {
							e = BoundCastExpression::AddCastToType(input.context, std::move(e), orig_type);
						}
						return;
					}
					if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
						auto &cast_expr = e->Cast<BoundCastExpression>();
						if (cast_expr.child->type == ExpressionType::BOUND_COLUMN_REF &&
						    cast_expr.child->return_type.id() == LogicalTypeId::LIST) {
							auto target_type = cast_expr.return_type;
							auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(cast_expr.child),
							                                                 MakeKeyhashRef());
							e = BoundCastExpression::AddCastToType(input.context, std::move(noised), target_type);
							return;
						}
					}
					ExpressionIterator::EnumerateChildren(*e, WrapListRefs);
				};
				WrapListRefs(expr);
			}
			proj->ResolveOperatorTypes();
		}
	}

	// ---- Determine the operator that the Window will sit above ----
	// For PATH A: Window sits above outermost projection
	// For PATH B: Window sits above aggregate directly
	// In both cases, we need to find the counters binding in the child's output.

	LogicalOperator *window_child_op;      // the operator below our Window
	ColumnBinding window_counters_binding; // counters column binding in window child's output
	ColumnBinding window_keyhash_binding;  // keyhash column binding in window child's output
	bool window_has_keyhash = false;

	if (ctx.has_intermediate_projection) {
		auto *outermost = ctx.intermediate_projections[0];
		window_child_op = outermost;

		// Trace the ranking PAC aggregate's binding through projections to find
		// its binding in the outermost projection's output
		auto ranking_agg_binding = pac_aggs[ranking_agg_idx].agg_binding;
		for (idx_t i = 0; i < outermost->expressions.size(); i++) {
			if (outermost->expressions[i]->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &ref = outermost->expressions[i]->Cast<BoundColumnRefExpression>();
			ColumnBinding resolved;
			if (TraceBindingThroughProjections(ref.binding, ctx.intermediate_projections, 1, resolved)) {
				if (resolved == ranking_agg_binding) {
					window_counters_binding = ColumnBinding(outermost->table_index, i);
					break;
				}
			}
		}

		// Similarly find keyhash binding in outermost projection
		if (has_keyhash) {
			for (idx_t i = 0; i < outermost->expressions.size(); i++) {
				if (outermost->expressions[i]->type != ExpressionType::BOUND_COLUMN_REF) {
					continue;
				}
				auto &ref = outermost->expressions[i]->Cast<BoundColumnRefExpression>();
				ColumnBinding resolved;
				if (TraceBindingThroughProjections(ref.binding, ctx.intermediate_projections, 1, resolved)) {
					if (resolved == keyhash_agg_binding) {
						window_keyhash_binding = ColumnBinding(outermost->table_index, i);
						window_has_keyhash = true;
						break;
					}
				}
			}
		}
	} else {
		window_child_op = ctx.aggregate;
		window_counters_binding = pac_aggs[ranking_agg_idx].agg_binding;
		if (has_keyhash) {
			window_keyhash_binding = keyhash_agg_binding;
			window_has_keyhash = true;
		}
	}

	// ---- Step 2a: Create LogicalWindow with pac_topk_superset OVER () ----
	idx_t window_table_idx = binder.GenerateTableIndex();

	// Build the BoundWindowExpression for pac_topk_superset
	auto aggr_func = make_uniq<AggregateFunction>("pac_topk_superset", vector<LogicalType> {list_type},
	                                              LogicalType::BOOLEAN, PacTopKSupersetStateSize, PacTopKSupersetInit,
	                                              nullptr, // update (unused)
	                                              nullptr, // combine (unused)
	                                              nullptr, // finalize (unused)
	                                              FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                                              nullptr, // simple_update
	                                              nullptr, // bind
	                                              PacTopKSupersetDestroy,
	                                              nullptr, // statistics
	                                              PacTopKSupersetWindow);
	aggr_func->window_init = PacTopKSupersetWindowInit;

	// Read expansion factor c (default 1.0). The window function selects
	// ceil(c * K) candidates per world for the union; the final TopN limits to K.
	double expansion = 1.0;
	Value exp_val;
	if (input.context.TryGetCurrentSetting("pac_topk_expansion", exp_val) && !exp_val.IsNull()) {
		expansion = exp_val.GetValue<double>();
		if (expansion < 1.0) {
			expansion = 1.0;
		}
	}
	idx_t expanded_k = static_cast<idx_t>(std::ceil(expansion * static_cast<double>(ctx.topn->limit)));

	auto bind_data = make_uniq<PacTopKSupersetBindData>(expanded_k, ranking_ascending);
	auto window_expr = make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_AGGREGATE, LogicalType::BOOLEAN,
	                                                    std::move(aggr_func), std::move(bind_data));
	// Counters list is the sole argument
	window_expr->children.push_back(make_uniq<BoundColumnRefExpression>(list_type, window_counters_binding));
	// OVER () = entire partition, no partitioning, no ordering
	window_expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	window_expr->end = WindowBoundary::UNBOUNDED_FOLLOWING;

	auto window_op = make_uniq<LogicalWindow>(window_table_idx);
	window_op->expressions.push_back(std::move(window_expr));

	// Wire up: Window's child = the operator below TopN (outermost proj or aggregate)
	if (ctx.has_intermediate_projection) {
		// Detach outermost proj from TopN and attach to Window
		window_op->children.push_back(std::move(plan->children[0]));
	} else {
		// Detach aggregate from TopN and attach to Window
		window_op->children.push_back(std::move(plan->children[0]));
	}
	window_op->ResolveOperatorTypes();

	// The window output = child columns + [window_table_idx, 0] (superset_flag)
	auto window_bindings = window_op->GetColumnBindings();
	auto superset_flag_binding = window_bindings.back(); // last column is the window result

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Window table_index=" + std::to_string(window_table_idx) + " superset_flag binding=[" +
	                std::to_string(superset_flag_binding.table_index) + "." +
	                std::to_string(superset_flag_binding.column_index) + "]");
#endif

	// ---- Step 2b: Create LogicalFilter (superset_flag = TRUE) ----
	auto filter_expr = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, superset_flag_binding);
	auto filter_op = make_uniq<LogicalFilter>(std::move(filter_expr));
	filter_op->children.push_back(std::move(window_op));
	filter_op->ResolveOperatorTypes();

	// ---- Step 2c: Create RankProj above Filter ----
	// RankProj uses pac_mean (mean of all 64 counters) for deterministic, low-variance
	// ranking. This is privacy-safe because pac_mean is independent of the secret world.
	// RankProj also passes through raw counters and keyhash so that NoisedProj (after
	// FinalTopN) can apply pac_noised to the final K rows only.
	//
	// RankProj outputs (in column order matching original binding order):
	//   - non-PAC columns: passthrough
	//   - PAC aggregate columns: pac_mean(counters) for ranking, THEN raw counters passthrough
	//   - keyhash: passthrough (for later noising)
	//
	// After FinalTopN selects K rows by pac_mean ranking, NoisedProj replaces
	// pac_mean with pac_noised(counters, keyhash) and strips intermediate columns.
	auto filter_bindings = filter_op->GetColumnBindings();
	auto &filter_types = filter_op->types;

	idx_t rank_proj_idx = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> rank_proj_exprs;

	// Map: original column binding -> RankProj column index (for ORDER BY remap to pac_mean)
	unordered_map<uint64_t, ColumnBinding> order_remap;

	// Per-column output spec for NoisedProj to reconstruct the correct column order
	enum class RankColKind { PASSTHROUGH, PAC_MEAN, PAC_COUNTERS, KEYHASH };
	struct RankColSpec {
		RankColKind kind;
		idx_t rank_col;      // column index in RankProj
		idx_t pac_idx;       // for PAC_MEAN/PAC_COUNTERS: index into pac_aggs
		LogicalType type;    // for PASSTHROUGH: original type
	};
	vector<RankColSpec> rank_specs;

	// Track PAC counters passthrough indices (for NoisedProj to reference)
	// pac_agg_idx -> RankProj column index of raw counters passthrough
	unordered_map<idx_t, idx_t> pac_counters_rank_col;
	idx_t keyhash_rank_col = idx_t(-1);

	auto child_bindings_before_window = ctx.has_intermediate_projection
	                                        ? ctx.intermediate_projections[0]->GetColumnBindings()
	                                        : agg.GetColumnBindings();

	for (idx_t i = 0; i < filter_bindings.size(); i++) {
		// Skip the superset_flag column (last binding from window)
		if (filter_bindings[i] == superset_flag_binding) {
			continue;
		}

		// Check if this is keyhash
		bool is_keyhash = false;
		if (i < child_bindings_before_window.size()) {
			if (ctx.has_intermediate_projection) {
				auto *outermost = ctx.intermediate_projections[0];
				if (filter_bindings[i].table_index == outermost->table_index) {
					auto col_idx = filter_bindings[i].column_index;
					if (col_idx < outermost->expressions.size() &&
					    outermost->expressions[col_idx]->type == ExpressionType::BOUND_COLUMN_REF) {
						ColumnBinding resolved;
						if (TraceBindingThroughProjections(
						        outermost->expressions[col_idx]->Cast<BoundColumnRefExpression>().binding,
						        ctx.intermediate_projections, 1, resolved)) {
							is_keyhash = (has_keyhash && resolved == keyhash_agg_binding);
						}
					}
				}
			} else {
				is_keyhash = (i == keyhash_agg_bindings_idx);
			}
		}

		if (is_keyhash) {
			// Passthrough keyhash for later noising
			keyhash_rank_col = rank_proj_exprs.size();
			auto ref = make_uniq<BoundColumnRefExpression>(filter_types[i], filter_bindings[i]);
			rank_proj_exprs.push_back(std::move(ref));
			rank_specs.push_back({RankColKind::KEYHASH, keyhash_rank_col, 0, filter_types[i]});
			continue;
		}

		// Check if this is a PAC aggregate column
		bool is_pac = false;
		idx_t pac_idx = 0;
		if (ctx.has_intermediate_projection) {
			auto *outermost = ctx.intermediate_projections[0];
			if (filter_bindings[i].table_index == outermost->table_index) {
				auto col_idx = filter_bindings[i].column_index;
				if (col_idx < outermost->expressions.size() &&
				    outermost->expressions[col_idx]->type == ExpressionType::BOUND_COLUMN_REF) {
					ColumnBinding resolved;
					if (TraceBindingThroughProjections(
					        outermost->expressions[col_idx]->Cast<BoundColumnRefExpression>().binding,
					        ctx.intermediate_projections, 1, resolved)) {
						for (idx_t j = 0; j < pac_aggs.size(); j++) {
							if (resolved == pac_aggs[j].agg_binding) {
								is_pac = true;
								pac_idx = j;
								break;
							}
						}
					}
				}
			}
		} else {
			for (idx_t j = 0; j < pac_aggs.size(); j++) {
				if (filter_bindings[i] == pac_aggs[j].agg_binding) {
					is_pac = true;
					pac_idx = j;
					break;
				}
			}
		}

		if (is_pac) {
			// PAC aggregate: emit pac_mean(counters) for ranking
			idx_t mean_col = rank_proj_exprs.size();
			auto counters_ref_mean = make_uniq<BoundColumnRefExpression>(list_type, filter_bindings[i]);
			auto mean_expr = input.optimizer.BindScalarFunction("pac_mean", std::move(counters_ref_mean));
			rank_proj_exprs.push_back(std::move(mean_expr));
			rank_specs.push_back({RankColKind::PAC_MEAN, mean_col, pac_idx, PacFloatLogicalType()});

			// Also emit raw counters passthrough for later noising
			idx_t counters_col = rank_proj_exprs.size();
			auto counters_passthrough = make_uniq<BoundColumnRefExpression>(list_type, filter_bindings[i]);
			rank_proj_exprs.push_back(std::move(counters_passthrough));
			rank_specs.push_back({RankColKind::PAC_COUNTERS, counters_col, pac_idx, list_type});
			pac_counters_rank_col[pac_idx] = counters_col;

			// ORDER BY remap points to pac_mean column (not noised, not counters)
			if (ctx.has_intermediate_projection) {
				auto *outermost = ctx.intermediate_projections[0];
				if (filter_bindings[i].table_index == outermost->table_index) {
					order_remap[HashBinding(filter_bindings[i])] = ColumnBinding(rank_proj_idx, mean_col);
				}
			} else {
				order_remap[HashBinding(filter_bindings[i])] = ColumnBinding(rank_proj_idx, mean_col);
			}
		} else {
			// Non-PAC column: passthrough
			idx_t col = rank_proj_exprs.size();
			auto ref = make_uniq<BoundColumnRefExpression>(filter_types[i], filter_bindings[i]);
			rank_proj_exprs.push_back(std::move(ref));
			rank_specs.push_back({RankColKind::PASSTHROUGH, col, 0, filter_types[i]});

			// ORDER BY remap for non-PAC columns
			if (ctx.has_intermediate_projection) {
				auto *outermost = ctx.intermediate_projections[0];
				if (filter_bindings[i].table_index == outermost->table_index) {
					order_remap[HashBinding(filter_bindings[i])] = ColumnBinding(rank_proj_idx, col);
				}
			} else {
				order_remap[HashBinding(filter_bindings[i])] = ColumnBinding(rank_proj_idx, col);
			}
		}
	}

	auto rank_proj = make_uniq<LogicalProjection>(rank_proj_idx, std::move(rank_proj_exprs));
	rank_proj->children.push_back(std::move(filter_op));
	rank_proj->ResolveOperatorTypes();

	// ---- Step 2d: Create FinalTopN above RankProj ----
	// ORDER BY references pac_mean columns (deterministic, privacy-safe ranking)
	vector<BoundOrderByNode> final_orders;
	for (auto &order : ctx.topn->orders) {
		auto expr = order.expression->Copy();
		std::function<void(unique_ptr<Expression> &)> RemapToRank = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = e->Cast<BoundColumnRefExpression>();
				auto it = order_remap.find(HashBinding(ref.binding));
				if (it != order_remap.end()) {
					ref.binding = it->second;
					// Update return type to match RankProj output
					auto rank_bindings = rank_proj->GetColumnBindings();
					for (idx_t ni = 0; ni < rank_bindings.size(); ni++) {
						if (rank_bindings[ni] == it->second && ni < rank_proj->types.size()) {
							ref.return_type = rank_proj->types[ni];
							break;
						}
					}
				}
			}
			ExpressionIterator::EnumerateChildren(*e, RemapToRank);
		};
		RemapToRank(expr);
		final_orders.push_back(BoundOrderByNode(order.type, order.null_order, std::move(expr)));
	}

	auto final_topn = make_uniq<LogicalTopN>(std::move(final_orders), ctx.topn->limit, ctx.topn->offset);
	final_topn->children.push_back(std::move(rank_proj));
	final_topn->ResolveOperatorTypes();

	// ---- Step 2e: Create NoisedProj above FinalTopN ----
	// Now that FinalTopN has selected the top K rows by pac_mean ranking,
	// apply pac_noised to raw counters for privacy-safe output values.
	// Output column order: PASSTHROUGH cols, then pac_noised(counters) for each PAC agg.
	// (pac_mean and raw counters/keyhash are stripped from final output.)
	idx_t noised_proj_idx = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> noised_proj_exprs;

	// First pass: emit columns in the same logical order as the original query output.
	// Iterate rank_specs and for each:
	//   PASSTHROUGH → passthrough from FinalTopN
	//   PAC_MEAN    → replace with pac_noised(counters, keyhash) cast to original type
	//   PAC_COUNTERS → skip (consumed by pac_noised above)
	//   KEYHASH     → skip (consumed by pac_noised above)
	for (auto &spec : rank_specs) {
		if (spec.kind == RankColKind::PASSTHROUGH) {
			auto topn_binding = ColumnBinding(rank_proj_idx, spec.rank_col);
			noised_proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(spec.type, topn_binding));
		} else if (spec.kind == RankColKind::PAC_MEAN) {
			// Replace pac_mean with pac_noised(raw_counters, keyhash) for the final output
			auto counters_it = pac_counters_rank_col.find(spec.pac_idx);
			D_ASSERT(counters_it != pac_counters_rank_col.end());
			auto counters_binding = ColumnBinding(rank_proj_idx, counters_it->second);
			auto counters_ref = make_uniq<BoundColumnRefExpression>(list_type, counters_binding);

			unique_ptr<Expression> keyhash_ref;
			if (keyhash_rank_col != idx_t(-1)) {
				auto keyhash_binding = ColumnBinding(rank_proj_idx, keyhash_rank_col);
				keyhash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, keyhash_binding);
			} else {
				keyhash_ref = make_uniq<BoundConstantExpression>(Value::UBIGINT(~uint64_t(0)));
			}

			auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(counters_ref),
			                                                 std::move(keyhash_ref));
			auto &orig_type = pac_aggs[spec.pac_idx].original_type;
			if (orig_type != noised->return_type) {
				noised = BoundCastExpression::AddCastToType(input.context, std::move(noised), orig_type);
			}
			noised_proj_exprs.push_back(std::move(noised));
		}
		// PAC_COUNTERS and KEYHASH columns are not emitted in the final output
	}

	auto noised_proj = make_uniq<LogicalProjection>(noised_proj_idx, std::move(noised_proj_exprs));
	noised_proj->children.push_back(std::move(final_topn));
	noised_proj->ResolveOperatorTypes();

	// Replace the original plan
	plan = std::move(noised_proj);

#if PAC_DEBUG
	PAC_DEBUG_PRINT("PACTopKRule: Successfully rewrote plan with mean-rank + noised output");
	PAC_DEBUG_PRINT(plan->ToString());
#endif
}

} // namespace duckdb
