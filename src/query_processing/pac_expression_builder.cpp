//
// Created by ila on 1/6/26.
//

#include "query_processing/pac_expression_builder.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "pac_debug.hpp"

#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "utils/pac_helpers.hpp"
#include "categorical/pac_categorical_detection.hpp"

namespace duckdb {

// Ensure projection_ids is populated before adding a new column to a LogicalGet.
// When projection_ids is empty, DuckDB generates bindings from column_ids.size();
// we must populate it with existing indices to maintain correct binding generation.
static void EnsureProjectionIdsPopulated(LogicalGet &get) {
	if (get.projection_ids.empty()) {
		for (idx_t i = 0; i < get.GetColumnIds().size(); i++) {
			get.projection_ids.push_back(i);
		}
	}
}

#if PAC_DEBUG
// Helper function to find a LogicalGet by table_index in the operator tree
static LogicalGet *FindLogicalGetByTableIndex(LogicalOperator &op, idx_t table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_index == table_index) {
			return &get;
		}
	}
	for (auto &child : op.children) {
		auto result = FindLogicalGetByTableIndex(*child, table_index);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Helper function to resolve a column name from a binding by finding the corresponding LogicalGet
static string ResolveColumnNameFromBinding(LogicalOperator &root, const ColumnBinding &binding) {
	auto get = FindLogicalGetByTableIndex(root, binding.table_index);
	if (!get) {
		return "[" + std::to_string(binding.table_index) + "." + std::to_string(binding.column_index) + "]";
	}

	const auto &column_ids = get->GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return "[" + std::to_string(binding.table_index) + "." + std::to_string(binding.column_index) + "]";
	}

	auto col_name = get->GetColumnName(column_ids[binding.column_index]);
	if (col_name.empty()) {
		return "[" + std::to_string(binding.table_index) + "." + std::to_string(binding.column_index) + "]";
	}

	return col_name;
}
#endif

// Ensure a column is projected in a LogicalGet and return its projection index
idx_t EnsureProjectedColumn(LogicalGet &g, const string &col_name) {
	// try existing projected columns by matching returned names via ColumnIndex primary
	for (idx_t cid = 0; cid < g.GetColumnIds().size(); ++cid) {
		auto col_idx = g.GetColumnIds()[cid];
		if (!col_idx.IsVirtualColumn()) {
			idx_t primary = col_idx.GetPrimaryIndex();
			if (primary < g.names.size() && g.names[primary] == col_name) {
				return cid;
			}
		}
	}
	// otherwise add column from table schema
	auto table_entry = g.GetTable();
	if (!table_entry) {
		return DConstants::INVALID_INDEX;
	}
	idx_t logical_idx = DConstants::INVALID_INDEX;
	idx_t ti = 0;
	for (auto &col : table_entry->GetColumns().Logical()) {
		if (col.Name() == col_name) {
			logical_idx = ti;
			break;
		}
		ti++;
	}
	if (logical_idx == DConstants::INVALID_INDEX) {
		return DConstants::INVALID_INDEX;
	}

	EnsureProjectionIdsPopulated(g);

	// Add the column to the LogicalGet
	g.AddColumnId(logical_idx);

	// The projection index is the position in the output, which equals the new size - 1
	idx_t new_proj_idx = g.GetColumnIds().size() - 1;
	g.projection_ids.push_back(new_proj_idx);

	// ResolveOperatorTypes() calls the protected ResolveTypes() which rebuilds
	// the types vector from column_ids/projection_ids
	g.ResolveOperatorTypes();

	return new_proj_idx;
}

// Ensure PK columns are present in a LogicalGet's column_ids and projection_ids
void AddPKColumns(LogicalGet &get, const vector<string> &pks) {
	auto table_entry_ptr = get.GetTable();
	if (!table_entry_ptr) {
		throw InternalException("PAC compiler: expected LogicalGet to be bound to a table when PKs are present");
	}
	for (auto &pk : pks) {
		idx_t proj_idx = EnsureProjectedColumn(get, pk);
		if (proj_idx == DConstants::INVALID_INDEX) {
			throw InternalException("PAC compiler: could not find PK column " + pk + " in table");
		}
	}
}

// Helper to ensure rowid is present in the output columns of a LogicalGet
void AddRowIDColumn(LogicalGet &get) {
	if (get.virtual_columns.find(COLUMN_IDENTIFIER_ROW_ID) != get.virtual_columns.end()) {
		get.virtual_columns[COLUMN_IDENTIFIER_ROW_ID] = TableColumn("rowid", LogicalTypeId::BIGINT);
	}

	EnsureProjectionIdsPopulated(get);

	get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
	get.projection_ids.push_back(get.GetColumnIds().size() - 1);

	// ResolveOperatorTypes() calls the protected ResolveTypes() which rebuilds
	// the types vector from column_ids/projection_ids
	get.ResolveOperatorTypes();
}

// Hash one or more column expressions: hash(c1) XOR hash(c2) XOR ...
// Works for any column type (VARCHAR, INTEGER, etc.)
unique_ptr<Expression> BuildXorHash(OptimizerExtensionInput &input, vector<unique_ptr<Expression>> cols) {
	if (cols.empty()) {
		throw InternalException("PAC compiler: BuildXorHash called with empty column list");
	}
	auto left = input.optimizer.BindScalarFunction("hash", std::move(cols[0]));
	for (size_t i = 1; i < cols.size(); ++i) {
		auto right = input.optimizer.BindScalarFunction("hash", std::move(cols[i]));
		left = input.optimizer.BindScalarFunction("xor", std::move(left), std::move(right));
	}
	return left;
}

// Build hash expression for the given LogicalGet's primary key columns
unique_ptr<Expression> BuildXorHashFromPKs(OptimizerExtensionInput &input, LogicalGet &get, const vector<string> &pks) {
	vector<unique_ptr<Expression>> pk_cols;
	for (auto &pk : pks) {
		idx_t proj_idx = EnsureProjectedColumn(get, pk);
		if (proj_idx == DConstants::INVALID_INDEX) {
			throw InternalException("PAC compiler: failed to find PK column " + pk);
		}
		auto col_binding = ColumnBinding(get.table_index, proj_idx);
		auto col_index_obj = get.GetColumnIds()[proj_idx];
		auto &col_type = get.GetColumnType(col_index_obj);
		pk_cols.push_back(make_uniq<BoundColumnRefExpression>(col_type, col_binding));
	}
	return BuildXorHash(input, std::move(pk_cols));
}

ColumnBinding InsertHashProjectionAboveGet(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                           LogicalGet &get, const vector<string> &key_columns, bool use_rowid) {
	// 1. Build hash expression from the get's columns
	unique_ptr<Expression> hash_expr;
	if (use_rowid) {
		AddRowIDColumn(get);
		auto rowid_binding = ColumnBinding(get.table_index, get.GetColumnIds().size() - 1);
		auto rowid_col = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, rowid_binding);
		hash_expr = input.optimizer.BindScalarFunction("hash", std::move(rowid_col));
	} else {
		AddPKColumns(get, key_columns);
		hash_expr = BuildXorHashFromPKs(input, get, key_columns);
	}

	// 1b. Wrap in pac_hash() to XOR with query_hash and optionally repair to 32 bits set
	hash_expr = input.optimizer.BindScalarFunction("pac_hash", std::move(hash_expr));
	hash_expr->alias = "pac_pu";

	// 2. Find the unique_ptr slot holding this get in the plan tree
	vector<unique_ptr<LogicalOperator> *> get_nodes;
	FindAllNodesByTableIndex(&plan, get.table_index, get_nodes);
	if (get_nodes.empty()) {
		throw InternalException("PAC compiler: InsertHashProjectionAboveGet could not find get #" +
		                        std::to_string(get.table_index));
	}
	auto &get_slot = *get_nodes[0];

	// 3. Collect old output bindings before we wrap the get
	auto old_bindings = get_slot->GetColumnBindings();
	idx_t num_get_cols = old_bindings.size();

	// 4. Create projection with passthrough + hash column
	auto &binder = input.optimizer.binder;
	idx_t proj_table_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;

	// Passthrough expressions for each existing get output column
	for (idx_t i = 0; i < num_get_cols; i++) {
		auto &binding = old_bindings[i];
		auto col_type = get_slot->types[i];
		proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(col_type, binding));
	}

	// Hash expression as the last column
	proj_expressions.push_back(std::move(hash_expr));

	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));

	// 5. Move get into projection as child, place projection in slot
	projection->children.push_back(std::move(get_slot));
	projection->ResolveOperatorTypes();

	// Before placing projection in slot, set it up
	LogicalOperator *proj_ptr = projection.get();
	get_slot = std::move(projection);

	// 6. Use ColumnBindingReplacer to remap [get.table_index, i] → [proj.table_index, i]
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < num_get_cols; i++) {
		ColumnBinding old_b = old_bindings[i];
		ColumnBinding new_b(proj_table_index, i);
		replacer.replacement_bindings.emplace_back(old_b, new_b);
	}
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);

	// 7. Return binding for the hash column (last expression in projection)
	return ColumnBinding(proj_table_index, num_get_cols);
}

ColumnBinding GetOrInsertHashProjection(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                        LogicalGet &get, const vector<string> &key_columns, bool use_rowid,
                                        std::unordered_map<idx_t, ColumnBinding> &cache) {
	auto it = cache.find(get.table_index);
	if (it != cache.end()) {
		return it->second;
	}
	auto binding = InsertHashProjectionAboveGet(input, plan, get, key_columns, use_rowid);
	cache[get.table_index] = binding;
	return binding;
}

// Insert a hash projection above a CTE_SCAN (LogicalCTERef) node.
// The CTE_SCAN must expose the key columns by name in its bound_columns.
// Returns the ColumnBinding for the new hash column, or INVALID if key columns not found.
// Find the unique_ptr slot holding a CTE_REF with the given table_index in the plan tree.
static unique_ptr<LogicalOperator> *FindCTERefSlot(unique_ptr<LogicalOperator> &root, idx_t table_index) {
	if (!root) {
		return nullptr;
	}
	if (root->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = root->Cast<LogicalCTERef>();
		if (ref.table_index == table_index) {
			return &root;
		}
	}
	for (auto &child : root->children) {
		auto *result = FindCTERefSlot(child, table_index);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

ColumnBinding InsertHashProjectionAboveCTERef(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                              LogicalCTERef &cte_ref, const vector<string> &key_columns) {
#if PAC_DEBUG
	PAC_DEBUG_PRINT("InsertHashProjectionAboveCTERef: CTE_SCAN table_index=" + std::to_string(cte_ref.table_index) +
	                " cte_index=" + std::to_string(cte_ref.cte_index));
#endif

	// 1. Resolve key column indices in the CTE_SCAN's bound_columns
	vector<unique_ptr<Expression>> key_col_exprs;
	for (auto &key : key_columns) {
		bool found = false;
		for (idx_t i = 0; i < cte_ref.bound_columns.size(); i++) {
			if (cte_ref.bound_columns[i] == key) {
				auto binding = ColumnBinding(cte_ref.table_index, i);
				key_col_exprs.push_back(make_uniq<BoundColumnRefExpression>(cte_ref.chunk_types[i], binding));
				found = true;
				break;
			}
		}
		if (!found) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("InsertHashProjectionAboveCTERef: key column '" + key + "' not found in bound_columns");
#endif
			return ColumnBinding(DConstants::INVALID_INDEX, 0);
		}
	}

	// 2. Build hash expression: hash(c1) XOR hash(c2) XOR ...
	auto hash_expr = BuildXorHash(input, std::move(key_col_exprs));

	// Wrap in pac_hash() to XOR with query_hash and optionally repair to 32 bits set
	hash_expr = input.optimizer.BindScalarFunction("pac_hash", std::move(hash_expr));
	hash_expr->alias = "pac_pu";

	// 3. Find the unique_ptr slot holding this CTE_SCAN in the plan tree
	auto *cte_slot_ptr = FindCTERefSlot(plan, cte_ref.table_index);
	if (!cte_slot_ptr) {
		throw InternalException("PAC compiler: InsertHashProjectionAboveCTERef could not find CTE_SCAN #" +
		                        std::to_string(cte_ref.table_index));
	}
	auto &cte_slot = *cte_slot_ptr;

	// 4. Create projection: passthrough all CTE columns + hash column
	auto old_bindings = cte_slot->GetColumnBindings();
	idx_t num_cte_cols = old_bindings.size();

	auto &binder = input.optimizer.binder;
	idx_t proj_table_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;
	for (idx_t i = 0; i < num_cte_cols; i++) {
		proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(cte_slot->types[i], old_bindings[i]));
	}
	proj_expressions.push_back(std::move(hash_expr));

	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(cte_slot));
	projection->ResolveOperatorTypes();

	// 5. Replace CTE_SCAN slot with the new projection and remap bindings
	LogicalOperator *proj_ptr = projection.get();
	cte_slot = std::move(projection);

	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < num_cte_cols; i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], ColumnBinding(proj_table_index, i));
	}
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);

#if PAC_DEBUG
	PAC_DEBUG_PRINT("InsertHashProjectionAboveCTERef: inserted hash projection (table_index=" +
	                std::to_string(proj_table_index) + ") above CTE_SCAN #" + std::to_string(cte_ref.table_index) +
	                ", hash at column " + std::to_string(num_cte_cols));
#endif

	return ColumnBinding(proj_table_index, num_cte_cols);
}

// Build AND expression from multiple hash expressions (for multiple PUs)
unique_ptr<Expression> BuildAndFromHashes(OptimizerExtensionInput &input, vector<unique_ptr<Expression>> &hash_exprs) {
	if (hash_exprs.empty()) {
		throw InternalException("PAC compiler: cannot build AND expression from empty hash list");
	}

	// If there is only one hash, return it directly
	if (hash_exprs.size() == 1) {
		return std::move(hash_exprs[0]);
	}

	// Build chain using optimizer.BindScalarFunction with operator "&" (bitwise AND)
	auto left = std::move(hash_exprs[0]);
	for (size_t i = 1; i < hash_exprs.size(); ++i) {
		auto right = std::move(hash_exprs[i]);
		left = input.optimizer.BindScalarFunction("&", std::move(left), std::move(right));
	}

	return left;
}

// Bind a PAC aggregate function with hash + value arguments and optional correction factor.
unique_ptr<Expression> BindPacAggregate(OptimizerExtensionInput &input, const string &pac_func_name,
                                        unique_ptr<Expression> hash_expr, unique_ptr<Expression> value_expr,
                                        unique_ptr<Expression> correction_expr) {
	FunctionBinder function_binder(input.context);
	ErrorData error;
	vector<LogicalType> arg_types = {hash_expr->return_type, value_expr->return_type};
	if (correction_expr) {
		arg_types.push_back(correction_expr->return_type);
	}

	auto &entry = Catalog::GetSystemCatalog(input.context)
	                  .GetEntry<AggregateFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, pac_func_name);
	auto best = function_binder.BindFunction(entry.name, entry.functions, arg_types, error);
	if (!best.IsValid()) {
		throw InternalException("PAC compiler: failed to bind " + pac_func_name);
	}
	auto func = entry.functions.GetFunctionByOffset(best.GetIndex());

	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(hash_expr));
	children.push_back(std::move(value_expr));
	if (correction_expr) {
		children.push_back(std::move(correction_expr));
	}
	return function_binder.BindAggregateFunction(func, std::move(children), nullptr, AggregateType::NON_DISTINCT);
}

// Bind bit_or(hash_expr) with IS NOT NULL filter on filter_col_expr.
unique_ptr<Expression> BindBitOrAggregate(OptimizerExtensionInput &input, unique_ptr<Expression> hash_expr,
                                          unique_ptr<Expression> filter_col_expr) {
	FunctionBinder function_binder(input.context);
	ErrorData error;
	vector<LogicalType> bit_or_types = {hash_expr->return_type};
	auto &bit_or_entry = Catalog::GetSystemCatalog(input.context)
	                         .GetEntry<AggregateFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, "bit_or");
	auto bit_or_best = function_binder.BindFunction(bit_or_entry.name, bit_or_entry.functions, bit_or_types, error);
	if (!bit_or_best.IsValid()) {
		throw InternalException("PAC compiler: failed to bind bit_or");
	}
	auto bit_or_func = bit_or_entry.functions.GetFunctionByOffset(bit_or_best.GetIndex());

	vector<unique_ptr<Expression>> bit_or_children;
	bit_or_children.push_back(std::move(hash_expr));
	auto bit_or_expr = function_binder.BindAggregateFunction(bit_or_func, std::move(bit_or_children), nullptr,
	                                                         AggregateType::NON_DISTINCT);

	// Add IS NOT NULL filter
	auto not_null_filter =
	    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	not_null_filter->children.push_back(std::move(filter_col_expr));
	bit_or_expr->Cast<BoundAggregateExpression>().filter = std::move(not_null_filter);

	return bit_or_expr;
}

// Map aggregate function name to PAC function name
static string GetPacAggregateFunctionName(const string &function_name, ClientContext *ctx = nullptr) {
	string pac_function_name;
	if (function_name == "sum" || function_name == "sum_no_overflow") {
		pac_function_name = "pac_noised_sum";
	} else if (function_name == "count" || function_name == "count_star") {
		pac_function_name = "pac_noised_count";
	} else if (function_name == "min") {
		pac_function_name = "pac_noised_min";
	} else if (function_name == "max") {
		pac_function_name = "pac_noised_max";
	} else if (function_name == "avg") {
		pac_function_name = "pac_noised_avg";
	} else {
		throw NotImplementedException("PAC compiler: unsupported aggregate function " + function_name);
	}
	return pac_function_name;
}

// Insert a pre-aggregation step for DISTINCT aggregates.
// Instead of pac_*_distinct (custom hash map), this leverages DuckDB's native GROUP BY
// to deduplicate values, then feeds bit_or(hash) results into standard pac_count/pac_sum/pac_avg.
//
// Plan transformation for COUNT(DISTINCT col) GROUP BY [g1]:
//   AGGREGATE [pac_count(combined_hash, 1)] GROUP BY [g1]
//     AGGREGATE [bit_or(hash) FILTER (col IS NOT NULL)] GROUP BY [g1, hash(col)]
//       <original child>
//
// Groups by the distinct value column directly and lets DuckDB's native hash table
// handle deduplication. The pac_hash (from PK/FK) is OR'd via bit_or per group.
static void InsertDistinctPreAggregation(OptimizerExtensionInput &input, LogicalAggregate *agg,
                                         unique_ptr<Expression> &hash_input_expr,
                                         unique_ptr<Expression> &distinct_value_expr) {
	auto &binder = input.optimizer.binder;

	// 1. Create inner aggregate with new table indices
	idx_t inner_group_index = binder.GenerateTableIndex();
	idx_t inner_agg_index = binder.GenerateTableIndex();

	// 2. Bind bit_or aggregate on the hash expression with IS NOT NULL filter
	auto bit_or_expr = BindBitOrAggregate(input, hash_input_expr->Copy(), distinct_value_expr->Copy());

	// 3. Build the inner aggregate node
	vector<unique_ptr<Expression>> inner_expressions;
	inner_expressions.push_back(std::move(bit_or_expr));
	auto inner_agg_node = make_uniq<LogicalAggregate>(inner_group_index, inner_agg_index, std::move(inner_expressions));

	// Copy outer groups into inner aggregate (passthrough group keys)
	idx_t num_original_groups = agg->groups.size();
	for (auto &g : agg->groups) {
		inner_agg_node->groups.push_back(g->Copy());
	}

	// Add the distinct value as an additional group key.
	// DuckDB's hash table handles deduplication natively — no need to pre-hash.
	inner_agg_node->groups.push_back(distinct_value_expr->Copy());

	// 4. Steal outer's child → inner's child, then inner → outer's child
	inner_agg_node->children.push_back(std::move(agg->children[0]));
	inner_agg_node->ResolveOperatorTypes();
	agg->children[0] = std::move(inner_agg_node);

	// 5. Remap outer aggregate's groups to reference inner aggregate's group output
	for (idx_t i = 0; i < num_original_groups; i++) {
		auto gtype = agg->groups[i]->return_type;
		agg->groups[i] = make_uniq<BoundColumnRefExpression>(gtype, ColumnBinding(inner_group_index, i));
	}

	// Inner aggregate output layout:
	//   groups:     [g1, ..., gN, distinct_col]  at inner_group_index
	//   aggregates: [bit_or(hash)]               at inner_agg_index
	ColumnBinding combined_hash_binding(inner_agg_index, 0);
	ColumnBinding distinct_val_binding(inner_group_index, num_original_groups);

	// 6. Replace each aggregate expression with the corresponding PAC function
	for (idx_t i = 0; i < agg->expressions.size(); i++) {
		auto &old_aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
		string function_name = old_aggr.function.name;
		string pac_name = GetPacAggregateFunctionName(function_name, &input.context);

		auto hash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, combined_hash_binding);
		unique_ptr<Expression> value_ref;
		if (function_name == "count" || function_name == "count_star") {
			value_ref = make_uniq_base<Expression, BoundConstantExpression>(Value::BIGINT(1));
		} else {
			// SUM/AVG/MIN/MAX DISTINCT: value comes from the distinct column (now a group key in inner agg)
			value_ref = make_uniq<BoundColumnRefExpression>(distinct_value_expr->return_type, distinct_val_binding);
		}

		agg->expressions[i] = BindPacAggregate(input, pac_name, std::move(hash_ref), std::move(value_ref));
	}

	agg->ResolveOperatorTypes();

#if PAC_DEBUG
	PAC_DEBUG_PRINT("InsertDistinctPreAggregation: Inserted inner GROUP BY aggregate (group_index=" +
	                std::to_string(inner_group_index) + ", agg_index=" + std::to_string(inner_agg_index) + ") with " +
	                std::to_string(num_original_groups) + " original groups + distinct value");
#endif
}

// Find the unique_ptr slot that holds the given operator pointer in the plan tree.
static unique_ptr<LogicalOperator> *FindSlotByPointer(unique_ptr<LogicalOperator> &root, LogicalOperator *target) {
	if (root.get() == target) {
		return &root;
	}
	for (auto &child : root->children) {
		auto *result = FindSlotByPointer(child, target);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

// Build a standalone aggregate branch for one DISTINCT column.
// Returns: outer_aggregate → inner_aggregate(bit_or) → child
// The outer aggregate produces [groups..., pac_agg(hash, value)] at its own indices.
static unique_ptr<LogicalOperator>
BuildDistinctBranch(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> child,
                    const vector<unique_ptr<Expression>> &groups, unique_ptr<Expression> hash_expr,
                    unique_ptr<Expression> distinct_value_expr,
                    const vector<std::pair<idx_t, string>> &agg_specs, // (original_agg_idx, original_func_name)
                    idx_t &out_group_index, idx_t &out_agg_index) {
	auto &binder = input.optimizer.binder;
	idx_t inner_group_index = binder.GenerateTableIndex();
	idx_t inner_agg_index = binder.GenerateTableIndex();

	// Inner aggregate: bit_or(hash) FILTER (distinct_col IS NOT NULL) GROUP BY [groups..., distinct_col]
	auto bit_or_expr = BindBitOrAggregate(input, hash_expr->Copy(), distinct_value_expr->Copy());
	vector<unique_ptr<Expression>> inner_expressions;
	inner_expressions.push_back(std::move(bit_or_expr));
	auto inner_agg = make_uniq<LogicalAggregate>(inner_group_index, inner_agg_index, std::move(inner_expressions));

	idx_t num_groups = groups.size();
	for (auto &g : groups) {
		inner_agg->groups.push_back(g->Copy());
	}
	inner_agg->groups.push_back(distinct_value_expr->Copy());
	inner_agg->children.push_back(std::move(child));
	inner_agg->ResolveOperatorTypes();

	// Outer aggregate: pac_*(combined_hash, value) GROUP BY [groups...]
	idx_t outer_group_index = binder.GenerateTableIndex();
	idx_t outer_agg_index = binder.GenerateTableIndex();

	ColumnBinding combined_hash_binding(inner_agg_index, 0);
	ColumnBinding distinct_val_binding(inner_group_index, num_groups);

	vector<unique_ptr<Expression>> outer_expressions;
	for (auto &spec : agg_specs) {
		string pac_name = GetPacAggregateFunctionName(spec.second, &input.context);
		auto hash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, combined_hash_binding);
		unique_ptr<Expression> value_ref;
		if (spec.second == "count" || spec.second == "count_star") {
			value_ref = make_uniq_base<Expression, BoundConstantExpression>(Value::BIGINT(1));
		} else {
			value_ref = make_uniq<BoundColumnRefExpression>(distinct_value_expr->return_type, distinct_val_binding);
		}
		outer_expressions.push_back(BindPacAggregate(input, pac_name, std::move(hash_ref), std::move(value_ref)));
	}

	auto outer_agg = make_uniq<LogicalAggregate>(outer_group_index, outer_agg_index, std::move(outer_expressions));
	for (idx_t i = 0; i < num_groups; i++) {
		auto gtype = groups[i]->return_type;
		outer_agg->groups.push_back(make_uniq<BoundColumnRefExpression>(gtype, ColumnBinding(inner_group_index, i)));
	}
	outer_agg->children.push_back(std::move(inner_agg));
	outer_agg->ResolveOperatorTypes();

	out_group_index = outer_group_index;
	out_agg_index = outer_agg_index;
	return outer_agg;
}

// Build a standalone aggregate branch for non-DISTINCT aggregates.
// Returns: aggregate(pac_*(hash, value)) → child
static unique_ptr<LogicalOperator> BuildNonDistinctBranch(
    OptimizerExtensionInput &input, unique_ptr<LogicalOperator> child, const vector<unique_ptr<Expression>> &groups,
    unique_ptr<Expression> hash_expr,
    const vector<std::pair<idx_t, const BoundAggregateExpression *>> &agg_specs, // (original_agg_idx, original_expr)
    idx_t &out_group_index, idx_t &out_agg_index) {
	auto &binder = input.optimizer.binder;
	idx_t group_index = binder.GenerateTableIndex();
	idx_t agg_index = binder.GenerateTableIndex();

	vector<unique_ptr<Expression>> expressions;
	for (auto &spec : agg_specs) {
		auto &old_aggr = *spec.second;
		string function_name = old_aggr.function.name;
		string pac_name = GetPacAggregateFunctionName(function_name, &input.context);

		unique_ptr<Expression> value_child;
		if (old_aggr.children.empty()) {
			value_child = make_uniq_base<Expression, BoundConstantExpression>(Value::BIGINT(1));
		} else {
			value_child = old_aggr.children[0]->Copy();
		}

		expressions.push_back(BindPacAggregate(input, pac_name, hash_expr->Copy(), std::move(value_child)));
	}

	auto agg_node = make_uniq<LogicalAggregate>(group_index, agg_index, std::move(expressions));
	for (auto &g : groups) {
		agg_node->groups.push_back(g->Copy());
	}
	agg_node->children.push_back(std::move(child));
	agg_node->ResolveOperatorTypes();

	out_group_index = group_index;
	out_agg_index = agg_index;
	return agg_node;
}

// Helper: remap expression bindings using an index map
static void RemapExpressionBindings(Expression &e, const std::unordered_map<idx_t, idx_t> &map) {
	if (e.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = e.Cast<BoundColumnRefExpression>();
		auto it = map.find(col_ref.binding.table_index);
		if (it != map.end()) {
			col_ref.binding.table_index = it->second;
		}
	}
	ExpressionIterator::EnumerateChildren(e, [&](Expression &child) { RemapExpressionBindings(child, map); });
}

// Orchestrator: splits a mixed/multi-DISTINCT aggregate into N branches joined together.
static void InsertMultiBranchPreAggregation(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                            LogicalAggregate *agg, unique_ptr<Expression> &hash_input_expr) {
	auto &binder = input.optimizer.binder;
	bool has_groups = !agg->groups.empty();

	// 1. Classify aggregates by distinct column (or non-distinct)
	struct BranchInfo {
		bool is_distinct;
		string distinct_col_str;
		unique_ptr<Expression> distinct_col_expr;
		vector<std::pair<idx_t, string>> distinct_agg_specs;
		vector<std::pair<idx_t, const BoundAggregateExpression *>> non_distinct_specs;
	};
	vector<BranchInfo> branches;
	std::unordered_map<string, idx_t> distinct_col_to_branch;

	for (idx_t i = 0; i < agg->expressions.size(); i++) {
		auto &aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
		bool is_distinct = aggr.IsDistinct() && !aggr.children.empty();

		if (is_distinct) {
			string col_str = aggr.children[0]->ToString();
			auto it = distinct_col_to_branch.find(col_str);
			if (it != distinct_col_to_branch.end()) {
				branches[it->second].distinct_agg_specs.emplace_back(i, aggr.function.name);
			} else {
				BranchInfo info;
				info.is_distinct = true;
				info.distinct_col_str = col_str;
				info.distinct_col_expr = aggr.children[0]->Copy();
				info.distinct_agg_specs.emplace_back(i, aggr.function.name);
				distinct_col_to_branch[col_str] = branches.size();
				branches.push_back(std::move(info));
			}
		} else {
			bool found = false;
			for (auto &b : branches) {
				if (!b.is_distinct) {
					b.non_distinct_specs.emplace_back(i, &aggr);
					found = true;
					break;
				}
			}
			if (!found) {
				BranchInfo info;
				info.is_distinct = false;
				info.non_distinct_specs.emplace_back(i, &aggr);
				branches.push_back(std::move(info));
			}
		}
	}

	// 2. Prepare per-branch inputs: copy child subtrees and remap expressions
	std::unordered_set<idx_t> all_plan_indices;
	CollectTableIndicesRecursive(plan.get(), all_plan_indices);

	// Make N-1 copies first (while original child is still alive), save index maps
	vector<unique_ptr<LogicalOperator>> child_copies;
	vector<std::unordered_map<idx_t, idx_t>> index_maps;
	for (idx_t bi = 1; bi < branches.size(); bi++) {
		auto copy = agg->children[0]->Copy(input.context);
		auto imap = RemapSubtreeIndices(copy.get(), binder, all_plan_indices);
		for (auto &kv : imap) {
			all_plan_indices.insert(kv.second);
		}
		child_copies.push_back(std::move(copy));
		index_maps.push_back(std::move(imap));
	}

	// 3. Build each branch
	struct BuiltBranch {
		unique_ptr<LogicalOperator> subtree;
		idx_t group_index;
		idx_t agg_index;
		vector<idx_t> original_agg_indices;
	};
	vector<BuiltBranch> built_branches;

	for (idx_t bi = 0; bi < branches.size(); bi++) {
		auto &branch = branches[bi];

		// Get child and prepare remapped expressions
		unique_ptr<LogicalOperator> child;
		unique_ptr<Expression> branch_hash;
		vector<unique_ptr<Expression>> branch_groups;

		if (bi == 0) {
			child = std::move(agg->children[0]);
			branch_hash = hash_input_expr->Copy();
			for (auto &g : agg->groups) {
				branch_groups.push_back(g->Copy());
			}
		} else {
			child = std::move(child_copies[bi - 1]);
			auto &imap = index_maps[bi - 1];

			branch_hash = hash_input_expr->Copy();
			RemapExpressionBindings(*branch_hash, imap);

			for (auto &g : agg->groups) {
				auto gcopy = g->Copy();
				RemapExpressionBindings(*gcopy, imap);
				branch_groups.push_back(std::move(gcopy));
			}
		}

		BuiltBranch bb;
		if (branch.is_distinct) {
			for (auto &s : branch.distinct_agg_specs) {
				bb.original_agg_indices.push_back(s.first);
			}
			auto distinct_col = branch.distinct_col_expr->Copy();
			if (bi > 0) {
				RemapExpressionBindings(*distinct_col, index_maps[bi - 1]);
			}
			bb.subtree =
			    BuildDistinctBranch(input, std::move(child), branch_groups, std::move(branch_hash),
			                        std::move(distinct_col), branch.distinct_agg_specs, bb.group_index, bb.agg_index);
		} else {
			for (auto &s : branch.non_distinct_specs) {
				bb.original_agg_indices.push_back(s.first);
			}
			// For non-distinct on copies, remap the value expressions too
			if (bi > 0) {
				auto &imap = index_maps[bi - 1];
				// Create remapped non_distinct_specs with updated value expressions
				vector<std::pair<idx_t, const BoundAggregateExpression *>> specs_copy;
				for (auto &s : branch.non_distinct_specs) {
					specs_copy.push_back(s);
				}
				// BuildNonDistinctBranch copies the children from the specs, so we need
				// the expressions to still reference original bindings. But wait — the child
				// subtree is remapped, so the aggregate value expressions (which reference the
				// child's output) need remapping too.
				// Since BuildNonDistinctBranch copies old_aggr.children[0], and those children
				// still reference the ORIGINAL indices, we need to remap them after building.
				// Actually, let's just build with original specs and then fix up.
				// A simpler approach: BuildNonDistinctBranch takes the specs and copies children
				// from the BoundAggregateExpression pointers. Those still reference original indices.
				// We need to remap the aggregate value children too.
				// Let's handle this in BuildNonDistinctBranch by accepting pre-built value expressions.
				// For now, use a workaround: build a temporary vector of remapped value expressions.
				bb.subtree = BuildNonDistinctBranch(input, std::move(child), branch_groups, std::move(branch_hash),
				                                    branch.non_distinct_specs, bb.group_index, bb.agg_index);
				// Fix up: remap value expression bindings inside the built branch's aggregate expressions
				auto &branch_agg = bb.subtree->Cast<LogicalAggregate>();
				for (auto &expr : branch_agg.expressions) {
					auto &bound_agg = expr->Cast<BoundAggregateExpression>();
					// Child 0 is hash (already from fresh branch_hash), child 1 is the value
					if (bound_agg.children.size() > 1) {
						RemapExpressionBindings(*bound_agg.children[1], imap);
					}
				}
				bb.subtree->ResolveOperatorTypes();
			} else {
				bb.subtree = BuildNonDistinctBranch(input, std::move(child), branch_groups, std::move(branch_hash),
				                                    branch.non_distinct_specs, bb.group_index, bb.agg_index);
			}
		}
		built_branches.push_back(std::move(bb));
	}

	// Save branch expression types before step 4 moves subtrees
	vector<vector<LogicalType>> branch_expr_types;
	for (auto &bb : built_branches) {
		auto &branch_agg = bb.subtree->Cast<LogicalAggregate>();
		vector<LogicalType> types;
		for (auto &expr : branch_agg.expressions) {
			types.push_back(expr->return_type);
		}
		branch_expr_types.push_back(std::move(types));
	}

	// 4. Join branches together
	unique_ptr<LogicalOperator> joined = std::move(built_branches[0].subtree);
	idx_t left_group_index = built_branches[0].group_index;

	for (idx_t bi = 1; bi < built_branches.size(); bi++) {
		auto &right = built_branches[bi];

		if (has_groups) {
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			for (idx_t gi = 0; gi < agg->groups.size(); gi++) {
				JoinCondition cond;
				cond.left = make_uniq<BoundColumnRefExpression>(agg->groups[gi]->return_type,
				                                                ColumnBinding(left_group_index, gi));
				cond.right = make_uniq<BoundColumnRefExpression>(agg->groups[gi]->return_type,
				                                                 ColumnBinding(right.group_index, gi));
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				join->conditions.push_back(std::move(cond));
			}
			join->children.push_back(std::move(joined));
			join->children.push_back(std::move(right.subtree));
			join->ResolveOperatorTypes();
			joined = std::move(join);
		} else {
			joined = LogicalCrossProduct::Create(std::move(joined), std::move(right.subtree));
			joined->ResolveOperatorTypes();
		}
	}

	// 5. Create output projection
	idx_t proj_table_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;

	idx_t num_groups = agg->groups.size();
	for (idx_t gi = 0; gi < num_groups; gi++) {
		proj_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(agg->groups[gi]->return_type, ColumnBinding(left_group_index, gi)));
	}

	// Build lookup: original_agg_idx → (branch_idx, position_within_branch)
	std::unordered_map<idx_t, std::pair<idx_t, idx_t>> agg_idx_to_branch;
	for (idx_t bi = 0; bi < built_branches.size(); bi++) {
		for (idx_t pos = 0; pos < built_branches[bi].original_agg_indices.size(); pos++) {
			agg_idx_to_branch[built_branches[bi].original_agg_indices[pos]] = {bi, pos};
		}
	}

	for (idx_t ai = 0; ai < agg->expressions.size(); ai++) {
		auto it = agg_idx_to_branch.find(ai);
		if (it == agg_idx_to_branch.end()) {
			throw InternalException("PAC compiler: multi-branch could not find aggregate " + std::to_string(ai));
		}
		idx_t branch_idx = it->second.first;
		idx_t pos_in_branch = it->second.second;
		auto &branch = built_branches[branch_idx];

		auto original_type = agg->expressions[ai]->return_type;
		auto branch_type = branch_expr_types[branch_idx][pos_in_branch];

		PAC_DEBUG_PRINT("MultiBranch output: ai=" + std::to_string(ai) + " branch=" + std::to_string(branch_idx) +
		                " pos=" + std::to_string(pos_in_branch) + " agg_index=" + std::to_string(branch.agg_index) +
		                " original_type=" + original_type.ToString() + " branch_type=" + branch_type.ToString());

		unique_ptr<Expression> ref =
		    make_uniq<BoundColumnRefExpression>(branch_type, ColumnBinding(branch.agg_index, pos_in_branch));
		if (branch_type != original_type) {
			ref = BoundCastExpression::AddCastToType(input.context, std::move(ref), original_type);
		}
		proj_expressions.push_back(std::move(ref));
	}

	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(joined));
	projection->ResolveOperatorTypes();

	// 6. Replace the aggregate node in the plan tree
	auto *agg_slot = FindSlotByPointer(plan, agg);
	if (!agg_slot) {
		throw InternalException("PAC compiler: InsertMultiBranchPreAggregation could not find aggregate in plan");
	}

	idx_t old_group_index = agg->group_index;
	idx_t old_agg_index = agg->aggregate_index;
	idx_t num_agg_exprs = agg->expressions.size();

	ColumnBindingReplacer replacer;
	for (idx_t gi = 0; gi < num_groups; gi++) {
		replacer.replacement_bindings.emplace_back(ColumnBinding(old_group_index, gi),
		                                           ColumnBinding(proj_table_index, gi));
	}
	for (idx_t ai = 0; ai < num_agg_exprs; ai++) {
		replacer.replacement_bindings.emplace_back(ColumnBinding(old_agg_index, ai),
		                                           ColumnBinding(proj_table_index, num_groups + ai));
	}

	LogicalOperator *proj_ptr = projection.get();
	*agg_slot = std::move(projection);
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);
}

/**
 * ModifyAggregatesWithPacFunctions: Transforms regular aggregate expressions to PAC aggregate expressions
 *
 * Purpose: This is the core transformation function that replaces standard aggregates (SUM, AVG, COUNT, MIN, MAX)
 * with their PAC equivalents (pac_sum, pac_avg, pac_count, pac_min, pac_max) by adding the hash expression
 * as the first argument.
 *
 * Arguments:
 * @param input - Optimizer extension input containing context and function binder
 * @param agg - The LogicalAggregate node whose expressions will be transformed
 * @param hash_input_expr - The hash expression identifying privacy units (e.g., hash(c_custkey) or hash(xor(pk1, pk2)))
 *
 * Logic:
 * 1. For each aggregate expression in the node:
 *    - Extract the original aggregate function name (sum, avg, count, etc.)
 *    - Extract the value expression (the data being aggregated)
 *      * For COUNT(*), create a constant 1 expression
 *      * For others (SUM(val), AVG(val)), use the existing child expression
 *    - Map to the PAC function name (sum -> pac_sum, avg -> pac_avg, etc.)
 *    - Bind the PAC aggregate function with two arguments:
 *      * First argument: hash expression (identifies which PU each row belongs to)
 *      * Second argument: value expression (the data to aggregate per PU)
 *    - Preserve the DISTINCT flag from the original aggregate
 *    - Replace the old aggregate expression with the new PAC aggregate
 * 2. Resolve operator types to ensure the plan is valid
 *
 * Transformation Examples:
 * - SUM(l_extendedprice) -> pac_sum(hash(c_custkey), l_extendedprice)
 * - AVG(l_quantity) -> pac_avg(hash(c_custkey), l_quantity)
 * - COUNT(*) -> pac_count(hash(c_custkey), 1)
 * - COUNT(DISTINCT l_orderkey) -> pac_count(hash(c_custkey), l_orderkey) with DISTINCT flag
 *
 * Nested Aggregate Handling (IMPORTANT):
 * This function is only called on aggregates that were filtered by the calling code to have
 * PU or FK-linked tables in their subtree. The filtering logic determines which aggregates get transformed:
 *
 * - If we have 2 aggregates stacked on top of each other:
 *   * Inner aggregate WITH PU/FK-linked tables -> Gets transformed (this function is called)
 *   * Outer aggregate WITHOUT PU/FK-linked tables -> NOT transformed (this function not called)
 *   * Outer aggregate WITH PU/FK-linked tables -> Also gets transformed (this function called separately)
 *
 * Example 1 - Both aggregates have PU tables (user's TPC-H Q17 example):
 *   SELECT sum(l_extendedprice) / 7.0 FROM lineitem WHERE l_quantity < (SELECT avg(l_quantity) FROM lineitem WHERE ...)
 *   - Inner: Has lineitem -> pac_avg(hash(rowid), l_quantity)
 *   - Outer: Also has lineitem -> pac_sum(hash(rowid), l_extendedprice)
 *
 * Example 2 - Only inner has PU tables:
 *   SELECT sum(inner_result) FROM (SELECT sum(customer_col) FROM customer) AS subq
 *   - Inner: Has customer -> pac_sum(hash(c_custkey), customer_col)
 *   - Outer: No PU tables -> Regular sum(inner_result), NOT transformed
 */
void ModifyAggregatesWithPacFunctions(OptimizerExtensionInput &input, LogicalAggregate *agg,
                                      unique_ptr<Expression> &hash_input_expr, unique_ptr<LogicalOperator> &plan,
                                      double correction) {
#if PAC_DEBUG
	PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Processing aggregate with " +
	                std::to_string(agg->expressions.size()) + " expressions");

	// Debug: Print hash input expression details
	PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Hash input expression: " + hash_input_expr->ToString());
	PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Hash input type: " + hash_input_expr->return_type.ToString());

	// Debug: Print all column references in the hash expression
	ExpressionIterator::EnumerateExpression(hash_input_expr, [&](Expression &expr) {
		if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = expr.Cast<BoundColumnRefExpression>();
			PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Hash expr references column [" +
			                std::to_string(col_ref.binding.table_index) + "." +
			                std::to_string(col_ref.binding.column_index) + "] type=" + col_ref.return_type.ToString());
		}
	});

	// Debug: Print aggregate's groups
	PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Aggregate has " + std::to_string(agg->groups.size()) +
	                " groups:");
	for (idx_t i = 0; i < agg->groups.size(); i++) {
		PAC_DEBUG_PRINT("  Group " + std::to_string(i) + ": " + agg->groups[i]->ToString());
	}

	// Debug: Print aggregate's group_index and aggregate_index
	PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: group_index=" + std::to_string(agg->group_index) +
	                ", aggregate_index=" + std::to_string(agg->aggregate_index));

	// Debug: Print child operator info
	if (!agg->children.empty()) {
		PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Child operator type=" +
		                std::to_string(static_cast<int>(agg->children[0]->type)));
		if (agg->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &child_get = agg->children[0]->Cast<LogicalGet>();
			PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Child is GET with table_index=" +
			                std::to_string(child_get.table_index) +
			                ", columns=" + std::to_string(child_get.GetColumnIds().size()));
		}
	}
#endif

	// Pre-check: verify ALL aggregate expressions are supported BEFORE modifying any.
	// This prevents partial plan modification if an unsupported aggregate (e.g., string_agg) is found mid-loop.
	for (idx_t i = 0; i < agg->expressions.size(); i++) {
		if (agg->expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			throw NotImplementedException("Not found expected aggregate expression in PAC compiler");
		}
		auto &check_aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
		GetPacAggregateFunctionName(check_aggr.function.name); // throws if unsupported
	}

	// === DISTINCT pre-aggregation ===
	// When ALL aggregates are DISTINCT on the same column, use DuckDB's native GROUP BY
	// for deduplication: an inner aggregate groups by the distinct column and ORs the
	// privacy-unit hashes with bit_or, then an outer pac_count/pac_sum operates on the result.
	{
		bool has_any_distinct = false;
		bool has_any_non_distinct = false;
		bool all_same_col = true;
		unique_ptr<Expression> common_distinct_value;

		for (idx_t i = 0; i < agg->expressions.size(); i++) {
			auto &check = agg->expressions[i]->Cast<BoundAggregateExpression>();
			if (check.IsDistinct() && !check.children.empty()) {
				has_any_distinct = true;
				if (!common_distinct_value) {
					common_distinct_value = check.children[0]->Copy();
				} else if (check.children[0]->ToString() != common_distinct_value->ToString()) {
					all_same_col = false;
				}
			} else {
				has_any_non_distinct = true;
			}
		}

		if (has_any_distinct && !has_any_non_distinct && all_same_col && common_distinct_value) {
			InsertDistinctPreAggregation(input, agg, hash_input_expr, common_distinct_value);
			return;
		}

		// Multi-branch case: mixed DISTINCT + non-DISTINCT, or multiple distinct columns
		if (has_any_distinct && (has_any_non_distinct || !all_same_col)) {
			InsertMultiBranchPreAggregation(input, plan, agg, hash_input_expr);
			return;
		}
	}

	// Process each aggregate expression
	for (idx_t i = 0; i < agg->expressions.size(); i++) {
		if (agg->expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			throw NotImplementedException("Not found expected aggregate expression in PAC compiler");
		}

		auto &old_aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
		string function_name = old_aggr.function.name;

#if PAC_DEBUG
		PAC_DEBUG_PRINT("ModifyAggregatesWithPacFunctions: Transforming " + function_name + " to PAC function");
#endif

		// Extract the original aggregate's value child expression
		unique_ptr<Expression> value_child;
		if (old_aggr.children.empty()) {
			if (function_name == "count_star" || function_name == "count") {
				value_child = make_uniq_base<Expression, BoundConstantExpression>(Value::BIGINT(1));
			} else {
				throw InternalException("PAC compiler: expected aggregate to have a child expression");
			}
		} else {
			value_child = old_aggr.children[0]->Copy();
		}

		string pac_function_name = GetPacAggregateFunctionName(function_name, &input.context);
		unique_ptr<Expression> correction_expr;
		if (correction != 1.0) {
			correction_expr = make_uniq_base<Expression, BoundConstantExpression>(Value::DOUBLE(correction));
		}
		agg->expressions[i] = BindPacAggregate(input, pac_function_name, hash_input_expr->Copy(),
		                                       std::move(value_child), std::move(correction_expr));
	}

	agg->ResolveOperatorTypes();
}

// ============================================================================
// Clip aggregate rewrite: pac_noised_* → pac_noised_clip_* / pac_clip_*
// with optional lower aggregate insertion for per-PU pre-aggregation
// ============================================================================

// Map pac function names to their clip variants
static string GetClipVariant(const string &name) {
	if (name == "pac_noised_sum") {
		return "pac_noised_clip_sum";
	}
	if (name == "pac_noised_count") {
		return "pac_noised_clip_count";
	}
	if (name == "pac_noised_min") {
		return "pac_noised_clip_min";
	}
	if (name == "pac_noised_max") {
		return "pac_noised_clip_max";
	}
	if (name == "pac_sum") {
		return "pac_clip_sum";
	}
	if (name == "pac_count") {
		return "pac_clip_count";
	}
	if (name == "pac_min") {
		return "pac_clip_min";
	}
	if (name == "pac_max") {
		return "pac_clip_max";
	}
	return ""; // not a pac aggregate
}

// Map pac function names to their original DuckDB aggregate
static string GetOriginalAggregate(const string &name) {
	if (name == "pac_noised_sum" || name == "pac_sum") {
		return "sum";
	}
	if (name == "pac_noised_count" || name == "pac_count") {
		return "count";
	}
	if (name == "pac_noised_min" || name == "pac_min") {
		return "min";
	}
	if (name == "pac_noised_max" || name == "pac_max") {
		return "max";
	}
	return "";
}

// Is this a noised (scalar) variant? If so, top aggregate uses pac_noised_clip_*
static bool IsNoisedVariant(const string &name) {
	return name.find("pac_noised_") == 0;
}

// Bind a plain DuckDB aggregate function (sum, count, min, max)
static unique_ptr<Expression> BindPlainAggregate(OptimizerExtensionInput &input, const string &func_name,
                                                 vector<unique_ptr<Expression>> children) {
	FunctionBinder function_binder(input.context);
	ErrorData error;
	vector<LogicalType> arg_types;
	for (auto &child : children) {
		arg_types.push_back(child->return_type);
	}
	auto &entry = Catalog::GetSystemCatalog(input.context)
	                  .GetEntry<AggregateFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, func_name);
	auto best = function_binder.BindFunction(entry.name, entry.functions, arg_types, error);
	if (!best.IsValid()) {
		throw InternalException("PAC clip rewrite: failed to bind " + func_name);
	}
	auto func = entry.functions.GetFunctionByOffset(best.GetIndex());
	return function_binder.BindAggregateFunction(func, std::move(children), nullptr, AggregateType::NON_DISTINCT);
}

// Check if an aggregate contains pac_noised_* or pac_* (counters) expressions
static bool IsPacAggregate(LogicalAggregate *agg) {
	for (auto &expr : agg->expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		if (!GetClipVariant(aggr.function.name).empty()) {
			return true;
		}
	}
	return false;
}

void RewriteClipAggregates(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                           const PACCompatibilityResult &check, const vector<string> &privacy_units) {
	// Find all aggregate nodes
	vector<LogicalAggregate *> all_aggregates;
	FindAllAggregates(plan, all_aggregates);

	for (auto *agg : all_aggregates) {
		if (!IsPacAggregate(agg)) {
			continue;
		}

		// Check Q13 exception: does the child aggregate already group by PU key?
		bool child_groups_by_pu = false;
		for (auto &child : agg->children) {
			auto *inner_agg = FindFirstChildAggregate(child.get());
			if (inner_agg && AggregateGroupsByPUKey(inner_agg, check, privacy_units)) {
				child_groups_by_pu = true;
				break;
			}
		}

		if (child_groups_by_pu) {
			// Q13 exception: just rename pac_noised_* → pac_noised_clip_* in place
			for (idx_t i = 0; i < agg->expressions.size(); i++) {
				if (agg->expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
					continue;
				}
				auto &aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
				string clip_name = GetClipVariant(aggr.function.name);
				if (clip_name.empty()) {
					continue;
				}
				// Rebind with the clip variant name
				vector<unique_ptr<Expression>> children;
				for (auto &child : aggr.children) {
					children.push_back(child->Copy());
				}
				agg->expressions[i] = RebindAggregate(input.context, clip_name, std::move(children), false);
			}
			agg->ResolveOperatorTypes();
			continue;
		}

		// Normal path: insert lower aggregate
		auto &binder = input.optimizer.binder;
		idx_t lower_group_index = binder.GenerateTableIndex();
		idx_t lower_agg_index = binder.GenerateTableIndex();

		// Identify the PU hash expression from the first pac aggregate's first child (hash arg)
		unique_ptr<Expression> pu_hash_expr;
		for (auto &expr : agg->expressions) {
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			auto &aggr = expr->Cast<BoundAggregateExpression>();
			if (!GetClipVariant(aggr.function.name).empty() && !aggr.children.empty()) {
				pu_hash_expr = aggr.children[0]->Copy();
				break;
			}
		}
		if (!pu_hash_expr) {
			continue; // shouldn't happen
		}

		idx_t num_original_groups = agg->groups.size();

		// Build lower aggregate expressions (plain DuckDB aggregates)
		vector<unique_ptr<Expression>> lower_expressions;
		for (idx_t i = 0; i < agg->expressions.size(); i++) {
			auto &aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
			string orig_name = GetOriginalAggregate(aggr.function.name);
			if (orig_name.empty()) {
				throw InternalException("PAC clip rewrite: unexpected aggregate " + aggr.function.name);
			}

			vector<unique_ptr<Expression>> plain_children;
			if (orig_name == "count" && (aggr.children.size() <= 1)) {
				// pac_noised_count(hash) or pac_count(hash) → count_star()
				// pac_noised_count(hash, col) → count(col) — but children[1] might be constant 1
				if (aggr.children.size() >= 2) {
					auto &val_child = aggr.children[1];
					// Check if it's a constant 1 (from count_star rewrite)
					if (val_child->type == ExpressionType::VALUE_CONSTANT) {
						auto &const_expr = val_child->Cast<BoundConstantExpression>();
						if (const_expr.value.IsNull() || const_expr.value == Value::BIGINT(1)) {
							// count_star — no children
						} else {
							plain_children.push_back(val_child->Copy());
						}
					} else {
						plain_children.push_back(val_child->Copy());
					}
				}
				lower_expressions.push_back(BindPlainAggregate(input, "count_star", std::move(plain_children)));
			} else if (orig_name == "count" && aggr.children.size() > 1) {
				// count with column reference
				plain_children.push_back(aggr.children[1]->Copy());
				lower_expressions.push_back(BindPlainAggregate(input, "count", std::move(plain_children)));
			} else {
				// sum, min, max — extract the value child (children[1])
				if (aggr.children.size() >= 2) {
					plain_children.push_back(aggr.children[1]->Copy());
				}
				lower_expressions.push_back(BindPlainAggregate(input, orig_name, std::move(plain_children)));
			}
		}

		// Create lower aggregate node
		auto lower_agg = make_uniq<LogicalAggregate>(lower_group_index, lower_agg_index, std::move(lower_expressions));

		// Copy original groups + add PU hash as extra group
		for (auto &g : agg->groups) {
			lower_agg->groups.push_back(g->Copy());
		}
		lower_agg->groups.push_back(pu_hash_expr->Copy());

		// Steal top's child → lower's child
		lower_agg->children.push_back(std::move(agg->children[0]));
		lower_agg->ResolveOperatorTypes();

		// Rewrite top aggregate's groups to reference lower's group output
		for (idx_t i = 0; i < num_original_groups; i++) {
			auto gtype = agg->groups[i]->return_type;
			agg->groups[i] = make_uniq<BoundColumnRefExpression>(gtype, ColumnBinding(lower_group_index, i));
		}

		// PU hash ref from lower's group output
		auto pu_hash_ref = make_uniq<BoundColumnRefExpression>(pu_hash_expr->return_type,
		                                                       ColumnBinding(lower_group_index, num_original_groups));

		// Rewrite top aggregate's expressions to clip variants
		for (idx_t i = 0; i < agg->expressions.size(); i++) {
			auto &aggr = agg->expressions[i]->Cast<BoundAggregateExpression>();
			string pac_name = aggr.function.name;
			bool noised = IsNoisedVariant(pac_name);
			string orig = GetOriginalAggregate(pac_name);

			// Reference to lower aggregate's result
			auto lower_type = lower_agg->types[num_original_groups + 1 + i];
			unique_ptr<Expression> lower_ref =
			    make_uniq<BoundColumnRefExpression>(lower_type, ColumnBinding(lower_agg_index, i));

			// count → sumcount (preserves BIGINT return type), others → clip variant
			string clip_func;
			if (orig == "count") {
				clip_func = noised ? "pac_noised_clip_sumcount" : "pac_clip_sum";
			} else {
				clip_func = GetClipVariant(pac_name);
			}

			agg->expressions[i] =
			    BindPacAggregate(input, clip_func, pu_hash_ref->Copy(), std::move(lower_ref), nullptr);
		}

		// Set lower as top's child
		agg->children[0] = std::move(lower_agg);
		agg->ResolveOperatorTypes();
	}
}

} // namespace duckdb
