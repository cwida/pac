//
// Created by ila on 1/6/26.
//

#include "query_processing/pac_plan_traversal.hpp"

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "pac_debug.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> *FindPrivacyUnitGetNode(unique_ptr<LogicalOperator> &plan, const string &pu_table_name) {
	unique_ptr<LogicalOperator> *found_ptr = nullptr;
	if (!plan) {
		return nullptr;
	}

	vector<unique_ptr<LogicalOperator> *> stack;
	stack.push_back(&plan);
	while (!stack.empty()) {
		auto cur_ptr = stack.back();
		stack.pop_back();
		auto &cur = *cur_ptr;
		if (!cur) {
			continue;
		}
		if (cur->type == LogicalOperatorType::LOGICAL_GET) {
			// If a specific table name is provided, only match that table
			if (!pu_table_name.empty()) {
				auto &get = cur->Cast<LogicalGet>();
				auto tblptr = get.GetTable();
				if (tblptr && tblptr->name == pu_table_name) {
					found_ptr = cur_ptr;
					break;
				}
			} else {
				// No specific table name provided, return the first LogicalGet
				found_ptr = cur_ptr;
				break;
			}
		}
		for (auto &c : cur->children) {
			stack.push_back(&c);
		}
	}

	if (!found_ptr) {
		if (!pu_table_name.empty()) {
			throw InternalException("PAC Compiler: could not find LogicalGet node for table " + pu_table_name +
			                        " in plan");
		} else {
			throw InternalException("PAC Compiler: could not find LogicalGet node in plan");
		}
	}

	return found_ptr;
}

// Find a LogicalGet node for a specific table within a given subtree
LogicalGet *FindTableScanInSubtree(LogicalOperator *subtree, const string &table_name) {
	if (!subtree) {
		return nullptr;
	}

	if (subtree->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = subtree->Cast<LogicalGet>();
		auto tblptr = get.GetTable();
		if (tblptr && tblptr->name == table_name) {
			return &get;
		}
	}

	for (auto &child : subtree->children) {
		if (auto *found = FindTableScanInSubtree(child.get(), table_name)) {
			return found;
		}
	}

	return nullptr;
}

LogicalAggregate *FindTopAggregate(unique_ptr<LogicalOperator> &op) {
	if (!op) {
		throw InternalException("PAC Compiler: could not find LogicalAggregate node in plan");
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return &op->Cast<LogicalAggregate>();
	}
	for (auto &child : op->children) {
		if (auto *agg = FindTopAggregate(child)) {
			return agg;
		}
	}
	throw InternalException("PAC Compiler: could not find LogicalAggregate node in plan");
}

// Find all LogicalAggregate nodes in the plan tree
void FindAllAggregates(unique_ptr<LogicalOperator> &op, vector<LogicalAggregate *> &aggregates) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		aggregates.push_back(&op->Cast<LogicalAggregate>());
	}
	for (auto &child : op->children) {
		FindAllAggregates(child, aggregates);
	}
}

LogicalProjection *FindParentProjection(unique_ptr<LogicalOperator> &root, LogicalOperator *target_child) {
	if (!root) {
		return nullptr;
	}
	for (auto &child : root->children) {
		if (child.get() == target_child && root->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			return &root->Cast<LogicalProjection>();
		}
		if (auto *proj = FindParentProjection(child, target_child)) {
			return proj;
		}
	}
	return nullptr;
}

unique_ptr<LogicalOperator> *FindNodeRefByTable(unique_ptr<LogicalOperator> *root, const string &table_name,
                                                LogicalOperator **parent_out, idx_t *child_idx_out) {
	if (!root || !root->get()) {
		return nullptr;
	}

	struct StackEntry {
		unique_ptr<LogicalOperator> *ptr;
		LogicalOperator *parent;
		idx_t child_idx;
	};

	vector<StackEntry> stack;
	stack.push_back({root, nullptr, 0});

	while (!stack.empty()) {
		auto entry = stack.back();
		stack.pop_back();

		auto &cur = *entry.ptr;
		if (!cur) {
			continue;
		}

		if (cur->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = cur->Cast<LogicalGet>();
			auto tblptr = get.GetTable();
			if (tblptr && tblptr->name == table_name) {
				if (parent_out) {
					*parent_out = entry.parent;
				}
				if (child_idx_out) {
					*child_idx_out = entry.child_idx;
				}
				return entry.ptr;
			}
		}

		for (idx_t i = 0; i < cur->children.size(); i++) {
			stack.push_back({&cur->children[i], cur.get(), i});
		}
	}

	return nullptr;
}

// Check if an operator has any leaf data source nodes (base table scans or CTE refs) in its subtree.
// IMPORTANT: This function stops at aggregates, because aggregates consume base table
// bindings and produce new output bindings. Base tables behind an aggregate are not
// directly accessible from operators above the aggregate.
bool HasBaseTableInSubtree(LogicalOperator *op) {
	if (!op) {
		return false;
	}

	// Check if this is a base table scan
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return true;
	}

	// CTE refs are leaf nodes that produce bindings from a materialized CTE,
	// functionally equivalent to a base table scan for this check
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return true;
	}

	// Don't traverse through aggregates - they consume base table bindings
	// and produce new output bindings
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}

	// Recursively check children
	for (auto &child : op->children) {
		if (HasBaseTableInSubtree(child.get())) {
			return true;
		}
	}

	return false;
}

// Check if an operator has a specific table (by name) in its subtree.
// Returns true if there's a LogicalGet for the given table name in the subtree.
bool HasTableInSubtree(LogicalOperator *op, const string &table_name) {
	if (!op) {
		return false;
	}

	// Check if this is a LogicalGet for the target table
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto tblptr = get.GetTable();
		if (tblptr && tblptr->name == table_name) {
			return true;
		}
	}

	// Recursively check children
	for (auto &child : op->children) {
		if (HasTableInSubtree(child.get(), table_name)) {
			return true;
		}
	}

	return false;
}

// ---- CTE-aware helpers ----

// Collect all table names referenced by LOGICAL_GET nodes in a subtree.
static void CollectTableNamesInSubtree(LogicalOperator *op, std::unordered_set<string> &table_names) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto tblptr = get.GetTable();
		if (tblptr) {
			table_names.insert(tblptr->name);
		}
	}
	for (auto &child : op->children) {
		CollectTableNamesInSubtree(child.get(), table_names);
	}
}

// Build a map from cte_index -> set of base table names that the CTE definition references.
// Also resolves transitive CTE references (CTE A references CTE B's tables).
void BuildCTETableMap(LogicalOperator *op, CTETableMap &cte_map) {
	if (!op) {
		return;
	}

	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		idx_t cte_idx = cte.table_index;

		// children[0] = CTE definition, children[1] = consumer
		if (!cte.children.empty()) {
			std::unordered_set<string> tables;
			CollectTableNamesInSubtree(cte.children[0].get(), tables);
			cte_map[cte_idx] = std::move(tables);
		}
	}

	for (auto &child : op->children) {
		BuildCTETableMap(child.get(), cte_map);
	}
}

// Merge tables from referenced CTEs into a target CTE's table set.
// Walks a subtree looking for CTE_REF nodes and merges their resolved tables.
static void MergeCTERefsIntoSet(LogicalOperator *op, idx_t target_cte_idx, CTETableMap &cte_map) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op->Cast<LogicalCTERef>();
		auto it = cte_map.find(ref.cte_index);
		if (it != cte_map.end()) {
			cte_map[target_cte_idx].insert(it->second.begin(), it->second.end());
		}
	}
	for (auto &child : op->children) {
		MergeCTERefsIntoSet(child.get(), target_cte_idx, cte_map);
	}
}

// Resolve transitive CTE references. Must be called after BuildCTETableMap.
// For each MATERIALIZED_CTE, merges tables from any CTE_REF nodes in its definition.
static void ResolveCTERefsInDefinitions(LogicalOperator *op, CTETableMap &cte_map) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		if (!cte.children.empty()) {
			MergeCTERefsIntoSet(cte.children[0].get(), cte.table_index, cte_map);
		}
	}
	for (auto &child : op->children) {
		ResolveCTERefsInDefinitions(child.get(), cte_map);
	}
}

// Fully build and resolve CTE table map from a plan root.
// First collects direct table names per CTE, then resolves transitive CTE references.
CTETableMap BuildAndResolveCTETableMap(LogicalOperator *plan_root) {
	CTETableMap cte_map;
	BuildCTETableMap(plan_root, cte_map);
	// Resolve transitive refs: CTEs that reference other CTEs get their tables merged
	ResolveCTERefsInDefinitions(plan_root, cte_map);

#if PAC_DEBUG
	if (!cte_map.empty()) {
		PAC_DEBUG_PRINT("CTE table map (" + std::to_string(cte_map.size()) + " CTEs):");
		for (auto &kv : cte_map) {
			string tables_str;
			for (auto &t : kv.second) {
				if (!tables_str.empty())
					tables_str += ", ";
				tables_str += t;
			}
			PAC_DEBUG_PRINT("  CTE index " + std::to_string(kv.first) + ": {" + tables_str + "}");
		}
	}
#endif

	return cte_map;
}

// CTE-aware version of HasTableInSubtree.
// Follows CTE_REF nodes through the cte_map to check if the referenced CTE
// transitively contains the target table.
bool HasTableInSubtreeCTE(LogicalOperator *op, const string &table_name, const CTETableMap &cte_map) {
	if (!op) {
		return false;
	}

	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto tblptr = get.GetTable();
		if (tblptr && tblptr->name == table_name) {
			return true;
		}
	}

	// Follow CTE references through the map
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op->Cast<LogicalCTERef>();
		auto it = cte_map.find(ref.cte_index);
		if (it != cte_map.end() && it->second.count(table_name) > 0) {
			return true;
		}
	}

	for (auto &child : op->children) {
		if (HasTableInSubtreeCTE(child.get(), table_name, cte_map)) {
			return true;
		}
	}

	return false;
}

// Find all LogicalGet nodes for a specific table name in the plan tree.
void FindAllNodesByTable(unique_ptr<LogicalOperator> *root, const string &table_name,
                         vector<unique_ptr<LogicalOperator> *> &results) {
	if (!root || !root->get()) {
		return;
	}

	auto &cur = *root;

	// Check if this is a LogicalGet for the target table
	if (cur->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = cur->Cast<LogicalGet>();
		auto tblptr = get.GetTable();
		if (tblptr && tblptr->name == table_name) {
			results.push_back(root);
		}
	}

	// Recursively check children
	for (auto &child : cur->children) {
		FindAllNodesByTable(&child, table_name, results);
	}
}

// Check if an operator has a LogicalGet with a specific table index in its subtree.
bool HasTableIndexInSubtree(LogicalOperator *op, idx_t table_index) {
	if (!op) {
		return false;
	}

	// Check if this is a LogicalGet with the target table index
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.table_index == table_index) {
			return true;
		}
	}

	// Recursively check children
	for (auto &child : op->children) {
		if (HasTableIndexInSubtree(child.get(), table_index)) {
			return true;
		}
	}

	return false;
}

// Find all LogicalGet nodes with a specific table index in the plan tree.
void FindAllNodesByTableIndex(unique_ptr<LogicalOperator> *root, idx_t table_index,
                              vector<unique_ptr<LogicalOperator> *> &results) {
	if (!root || !root->get()) {
		return;
	}

	auto &cur = *root;

	// Check if this is a LogicalGet with the target table index
	if (cur->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = cur->Cast<LogicalGet>();
		if (get.table_index == table_index) {
			results.push_back(root);
		}
	}

	// Recursively check children
	for (auto &child : cur->children) {
		FindAllNodesByTableIndex(&child, table_index, results);
	}
}

// Filter aggregates to only those that have specified tables in their subtree
// AND have base tables in their DIRECT children (not through nested aggregates).
// This filters out outer aggregates that only depend on inner aggregate results.
vector<LogicalAggregate *> FilterTargetAggregates(const vector<LogicalAggregate *> &all_aggregates,
                                                  const vector<string> &target_table_names,
                                                  const CTETableMap &cte_map) {
	vector<LogicalAggregate *> target_aggregates;

	for (auto *agg : all_aggregates) {
		// Check if this aggregate has at least one target table in its subtree
		// Use CTE-aware version if we have a CTE map
		bool has_target_table = false;
		for (auto &table_name : target_table_names) {
			bool found =
			    cte_map.empty() ? HasTableInSubtree(agg, table_name) : HasTableInSubtreeCTE(agg, table_name, cte_map);
			if (found) {
				has_target_table = true;
				break;
			}
		}

		if (!has_target_table) {
			continue;
		}

		// Check if this aggregate has base tables in its DIRECT children (not nested aggregates)
		// HasBaseTableInSubtree already recognizes CTE_REF as a valid leaf data source
		bool has_direct_base_table = false;
		for (auto &child : agg->children) {
			// Skip if the child is another aggregate
			if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				continue;
			}
			if (HasBaseTableInSubtree(child.get())) {
				has_direct_base_table = true;
				break;
			}
		}

		if (has_direct_base_table) {
			target_aggregates.push_back(agg);
		}
	}

	return target_aggregates;
}

// Check if a target node is inside a DELIM_JOIN's subquery branch (children[1]).
// This is important for correlated subqueries where nodes in the subquery branch
// cannot directly access tables from the outer query.
bool IsInDelimJoinSubqueryBranch(unique_ptr<LogicalOperator> *root, LogicalOperator *target_node) {
	if (!root || !root->get() || !target_node) {
		return false;
	}

	auto &cur = *root;

	// If this is a DELIM_JOIN, check if target is in children[1] (subquery side)
	if (cur->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (cur->children.size() >= 2) {
			// Check if target_node is in the subquery branch (children[1])
			std::function<bool(LogicalOperator *)> find_target = [&](LogicalOperator *op) -> bool {
				if (op == target_node) {
					return true;
				}
				for (auto &child : op->children) {
					if (find_target(child.get())) {
						return true;
					}
				}
				return false;
			};

			if (find_target(cur->children[1].get())) {
				return true;
			}
		}
	}

	// Recursively check children
	for (auto &child : cur->children) {
		if (IsInDelimJoinSubqueryBranch(&child, target_node)) {
			return true;
		}
	}

	return false;
}

// Check if a table's columns are accessible from the given starting operator.
// Returns false if the table is in the right child of a MARK/SEMI/ANTI join,
// because those join types don't output right-side columns (only the boolean mark).
bool AreTableColumnsAccessible(LogicalOperator *from_op, idx_t table_index) {
	if (!from_op) {
		return false;
	}

	// Helper to check if table_index is in a subtree
	std::function<bool(LogicalOperator *)> has_table_in_subtree = [&](LogicalOperator *op) -> bool {
		if (!op) {
			return false;
		}
		if (op->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op->Cast<LogicalGet>();
			if (get.table_index == table_index) {
				return true;
			}
		}
		for (auto &child : op->children) {
			if (has_table_in_subtree(child.get())) {
				return true;
			}
		}
		return false;
	};

	// Recursive helper that returns:
	// - true if table is accessible (found in an accessible path)
	// - false if table is not found or blocked by MARK/SEMI/ANTI join
	std::function<bool(LogicalOperator *)> check_accessible = [&](LogicalOperator *op) -> bool {
		if (!op) {
			return false;
		}

		// If this is the target table, it's accessible from here
		if (op->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op->Cast<LogicalGet>();
			if (get.table_index == table_index) {
				return true;
			}
		}

		// Check for join types that block right-side column access
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			auto &join = op->Cast<LogicalJoin>();

			// MARK, SEMI, and ANTI joins don't output right-side columns
			if (join.join_type == JoinType::MARK || join.join_type == JoinType::SEMI ||
			    join.join_type == JoinType::ANTI || join.join_type == JoinType::RIGHT_SEMI ||
			    join.join_type == JoinType::RIGHT_ANTI) {

				// Check if table is in the right child (blocked side)
				if (op->children.size() >= 2 && has_table_in_subtree(op->children[1].get())) {
					// Table is in the right child of a MARK/SEMI/ANTI join - columns NOT accessible
					return false;
				}

				// Check left child (accessible side)
				if (!op->children.empty() && check_accessible(op->children[0].get())) {
					return true;
				}

				return false;
			}
		}

		// For DELIM_JOIN, accessibility depends on the join type:
		// - RIGHT_SEMI/RIGHT_ANTI: only RIGHT child columns are accessible (left is filtered out)
		// - SEMI/ANTI: only LEFT child columns are accessible (right is filtered out)
		// - INNER/LEFT/etc: left child columns are accessible (right side is correlated subquery)
		if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			auto &delim_join = op->Cast<LogicalJoin>();

			// For RIGHT_SEMI/RIGHT_ANTI, only right child columns flow through
			if (delim_join.join_type == JoinType::RIGHT_SEMI || delim_join.join_type == JoinType::RIGHT_ANTI) {
				// Table in left child is NOT accessible (filtered out by RIGHT_SEMI/RIGHT_ANTI)
				if (!op->children.empty() && has_table_in_subtree(op->children[0].get())) {
					return false;
				}
				// Check right child (accessible side for RIGHT_SEMI/RIGHT_ANTI)
				if (op->children.size() >= 2 && check_accessible(op->children[1].get())) {
					return true;
				}
				return false;
			}

			// For SEMI/ANTI, only left child columns flow through
			if (delim_join.join_type == JoinType::SEMI || delim_join.join_type == JoinType::ANTI) {
				// Table in right child is NOT accessible
				if (op->children.size() >= 2 && has_table_in_subtree(op->children[1].get())) {
					return false;
				}
				// Check left child (accessible side for SEMI/ANTI)
				if (!op->children.empty() && check_accessible(op->children[0].get())) {
					return true;
				}
				return false;
			}

			// For other join types (INNER, LEFT, etc.), right side is correlated subquery
			// and left child columns are accessible
			if (op->children.size() >= 2 && has_table_in_subtree(op->children[1].get())) {
				// Table is in the subquery branch - columns NOT accessible from above
				return false;
			}
			// Check left child
			if (!op->children.empty() && check_accessible(op->children[0].get())) {
				return true;
			}
			return false;
		}

		// For all other operators, check all children
		for (auto &child : op->children) {
			if (check_accessible(child.get())) {
				return true;
			}
		}

		return false;
	};

	return check_accessible(from_op);
}

// Helper function to get table name and column name from a column binding
// Returns pair<table_name, column_name>, empty strings if not found
static std::pair<string, string> GetColumnInfoFromBinding(LogicalOperator *subtree, const ColumnBinding &binding) {
	if (!subtree) {
		return {"", ""};
	}

	// Find the LogicalGet with matching table_index
	std::function<LogicalGet *(LogicalOperator *)> find_get = [&](LogicalOperator *op) -> LogicalGet * {
		if (!op) {
			return nullptr;
		}
		if (op->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op->Cast<LogicalGet>();
			if (get.table_index == binding.table_index) {
				return &get;
			}
		}
		for (auto &child : op->children) {
			if (auto *found = find_get(child.get())) {
				return found;
			}
		}
		return nullptr;
	};

	auto *get = find_get(subtree);
	if (!get) {
		return {"", ""};
	}

	auto table_entry = get->GetTable();
	if (!table_entry) {
		return {"", ""};
	}

	string table_name = table_entry->name;

	// Get column name from the binding
	const auto &column_ids = get->GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return {table_name, ""};
	}

	string col_name = get->GetColumnName(column_ids[binding.column_index]);
	return {table_name, col_name};
}

// Check if an aggregate's GROUP BY keys contain a protected column (PU PK, LINK FK, or metadata PROTECTED).
// This is used to detect the edge case where inner aggregate groups by PU key.
bool AggregateGroupsByPUKey(LogicalAggregate *agg, const PACCompatibilityResult &check,
                            const vector<string> &privacy_units) {
	if (!agg || agg->groups.empty()) {
		return false;
	}

	// Collect all column references from the GROUP BY expressions
	vector<ColumnBinding> group_bindings;
	for (auto &group_expr : agg->groups) {
		if (!group_expr) {
			continue;
		}
		ExpressionIterator::EnumerateExpression(group_expr, [&](Expression &expr) {
			if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expr.Cast<BoundColumnRefExpression>();
				group_bindings.push_back(col_ref.binding);
			}
		});
	}

	if (group_bindings.empty()) {
		return false;
	}

	// For each group binding, check against the unified protected_columns set
	for (auto &binding : group_bindings) {
		auto col_info = GetColumnInfoFromBinding(agg, binding);
		auto &table_name = col_info.first;
		auto &col_name = col_info.second;
#if PAC_DEBUG
		PAC_DEBUG_PRINT("AggregateGroupsByPUKey: binding [" + std::to_string(binding.table_index) + "." +
		                std::to_string(binding.column_index) + "] -> table='" + table_name + "' col='" + col_name +
		                "'");
#endif
		if (table_name.empty() || col_name.empty()) {
			continue;
		}

		string table_lower = StringUtil::Lower(table_name);
		string col_lower = StringUtil::Lower(col_name);

		auto it = check.protected_columns.find(table_lower);
#if PAC_DEBUG
		if (it != check.protected_columns.end()) {
			string cols_str;
			for (auto &c : it->second) {
				cols_str += c + ", ";
			}
			PAC_DEBUG_PRINT("AggregateGroupsByPUKey: protected_columns['" + table_lower + "'] = {" + cols_str + "}");
		} else {
			PAC_DEBUG_PRINT("AggregateGroupsByPUKey: no protected_columns entry for '" + table_lower + "'");
		}
#endif
		if (it != check.protected_columns.end() && it->second.count(col_lower) > 0) {
			return true;
		}
	}

	return false;
}

// Extended version of FilterTargetAggregates that handles the edge case where inner aggregate
// groups by PU key (PAC key/PK of Privacy Unit or FK referencing it).
// In this case, the inner aggregate is skipped and the outer aggregate is noised instead.
vector<LogicalAggregate *> FilterTargetAggregatesWithPUKeyCheck(const vector<LogicalAggregate *> &all_aggregates,
                                                                const vector<string> &target_table_names,
                                                                const PACCompatibilityResult &check,
                                                                const vector<string> &privacy_units,
                                                                const CTETableMap &cte_map) {
	vector<LogicalAggregate *> target_aggregates;

	// First, identify which aggregates have inner aggregates that group by PU key
	// For these, we want to skip the inner aggregate and include the outer aggregate.
	// Also detect CTE boundary patterns: when an outer aggregate reads target tables
	// only through CTE_SCAN, the CTE definition's aggregate handles PAC transformation.
	std::unordered_set<LogicalAggregate *> skip_aggregates;
	std::unordered_set<LogicalAggregate *> include_outer_aggregates;

	for (auto *agg : all_aggregates) {
		// Check if this aggregate has a child aggregate (nested aggregate — direct subtree)
		for (auto &child : agg->children) {
			LogicalAggregate *inner_agg = nullptr;

			// Traverse down to find the immediate child aggregate
			std::function<LogicalAggregate *(LogicalOperator *)> find_inner_agg =
			    [&](LogicalOperator *op) -> LogicalAggregate * {
				if (!op) {
					return nullptr;
				}
				if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
					return &op->Cast<LogicalAggregate>();
				}
				// Don't traverse through projections/filters that might just be wrapping
				for (auto &c : op->children) {
					if (auto *found = find_inner_agg(c.get())) {
						return found;
					}
				}
				return nullptr;
			};

			inner_agg = find_inner_agg(child.get());

			if (inner_agg) {
				// Check if the inner aggregate groups by PU key
				if (AggregateGroupsByPUKey(inner_agg, check, privacy_units)) {
					// Skip the inner aggregate, include the outer aggregate instead
					skip_aggregates.insert(inner_agg);
					include_outer_aggregates.insert(agg);
				}
			}
		}

		// CTE boundary pattern: check if this aggregate reaches target tables only
		// through CTE_SCAN (not direct scans). If so, find the CTE definition's aggregate
		// and decide which one to noise based on Q13 pattern logic.
		if (!cte_map.empty()) {
			bool has_direct_target = false;
			for (auto &table_name : target_table_names) {
				if (HasTableInSubtree(agg, table_name)) {
					has_direct_target = true;
					break;
				}
			}
			if (!has_direct_target) {
				// This aggregate only reaches target tables via CTE_SCAN.
				// Find the CTE definition's aggregate (the one with direct access).
				for (auto *other_agg : all_aggregates) {
					if (other_agg == agg) {
						continue;
					}
					bool other_has_direct = false;
					for (auto &table_name : target_table_names) {
						if (HasTableInSubtree(other_agg, table_name)) {
							other_has_direct = true;
							break;
						}
					}
					if (other_has_direct) {
						if (AggregateGroupsByPUKey(other_agg, check, privacy_units)) {
							// CTE definition groups by PU key → can't noise it.
							// Include outer aggregate instead (Q13 across CTE boundary).
							skip_aggregates.insert(other_agg);
							include_outer_aggregates.insert(agg);
						} else {
							// CTE definition doesn't group by PU key → noise it.
							// Skip this outer aggregate (it re-aggregates noised output).
							skip_aggregates.insert(agg);
						}
						break;
					}
				}
			}
		}
	}

	for (auto *agg : all_aggregates) {
		// Skip aggregates that are marked to be skipped (inner aggregates that group by PU key)
		if (skip_aggregates.count(agg) > 0) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("FilterTargetAggregatesWithPUKeyCheck: skipping aggregate (inner groups by PU key)");
#endif
			continue;
		}

		// Check if this aggregate should be included because its inner aggregate groups by PU key
		if (include_outer_aggregates.count(agg) > 0) {
			// Include this outer aggregate - it needs to be noised instead of the inner one
#if PAC_DEBUG
			PAC_DEBUG_PRINT("FilterTargetAggregatesWithPUKeyCheck: including outer aggregate (inner groups by PU key)");
#endif
			target_aggregates.push_back(agg);
			continue;
		}

		// Standard filtering logic: check if this aggregate has target tables in its subtree
		// Use CTE-aware version if we have a CTE map
		bool has_target_table = false;
		for (auto &table_name : target_table_names) {
			bool found =
			    cte_map.empty() ? HasTableInSubtree(agg, table_name) : HasTableInSubtreeCTE(agg, table_name, cte_map);
			if (found) {
				has_target_table = true;
				break;
			}
		}

		if (!has_target_table) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT(
			    "FilterTargetAggregatesWithPUKeyCheck: aggregate has no target tables in subtree, skipping");
#endif
			continue;
		}

		// Check if this aggregate has base tables in its DIRECT children (not nested aggregates)
		// HasBaseTableInSubtree already recognizes CTE_REF as a valid leaf data source
		bool has_direct_base_table = false;
		for (auto &child : agg->children) {
			// Skip if the child is another aggregate
			if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				continue;
			}
			if (HasBaseTableInSubtree(child.get())) {
				has_direct_base_table = true;
				break;
			}
		}

		if (has_direct_base_table) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("FilterTargetAggregatesWithPUKeyCheck: including aggregate (has target tables + direct "
			                "base table/CTE ref)");
#endif
			target_aggregates.push_back(agg);
		} else {
#if PAC_DEBUG
			PAC_DEBUG_PRINT(
			    "FilterTargetAggregatesWithPUKeyCheck: aggregate has target tables but no direct base table, skipping");
#endif
		}
	}

#if PAC_DEBUG
	PAC_DEBUG_PRINT("FilterTargetAggregatesWithPUKeyCheck: selected " + std::to_string(target_aggregates.size()) +
	                " of " + std::to_string(all_aggregates.size()) + " aggregates");
#endif

	return target_aggregates;
}

// Find the inner aggregate (child of target_agg) that groups by PU key.
// Returns the inner aggregate and the column binding of the PU key group column in its output.
LogicalAggregate *FindInnerAggregateWithPUKeyGroup(LogicalAggregate *target_agg, const PACCompatibilityResult &check,
                                                   const vector<string> &privacy_units, ColumnBinding &out_pk_binding) {
	if (!target_agg) {
		return nullptr;
	}

	// Find the inner aggregate in the target's subtree, also collecting the path of operators
	// between the outer aggregate and the inner aggregate (for binding propagation)
	vector<LogicalOperator *> path_to_inner;

	std::function<LogicalAggregate *(LogicalOperator *, vector<LogicalOperator *> &)> find_inner_agg =
	    [&](LogicalOperator *op, vector<LogicalOperator *> &path) -> LogicalAggregate * {
		if (!op) {
			return nullptr;
		}
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return &op->Cast<LogicalAggregate>();
		}
		path.push_back(op);
		for (auto &c : op->children) {
			if (auto *found = find_inner_agg(c.get(), path)) {
				return found;
			}
		}
		path.pop_back();
		return nullptr;
	};

	for (auto &child : target_agg->children) {
		path_to_inner.clear();
		LogicalAggregate *inner_agg = find_inner_agg(child.get(), path_to_inner);
		if (!inner_agg) {
			continue;
		}

		// Check if this inner aggregate groups by PU key
		if (!AggregateGroupsByPUKey(inner_agg, check, privacy_units)) {
			continue;
		}

		// Found an inner aggregate that groups by PU key
		// Now find which group column is the PU key and get its output binding
		for (idx_t group_idx = 0; group_idx < inner_agg->groups.size(); group_idx++) {
			auto &group_expr = inner_agg->groups[group_idx];
			if (!group_expr || group_expr->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			auto &col_ref = group_expr->Cast<BoundColumnRefExpression>();
			auto col_info = GetColumnInfoFromBinding(inner_agg, col_ref.binding);
			auto &table_name = col_info.first;
			auto &col_name = col_info.second;
			if (table_name.empty() || col_name.empty()) {
				continue;
			}

			string table_lower = StringUtil::Lower(table_name);
			string col_lower = StringUtil::Lower(col_name);

			// Check against the unified protected_columns set
			auto prot_it = check.protected_columns.find(table_lower);
			if (prot_it == check.protected_columns.end() || prot_it->second.count(col_lower) == 0) {
				continue;
			}

			// Found the PU key group column
			// The group columns are output as the first N columns of the aggregate
			// Their bindings use the aggregate's group_index
			ColumnBinding current_binding(inner_agg->group_index, group_idx);

			// Now propagate this binding through any projections between inner_agg and target_agg
			// The path_to_inner contains operators from outer's child down to (but not including) inner_agg
			// We need to trace how the binding transforms through each operator
			for (auto it = path_to_inner.rbegin(); it != path_to_inner.rend(); ++it) {
				LogicalOperator *op = *it;

				if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &proj = op->Cast<LogicalProjection>();
					// Find which expression in the projection references our current binding
					bool found = false;
					for (idx_t expr_idx = 0; expr_idx < proj.expressions.size(); expr_idx++) {
						auto &expr = proj.expressions[expr_idx];
						if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
							auto &proj_col_ref = expr->Cast<BoundColumnRefExpression>();
							if (proj_col_ref.binding == current_binding) {
								// This projection expression references our binding
								// The new binding is the projection's output
								current_binding = ColumnBinding(proj.table_index, expr_idx);
								found = true;
								break;
							}
						}
					}
					if (!found) {
						// The binding might not be directly in the projection - it could be passed through
						// For now, if we can't find it, we'll return the last known binding
						// This handles cases where there's no projection remapping
					}
				}
				// For other operator types (FILTER, etc.), bindings pass through unchanged
			}

			out_pk_binding = current_binding;
			return inner_agg;
		}
	}

	return nullptr;
}

} // namespace duckdb
