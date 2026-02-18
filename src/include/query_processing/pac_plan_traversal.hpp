//
// Created by ila on 1/6/26.
//

#ifndef PAC_PLAN_TRAVERSAL_HPP
#define PAC_PLAN_TRAVERSAL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "metadata/pac_compatibility_check.hpp"

#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Map from CTE table_index to the set of base table names referenced in that CTE's definition.
using CTETableMap = std::unordered_map<idx_t, std::unordered_set<string>>;

// Find the first LogicalGet node in `plan`. Returns a pointer to the unique_ptr that holds the
// found node (so it can be replaced). If pu_table_name is provided, only returns a LogicalGet
// matching that table name. Throws InternalException if not found.
unique_ptr<LogicalOperator> *FindPrivacyUnitGetNode(unique_ptr<LogicalOperator> &plan,
                                                    const string &pu_table_name = "");

// Find the first LogicalAggregate node in the plan tree.
// Throws InternalException if not found.
LogicalAggregate *FindTopAggregate(unique_ptr<LogicalOperator> &op);

// Find all LogicalAggregate nodes in the plan tree.
// Returns a vector of pointers to all aggregates found.
void FindAllAggregates(unique_ptr<LogicalOperator> &op, vector<LogicalAggregate *> &aggregates);

// Find a LogicalGet node for a specific table within a given subtree.
// Unlike FindPrivacyUnitGetNode which searches the entire plan, this searches only within
// the specified subtree (useful for finding the correct table scan when the same table
// is scanned multiple times in different subqueries).
LogicalGet *FindTableScanInSubtree(LogicalOperator *subtree, const string &table_name);

// Find the parent LogicalProjection of a given child node.
// Returns nullptr if not found.
LogicalProjection *FindParentProjection(unique_ptr<LogicalOperator> &root, LogicalOperator *target_child);

// Find the unique_ptr reference to a LogicalGet node by table name.
// Optionally returns the parent node and child index.
// Returns nullptr if not found.
unique_ptr<LogicalOperator> *FindNodeRefByTable(unique_ptr<LogicalOperator> *root, const string &table_name,
                                                LogicalOperator **parent_out = nullptr, idx_t *child_idx_out = nullptr);

// Check if an operator has any leaf data source nodes (base table scans or CTE refs) in its subtree.
bool HasBaseTableInSubtree(LogicalOperator *op);

// Check if an operator has a specific table (by name) in its subtree.
// Returns true if there's a LogicalGet for the given table name in the subtree.
bool HasTableInSubtree(LogicalOperator *op, const string &table_name);

// Build the CTE table map from a plan root. Maps cte_index -> set of base table names
// that the CTE definition transitively references (including through nested CTE refs).
CTETableMap BuildAndResolveCTETableMap(LogicalOperator *plan_root);

// CTE-aware version of HasTableInSubtree. Follows CTE_REF nodes through the cte_map
// to check if the referenced CTE transitively contains the target table.
bool HasTableInSubtreeCTE(LogicalOperator *op, const string &table_name, const CTETableMap &cte_map);

// Find all LogicalGet nodes for a specific table name in the plan tree.
// Returns a vector of pointers to the unique_ptrs holding the LogicalGet nodes.
void FindAllNodesByTable(unique_ptr<LogicalOperator> *root, const string &table_name,
                         vector<unique_ptr<LogicalOperator> *> &results);

// Check if an operator has a LogicalGet with a specific table index in its subtree.
bool HasTableIndexInSubtree(LogicalOperator *op, idx_t table_index);

// Find all LogicalGet nodes with a specific table index in the plan tree.
void FindAllNodesByTableIndex(unique_ptr<LogicalOperator> *root, idx_t table_index,
                              vector<unique_ptr<LogicalOperator> *> &results);

// Filter aggregates to only those that have specified tables in their subtree
// AND have base tables in their DIRECT children (not through nested aggregates).
// This filters out outer aggregates that only depend on inner aggregate results.
vector<LogicalAggregate *> FilterTargetAggregates(const vector<LogicalAggregate *> &all_aggregates,
                                                  const vector<string> &target_table_names,
                                                  const CTETableMap &cte_map = {});

// Extended version of FilterTargetAggregates that handles the edge case where inner aggregate
// groups by PU key (PAC key/PK of Privacy Unit or FK referencing it).
// In this case, the inner aggregate is skipped and the outer aggregate is noised instead.
// @param check - PACCompatibilityResult containing table metadata (PKs, FKs)
// @param privacy_units - List of privacy unit table names
vector<LogicalAggregate *> FilterTargetAggregatesWithPUKeyCheck(const vector<LogicalAggregate *> &all_aggregates,
                                                                const vector<string> &target_table_names,
                                                                const PACCompatibilityResult &check,
                                                                const vector<string> &privacy_units,
                                                                const CTETableMap &cte_map = {});

// Check if an aggregate's GROUP BY keys contain the PU's primary key columns or FK columns
// referencing a PU. This is used to detect the edge case where inner aggregate groups by PU key.
// @param agg - The aggregate to check
// @param check - PACCompatibilityResult containing table metadata
// @param privacy_units - List of privacy unit table names
// @return true if the aggregate groups by PU key (PK or FK to PU)
bool AggregateGroupsByPUKey(LogicalAggregate *agg, const PACCompatibilityResult &check,
                            const vector<string> &privacy_units);

// Check if a target node is inside a DELIM_JOIN's subquery branch (children[1]).
// This is important for correlated subqueries where nodes in the subquery branch
// cannot directly access tables from the outer query.
bool IsInDelimJoinSubqueryBranch(unique_ptr<LogicalOperator> *root, LogicalOperator *target_node);

// Check if a table's columns are accessible from the given starting operator.
// Returns false if the table is in the right child of a MARK/SEMI/ANTI join,
// because those join types don't output right-side columns (only the boolean mark).
// This is important for IN/EXISTS subqueries where the subquery's columns aren't accessible.
bool AreTableColumnsAccessible(LogicalOperator *from_op, idx_t table_index);

// Find the inner aggregate (child of target_agg) that groups by PU key.
// Returns the inner aggregate and the column binding of the PU key group column in its output.
// This is used for the Q13 pattern where inner aggregate groups by PU key and outer aggregate
// needs to use that group column as the hash input.
// @param target_agg - The outer aggregate that was selected for transformation
// @param check - PACCompatibilityResult containing table metadata
// @param privacy_units - List of privacy unit table names
// @param out_pk_binding - Output: the column binding of the PU key in the inner aggregate's output
// @return The inner aggregate that groups by PU key, or nullptr if not found
LogicalAggregate *FindInnerAggregateWithPUKeyGroup(LogicalAggregate *target_agg, const PACCompatibilityResult &check,
                                                   const vector<string> &privacy_units, ColumnBinding &out_pk_binding);

} // namespace duckdb

#endif // PAC_PLAN_TRAVERSAL_HPP
