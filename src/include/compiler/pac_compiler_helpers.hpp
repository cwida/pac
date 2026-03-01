//
// Created by ila on 12/23/25.
//

#ifndef PAC_COMPILER_HELPERS_HPP
#define PAC_COMPILER_HELPERS_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "query_processing/pac_expression_builder.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "query_processing/pac_projection_propagation.hpp"

namespace duckdb {

class LogicalAggregate;

// Build join conditions from FK columns to PK columns
void BuildJoinConditions(LogicalGet *left_get, LogicalGet *right_get, const vector<string> &left_cols,
                         const vector<string> &right_cols, const string &left_table_name,
                         const string &right_table_name, vector<JoinCondition> &conditions);

// Create a logical join operator based on FK relationships in the compatibility check metadata
unique_ptr<LogicalOperator> CreateLogicalJoin(const PACCompatibilityResult &check, ClientContext &context,
                                              unique_ptr<LogicalOperator> left_operator, unique_ptr<LogicalGet> right);

// Create a LogicalGet operator for a table by name, projecting only the specified columns
// If required_columns is empty, projects all columns (backward compatible)
unique_ptr<LogicalGet> CreateLogicalGet(ClientContext &context, unique_ptr<LogicalOperator> &plan, const string &table,
                                        idx_t idx, const vector<string> &required_columns = {});

// Examine PACCompatibilityResult.fk_paths and populate gets_present / gets_missing
void PopulateGetsFromFKPath(const PACCompatibilityResult &check, vector<string> &gets_present,
                            vector<string> &gets_missing, string &start_table_out, vector<string> &target_pus_out);

// Find the FK columns from a table that reference any privacy unit.
// Returns the FK column names, or empty vector if no FK to any PU exists.
vector<string> FindFKColumnsToPU(const PACCompatibilityResult &check, const string &table_name,
                                 const vector<string> &privacy_units);

// Find a LogicalGet for a table that is accessible within a subtree root's descendants.
// Searches all instances of the table in the plan and returns the first whose table_index
// appears in subtree_root's subtree.  Returns nullptr if none found.
LogicalGet *FindAccessibleGetInSubtree(unique_ptr<LogicalOperator> &plan, LogicalOperator *subtree_root,
                                       const string &table_name);

// Insert a hash projection above a LogicalGet, propagate the hash binding to a target
// aggregate, and transform the aggregate's expressions to PAC functions.
// Returns true on success, false if propagation fails (caller should try alternative path).
bool InsertHashAndTransformAggregate(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, LogicalGet &get,
                                     const vector<string> &key_cols, bool use_rowid, LogicalAggregate *target_agg,
                                     std::unordered_map<idx_t, ColumnBinding> &hash_cache);

} // namespace duckdb

#endif // PAC_COMPILER_HELPERS_HPP
