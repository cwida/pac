//
// Created by ila on 1/6/26.
//

#ifndef PAC_EXPRESSION_BUILDER_HPP
#define PAC_EXPRESSION_BUILDER_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"

namespace duckdb {

// Ensure a column is projected in a LogicalGet and return its projection index
// Returns DConstants::INVALID_INDEX if the column cannot be found
idx_t EnsureProjectedColumn(LogicalGet &g, const string &col_name);

// Ensure PK columns are present in a LogicalGet's column_ids and projection_ids
void AddPKColumns(LogicalGet &get, const vector<string> &pks);

// Helper to ensure rowid is present in the output columns of a LogicalGet
void AddRowIDColumn(LogicalGet &get);

// Hash one or more column expressions: hash(c1) XOR hash(c2) XOR ...
unique_ptr<Expression> BuildXorHash(OptimizerExtensionInput &input, vector<unique_ptr<Expression>> cols);

// Build hash expression for the given LogicalGet's primary key columns
unique_ptr<Expression> BuildXorHashFromPKs(OptimizerExtensionInput &input, LogicalGet &get, const vector<string> &pks);

// Insert a LogicalProjection above a LogicalGet that computes hash(key_columns) as an extra column.
// Remaps all existing references to the get's bindings using ColumnBindingReplacer.
// Returns the ColumnBinding for the new hash column in the projection's output.
ColumnBinding InsertHashProjectionAboveGet(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                           LogicalGet &get, const vector<string> &key_columns, bool use_rowid);

// Caching wrapper around InsertHashProjectionAboveGet. Keyed by get.table_index.
// Returns cached binding if hash projection was already inserted for this get.
ColumnBinding GetOrInsertHashProjection(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                        LogicalGet &get, const vector<string> &key_columns, bool use_rowid,
                                        std::unordered_map<idx_t, ColumnBinding> &cache);

// Insert a hash projection above a CTE_SCAN (LogicalCTERef) node.
// The CTE_SCAN must expose the key columns by name in its bound_columns.
// Returns the ColumnBinding for the hash column, or INVALID if key columns not found.
ColumnBinding InsertHashProjectionAboveCTERef(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                              LogicalCTERef &cte_ref, const vector<string> &key_columns);

// Build AND expression from multiple hash expressions (for multiple PUs)
unique_ptr<Expression> BuildAndFromHashes(OptimizerExtensionInput &input, vector<unique_ptr<Expression>> &hash_exprs);

// Modify aggregate expressions to use PAC functions (replaces the aggregate loop logic)
void ModifyAggregatesWithPacFunctions(OptimizerExtensionInput &input, LogicalAggregate *agg,
                                      unique_ptr<Expression> &hash_input_expr);

} // namespace duckdb

#endif // PAC_EXPRESSION_BUILDER_HPP
