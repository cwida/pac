//
// PAC Bitslice Join Chain Builder
//
// Handles Phase 1 of PAC bitslice compilation when the privacy unit table
// is NOT directly scanned: determines which FK tables are missing from the
// plan, creates LogicalGet nodes for them, and inserts join chains to connect
// them to existing tables.
//
// Created by refactoring from pac_bitslice_compiler.cpp
//

#ifndef PAC_BITSLICE_JOIN_CHAIN_HPP
#define PAC_BITSLICE_JOIN_CHAIN_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "query_processing/pac_plan_traversal.hpp"

namespace duckdb {

// Maps connecting_table.table_index -> fk_table.table_index
// Used to look up which FK table instance (e.g., orders) corresponds to
// a given connecting table instance (e.g., lineitem) for hash generation.
using ConnectingTableMap = std::unordered_map<idx_t, idx_t>;

// Ensures all FK tables needed for PAC compilation are present in the plan.
// For each connecting table instance, inserts join chains to missing FK tables
// and builds a mapping from connecting table index to FK table index.
ConnectingTableMap EnsureFKTablesInPlan(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                                        unique_ptr<LogicalOperator> &plan, const vector<string> &gets_missing,
                                        const vector<string> &gets_present, const vector<string> &fk_path,
                                        const vector<string> &privacy_units, const CTETableMap &cte_map);

} // namespace duckdb

#endif // PAC_BITSLICE_JOIN_CHAIN_HPP
