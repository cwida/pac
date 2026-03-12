//
// Created by ila on 12/21/25.
//

#ifndef PAC_BITSLICE_COMPILER_HPP
#define PAC_BITSLICE_COMPILER_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "query_processing/pac_plan_traversal.hpp"

namespace duckdb {

// forward-declare LogicalGet to avoid including heavy headers in this header
class LogicalGet;

// Helper to ensure PK columns are present in a LogicalGet's column_ids and projection_ids.
void AddPKColumns(LogicalGet &get, const vector<string> &pks);

// Helper to inspect FK paths and populate lists of GETs present/missing. Defined in pac_bitslice_compiler.cpp
void PopulateGetsFromFKPath(const PACCompatibilityResult &check, vector<string> &gets_present,
                            vector<string> &gets_missing, string &start_table_out, vector<string> &target_pus_out);

// Unified aggregate transformation: builds hash expressions from PU/FK tables and
// transforms aggregates to use PAC functions. Works for both direct-PU and FK-joined plans.
void ModifyPlanWithPU(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                      const vector<string> &pu_table_names, const PACCompatibilityResult &check,
                      const CTETableMap &cte_map);

// Bitslice-style PAC compiler entrypoint
void CompilePacBitsliceQuery(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                             unique_ptr<LogicalOperator> &plan, const vector<string> &privacy_units,
                             const string &query, const string &query_hash);

} // namespace duckdb

#endif // PAC_BITSLICE_COMPILER_HPP
