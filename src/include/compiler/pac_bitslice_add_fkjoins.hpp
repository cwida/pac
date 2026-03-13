#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "query_processing/pac_plan_traversal.hpp"

namespace duckdb {

// Insert missing FK joins into the plan so that every table on the FK path
// from the queried tables to the privacy-unit table is present.
// After this function returns the plan is structurally equivalent to one where
// the PU table was scanned directly (all FK-linked tables are reachable).
void AddMissingFKJoins(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                       unique_ptr<LogicalOperator> &plan, const vector<string> &gets_missing,
                       const vector<string> &gets_present, const vector<string> &fk_path,
                       const vector<string> &privacy_units, const CTETableMap &cte_map);

} // namespace duckdb
