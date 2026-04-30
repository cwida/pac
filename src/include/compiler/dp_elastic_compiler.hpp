#ifndef DP_ELASTIC_COMPILER_HPP
#define DP_ELASTIC_COMPILER_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "metadata/privacy_compatibility_check.hpp"

namespace duckdb {

// Entry point for the elastic-sensitivity differential privacy compiler.
// Selected via `SET privacy_mode = 'dp_elastic'`. Mirrors CompilePacBitsliceQuery's signature.
void CompileDPElasticQuery(const PrivacyCompatibilityResult &check, OptimizerExtensionInput &input,
                           unique_ptr<LogicalOperator> &plan, const vector<string> &privacy_units,
                           const string &query_hash);

} // namespace duckdb

#endif // DP_ELASTIC_COMPILER_HPP
