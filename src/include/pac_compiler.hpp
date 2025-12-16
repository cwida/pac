//
// Created by ila on 12/12/25.
//

#ifndef PAC_COMPILER_HPP
#define PAC_COMPILER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Compile PAC-compatible query into intermediate artifacts (entry point)
// privacy_unit: single privacy unit name (must be non-empty to compile)
DUCKDB_API void CompilePACQuery(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                const std::string &privacy_unit);

// Create the sample CTE and write it to a file; returns the full path of the written file
DUCKDB_API std::string CreateSampleCTE(ClientContext &context, const std::string &privacy_unit,
                                       const std::string &query_normalized, const std::string &query_hash);

} // namespace duckdb

#endif //PAC_COMPILER_HPP
