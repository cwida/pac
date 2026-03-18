#ifndef PAC_DERIVED_REWRITER_HPP
#define PAC_DERIVED_REWRITER_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Write path: after CompilePacBitsliceQuery, convert pac_noised_* aggregates to pac_* counter variants
// for DML operations targeting derived_pu tables.
void ConvertDerivedPuToCounters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

// Read path: for SELECT queries on derived_pu tables, inject pac_finalize projections
// for LIST<DOUBLE> columns so downstream operators see scalar values.
void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

#endif // PAC_DERIVED_REWRITER_HPP
