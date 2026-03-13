//
// Created by ila on 02/18/26.
//

#ifndef PAC_SQLSTORM_BENCHMARK_HPP
#define PAC_SQLSTORM_BENCHMARK_HPP

#include <string>
#include <vector>
#include "duckdb.hpp"

namespace duckdb {

// Run the SQLStorm benchmark with two passes:
// 1. Clear PAC metadata, run all queries (baseline mode, no PAC)
// 2. Load PAC schema, run all queries again (PAC mode)
// 3. Print statistics for both modes
// PAC metadata is NOT cleared at the end.
//
// Parameters:
// - queries_dir: directory containing SQLStorm .sql query files
// - out_csv: output CSV path (if empty, auto-named)
// - timeout_s: per-query timeout in seconds
// - tpch_sf: TPC-H scale factor (e.g. 1.00, 0.01)
// - benchmark: which benchmark to run ("tpch", "stackoverflow", or "both")
//
// Returns 0 on success, non-zero on error.
int RunSQLStormBenchmark(const string &queries_dir = "",
                         const string &out_csv = "",
                         double timeout_s = 10.0,
                         double tpch_sf = 1.0,
                         const string &benchmark = "both");

} // namespace duckdb

#endif // PAC_SQLSTORM_BENCHMARK_HPP
