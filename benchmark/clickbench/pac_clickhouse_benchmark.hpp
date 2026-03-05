//
// Created by ila on 02/12/26.
//

#ifndef PAC_CLICKHOUSE_BENCHMARK_HPP
#define PAC_CLICKHOUSE_BENCHMARK_HPP

#include <string>
#include <vector>
#include "duckdb.hpp"

namespace duckdb {

// Run the ClickHouse Hits dataset benchmark (ClickBench).
// Uses fork-based process isolation: the parent never opens DuckDB for
// benchmarking; a child process handles all DB access. If the child is
// killed (OOM, crash), the parent survives and spawns a new one.
//
// Parameters:
// - db_path: path to DuckDB database file (default "clickbench.db")
// - queries_dir: directory containing create.sql, load.sql, queries.sql, setup.sql
//                (default "benchmark/clickbench/clickbench_queries")
// - out_csv: output CSV path (if empty, auto-named)
// - micro: if true, use a smaller subset for quick testing
//
// Returns 0 on success, non-zero on error.
int RunClickHouseBenchmark(const string &db_path = "clickbench.db",
                           const string &queries_dir = "benchmark/clickbench/clickbench_queries",
                           const string &out_csv = "",
                           bool micro = false);

} // namespace duckdb

#endif // PAC_CLICKHOUSE_BENCHMARK_HPP
