# SQLStorm Benchmark

This document describes the SQLStorm benchmark for measuring PAC overhead on LLM-generated analytical workloads.

## Overview

[SQLStorm](https://db.in.tum.de/~schmidt/papers/sqlstorm.pdf) is an LLM-generated analytical benchmark from TU Munich. Unlike hand-written benchmarks (TPC-H, ClickBench), SQLStorm uses large language models to produce thousands of diverse SQL queries, stress-testing query engines with realistic but unpredictable workloads.

The PAC SQLStorm benchmark uses a **two-pass design**:
1. **Baseline**: Clear PAC metadata, run all queries with standard DuckDB
2. **PAC**: Load PAC schema, run all queries again with privacy-preserving aggregation

Two datasets are supported:
- **TPC-H**: Generated via `dbgen` at a configurable scale factor
- **StackOverflow DBA**: ~1GB real-world dataset from the DBA StackExchange site (auto-downloaded)

Queries come from the SQLStorm v1.0 release (`benchmark/sqlstorm/SQLStorm/v1.0/`).

## Building

```bash
# From repository root
cmake --build build/release --target pac_sqlstorm_benchmark
```

## Running

```bash
# Run both TPC-H and StackOverflow benchmarks (default)
./build/release/extension/pac/pac_sqlstorm_benchmark

# Run only TPC-H at scale factor 10
./build/release/extension/pac/pac_sqlstorm_benchmark --benchmark tpch --tpch_sf 10

# Run only StackOverflow
./build/release/extension/pac/pac_sqlstorm_benchmark --benchmark stackoverflow

# Custom timeout and output path
./build/release/extension/pac/pac_sqlstorm_benchmark --timeout 30 --out results.csv

# Small-scale TPC-H for quick testing
./build/release/extension/pac/pac_sqlstorm_benchmark --benchmark tpch --tpch_sf 0.01

# Custom queries directory
./build/release/extension/pac/pac_sqlstorm_benchmark --queries /path/to/SQLStorm/v1.0/tpch/queries
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--queries <dir>` | SQLStorm queries directory | Auto-detect `benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries` |
| `--out <csv>` | Output CSV path | `benchmark/sqlstorm/sqlstorm_benchmark_results_*.csv` |
| `--timeout <sec>` | Per-query timeout in seconds | `0.1` |
| `--tpch_sf <float>` | TPC-H scale factor (can be fractional) | `1.0` |
| `--benchmark <name>` | Benchmark to run: `tpch`, `stackoverflow`, or `both` | `both` |
| `-h, --help` | Show help message | |

When running `both` benchmarks, the `--queries` option is ignored and queries directories are auto-detected for each dataset. When running a single benchmark, `--queries` overrides the auto-detected directory for that dataset.

## Database Setup

### TPC-H

If the database file does not exist, the benchmark will:
1. Install and load the TPC-H and ICU extensions
2. Generate TPC-H data using `CALL dbgen(sf=<scale_factor>)`
3. Save to `tpch_sf{SF}_sqlstorm.db`

If the database already exists, data generation is skipped.

**PAC schema**: `pac_tpch_schema.sql` (shared with the TPC-H benchmark). Creates the privacy unit chain `lineitem -> orders -> customer`.

### StackOverflow

If the database file does not exist, the benchmark will:
1. Download the dataset from `https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz` (~1GB) into `benchmark/sqlstorm/data/`
2. Extract CSV files (Users, Posts, Comments, Badges, Votes, PostHistory, PostLinks, Tags, etc.)
3. Create the schema from `SQLStorm/v1.0/stackoverflow/schema_nofk.sql`
4. Load all CSVs into DuckDB
5. Save to `stackoverflow_dba_sqlstorm.db`

If the database already exists, setup is skipped. If setup fails partway through, the database file is removed so the next run starts fresh.

**PAC schema**: `benchmark/sqlstorm/pac_stackoverflow_schema.sql`. Defines `Users` as the privacy unit with the following link graph:

```
Users (PU)
  +-- Badges.UserId -> Users.Id
  +-- Posts.OwnerUserId -> Users.Id
  |     +-- PostLinks.PostId -> Posts.Id
  +-- Comments.UserId -> Users.Id
  +-- PostHistory.UserId -> Users.Id
  +-- Votes.UserId -> Users.Id
```

## Output

### CSV Format

Results are written to CSV files with columns:

```
query,baseline_state,baseline_time_ms,baseline_rows,pac_state,pac_time_ms,pac_rows,pac_applied,pac_error
```

| Column | Description |
|--------|-------------|
| `query` | Query name (derived from filename, e.g. `10001`) |
| `baseline_state` | Baseline result: `success`, `error`, `timeout`, or `crash` |
| `baseline_time_ms` | Baseline execution time in milliseconds |
| `baseline_rows` | Number of result rows (baseline) |
| `pac_state` | PAC result: `success`, `error`, `timeout`, or `crash` |
| `pac_time_ms` | PAC execution time in milliseconds |
| `pac_rows` | Number of result rows (PAC) |
| `pac_applied` | `yes` if EXPLAIN shows `pac_` functions, `no` otherwise |
| `pac_error` | Error message for failed PAC queries |

Default output paths:
- TPC-H: `benchmark/sqlstorm/sqlstorm_benchmark_results_tpch_sf{SF}.csv`
- StackOverflow: `benchmark/sqlstorm/sqlstorm_benchmark_results_so.csv`

### Console Output

The benchmark prints timestamped progress and summary statistics:

```
[2026-02-18 14:30:00] SQLStorm benchmark (two-pass: baseline + PAC)
[2026-02-18 14:30:00] Benchmark: both
[2026-02-18 14:30:00] Timeout: 0.1s
[2026-02-18 14:30:00] TPC-H scale factor: 1
[2026-02-18 14:30:00] Found 5000 queries
[2026-02-18 14:30:00] === baseline pass: running 5000 queries ===
...
[2026-02-18 14:35:00] === TPC-H RESULTS ===
[2026-02-18 14:35:00] --- baseline ---
[2026-02-18 14:35:00]   Total:   5000
[2026-02-18 14:35:00]   Success: 4200 (84%)
[2026-02-18 14:35:00]   Failed:  700 (14%)
[2026-02-18 14:35:00]   Crash:   10 (0.2%)
[2026-02-18 14:35:00]   Timeout: 90 (1.8%)
[2026-02-18 14:35:00] --- PAC ---
[2026-02-18 14:35:00]   PAC applied:     2100 (42% of total, 55% of successful)
[2026-02-18 14:35:00]   PAC not applied: 1700 (34% of total, 45% of successful)
[2026-02-18 14:35:00] --- Comparison ---
[2026-02-18 14:35:00]   Both succeeded: 3500
[2026-02-18 14:35:00]   baseline only:  700
[2026-02-18 14:35:00]   PAC only:       300
[2026-02-18 14:35:00] --- Timing (queries succeeding in both passes) ---
[2026-02-18 14:35:00]   PAC is 12.5% slower than DuckDB
```

Statistics reported per pass:
- Success, failed, crash, and timeout counts with percentages
- PAC applied count (PAC pass only, verified via EXPLAIN)
- Total and average execution time for successful queries

Comparison statistics:
- Queries succeeding in both passes, baseline only, or PAC only
- Timing overhead percentage for queries succeeding in both passes
- Separate overhead for PAC-transformed queries only

Error breakdown:
- Grouped error messages with counts and percentages
- Unsupported aggregate function breakdown (PAC pass)
- Internal error details with affected query names
- Lists of timed-out and crashed queries

## Benchmark Methodology

For each pass (baseline, PAC):
1. All `.sql` files in the queries directory are collected and sorted
2. Each query is executed once with a per-query timeout
3. Queries that exceed the timeout are interrupted via `Connection::Interrupt()`
4. If a query crashes the database, the benchmark reconnects and retries the next query
5. Crash attribution: when a "database has been invalidated" error is detected, the crash is attributed to the preceding query
6. The database is checkpointed every 1000 queries to reclaim memory

Between passes:
- Baseline pass starts with `PRAGMA clear_pac_metadata` to ensure no PAC transforms
- PAC pass loads the appropriate PAC schema before running

PAC verification:
- After each successful PAC query, `EXPLAIN` is run on the same SQL
- If the plan contains `pac_` functions, the query is counted as PAC-applied

## Directory Structure

```
benchmark/sqlstorm/
+-- pac_sqlstorm_benchmark.cpp       # Benchmark runner source
+-- pac_sqlstorm_benchmark.hpp       # Header
+-- pac_stackoverflow_schema.sql     # StackOverflow PAC schema
+-- SQLStorm/                        # SQLStorm v1.0 (git submodule)
|   +-- v1.0/
|       +-- tpch/
|       |   +-- queries/             # TPC-H LLM-generated queries
|       +-- stackoverflow/
|           +-- queries/             # StackOverflow LLM-generated queries
|           +-- schema_nofk.sql      # StackOverflow table schema
+-- data/
|   +-- stackoverflow_dba.tar.gz     # Downloaded dataset (auto)
|   +-- stackoverflow_dba/           # Extracted CSV files
+-- sqlstorm_benchmark_results_*.csv # Output CSV files
```

Database files are created in the current working directory:
- `tpch_sf{SF}_sqlstorm.db` (e.g. `tpch_sf1_sqlstorm.db`)
- `stackoverflow_dba_sqlstorm.db`

## See Also

- [TPC-H Benchmark](tpch.md) - Decision support benchmark with hand-written PAC queries
- [ClickBench Benchmark](clickbench.md) - Web analytics benchmark
- [Microbenchmarks](microbenchmarks.md) - Individual aggregate performance tests
- [SQLStorm paper](https://db.in.tum.de/~schmidt/papers/sqlstorm.pdf) - Original SQLStorm paper from TU Munich
