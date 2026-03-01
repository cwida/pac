# DuckDB CLI Utility Scripts

Lightweight SQL scripts for quick PAC testing directly through the DuckDB CLI, without compiling or running the standalone benchmark executables.

## Overview

The utility scripts provide a fast way to exercise the PAC query compiler against TPC-H and ClickBench workloads. Each script sets the `pac_diffcols` configuration before every query, enabling PAC differential column tracking for the query's output.

### What is `pac_diffcols`?

The `pac_diffcols` setting tells DuckDB which columns to use for PAC privacy tracking. The format is:

```
'N:filename.csv'
```

Where `N` is the number of `GROUP BY` columns in the query and `filename.csv` is the output file for differential results. For example, `'2:q01.csv'` indicates the query has two group-by columns and results are written to `q01.csv`.

## Prerequisites

- A PAC-enabled DuckDB build with the TPC-H extension compiled in:
  ```bash
  BUILD_TPCH=1 make release
  ```
- A pre-existing database file with the appropriate schema and data loaded (e.g., `tpch_sf30.db`, `clickbench.db`).

## TPC-H Utility

**Script:** `benchmark/tpch/utility_tpch.sql`

Runs 17 TPC-H queries via `PRAGMA tpch(N)` with `pac_diffcols` set before each query. The covered queries are: Q1, Q4, Q5, Q6, Q7, Q8, Q9, Q11, Q12, Q13, Q14, Q15, Q17, Q19, Q20, Q21, Q22.

Each entry in the script follows this pattern:

```sql
set pac_diffcols = '2:q01.csv';
pragma tpch(1);
```

### Running

From the project root:

```bash
# Single run (requires tpch extension to be loaded)
echo "INSTALL tpch; LOAD tpch;" | cat - benchmark/tpch/utility_tpch.sql | ./duckdb/build/release/duckdb tpch_sf30.db
```

Or interactively:
```sql
-- Inside the DuckDB CLI
INSTALL tpch;
LOAD tpch;
.read benchmark/tpch/utility_tpch.sql
```

### Repeated Stability Testing

**Script:** `benchmark/tpch/run_utility_tpch_100.sh`

Runs the full TPC-H utility script 100 times in sequence with progress display. Each iteration appends results to the per-query CSV files (e.g., `q01.csv`, `q04.csv`, ...), so after 100 runs each CSV will contain 100 rows per query.

```bash
# 100 repeated runs (with progress display)
bash benchmark/tpch/run_utility_tpch_100.sh [database] [duckdb_binary]

# Using defaults (tpch_sf30.db, ./build/release/duckdb)
bash benchmark/tpch/run_utility_tpch_100.sh

# With explicit arguments
bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db ./build/release/duckdb
```

### `pac_diffcols` Settings

| Query | `pac_diffcols` | Group-by columns |
|-------|----------------|------------------|
| Q1    | `'2:q01.csv'`  | 2                |
| Q4    | `'1:q04.csv'`  | 1                |
| Q5    | `'1:q05.csv'`  | 1                |
| Q6    | `'0:q06.csv'`  | 0                |
| Q7    | `'3:q07.csv'`  | 3                |
| Q8    | `'1:q08.csv'`  | 1                |
| Q9    | `'2:q09.csv'`  | 2                |
| Q11   | `'1:q11.csv'`  | 1                |
| Q12   | `'1:q12.csv'`  | 1                |
| Q13   | `'1:q13.csv'`  | 1                |
| Q14   | `'0:q14.csv'`  | 0                |
| Q15   | `'1:q15.csv'`  | 1                |
| Q17   | `'0:q17.csv'`  | 0                |
| Q19   | `'0:q19.csv'`  | 0                |
| Q20   | `'1:q20.csv'`  | 1                |
| Q21   | `'1:q21.csv'`  | 1                |
| Q22   | `'1:q22.csv'`  | 1                |

## ClickBench Utility

**Script:** `benchmark/clickbench/clickbench_queries/utility.sql`

Runs all 43 ClickBench queries (Q1--Q43) with `pac_diffcols` set before each query. Unlike the TPC-H utility, these are full SQL queries executed directly against the `hits` table rather than pragmas.

Each entry follows this pattern:

```sql
set pac_diffcols='0:q01.csv';
SELECT COUNT(*) FROM hits;
```

### Running

From the project root:

```bash
./duckdb/build/release/duckdb clickbench.db < benchmark/clickbench/clickbench_queries/utility.sql
```

### Query Coverage

The 43 queries cover a range of aggregate patterns:

| Pattern | Examples |
|---------|----------|
| Simple aggregates (`COUNT(*)`, `SUM`, `AVG`) | Q1, Q2, Q3, Q4 |
| `COUNT(DISTINCT ...)` | Q5, Q6, Q9, Q11, Q14 |
| `GROUP BY` with `ORDER BY` / `LIMIT` | Q8--Q19, Q28--Q43 |
| Filtered aggregates (`WHERE`, `HAVING`) | Q2, Q21, Q28, Q37--Q43 |
| Wide aggregates (many `SUM` columns) | Q30 |
| String matching (`LIKE`, `REGEXP_REPLACE`) | Q21--Q27, Q29 |

## See Also

- [Benchmark Overview](README.md)
- [TPC-H Benchmark](tpch.md) - Full TPC-H benchmark executable
- [TPC-H Compiler Benchmark](tpch_compiler.md) - Compiler correctness testing
- [ClickBench Benchmark](clickbench.md) - Full ClickBench benchmark executable
- [Microbenchmarks](microbenchmarks.md) - Individual aggregate function tests
