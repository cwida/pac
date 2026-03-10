# Reproducibility Guide

Instructions to reproduce all experiments from:
**SIMD-PAC-DB: Pretty Performant PAC Privacy** (PVLDB)

All experiments use DuckDB v1.4.2 with the PAC extension, compiled with Clang 22.1.0.

## Hardware

| Machine | CPU | RAM | Storage |
|---------|-----|-----|---------|
| MacBook Pro | 12-core ARM M2 Max | 32GB | SSD |
| c8gd.4xlarge | 16-core ARM Graviton4 | 32GB | 950GB instance SSD |
| c8i.4xlarge | 16-core x86 Intel Granite Rapids | 32GB | EBS io2 |
| c8a.4xlarge | 16-core x86 AMD EPYC 9R45 | 32GB | EBS io2 |

### EC2 instance setup

Use Ubuntu 24 (`ssh ubuntu@<ip> -i yourkey.pem`).

**Graviton SSD setup (c8gd only)**:
```bash
sudo parted /dev/nvme0n1 -- mklabel gpt
sudo parted /dev/nvme0n1 -- mkpart primary ext4 0% 200GB
sudo mkfs.ext4 /dev/nvme0n1p1
sudo mkdir -p /mnt/instance-store
sudo mount /dev/nvme0n1p1 /mnt/instance-store
cd /mnt/instance-store/ && chmod 777 .
```

### Build toolchain (all machines)

```bash
sudo apt update
sudo apt install -y clang cmake ninja-build python3
export CC=clang
export CXX=clang++

# Build Clang 22.1.0 from source
git clone --depth 1 https://github.com/llvm/llvm-project.git
cd llvm-project
cmake -S llvm -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_TARGETS_TO_BUILD="AArch64"  # or "X86" for Intel/AMD
ninja -C build clang lld
cd ..
```

### Build PAC

```bash
git clone --recurse-submodules https://github.com/cwida/pac.git
cd pac
GEN=ninja make
```

---

## RQ1: Aggregation Optimizations (Figures 3–6, Table 1)

Microbenchmarks on synthetic data with N ∈ {10M, 100M, 1G} rows. Value distributions: uniform random, monotonically increasing, monotonically decreasing. Domain sizes: tiny (0–100), small (0–10⁴), medium (0–10⁵), large (0–10⁶). Grouped ungrouped, and with K ∈ {10, 1K, 10K, 100K, 10M} distinct GROUP BY keys (scattered and consecutive).

### Build DuckDB variants

Seven variants with different optimization flags:

| Variant | Description |
|---------|-------------|
| `default` | All optimizations (approx sum, cascading, buffering, SWAR, pruning) |
| `nobuffering` | Disable input buffering |
| `noboundopt` | Disable bound pruning (affects MIN/MAX) |
| `signedsum` | Disable two-sided SUM (single signed counters) |
| `exactsum` | Disable approximate sum, use exact cascading |
| `nocascading` | Disable cascading (implies exact sum) |
| `nosimd` | SIMD-unfriendly: no cascading, no auto-vectorization |

```bash
cd benchmark/pac_microbench
./build_variants.sh          # builds all 7 variants (~30 min each)
./create_test_db.sh          # creates test_data.duckdb with views data1/data10/data100
```

### Run all microbenchmarks

```bash
nohup ./run_all.sh &         # runs COUNT, SUM/AVG, MIN/MAX (~6 hours)
# Results in benchmark/pac_microbench/results/
```

### Figure 3: COUNT optimization impact (Granite Rapids, 1G rows, scattered groups)

```bash
# Run on c8i.4xlarge
./bench_count.sh
python3 plot_count_optimizations.py    # edit filenames inside to point to result CSVs
```

### Figure 4: SUM optimization impact (MacBook, 1G rows, scattered groups)

```bash
# Run on MacBook Pro
./bench_sum_avg.sh
python3 plot_sum_optimizations.py
```

### Figure 5: MAX optimization impact (Graviton4, 1G rows, ungrouped)

```bash
# Run on c8gd.4xlarge
./bench_min_max.sh
python3 plot_minmax_optimizations.py
```

### Figure 6: SIMD improvements cross-platform (100M rows, ungrouped)

```bash
# Run microbenchmarks on all 4 machines, then combine results
python3 plot_simd_improvements.py      # edit filenames to point to per-machine CSVs
```

### Table 1: Approximate SUM accuracy (10 distributions, 1M values)

```bash
# Compare default (approximate two-sided) vs signedsum (single-sided) counters
cd benchmark/pac_microbench
./binaries/duckdb_default < approx_sum_stability.sql
./binaries/duckdb_signedsum < approx_sum_stability.sql
```

### Figure 2: Hash distribution

```bash
./binaries/duckdb_default < benchmark/pac_microbench/binomial.sql
python3 benchmark/pac_microbench/plot_hash_distribution.py
```

---

## RQ2: Performance Impact (Figures 1, 7)

### Figure 1: TPC-H at SF30 (MacBook M2 Max)

PAC schema: customer as PU with five protected columns (c_custkey, c_name, c_address, c_acctbal, c_comment). PAC links propagate the PU hash from lineitem → orders → customer.

```bash
# Build the benchmark executable
cmake --build build/release --target pac_tpch_benchmark

# Run TPC-H SF30 (creates database if needed, 8 threads default)
./build/release/extension/pac/pac_tpch_benchmark 30 tpch_sf30.db benchmark

# Plot
Rscript --vanilla benchmark/tpch/plot_tpch_results.R \
    benchmark/tpch/tpch_benchmark_results_sf30.csv benchmark/tpch/
```

See [docs/benchmark/tpch.md](docs/benchmark/tpch.md) for full options.

### Figure 7: ClickBench (Graviton4)

ClickBench declares `hits` as the PU table with `UserID` and `ClientIP` as protected columns. No PU-join needed (PU is the scanned table).

```bash
# Build the benchmark executable
cmake --build build/release --target pac_clickhouse_benchmark

# Download ClickBench data (https://benchmark.clickhouse.com/)
# Create database and load data using benchmark/clickbench/clickbench_queries/create.sql and load.sql

# Run on c8gd.4xlarge
./build/release/extension/pac/pac_clickhouse_benchmark

# Plot
Rscript --vanilla benchmark/clickbench/plot_clickbench_results.R
```

---

## RQ3: Quality (Figures 8–10)

### Figure 8: Utility — TPC-H and ClickBench (SF30, 100 runs)

Runs each non-rejected query 100 times with `pac_diffcols` and compares PAC-privatized output against the non-private reference. Reports MAPE (Mean Absolute Percentage Error), recall, and precision per query.

```bash
# TPC-H utility (requires tpch_sf30.db from RQ2)
bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db ./build/release/duckdb

# ClickBench utility (requires clickbench database from RQ2)
bash benchmark/clickbench/run_utility_clickbench_100.sh clickbench.db ./build/release/duckdb 100

# Plot both
Rscript --vanilla benchmark/tpch/plot_utility.R
```

### Figure 9: Runtime overhead distribution (TPC-H SF30)

Generated from the same TPC-H benchmark results as Figure 1. The plot shows the CDF of per-query overhead (PAC time / DuckDB time).

### Figure 10: Naive vs lambda approach — list_transform utility (TPC-H SF1, 10 runs)

Compares naive (N independent `pac_sum_noised` calls, noised N times) vs optimized (`pac_sum_counters` + `list_transform` + single `pac_noised`) on 20 queries with increasing numbers of ratio expressions.

```bash
# Create SF1 database
./build/release/extension/pac/pac_tpch_benchmark 1 tpch_sf1.db benchmark

# Run 10 iterations
bash benchmark/utility_listtransform/run.sh tpch_sf1.db ./build/release/duckdb 10

# Results in benchmark/utility_listtransform/results.csv
Rscript --vanilla benchmark/utility_listtransform/plot.R
```

---

## RQ4: SQL Coverage (Section 6.4)

SQLStorm generates thousands of diverse SQL queries over the TPC-H schema (SF1, timeout 5s) to stress-test PAC rewriter coverage.

```bash
# Build the SQLStorm benchmark executable
cmake --build build/release --target pac_sqlstorm_benchmark

# Run
./build/release/extension/pac/pac_sqlstorm_benchmark

# Plot runtime slowdown distribution
Rscript --vanilla benchmark/sqlstorm/plot_sqlstorm_degradation.R
```
