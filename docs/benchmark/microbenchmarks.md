# PAC Microbenchmark Suite

Comprehensive performance benchmarks for PAC aggregate functions (pac_count, pac_sum, pac_avg, pac_min, pac_max) with various optimization configurations. The extension should be compiled with clang.

## Quick Start
Install the required dependencies (assuming a Debian-based system):
```bash
sudo apt install make ninja-build cmake clang
export CC=clang
export CXX=clang++
```
Then, run the scripts (from the `microbenchmark` directory):
```bash
./create_test_db.sh

# Build all variants and run all benchmarks
./run_all.sh

# Quick test (smaller data, single run)
./run_all.sh -q

# Build only
./run_all.sh -b

# Run specific benchmark only
./run_all.sh -r count
./run_all.sh -r min_max

# Analyze results
./analyze_results.py
```

## Build Variants

The suite builds multiple DuckDB binaries with different optimization flags:

| Variant | Flags | Description |
|---------|-------|-------------|
| `default` | (none) | All optimizations enabled |
| `nobuffering` | `-DPAC_NOBUFFERING` | Disable input buffering (lazy allocation) |
| `nocascading` | `-DPAC_NOCASCADING` | Pre-allocate all accumulator levels |
| `nosimd` | `-DPAC_NOSIMD` | SIMD-unfriendly kernels, auto-vectorization disabled |
| `noboundopt` | `-DPAC_NOBOUNDOPT` | Disable bound optimization (min/max only) |
| `signedsum` | `-DPAC_SIGNEDSUM` | Disable separate negative counter handling |
| `exactsum` | `-DPAC_EXACTSUM` | Exact cascading instead of approximate sum |

## Benchmarks

### pac_count (`bench_count.sh`)

Tests counter overflow handling and banked vs non-banked performance:

- **Ungrouped**: 10M, 100M, 500M, 1B+ rows (tests 16→32→64 bit counter upgrade)
- **Grouped**: 1 to 10M groups (tests per-group state overhead)

### pac_sum/avg (`bench_sum_avg.sh`)

Tests cascading accumulator performance:

- **Data types**: int8, int16, int32, int64, float, double
- **Distributions**: small (0-100), medium (0-10K), large (0-1M)
- **Grouped vs ungrouped**
- **Scaling**: 10M to 500M rows

### pac_min/max (`bench_min_max.sh`)

Tests bound optimization and bank allocation:

- **Data types**: int8-64, uint8-64, float, double
- **Distributions**: random, monotonic increasing, monotonic decreasing
- **Bound optimization impact**: worst case (random) vs best case (monotonic)
- **Grouped vs ungrouped**

## Data Generation

All data is generated using DuckDB's `range()` function:

```sql
-- Random values via hash
(hash(i) % domain)::TYPE

-- Monotonic increasing
(i % domain)::TYPE

-- Monotonic decreasing
((domain - i % domain))::TYPE
```

## Results

Results are saved to `results/` as CSV files with timestamps:

```
results/
├── count_YYYYMMDD_HHMMSS.csv
├── sum_avg_YYYYMMDD_HHMMSS.csv
└── min_max_YYYYMMDD_HHMMSS.csv
```

CSV columns:
- `test`: test category (ungrouped, grouped, scaling, etc.)
- `aggregate`: function (count, sum, avg, min, max)
- `variant`: build variant (default, nonbanked, etc.)
- `data_size_m`: data size in millions
- `groups`: number of groups
- `dtype`: data type
- `distribution`: value distribution
- `time_sec`: median time in seconds
- `times`: all run times

## Environment Variables

```bash
WARMUP_RUNS=1    # Warmup iterations (default: 1)
BENCH_RUNS=3     # Benchmark iterations (default: 3)
FORCE_REBUILD=1  # Force rebuild binaries
```

## Reproducibility

### EC2 Instances

| Instance | CPU | RAM | Storage |
|----------|-----|-----|---------|
| i8gd.4xlarge | 16x Graviton4 2.7GHz | 32GB | 950GB SSD |
| c8i.4xlarge | 16x Granite Rapids 3.9GHz | 32GB | EBS io2 90GB |
| c8a.4xlarge | 16x EPYC 9R45 4.5GHz | 32GB | EBS io2 90GB |

Use Ubuntu 24 as the OS.

### Graviton instance SSD setup
```bash
sudo parted /dev/nvme0n1 -- mklabel gpt
sudo parted /dev/nvme0n1 -- mkpart primary ext4 0% 200GB
sudo mkfs.ext4 /dev/nvme0n1p1
sudo mkdir -p /mnt/instance-store
sudo mount /dev/nvme0n1p1 /mnt/instance-store
cd /mnt/instance-store/
chmod 777 .
```

### Build toolchain (all instances)
```bash
sudo apt update
sudo apt install -y clang cmake ninja-build python3
export CC=clang
export CXX=clang++
git clone --depth 1 https://github.com/llvm/llvm-project.git
cd llvm-project
cmake -S llvm -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_TARGETS_TO_BUILD="X86"  # or "AArch64" for Graviton
ninja -C build clang lld
```

### Run benchmarks
```bash
git clone --recurse-submodules https://github.com/cwida/pac.git
cd pac/benchmark/pac_microbench
./build_variants.sh
./create_test_db.sh
nohup ./run_all.sh &
# Results in results/ directory. Experiments take ~6 hours, ~$3/machine.
```

### Analysis scripts
```
benchmark/pac_microbench/plot_simd_improvements.py
benchmark/pac_microbench/plot_count_optimizations.py
benchmark/pac_microbench/plot_minmax_optimizations.py
benchmark/pac_microbench/plot_sum_optimizations.py
benchmark/pac_microbench/approx_sum_stability.sql    # run with duckdb_default and duckdb_exactsum
benchmark/pac_microbench/bionmial.sql                # generates output for plot_hash_distribution.py
```
