#!/bin/bash
# Utility experiment: naive (N independent pac_sum) vs optimized (pac_sum_counters + list_transform + pac_noised)
#
# Usage:
#   bash benchmark/utility_listtransform/run.sh [database] [duckdb_binary] [runs]
#
# Example:
#   bash benchmark/utility_listtransform/run.sh tpch_sf1.db ./build/release/duckdb 100

set -euo pipefail

DB="${1:-tpch_sf1.db}"
DUCKDB="${2:-./build/release/duckdb}"
RUNS="${3:-100}"
DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$DIR/results.csv"

SETUP=$(cat "$DIR/setup.sql")

# Split a SQL file into individual queries delimited by "-- Q" comments
# Returns queries in array variable QUERIES
split_queries() {
    local file="$1"
    QUERIES=()
    local current=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^--\ Q[0-9] ]] && [ -n "$current" ]; then
            QUERIES+=("$current")
            current=""
        fi
        # Skip comment-only lines for the query body
        if [[ ! "$line" =~ ^-- ]]; then
            current+="$line "
        fi
    done < "$file"
    if [ -n "$current" ]; then
        QUERIES+=("$current")
    fi
}

# Run a single query, return the scalar result (first column, first row)
# $1 = sql, $2 = seed (optional)
run_query() {
    local sql="$1"
    local seed="${2:-}"
    local seed_sql=""
    if [ -n "$seed" ]; then
        seed_sql="SET pac_seed = $seed;"
    fi
    echo "$SETUP $seed_sql $sql" | "$DUCKDB" "$DB" -csv -noheader 2>/dev/null | head -1
}

echo "=== Utility Experiment: naive vs optimized ==="
echo "Database: $DB"
echo "Runs: $RUNS"
echo "Output: $OUT"

# Parse query files
split_queries "$DIR/true_queries.sql"
TRUE_QUERIES=("${QUERIES[@]}")
NQUERIES=${#TRUE_QUERIES[@]}
echo "Queries: $NQUERIES"

split_queries "$DIR/naive_queries.sql"
NAIVE_QUERIES=("${QUERIES[@]}")

split_queries "$DIR/optimized_queries.sql"
OPT_QUERIES=("${QUERIES[@]}")

# Get ground truth values
echo -n "Computing ground truth..."
TRUE_VALUES=()
for q in $(seq 0 $((NQUERIES - 1))); do
    val=$(run_query "${TRUE_QUERIES[$q]}")
    TRUE_VALUES+=("$val")
done
echo " done"
echo "True values: ${TRUE_VALUES[*]}"

# Write CSV header
echo "run,query,num_ratios,variant,value,true_value" > "$OUT"

# Run experiment
START=$(date +%s)
trap 'echo ""; echo "Interrupted after run $i/$RUNS"; exit 130' INT TERM

for i in $(seq 1 "$RUNS"); do
    ELAPSED=$(( $(date +%s) - START ))
    printf "\rRun %d/%d  [%dm%02ds elapsed]" "$i" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60))

    for q in $(seq 0 $((NQUERIES - 1))); do
        qnum=$((q + 1))
        true_val="${TRUE_VALUES[$q]}"

        ELAPSED=$(( $(date +%s) - START ))
        printf "\rRun %d/%d  Q%d/%d  [%dm%02ds elapsed]" "$i" "$RUNS" "$qnum" "$NQUERIES" $((ELAPSED/60)) $((ELAPSED%60))

        # Naive
        val=$(run_query "${NAIVE_QUERIES[$q]}" "$i")
        echo "$i,$qnum,$qnum,naive,$val,$true_val" >> "$OUT"

        # Optimized
        val=$(run_query "${OPT_QUERIES[$q]}" "$i")
        echo "$i,$qnum,$qnum,optimized,$val,$true_val" >> "$OUT"
    done
done

ELAPSED=$(( $(date +%s) - START ))
printf "\rDone: %d runs, %dm%02ds total\n" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60))
echo "Results: $OUT"

# Quick summary
echo ""
echo "=== Summary ==="
awk -F, 'NR>1 {
    key = $3 "," $4
    err = ($6 != 0) ? 100.0 * ($5 - $6) / (($6 < 0 ? -$6 : $6) > 0.00001 ? ($6 < 0 ? -$6 : $6) : 0.00001) : 0
    if (err < 0) err = -err
    sum[key] += err
    cnt[key]++
}
END {
    printf "%-10s %-12s %s\n", "num_ratios", "variant", "mean_rel_error_%"
    for (k in sum) {
        split(k, a, ",")
        printf "%-10s %-12s %.4f\n", a[1], a[2], sum[k] / cnt[k]
    }
}' "$OUT" | sort -t' ' -k1,1n -k2,2
