#!/bin/bash
# Usage from the main project folder:
#   bash benchmark/clickbench/run_utility_clickbench_100.sh [database] [duckdb_binary]
#
# Examples:
#   bash benchmark/clickbench/run_utility_clickbench_100.sh clickbench_micro.db
#   bash benchmark/clickbench/run_utility_clickbench_100.sh clickbench.db ./build/release/duckdb

DB="${1:-clickbench_micro.db}"
DUCKDB="${2:-./build/release/duckdb}"
SCRIPT="benchmark/clickbench/clickbench_queries/utility.sql"
RUNS=100

# PAC setup: read from setup.sql
SETUP_FILE="benchmark/clickbench/clickbench_queries/setup.sql"
ERRORS=0
START=$(date +%s)

trap 'echo ""; echo "Interrupted after $i/$RUNS runs ($ERRORS errors)"; exit 130' INT TERM

for i in $(seq 1 $RUNS); do
    ELAPSED=$(( $(date +%s) - START ))
    printf "\rRun %d/%d  [%dm%02ds elapsed, %d errors]" "$i" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60)) "$ERRORS"
    OUTPUT=$(cat "$SETUP_FILE" "$SCRIPT" | "$DUCKDB" "$DB" 2>&1)
    if [ $? -ne 0 ]; then
        ERRORS=$((ERRORS + 1))
        echo ""
        echo "ERROR: Run $i failed:"
        echo "$OUTPUT" | grep -i -E "error|exception|abort|fatal|catalog" | head -5
    fi
done

ELAPSED=$(( $(date +%s) - START ))
printf "\rDone: %d runs, %d errors, %dm%02ds total\n" "$RUNS" "$ERRORS" $((ELAPSED/60)) $((ELAPSED%60))
