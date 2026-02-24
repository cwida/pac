#!/bin/bash
# Usage from the main project folder:
#   bash benchmark/tpch/run_utility_tpch_100.sh [database] [duckdb_binary]
#
# Examples:
#   bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db
#   bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db ./build/release/duckdb

DB="${1:-tpch_sf30.db}"
DUCKDB="${2:-./build/release/duckdb}"
SCRIPT="benchmark/tpch/utility_tpch.sql"
RUNS=100

SETUP="INSTALL tpch; LOAD tpch;"
ERRORS=0
START=$(date +%s)

for i in $(seq 1 $RUNS); do
    ELAPSED=$(( $(date +%s) - START ))
    printf "\rRun %d/%d  [%dm%02ds elapsed, %d errors]" "$i" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60)) "$ERRORS"
    if ! echo "$SETUP" | cat - "$SCRIPT" | "$DUCKDB" "$DB" > /dev/null 2>&1; then
        ERRORS=$((ERRORS + 1))
        echo ""
        echo "ERROR: Run $i failed"
    fi
done

ELAPSED=$(( $(date +%s) - START ))
printf "\rDone: %d runs, %d errors, %dm%02ds total\n" "$RUNS" "$ERRORS" $((ELAPSED/60)) $((ELAPSED%60))
