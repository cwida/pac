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

trap 'echo ""; echo "Interrupted after $i/$RUNS runs ($ERRORS errors)"; exit 130' INT TERM

for i in $(seq 1 $RUNS); do
    ELAPSED=$(( $(date +%s) - START ))
    printf "\rRun %d/%d  [%dm%02ds elapsed, %d errors]" "$i" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60)) "$ERRORS"
    OUTPUT=$(echo "$SETUP" | cat - "$SCRIPT" | "$DUCKDB" "$DB" 2>&1)
    if [ $? -ne 0 ]; then
        ERRORS=$((ERRORS + 1))
        echo ""
        echo "ERROR: Run $i failed:"
        echo "$OUTPUT" | grep -i -E "error|exception|abort|fatal|catalog" | head -5
    fi
done

ELAPSED=$(( $(date +%s) - START ))
printf "\rDone: %d runs, %d errors, %dm%02ds total\n" "$RUNS" "$ERRORS" $((ELAPSED/60)) $((ELAPSED%60))

# Move result CSVs into the utility folder
for DEST in "benchmark/tpch/utility" "../benchmark/tpch/utility" "../../benchmark/tpch/utility"; do
    if [ -d "$DEST" ]; then
        mv -f q*.csv "$DEST/" 2>/dev/null
        echo "Results moved to $DEST/"
        break
    fi
done
