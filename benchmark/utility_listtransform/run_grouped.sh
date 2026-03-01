#!/bin/bash
# Utility experiment with GROUP BY: naive vs optimized on grouped data
# Adds GROUP BY o_orderkey % NGROUPS to existing queries for amplified noise effect
#
# Usage:
#   bash benchmark/utility_listtransform/run_grouped.sh [database] [duckdb_binary] [runs] [ngroups] [skew_alpha]
#
# skew_alpha: Zipf skew parameter for l_extendedprice (0 = no skew, 20 = heavy skew)
#   The database must already have TPC-H tables loaded.
#   When skew_alpha > 0, the script applies: l_extendedprice = 900 + 104100 * pow(random(), alpha)

set -euo pipefail

DB="${1:-tpch_sf1.db}"
DUCKDB="${2:-./build/release/duckdb}"
RUNS="${3:-1}"
NGROUPS="${4:-100}"
SKEW_ALPHA="${5:-0}"
DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$DIR/results_grouped.csv"

SETUP=$(cat "$DIR/setup.sql")

# Apply skew if requested — work on a copy to avoid modifying the original
if [ "$SKEW_ALPHA" != "0" ]; then
    ORIG_DB="$DB"
    DB="${DB%.db}_skew${SKEW_ALPHA}.db"
    echo "Copying $ORIG_DB -> $DB (to preserve original)..."
    cp "$ORIG_DB" "$DB"
    echo "Applying Zipf skew (alpha=$SKEW_ALPHA) to l_extendedprice..."
    echo "$SETUP SELECT setseed(0.42); UPDATE lineitem SET l_extendedprice = 900.0 + 104100.0 * pow(random(), $SKEW_ALPHA);" \
        | "$DUCKDB" "$DB" 2>/dev/null
    echo "Done."
fi

# Transform a query: add GROUP BY o_orderkey % N
# Input: "SELECT CAST(... AS DOUBLE) AS result FROM ... WHERE ...;"
# Output: "SELECT o_orderkey % N AS grp, CAST(... AS DOUBLE) AS result FROM ... WHERE ... GROUP BY grp ORDER BY grp;"
add_groupby() {
    local sql="$1"
    local ng="$2"
    # Add grp column after SELECT
    sql="${sql/SELECT CAST/SELECT o_orderkey % $ng AS grp, CAST}"
    # Strip trailing whitespace and semicolon, then add GROUP BY
    sql="$(echo "$sql" | sed 's/[[:space:]]*;[[:space:]]*$//')"
    sql="$sql GROUP BY grp ORDER BY grp;"
    echo "$sql"
}

# Split a SQL file into individual queries delimited by "-- Q" comments
split_queries() {
    local file="$1"
    QUERIES=()
    local current=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^--\ Q[0-9] ]] && [ -n "$current" ]; then
            QUERIES+=("$current")
            current=""
        fi
        if [[ ! "$line" =~ ^-- ]]; then
            current+="$line "
        fi
    done < "$file"
    if [ -n "$current" ]; then
        QUERIES+=("$current")
    fi
}

echo "=== Utility Experiment (Grouped): naive vs optimized ==="
echo "Database: $DB"
echo "Runs: $RUNS"
echo "Groups: $NGROUPS"
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

# Get ground truth values (per group)
echo -n "Computing ground truth ($NGROUPS groups per query)..."
declare -A TRUE_MAP  # TRUE_MAP[q,grp] = value
for q in $(seq 0 $((NQUERIES - 1))); do
    gsql=$(add_groupby "${TRUE_QUERIES[$q]}" "$NGROUPS")
    while IFS=, read -r grp val; do
        TRUE_MAP["$q,$grp"]="$val"
    done < <(echo "$SETUP $gsql" | "$DUCKDB" "$DB" -csv -noheader 2>/dev/null)
done
echo " done"

# Write CSV header
echo "run,query,num_ratios,grp,variant,value,true_value" > "$OUT"

# Run experiment
START=$(date +%s)
trap 'echo ""; echo "Interrupted after run $i/$RUNS"; exit 130' INT TERM

for i in $(seq 1 "$RUNS"); do
    for q in $(seq 0 $((NQUERIES - 1))); do
        qnum=$((q + 1))

        ELAPSED=$(( $(date +%s) - START ))
        printf "\rRun %d/%d  Q%d/%d  [%dm%02ds elapsed]" "$i" "$RUNS" "$qnum" "$NQUERIES" $((ELAPSED/60)) $((ELAPSED%60))

        seed_sql="SET pac_seed = $i;"

        # Naive
        nsql=$(add_groupby "${NAIVE_QUERIES[$q]}" "$NGROUPS")
        while IFS=, read -r grp val; do
            true_val="${TRUE_MAP["$q,$grp"]}"
            echo "$i,$qnum,$qnum,$grp,naive,$val,$true_val" >> "$OUT"
        done < <(echo "$SETUP $seed_sql $nsql" | "$DUCKDB" "$DB" -csv -noheader 2>/dev/null)

        # Optimized
        osql=$(add_groupby "${OPT_QUERIES[$q]}" "$NGROUPS")
        while IFS=, read -r grp val; do
            true_val="${TRUE_MAP["$q,$grp"]}"
            echo "$i,$qnum,$qnum,$grp,optimized,$val,$true_val" >> "$OUT"
        done < <(echo "$SETUP $seed_sql $osql" | "$DUCKDB" "$DB" -csv -noheader 2>/dev/null)
    done
done

ELAPSED=$(( $(date +%s) - START ))
printf "\rDone: %d runs, %dm%02ds total\n" "$RUNS" $((ELAPSED/60)) $((ELAPSED%60))
echo "Results: $OUT"

# Quick summary
echo ""
echo "=== Summary (mean relative error % across all groups) ==="
awk -F, 'NR>1 {
    key = $3 "," $5
    err = ($7 != 0) ? 100.0 * ($6 - $7) / (($7 < 0 ? -$7 : $7) > 0.00001 ? ($7 < 0 ? -$7 : $7) : 0.00001) : 0
    if (err < 0) err = -err
    sum[key] += err
    cnt[key]++
    if (err > mx[key]) mx[key] = err
}
END {
    printf "%-10s %-12s %8s %8s\n", "num_ratios", "variant", "mean_err", "max_err"
    for (k in sum) {
        split(k, a, ",")
        printf "%-10s %-12s %8.4f %8.4f\n", a[1], a[2], sum[k] / cnt[k], mx[k]
    }
}' "$OUT" | sort -t' ' -k1,1n -k2,2
