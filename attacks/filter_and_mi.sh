#!/usr/bin/env bash
# =============================================================================
# MIA v2: Three focused attacks against PAC Privacy
#   A) WHERE filter to narrow population (isolate target)
#   B) Vary MI budget (verify theoretical bounds)
#   C) Multi-query averaging via post-hoc analysis of single-query results
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
N_USERS=1000
TARGET_VALUE=50000
NUM_TRIALS=200

BG_SUM=$($DUCKDB -noheader -list -c "
SELECT SUM((hash(i * 31 + 7) % 10000 + 1)::INTEGER) FROM generate_series(1, ${N_USERS}) t(i);
" | tr -d '[:space:]')

run_query() {
    local condition=$1 seed=$2 mi=$3 filter=$4
    local insert="" where=""
    [ "$condition" = "in" ] && insert="INSERT INTO users VALUES (0, ${TARGET_VALUE});"
    [ "$filter" != "none" ] && where="WHERE user_id <= ${filter} OR user_id = 0"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = ${mi};
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users ${where};
SQL
}

echo "============================================================"
echo " MIA v2: Advanced Attacks against PAC Privacy"
echo "============================================================"
echo "Background SUM = $BG_SUM | Target = $TARGET_VALUE"
echo ""

# ============================================================
# ATTACK A: WHERE filter to narrow population
# ============================================================
echo "=== ATTACK A: Narrow WHERE filter ==="
echo "(Fewer background users => target is larger fraction of total)"
echo ""

for FILTER_SIZE in 50 10 5; do
    label="all"
    filter_arg="none"
    if [ "$FILTER_SIZE" -lt "$N_USERS" ]; then
        label="<=$FILTER_SIZE"
        filter_arg="$FILTER_SIZE"
    fi

    IN_F=$(mktemp); OUT_F=$(mktemp)
    for seed in $(seq 1 $NUM_TRIALS); do
        run_query "in"  "$seed" "0.0078125" "$filter_arg" >> "$IN_F"
        run_query "out" "$seed" "0.0078125" "$filter_arg" >> "$OUT_F"
    done

    # Exact filtered bg sum
    if [ "$filter_arg" = "none" ]; then
        FBG=$BG_SUM
    else
        FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILTER_SIZE}) t(i);" | tr -d '[:space:]')
    fi

    $DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s,'in' AS truth FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s,'out' AS truth FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE allr AS SELECT * FROM tin UNION ALL SELECT * FROM tout;
CREATE TABLE decisions AS SELECT *,CASE WHEN s>${FBG}+${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess FROM allr;
SELECT 'filter=${label}' AS attack,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS noise_std,
    printf('%.1f',${TARGET_VALUE}::DOUBLE/NULLIF((SELECT STDDEV(s) FROM tin),0)) AS snr
FROM decisions;
SQL
    rm -f "$IN_F" "$OUT_F"
    echo "  (filter=$label done)"
done

echo ""

# ============================================================
# ATTACK B: Vary MI budget
# ============================================================
echo "=== ATTACK B: Vary MI Budget ==="
echo "(Higher MI => less noise => easier to attack)"
echo ""

for MI in "0.0078125" "0.25" "1.0"; do
    IN_F=$(mktemp); OUT_F=$(mktemp)
    for seed in $(seq 1 $NUM_TRIALS); do
        run_query "in"  "$seed" "$MI" "none" >> "$IN_F"
        run_query "out" "$seed" "$MI" "none" >> "$OUT_F"
    done

    $DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s,'in' AS truth FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s,'out' AS truth FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE allr AS SELECT * FROM tin UNION ALL SELECT * FROM tout;
CREATE TABLE decisions AS SELECT *,CASE WHEN s>${BG_SUM}+${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess FROM allr;
SELECT 'MI=${MI}' AS setting,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS noise_std,
    printf('%.2f',${TARGET_VALUE}::DOUBLE/NULLIF((SELECT STDDEV(s) FROM tin),0)) AS snr
FROM decisions;
SQL
    rm -f "$IN_F" "$OUT_F"
    echo "  (MI=$MI done)"
done

echo ""

# ============================================================
# ATTACK C: Multi-query composition (post-hoc averaging)
# ============================================================
echo "=== ATTACK C: Multi-Query Averaging (post-hoc) ==="
echo "(Collect many single-query results, average K at a time)"
echo ""

# Collect 300 results for each condition (reuse from Attack B MI=1/128)
MANY=200
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $MANY); do
    run_query "in"  "$seed" "0.0078125" "none" >> "$IN_F"
    run_query "out" "$seed" "0.0078125" "none" >> "$OUT_F"
done

$DUCKDB -markdown <<SQL
CREATE TABLE in_raw AS SELECT row_number() OVER () AS id, column0::DOUBLE AS s FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE out_raw AS SELECT row_number() OVER () AS id, column0::DOUBLE AS s FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;

-- Simulate averaging K queries by grouping consecutive results
-- For K=1,5,10,20, compute the average of each group, then threshold

WITH ks(k) AS (VALUES (1),(5),(10),(20))
SELECT
    'K=' || k AS queries_per_trial,
    printf('%.1f%%', 100.0 *
        (SUM(CASE WHEN truth = 'in' AND avg_s > ${BG_SUM} + ${TARGET_VALUE}/2.0 THEN 1
                  WHEN truth = 'out' AND avg_s <= ${BG_SUM} + ${TARGET_VALUE}/2.0 THEN 1
                  ELSE 0 END)::DOUBLE / COUNT(*))) AS accuracy,
    printf('%.0f', STDDEV(avg_s) FILTER (WHERE truth='in')) AS avg_noise_std,
    COUNT(*) AS total_decisions
FROM (
    SELECT k, 'in' AS truth, (id-1)/k AS grp, AVG(s) AS avg_s
    FROM in_raw, ks GROUP BY k, grp
    HAVING COUNT(*) = k
    UNION ALL
    SELECT k, 'out' AS truth, (id-1)/k AS grp, AVG(s) AS avg_s
    FROM out_raw, ks GROUP BY k, grp
    HAVING COUNT(*) = k
) sub
GROUP BY k ORDER BY k;
SQL

rm -f "$IN_F" "$OUT_F"

echo ""
echo "============================================================"
echo "PAC theoretical bound: MI=1/128 => ~53% max MIA success"
echo "Random guess baseline: 50%"
echo "============================================================"
