#!/usr/bin/env bash
# =============================================================================
# MIA v3: Deep investigation of the filter<=5 result (54.2%)
# =============================================================================
# Attack A with filter<=5 showed 54.2%, marginally above the 53% PAC bound.
# Let's run with more trials to get statistical confidence, and also try
# filter<=2 and filter<=1 to push harder.
#
# Also test: does the noise variance actually scale properly for small groups?
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
N_USERS=1000
TARGET_VALUE=50000
NUM_TRIALS=500  # More trials for statistical confidence

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
echo " MIA v3: Statistical Deep-Dive"
echo "============================================================"
echo ""

# ============================================================
# TEST 1: filter<=5 with 500 trials (check if 54.2% was real)
# ============================================================
echo "=== TEST 1: filter<=5, MI=1/128, 500 trials ==="

IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NUM_TRIALS); do
    run_query "in"  "$seed" "0.0078125" "5" >> "$IN_F"
    run_query "out" "$seed" "0.0078125" "5" >> "$OUT_F"
done

FBG5=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,5) t(i);" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s,'in' AS truth FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s,'out' AS truth FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE allr AS SELECT * FROM tin UNION ALL SELECT * FROM tout;
CREATE TABLE decisions AS SELECT *,CASE WHEN s>${FBG5}+${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess FROM allr;
SELECT 'filter<=5 (n=500)' AS test,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',ABS((SELECT AVG(s) FROM tin)-(SELECT AVG(s) FROM tout))) AS mean_gap,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS noise_std
FROM decisions;

-- Statistical significance test
-- Under null hypothesis (attack = random), accuracy ~ Binomial(n=1000, p=0.5)
-- std of accuracy = sqrt(0.5*0.5/1000) = 0.0158 = 1.58%
-- So 54.2% is (54.2-50)/1.58 = 2.66 std devs from 50% (p<0.01)
-- But 53% bound means we should compare against 53%, not 50%
SELECT 'Statistical test' AS note,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.2f',(100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 50.0) / (100.0*sqrt(0.25/COUNT(*)))) AS z_vs_50pct,
    printf('%.2f',(100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 53.0) / (100.0*sqrt(0.25/COUNT(*)))) AS z_vs_53pct_bound
FROM decisions;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# TEST 2: filter<=2 - even smaller group
# ============================================================
echo "=== TEST 2: filter<=2, MI=1/128, 500 trials ==="

IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NUM_TRIALS); do
    run_query "in"  "$seed" "0.0078125" "2" >> "$IN_F"
    run_query "out" "$seed" "0.0078125" "2" >> "$OUT_F"
done

FBG2=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,2) t(i);" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s,'in' AS truth FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s,'out' AS truth FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE allr AS SELECT * FROM tin UNION ALL SELECT * FROM tout;
CREATE TABLE decisions AS SELECT *,CASE WHEN s>${FBG2}+${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess FROM allr;
SELECT 'filter<=2 (n=500)' AS test,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',ABS((SELECT AVG(s) FROM tin)-(SELECT AVG(s) FROM tout))) AS mean_gap,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS noise_std
FROM decisions;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# TEST 3: filter<=1 (only target + 1 bg user) - extreme case
# ============================================================
echo "=== TEST 3: filter<=1 (near-singleton), MI=1/128, 500 trials ==="

IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NUM_TRIALS); do
    run_query "in"  "$seed" "0.0078125" "1" >> "$IN_F"
    run_query "out" "$seed" "0.0078125" "1" >> "$OUT_F"
done

FBG1=$($DUCKDB -noheader -list -c "SELECT (hash(1*31+7)%10000+1)::INTEGER;" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s,'in' AS truth FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s,'out' AS truth FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;

-- Check NULLs: PAC may return NULL for very sparse groups
SELECT 'filter<=1' AS test,
    (SELECT COUNT(*) FROM tin) AS in_results,
    (SELECT COUNT(*) FROM tout) AS out_results,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS noise_std_in,
    printf('%.0f',(SELECT STDDEV(s) FROM tout)) AS noise_std_out;

CREATE TABLE allr AS SELECT * FROM tin UNION ALL SELECT * FROM tout;
CREATE TABLE decisions AS SELECT *,CASE WHEN s>${FBG1}+${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess FROM allr;
SELECT 'filter<=1 (n=500)' AS test,
    printf('%.1f%%',100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy
FROM decisions;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# TEST 4: NULL analysis - does PAC return NULLs for small groups?
# ============================================================
echo "=== TEST 4: NULL frequency analysis ==="
echo "(PAC probabilistically returns NULL for sparse groups)"

IN_F=$(mktemp)
for seed in $(seq 1 200); do
    # Run query, capture output including empty lines (NULLs)
    result=$($DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= 2 OR user_id = 0;
SQL
    )
    if [ -z "$result" ] || [ "$result" = "" ]; then
        echo "NULL" >> "$IN_F"
    else
        echo "$result" >> "$IN_F"
    fi
done

NULL_COUNT=$(grep -c "NULL" "$IN_F" 2>/dev/null || echo "0")
TOTAL=$(wc -l < "$IN_F")
echo "  Filter<=2 with target: $NULL_COUNT NULLs out of $TOTAL queries ($(echo "scale=1; $NULL_COUNT * 100 / $TOTAL" | bc)%)"
rm -f "$IN_F"

echo ""
echo "============================================================"
echo " SUMMARY"
echo "============================================================"
echo "PAC theoretical MIA bound: ~53% (MI=1/128)"
echo "Random guess baseline:     50%"
echo ""
echo "Key findings:"
echo "  - Full population attack:  ~50% (noise completely drowns signal)"
echo "  - Narrow filter attack:    investigate if small groups leak"
echo "  - Per-query privacy:       each seed gives independent noise"
echo "============================================================"
