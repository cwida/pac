#!/usr/bin/env bash
# =============================================================================
# MIA FINAL: Definitive test with 1000 trials on the best attack vectors
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
N_USERS=1000
TARGET_VALUE=50000
NUM_TRIALS=1000

run_query() {
    local condition=$1 seed=$2 filter=$3
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
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users ${where};
SQL
}

echo "============================================================"
echo " MIA FINAL: Definitive Statistical Tests (n=1000 per cell)"
echo "============================================================"
echo ""

# Test 1: Full population (baseline - should be ~50%)
echo "--- Test 1: Full population (n=$NUM_TRIALS) ---"
IN_F=$(mktemp); OUT_F=$(mktemp)
for s in $(seq 1 $NUM_TRIALS); do
    run_query "in" "$s" "none" >> "$IN_F"
    run_query "out" "$s" "none" >> "$OUT_F"
done

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
SELECT 'Full pop' AS test,
    (SELECT COUNT(*) FROM tin) AS n_in,
    (SELECT COUNT(*) FROM tout) AS n_out,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS std_in,
    printf('%.0f',(SELECT STDDEV(s) FROM tout)) AS std_out;
-- Accuracy
WITH allr AS (
    SELECT s,'in' AS truth FROM tin UNION ALL SELECT s,'out' AS truth FROM tout
),
dec AS (
    SELECT *, CASE WHEN s > 4876582 + 25000 THEN 'in' ELSE 'out' END AS guess FROM allr
)
SELECT printf('%.1f%%', 100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.2f', (SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 0.5) / sqrt(0.25/COUNT(*))) AS z_score
FROM dec;
SQL
rm -f "$IN_F" "$OUT_F"
echo ""

# Test 2: filter<=2 (best attack)
echo "--- Test 2: filter<=2 (n=$NUM_TRIALS) ---"
IN_F=$(mktemp); OUT_F=$(mktemp)
for s in $(seq 1 $NUM_TRIALS); do
    run_query "in" "$s" "2" >> "$IN_F"
    run_query "out" "$s" "2" >> "$OUT_F"
done

FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,2) t(i);" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
SELECT 'filter<=2' AS test,
    (SELECT COUNT(*) FROM tin) AS n_in,
    (SELECT COUNT(*) FROM tout) AS n_out,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS std_in,
    printf('%.0f',(SELECT STDDEV(s) FROM tout)) AS std_out;
WITH allr AS (
    SELECT s,'in' AS truth FROM tin UNION ALL SELECT s,'out' AS truth FROM tout
),
dec AS (
    SELECT *, CASE WHEN s > ${FBG} + 25000 THEN 'in' ELSE 'out' END AS guess FROM allr
)
SELECT printf('%.1f%%', 100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.2f', (SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 0.5) / sqrt(0.25/COUNT(*))) AS "z vs 50%",
    printf('%.2f', (SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 0.53) / sqrt(0.25/COUNT(*))) AS "z vs 53%"
FROM dec;
SQL
rm -f "$IN_F" "$OUT_F"
echo ""

# Test 3: filter<=5
echo "--- Test 3: filter<=5 (n=$NUM_TRIALS) ---"
IN_F=$(mktemp); OUT_F=$(mktemp)
for s in $(seq 1 $NUM_TRIALS); do
    run_query "in" "$s" "5" >> "$IN_F"
    run_query "out" "$s" "5" >> "$OUT_F"
done

FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,5) t(i);" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE tin AS SELECT column0::DOUBLE AS s FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
CREATE TABLE tout AS SELECT column0::DOUBLE AS s FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false) WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;
SELECT 'filter<=5' AS test,
    (SELECT COUNT(*) FROM tin) AS n_in,
    (SELECT COUNT(*) FROM tout) AS n_out,
    printf('%.0f',(SELECT AVG(s) FROM tin)) AS mean_in,
    printf('%.0f',(SELECT AVG(s) FROM tout)) AS mean_out,
    printf('%.0f',(SELECT STDDEV(s) FROM tin)) AS std_in,
    printf('%.0f',(SELECT STDDEV(s) FROM tout)) AS std_out;
WITH allr AS (
    SELECT s,'in' AS truth FROM tin UNION ALL SELECT s,'out' AS truth FROM tout
),
dec AS (
    SELECT *, CASE WHEN s > ${FBG} + 25000 THEN 'in' ELSE 'out' END AS guess FROM allr
)
SELECT printf('%.1f%%', 100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.2f', (SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 0.5) / sqrt(0.25/COUNT(*))) AS "z vs 50%",
    printf('%.2f', (SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*) - 0.53) / sqrt(0.25/COUNT(*))) AS "z vs 53%"
FROM dec;
SQL
rm -f "$IN_F" "$OUT_F"

echo ""
echo "============================================================"
echo " FINAL RESULTS SUMMARY"
echo "============================================================"
echo ""
echo "Interpretation of z-scores:"
echo "  |z| > 1.96 => statistically significant at p<0.05"
echo "  |z| > 2.58 => statistically significant at p<0.01"
echo ""
echo "PAC Privacy Bounds:"
echo "  Random guess:    50%"
echo "  MI=1/128 bound:  ~53%"
echo "  MI=1/4 bound:    ~84%"
echo ""
echo "If 'z vs 53%' > 1.96, the attack EXCEEDS the PAC theoretical bound."
echo "============================================================"
