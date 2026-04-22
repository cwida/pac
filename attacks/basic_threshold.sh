#!/usr/bin/env bash
# =============================================================================
# Membership Inference Attack (MIA) against PAC Privacy
# =============================================================================
# The adversary knows the full background data and the target's value.
# They observe a PAC-noised SUM(acctbal) and try to infer if the target
# is in the database, using a simple threshold test (optimal for Gaussian noise).
#
# PAC theoretical bounds (Theorem 4.1):
#   Prior = 50% | MI=1/128 => ~53% | MI=1/4 => ~84%
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N_USERS=1000
TARGET_VALUE=50000
NUM_TRIALS=500

echo "============================================================"
echo " PAC Privacy - Membership Inference Attack"
echo "============================================================"
echo "  Population:   $N_USERS users, acctbal in [1..10000]"
echo "  Target:       acctbal = $TARGET_VALUE (outlier)"
echo "  Trials:       $NUM_TRIALS per condition (in/out)"
echo "  MI budget:    1/128"
echo ""

# Get exact background sum
BG_SUM=$($DUCKDB -noheader -list -c "
SELECT SUM((hash(i * 31 + 7) % 10000 + 1)::INTEGER)
FROM generate_series(1, ${N_USERS}) t(i);
" | tr -d '[:space:]')

echo "Background SUM     = $BG_SUM"
echo "Expected w/ target = $((BG_SUM + TARGET_VALUE))"
echo "Target contribution= $TARGET_VALUE"
echo ""

# Helper: run one PAC query, return the noised SUM
run_pac_query() {
    local condition=$1  # "in" or "out"
    local seed=$2

    local insert_target=""
    if [ "$condition" = "in" ]; then
        insert_target="INSERT INTO users VALUES (0, ${TARGET_VALUE});"
    fi

    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users
SELECT i, ((hash(i * 31 + 7) % 10000) + 1)::INTEGER
FROM generate_series(1, ${N_USERS}) t(i);
${insert_target}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users;
SQL
}

# Collect results
echo "Collecting target-IN results..."
IN_FILE=$(mktemp)
for seed in $(seq 1 $NUM_TRIALS); do
    run_pac_query "in" "$seed"
done > "$IN_FILE"

echo "  Got $(wc -l < "$IN_FILE") results"
echo "Collecting target-OUT results..."
OUT_FILE=$(mktemp)
for seed in $(seq 1 $NUM_TRIALS); do
    run_pac_query "out" "$seed"
done > "$OUT_FILE"

echo "  Got $(wc -l < "$OUT_FILE") results"
echo ""

# Analyze in DuckDB
$DUCKDB <<ANALYSIS
.mode markdown

CREATE TABLE in_results AS
SELECT column0::DOUBLE AS pac_sum, 'in' AS truth
FROM read_csv('${IN_FILE}', columns={'column0': 'VARCHAR'}, header=false)
WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;

CREATE TABLE out_results AS
SELECT column0::DOUBLE AS pac_sum, 'out' AS truth
FROM read_csv('${OUT_FILE}', columns={'column0': 'VARCHAR'}, header=false)
WHERE TRY_CAST(column0 AS DOUBLE) IS NOT NULL;

CREATE TABLE all_results AS
SELECT * FROM in_results UNION ALL SELECT * FROM out_results;

-- Optimal threshold = bg_sum + target_value/2 (midpoint between hypotheses)
CREATE TABLE decisions AS
SELECT *,
    CASE WHEN pac_sum > ${BG_SUM} + ${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess
FROM all_results;

SELECT '=== CONFUSION MATRIX ===' AS '';
SELECT
    truth AS actual,
    guess AS predicted,
    COUNT(*) AS count,
    printf('%.1f%%', 100.0 * COUNT(*)::DOUBLE /
        SUM(COUNT(*)) OVER (PARTITION BY truth)) AS rate
FROM decisions
GROUP BY truth, guess
ORDER BY truth DESC, guess DESC;

SELECT '=== ATTACK PERFORMANCE ===' AS '';
SELECT
    printf('%.1f%%', 100.0 * SUM(CASE WHEN truth = guess THEN 1 ELSE 0 END)::DOUBLE / COUNT(*))
        AS attack_accuracy,
    printf('%.1f%%', 100.0 * SUM(CASE WHEN truth='in' AND guess='in' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN truth='in' THEN 1 ELSE 0 END),0))
        AS true_positive_rate,
    printf('%.1f%%', 100.0 * SUM(CASE WHEN truth='out' AND guess='in' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN truth='out' THEN 1 ELSE 0 END),0))
        AS false_positive_rate
FROM decisions;

SELECT '=== OBSERVED SUM DISTRIBUTIONS ===' AS '';
SELECT
    truth,
    COUNT(*) AS n,
    printf('%.0f', AVG(pac_sum)) AS mean,
    printf('%.0f', STDDEV(pac_sum)) AS stddev,
    printf('%.0f', MIN(pac_sum)) AS min,
    printf('%.0f', MAX(pac_sum)) AS max
FROM all_results GROUP BY truth ORDER BY truth;

SELECT '=== vs THEORETICAL BOUNDS ===' AS '';
SELECT
    printf('%.1f%%', 100.0 * SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE / COUNT(*))
        AS "Measured Attack Success",
    '50.0%' AS "Random Guess",
    '~53%' AS "PAC Bound (MI=1/128)",
    '~84%' AS "PAC Bound (MI=1/4)"
FROM decisions;

ANALYSIS

echo ""
echo "============================================================"
echo "INTERPRETATION:"
echo "  ≈50% => PAC noise fully prevents MIA"
echo "  >53% => exceeds PAC theoretical bound for MI=1/128"
echo "============================================================"

rm -f "$IN_FILE" "$OUT_FILE"
