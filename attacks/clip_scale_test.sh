#!/usr/bin/env bash
# Test pac_clip_scale=true (scale outliers to nearest supported level)
# vs default false (hard-zero / omit). Peter's hypothesis: scaling should
# be safe because outliers become a minority in an already-supported bucket.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

run_sum() {
    local cond=$1 seed=$2 n_users=$3 target_val=$4 filter=$5 clip=$6 scale=$7
    local insert=""
    [ "$cond" = "in" ] && insert="$target_val"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${n_users}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SET pac_clip_support = ${clip};
SET pac_clip_scale = ${scale};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filter} OR user_id = 0;
SQL
}

analyze() {
    local label=$1 in_f=$2 out_f=$3 fbg=$4 tv=$5
    echo "=== $label ==="
    $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${in_f}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${out_f}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std,
    printf('%.0f', MIN(v)) AS min, printf('%.0f', MAX(v)) AS max, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;

-- Best threshold search
SELECT 'Best threshold' AS clf,
    printf('%.1f%%', MAX(acc)) AS best_accuracy,
    FIRST(threshold ORDER BY acc DESC) AS at_threshold
FROM (
    SELECT threshold,
        100.0*SUM(CASE
            WHEN truth='in' AND ABS(v - ${fbg}) > threshold THEN 1
            WHEN truth='out' AND ABS(v - ${fbg}) <= threshold THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM raw, generate_series(10000, 500000, 10000) thresholds(threshold)
    WHERE v IS NOT NULL
    GROUP BY threshold
);

-- Midpoint classifier
SELECT 'Midpoint clf' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > (${fbg} + ${fbg} + ${tv}) / 2.0 THEN 1
        WHEN truth='out' AND v <= (${fbg} + ${fbg} + ${tv}) / 2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;

-- Likelihood ratio
SELECT 'LR clf' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ABS(v - ${fbg}::DOUBLE - ${tv}) < ABS(v - ${fbg}::DOUBLE) THEN 1
        WHEN truth='out' AND ABS(v - ${fbg}::DOUBLE - ${tv}) >= ABS(v - ${fbg}::DOUBLE) THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;
SQL
    echo ""
}

FBG=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,3) t(i);" | tr -d '[:space:]')
FBG999=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,999) t(i);" | tr -d '[:space:]')

echo "================================================================="
echo "  pac_clip_scale COMPARISON TEST"
echo "  Comparing scale=true (Peter's preference) vs scale=false (hard-zero)"
echo "================================================================="
echo "Background: filter<=3 sum=$FBG, filter<=999 sum=$FBG999"
echo ""

run_test() {
    local test_name=$1 n_users=$2 target_sql=$3 filter=$4 tv=$5 fbg=$6
    local nt=30

    for clip in 2 10 50; do
        for scale in false true; do
            local label="${test_name} [clip=${clip}, scale=${scale}]"
            IN_F=$(mktemp); OUT_F=$(mktemp)
            for seed in $(seq 1 $nt); do
                echo "in,$(run_sum in $seed $n_users "$target_sql" $filter $clip $scale)" >> "$IN_F"
                echo "out,$(run_sum out $seed $n_users "" $filter $clip $scale)" >> "$OUT_F"
            done
            analyze "$label" "$IN_F" "$OUT_F" "$fbg" "$tv"
            rm -f "$IN_F" "$OUT_F"
        done
    done
}

# ---------------------------------------------------------------
# TEST 1: Extreme outlier, small filter
# Hard-zero baseline: 52.4% (random) at clip=2
# ---------------------------------------------------------------
echo "## TEST 1: Extreme outlier (tv=999999), small filter (<=3), N=1000"
echo ""
run_test "T1" 1000 "INSERT INTO users VALUES (0, 999999);" 3 999999 "$FBG"

# ---------------------------------------------------------------
# TEST 2: Extreme outlier, wide filter
# Hard-zero baseline: ~55% at clip=2
# ---------------------------------------------------------------
echo "## TEST 2: Extreme outlier (tv=999999), wide filter (<=999), N=1000"
echo ""
run_test "T2" 1000 "INSERT INTO users VALUES (0, 999999);" 999 999999 "$FBG999"

# ---------------------------------------------------------------
# TEST 3: Moderate outlier (same magnitude level as normal users)
# Hard-zero baseline: 76.5% at clip=2 (already leaks)
# ---------------------------------------------------------------
echo "## TEST 3: Moderate outlier (tv=50000), small filter (<=3)"
echo "Normal users ~5000, target ~50000 — both in level 2 (4096-65535)"
echo ""
run_test "T3" 1000 "INSERT INTO users VALUES (0, 50000);" 3 50000 "$FBG"

# ---------------------------------------------------------------
# TEST 4: 20K small items (multi-row outlier)
# Hard-zero baseline: 52.9% at clip=2
# ---------------------------------------------------------------
echo "## TEST 4: 20K small items (50 x 20000 = 1M), small filter (<=3)"
echo ""
run_test "T4" 1000 "INSERT INTO users SELECT 0, 50 FROM generate_series(1,20000) t(i);" 3 1000000 "$FBG"

# ---------------------------------------------------------------
# TEST 5: Borderline outlier (exactly at level boundary)
# Hard-zero baseline: 52.9% at clip=2
# ---------------------------------------------------------------
echo "## TEST 5: Borderline outlier (tv=65536, level 3 boundary), small filter (<=3)"
echo ""
run_test "T5" 1000 "INSERT INTO users VALUES (0, 65536);" 3 65536 "$FBG"

echo "================================================================="
echo "  SCALE TEST COMPLETE"
echo "================================================================="
