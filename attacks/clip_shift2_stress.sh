#!/usr/bin/env bash
# Stress-test shift=2 (4x levels) with hard-zero clipping.
# Focus on edge cases that 4x granularity might miss.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
CLIP=2

run_sum() {
    local cond=$1 seed=$2 n_users=$3 target_insert="$4" filter=$5
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${n_users}) t(i);
$([ "$cond" = "in" ] && echo "$target_insert")
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SET pac_clip_support = ${CLIP};
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

SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;

SELECT 'Best threshold' AS clf,
    printf('%.1f%%', MAX(acc)) AS best_accuracy,
    FIRST(threshold ORDER BY acc DESC) AS at_threshold
FROM (
    SELECT threshold,
        100.0*SUM(CASE
            WHEN truth='in' AND ABS(v - ${fbg}) > threshold THEN 1
            WHEN truth='out' AND ABS(v - ${fbg}) <= threshold THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM raw, generate_series(5000, 500000, 5000) thresholds(threshold)
    WHERE v IS NOT NULL
    GROUP BY threshold
);
SQL
    echo ""
}

FBG=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,3) t(i);" | tr -d '[:space:]')

echo "============================================="
echo "  SHIFT=2 STRESS TEST (4x levels, hard-zero)"
echo "  pac_clip_support=$CLIP"
echo "============================================="
echo ""

NT=30

# ---------------------------------------------------------------
# TEST 1: 3.5x outlier (within 4x boundary — should NOT be caught)
# Normal ~5000, target=17000 (3.4x)
# Both in level 3 (4096-16383)? Let's check:
# 5000: bit_pos=12, (12-4)>>1 = 4. Level 4.
# 17000: bit_pos=14, (14-4)>>1 = 5. Level 5. DIFFERENT!
# Actually 17000 might be caught. Let's try 15000:
# 15000: bit_pos=13, (13-4)>>1 = 4. Level 4. SAME as 5000!
# ---------------------------------------------------------------
echo "## TEST 1: 3x outlier (target=15000, same level as normal)"
echo "5000→level 4, 15000→level 4 (bit_pos 13, (13-4)/2=4). Same level."
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 15000);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "3x outlier tv=15000" "$IN_F" "$OUT_F" "$FBG" 15000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 2: Just above 4x (target=20000)
# 20000: bit_pos=14, (14-4)>>1 = 5. Level 5. Different from 5000 (level 4).
# ---------------------------------------------------------------
echo "## TEST 2: 4x outlier (target=20000, different level)"
echo "5000→level 4, 20000→level 5. Should be caught."
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 20000);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "4x outlier tv=20000" "$IN_F" "$OUT_F" "$FBG" 20000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 3: Two colluding outliers (still breaks it?)
# ---------------------------------------------------------------
echo "## TEST 3: Two colluding outliers (999999)"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 999999); INSERT INTO users VALUES (-1, 999999);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "Two colluders tv=999999" "$IN_F" "$OUT_F" "$FBG" 999999
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 4: Outlier at exact level boundary (16384 = start of level 5)
# 16384: bit_pos=14, (14-4)>>1 = 5. Normal at level 4.
# ---------------------------------------------------------------
echo "## TEST 4: Boundary outlier (target=16384, exact level 5 start)"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 16384);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "Boundary tv=16384" "$IN_F" "$OUT_F" "$FBG" 16384
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 5: Outlier just below boundary (16383 = max of level 4)
# 16383: bit_pos=13, (13-4)>>1 = 4. Same level as 5000.
# ---------------------------------------------------------------
echo "## TEST 5: Just-below-boundary (target=16383, still level 4)"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 16383);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "Just-below tv=16383" "$IN_F" "$OUT_F" "$FBG" 16383
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 6: Many small outliers (10 users at 15000, all in same level)
# They all go to level 4 like normal users → supported → no clipping
# ---------------------------------------------------------------
echo "## TEST 6: 10 users at 15000 (3x, same level, all 'supported')"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
MULTI="INSERT INTO users SELECT -(i+1), 15000 FROM generate_series(1,10) t(i); INSERT INTO users VALUES (0, 15000);"
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "$MULTI" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "10 users at 15000" "$IN_F" "$OUT_F" "$FBG" 15000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 7: Wide filter + moderate outlier (best case for clipping)
# ---------------------------------------------------------------
FBG999=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,999) t(i);" | tr -d '[:space:]')
echo "## TEST 7: Wide filter + moderate outlier (tv=50000)"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 50000);" 999)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 999)" >> "$OUT_F"
done
analyze "Wide filter tv=50000" "$IN_F" "$OUT_F" "$FBG999" 50000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 8: 20K small items (pre-aggregation test)
# ---------------------------------------------------------------
echo "## TEST 8: 20K small items (\$50 x 20000 = \$1M)"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users SELECT 0, 50 FROM generate_series(1,20000) t(i);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "20K items" "$IN_F" "$OUT_F" "$FBG" 1000000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 9: Composed queries (10 queries averaged)
# ---------------------------------------------------------------
echo "## TEST 9: 10 composed queries, tv=999999"
echo ""
IN_F=$(mktemp); OUT_F=$(mktemp)
for trial in $(seq 1 $NT); do
    for q in $(seq 1 10); do
        s=$((trial * 1000 + q))
        echo "in,${trial},${q},$(run_sum in $s 1000 "INSERT INTO users VALUES (0, 999999);" 3)" >> "$IN_F"
        echo "out,${trial},${q},$(run_sum out $s 1000 "" 3)" >> "$OUT_F"
    done
done
echo "=== 10 composed queries ==="
$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, split_part(c,',',2)::INT AS trial,
    split_part(c,',',3)::INT AS qid, TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',4) != '';

WITH votes AS (
    SELECT truth, trial,
        SUM(CASE WHEN v > ${FBG} + 999999/2.0 THEN 1 ELSE 0 END) AS yes, COUNT(*) AS total
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'Majority vote (10q)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes > total/2.0 THEN 1
        WHEN truth='out' AND yes <= total/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM votes;

WITH trial_stats AS (
    SELECT truth, trial, STDDEV(v) AS trial_std
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'Per-trial variance' AS method,
    printf('in_std=%.0f out_std=%.0f ratio=%.2f',
        AVG(CASE WHEN truth='in' THEN trial_std END),
        AVG(CASE WHEN truth='out' THEN trial_std END),
        AVG(CASE WHEN truth='in' THEN trial_std END) /
        NULLIF(AVG(CASE WHEN truth='out' THEN trial_std END), 0)) AS stats
FROM trial_stats;
SQL
echo ""
rm -f "$IN_F" "$OUT_F"

echo "============================================="
echo "  STRESS TEST COMPLETE"
echo "============================================="
