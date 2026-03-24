#!/usr/bin/env bash
# Stress-test hard-zero clipping: try every attack angle we can think of.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
CLIP=2

run_sum() {
    local cond=$1 seed=$2 n_users=$3 target_val=$4 filter=$5 extra_sql="${6:-}"
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
SET pac_clip_support = ${CLIP};
${extra_sql}
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

-- Try MANY thresholds to find optimal
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

-- Mean-based classifier (v > midpoint)
SELECT 'Midpoint clf' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > (${fbg} + ${fbg} + ${tv}) / 2.0 THEN 1
        WHEN truth='out' AND v <= (${fbg} + ${fbg} + ${tv}) / 2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;

-- Likelihood ratio: compare distance to expected in vs expected out
SELECT 'LR clf' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ABS(v - ${fbg}::DOUBLE - ${tv}) < ABS(v - ${fbg}::DOUBLE) THEN 1
        WHEN truth='out' AND ABS(v - ${fbg}::DOUBLE - ${tv}) >= ABS(v - ${fbg}::DOUBLE) THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;
SQL
    echo ""
}

analyze_composed() {
    local label=$1 in_f=$2 out_f=$3 fbg=$4 tv=$5 nq=$6
    echo "=== $label ==="
    $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, split_part(c,',',2)::INT AS trial,
    split_part(c,',',3)::INT AS qid, TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${in_f}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${out_f}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',4) != '';

-- Running average accuracy
WITH cum AS (
    SELECT truth, trial, qid,
        AVG(v) OVER (PARTITION BY truth, trial ORDER BY qid) AS ravg
    FROM raw WHERE v IS NOT NULL
)
SELECT qid AS n_queries,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ravg > ${fbg} + ${tv}/2.0 THEN 1
        WHEN truth='out' AND ravg <= ${fbg} + ${tv}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM cum GROUP BY qid ORDER BY qid;

-- Majority vote
WITH votes AS (
    SELECT truth, trial,
        SUM(CASE WHEN v > ${fbg} + ${tv}/2.0 THEN 1 ELSE 0 END) AS yes, COUNT(*) AS total
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'Majority vote (${nq}q)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes > total/2.0 THEN 1
        WHEN truth='out' AND yes <= total/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM votes;

-- Variance-based: use per-trial variance across queries
WITH trial_stats AS (
    SELECT truth, trial, STDDEV(v) AS trial_std
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'Variance of queries' AS method,
    printf('in_std=%.0f out_std=%.0f ratio=%.2f',
        AVG(CASE WHEN truth='in' THEN trial_std END),
        AVG(CASE WHEN truth='out' THEN trial_std END),
        AVG(CASE WHEN truth='in' THEN trial_std END) /
        NULLIF(AVG(CASE WHEN truth='out' THEN trial_std END), 0)) AS stats
FROM trial_stats;
SQL
    echo ""
}

FBG=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,3) t(i);" | tr -d '[:space:]')
FBG999=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,999) t(i);" | tr -d '[:space:]')

echo "============================================="
echo "  HARD-ZERO STRESS TEST"
echo "  pac_clip_support=$CLIP, trying to break it"
echo "============================================="
echo "Background: filter<=3 sum=$FBG, filter<=999 sum=$FBG999"
echo ""

# ---------------------------------------------------------------
# TEST 1: High trial count (100 trials for statistical power)
# ---------------------------------------------------------------
echo "## TEST 1: High trial count (60 trials)"
echo ""
NT=60
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 999999);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "100 trials, N=1000, tv=999999, filt<=3" "$IN_F" "$OUT_F" "$FBG" 999999
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 2: Composed queries (10 queries, reduce noise by averaging)
# ---------------------------------------------------------------
echo "## TEST 2: Composed queries (10 queries)"
echo ""
NT=30; NQ=10
IN_F=$(mktemp); OUT_F=$(mktemp)
for trial in $(seq 1 $NT); do
    for q in $(seq 1 $NQ); do
        s=$((trial * 1000 + q))
        echo "in,${trial},${q},$(run_sum in $s 1000 "INSERT INTO users VALUES (0, 999999);" 3)" >> "$IN_F"
        echo "out,${trial},${q},$(run_sum out $s 1000 "" 3)" >> "$OUT_F"
    done
done
analyze_composed "50 trials x 10 queries" "$IN_F" "$OUT_F" "$FBG" 999999 10
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 3: Moderate outlier (50000 — same magnitude level as normal)
# ---------------------------------------------------------------
echo "## TEST 3: Moderate outlier (target=50000, same magnitude level)"
echo "Normal users ~5000, target ~50000 — both in level 2 (4096-65535)"
echo "The bitmap should show this level as supported, so NO clipping occurs"
echo ""
NT=30
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 50000);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "Moderate outlier tv=50000" "$IN_F" "$OUT_F" "$FBG" 50000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 4: Two colluding outliers
# ---------------------------------------------------------------
echo "## TEST 4: Two colluding outliers"
echo "Two users with 999999 — level 3 now has 2 bitmap bits (meets threshold=2)"
echo "Hard-zero might NOT clip because level has enough support!"
echo ""
NT=30
IN_F=$(mktemp); OUT_F=$(mktemp)
TWO_INSERT="INSERT INTO users VALUES (0, 999999); INSERT INTO users VALUES (-1, 999999);"
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "$TWO_INSERT" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
# For two outliers: "in" filter catches user 0 (user -1 is NOT in filter <= 3)
# But user -1's value still goes into the table and affects the bitmap!
analyze "Two outliers (0 and -1), filt<=3" "$IN_F" "$OUT_F" "$FBG" 999999
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 5: Dandan's filter probing attack
# ---------------------------------------------------------------
echo "## TEST 5: Dandan's filter probing"
echo "Attacker tries different filters to see if clipping behavior changes."
echo "If the outlier is present, the bitmap at level 3 has a bit set."
echo "Query 1: filter<=3 (includes user 0 if present)"
echo "Query 2: filter<=999 (includes everyone)"
echo "Difference in results might reveal membership."
echo ""
NT=30
IN_F1=$(mktemp); OUT_F1=$(mktemp)
IN_F2=$(mktemp); OUT_F2=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 999999);" 3)" >> "$IN_F1"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F1"
    echo "in,$(run_sum in $((seed+10000)) 1000 "INSERT INTO users VALUES (0, 999999);" 999)" >> "$IN_F2"
    echo "out,$(run_sum out $((seed+10000)) 1000 "" 999)" >> "$OUT_F2"
done
analyze "Filter<=3 (narrow)" "$IN_F1" "$OUT_F1" "$FBG" 999999
analyze "Filter<=999 (wide)" "$IN_F2" "$OUT_F2" "$FBG999" 999999

echo "=== Cross-filter differential ==="
$DUCKDB -markdown <<SQL
CREATE TABLE narrow AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v,
    ROW_NUMBER() OVER (PARTITION BY split_part(c,',',1)) AS trial
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F1}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F1}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

CREATE TABLE wide AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v,
    ROW_NUMBER() OVER (PARTITION BY split_part(c,',',1)) AS trial
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F2}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F2}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

-- Use wide-narrow difference as a feature
SELECT 'Wide-narrow diff' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN n.truth='in'  AND w.v - n.v > 0 THEN 1
        WHEN n.truth='out' AND w.v - n.v <= 0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM narrow n JOIN wide w ON n.truth = w.truth AND n.trial = w.trial
WHERE n.v IS NOT NULL AND w.v IS NOT NULL;
SQL
echo ""
rm -f "$IN_F1" "$OUT_F1" "$IN_F2" "$OUT_F2"

# ---------------------------------------------------------------
# TEST 6: 20K small items with high trial count
# ---------------------------------------------------------------
echo "## TEST 6: 20K small items, 50 trials"
echo ""
NT=30
IN_F=$(mktemp); OUT_F=$(mktemp)
MULTI_INSERT="INSERT INTO users SELECT 0, 50 FROM generate_series(1,20000) t(i);"
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "$MULTI_INSERT" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "20K items x \$50, filt<=3" "$IN_F" "$OUT_F" "$FBG" 1000000
rm -f "$IN_F" "$OUT_F"

# ---------------------------------------------------------------
# TEST 7: Borderline outlier (value at level boundary)
# ---------------------------------------------------------------
echo "## TEST 7: Borderline outlier (target=65536, exactly level 3 boundary)"
echo "Just barely crosses into level 3 — minimum unsupported value"
echo ""
NT=30
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum in $seed 1000 "INSERT INTO users VALUES (0, 65536);" 3)" >> "$IN_F"
    echo "out,$(run_sum out $seed 1000 "" 3)" >> "$OUT_F"
done
analyze "Borderline tv=65536" "$IN_F" "$OUT_F" "$FBG" 65536
rm -f "$IN_F" "$OUT_F"

echo "============================================="
echo "  STRESS TEST COMPLETE"
echo "============================================="
