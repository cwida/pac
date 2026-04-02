#!/usr/bin/env bash
# Test pac_clip_sum against membership inference attacks.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

run_sum() {
    local cond=$1 seed=$2 n_users=$3 target_val=$4 filter=$5 clip=$6
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${target_val});"
    local clip_sql=""
    [ "$clip" != "off" ] && clip_sql="SET pac_clip_support = ${clip};"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${n_users}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
${clip_sql}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filter} OR user_id = 0;
SQL
}

# Run one scenario: collect trials, analyze, print results
run_scenario() {
    local label=$1 n=$2 tv=$3 filt=$4 clip=$5 ntrials=$6

    local FBG=$($DUCKDB -noheader -list -c \
        "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${filt}) t(i);" | tr -d '[:space:]')

    local IN_F=$(mktemp) OUT_F=$(mktemp)
    for seed in $(seq 1 $ntrials); do
        echo "in,$(run_sum in $seed $n $tv $filt $clip)" >> "$IN_F"
        echo "out,$(run_sum out $seed $n $tv $filt $clip)" >> "$OUT_F"
    done

    echo "=== $label | N=$n filt<=$filt tv=$tv clip=$clip ==="
    $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;

SELECT 'Standard' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > ${FBG} + ${tv}/2.0 THEN 1
        WHEN truth='out' AND v <= ${FBG} + ${tv}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL
UNION ALL
SELECT 'Var>200k',
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ABS(v - ${FBG}) > 200000 THEN 1
        WHEN truth='out' AND ABS(v - ${FBG}) <= 200000 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*))
FROM raw WHERE v IS NOT NULL;
SQL
    echo ""
    rm -f "$IN_F" "$OUT_F"
}

NT=30

echo "=========================================="
echo "  pac_clip_sum ATTACK EVALUATION"
echo "=========================================="
echo ""

# --- Attack 1: Baseline variance classifier (simplest) ---
echo "## ATTACK 1: Single-query variance classifier"
echo "N=1000, target=999999, filter<=3, $NT trials"
echo ""
for CLIP in off 2 3; do
    run_scenario "atk1" 1000 999999 3 "$CLIP" $NT
done

# --- Attack 2: Wide filter (clipping best-case) ---
echo "## ATTACK 2: Wide filter (all users in aggregation)"
echo "N=1000, target=999999, filter<=999, $NT trials"
echo ""
for CLIP in off 2 5 10; do
    run_scenario "atk2" 1000 999999 999 "$CLIP" $NT
done

# --- Attack 3: 10K users ---
echo "## ATTACK 3: 10K users, extreme outlier"
echo "N=10000, target=9999999, filter<=2, $NT trials"
echo ""
for CLIP in off 2 3; do
    run_scenario "atk3" 10000 9999999 2 "$CLIP" $NT
done

# --- Attack 4: Over-clipping ---
echo "## ATTACK 4: Over-clipping (too aggressive)"
echo "N=1000, target=999999, filter<=3, 15 trials"
echo "clip_support=10 with only 3-4 users => no supported levels"
echo ""
for CLIP in off 5 10; do
    run_scenario "atk4" 1000 999999 3 "$CLIP" 15
done

# --- Attack 5: Wide filter + over-clipping ---
echo "## ATTACK 5: Wide filter + aggressive clipping"
echo "N=1000, target=999999, filter<=999, 15 trials"
echo ""
for CLIP in off 50 100; do
    run_scenario "atk5" 1000 999999 999 "$CLIP" 15
done

# --- Attack 6: Clip after filter vs clip on full table (Dandan's concern) ---
# pac_clip_sum clips AFTER filtering (only filtered rows enter the aggregate).
# An adversary might exploit this: the clipping behavior differs depending on
# which users are in the filter. Compare filter-then-clip (what pac_clip_sum does)
# vs clip-all-then-filter (manual pre-clipping of the full table, then query).
echo "## ATTACK 6: Clip-after-filter vs clip-full-table"
echo "N=1000, target=999999, filter<=3, $NT trials"
echo "Tests Dandan's hypothesis: clipping after filtering leaks more than"
echo "clipping the entire dataset. We compare pac_clip_sum (clips filtered rows)"
echo "vs manual pre-clipping of all rows then querying without clip_support."
echo ""

# 6a: pac_clip_sum (clip after filter) — already covered in atk1 clip=2
echo "### 6a: clip-after-filter (pac_clip_support=2)"
echo "(Same as Attack 1 clip=2)"
echo ""

# 6b: clip-full-table-then-query (no pac_clip_support, but data is pre-clipped)
run_sum_preclipped() {
    local cond=$1 seed=$2 n_users=$3 target_val=$4 filter=$5 clip_support=$6
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${target_val});"
    # Pre-clip ALL rows at percentile bounds (simulating clip-on-full-table)
    # Use the same magnitude-level idea: clip values to level-2 max (65535)
    # This ensures the billionaire is clipped BEFORE filtering.
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${n_users}) t(i);
${insert}
-- Pre-clip entire table: clamp to [0, mu + 3*sigma] of the baseline distribution
-- Baseline: ~U(1,10000), mu~5000, sigma~2887, so 3sigma clip ~ 13661
UPDATE users SET acctbal = LEAST(acctbal, 13661) WHERE acctbal > 13661;
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filter} OR user_id = 0;
SQL
}

echo "### 6b: clip-full-table-then-query (pre-clip all to mu+3sigma=13661)"
echo ""
FBG_CLIP=$($DUCKDB -noheader -list -c \
    "SELECT SUM(LEAST((hash(i*31+7)%10000+1)::INTEGER, 13661)) FROM generate_series(1,3) t(i);" | tr -d '[:space:]')
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_sum_preclipped in $seed 1000 999999 3 off)" >> "$IN_F"
    echo "out,$(run_sum_preclipped out $seed 1000 999999 3 off)" >> "$OUT_F"
done
echo "=== atk6b | N=1000 filt<=3 tv=999999 pre-clip=13661 ==="
$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';
SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;
SELECT 'Standard' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > ${FBG_CLIP} + 999999/2.0 THEN 1
        WHEN truth='out' AND v <= ${FBG_CLIP} + 999999/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL
UNION ALL
SELECT 'Var>200k',
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ABS(v - ${FBG_CLIP}) > 200000 THEN 1
        WHEN truth='out' AND ABS(v - ${FBG_CLIP}) <= 200000 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*))
FROM raw WHERE v IS NOT NULL;
SQL
echo ""
rm -f "$IN_F" "$OUT_F"
