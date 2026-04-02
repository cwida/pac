#!/usr/bin/env bash
# Attack 7: "20K small items" user — tests whether clipping catches
# a user whose individual rows are normal but total contribution is huge.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
NT=30
N=1000; FILT=3; TV_PER_ROW=50; TV_ROWS=20000
# Total contribution: 20000 * 50 = 1,000,000

echo "============================================="
echo "  ATTACK 7: 20K small items user"
echo "============================================="
echo "  N=$N background users (1 row each)"
echo "  Target user_id=0: $TV_ROWS rows x \$$TV_PER_ROW = \$$(( TV_ROWS * TV_PER_ROW ))"
echo "  filter<=3, $NT trials"
echo ""

FBG=$($DUCKDB -noheader -list -c \
    "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILT}) t(i);" | tr -d '[:space:]')
TV_TOTAL=$((TV_ROWS * TV_PER_ROW))
echo "Background SUM=$FBG, target total=$TV_TOTAL"
echo ""

# --- 7a: No clipping (baseline) ---
run_noprotection() {
    local cond=$1 seed=$2 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users SELECT 0, ${TV_PER_ROW} FROM generate_series(1,${TV_ROWS}) t(i);"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# --- 7b: Full-table Winsorization (clip each row to mu+3sigma=13661) ---
run_winsorized() {
    local cond=$1 seed=$2 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users SELECT 0, ${TV_PER_ROW} FROM generate_series(1,${TV_ROWS}) t(i);"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) t(i);
${insert}
UPDATE users SET acctbal = LEAST(acctbal, 13661) WHERE acctbal > 13661;
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# --- 7c: pac_clip_sum (clip after filter, with pre-aggregation) ---
run_clipsum() {
    local cond=$1 seed=$2 clip=$3 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users SELECT 0, ${TV_PER_ROW} FROM generate_series(1,${TV_ROWS}) t(i);"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SET pac_clip_support = ${clip};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

analyze() {
    local label=$1 in_f=$2 out_f=$3
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

SELECT 'Standard' AS clf,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > ${FBG} + ${TV_TOTAL}/2.0 THEN 1
        WHEN truth='out' AND v <= ${FBG} + ${TV_TOTAL}/2.0 THEN 1
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
}

# 7a: No clipping
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_noprotection in $seed)" >> "$IN_F"
    echo "out,$(run_noprotection out $seed)" >> "$OUT_F"
done
analyze "7a: No clipping (baseline)" "$IN_F" "$OUT_F"
rm -f "$IN_F" "$OUT_F"

# 7b: Full-table Winsorization
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_winsorized in $seed)" >> "$IN_F"
    echo "out,$(run_winsorized out $seed)" >> "$OUT_F"
done
analyze "7b: Winsorization (per-row clip to 13661)" "$IN_F" "$OUT_F"
rm -f "$IN_F" "$OUT_F"

# 7c: pac_clip_sum with clip_support=2
IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NT); do
    echo "in,$(run_clipsum in $seed 2)" >> "$IN_F"
    echo "out,$(run_clipsum out $seed 2)" >> "$OUT_F"
done
analyze "7c: pac_clip_sum (clip_support=2)" "$IN_F" "$OUT_F"
rm -f "$IN_F" "$OUT_F"
