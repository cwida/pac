#!/usr/bin/env bash
# Real-world skewed data: power-law distributed attack evaluation.
# Tests whether clipping works on naturally skewed distributions.
#
# METHODOLOGY:
# - N background users with power-law earnings (95% $100-500, 3% $500-1500, 2% $2K-5K)
# - Target user injected with extreme value (e.g. $50,000)
# - For each trial: run query with target ("in") and without ("out")
# - Best-threshold accuracy: search over 50 thresholds to find the one that
#   maximizes classification accuracy (assigns "in" if result > threshold, "out" otherwise)
# - An accuracy of 50% = random guessing (no information leakage)
# - An accuracy of 100% = perfect MIA (complete privacy breach)
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

MI=0.0078125  # pac_mi = 1/128
NT=30

run_skewed() {
    local cond=$1 seed=$2 n=$3 tv=$4 filt=$5 clip=$6
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO drivers VALUES (0, ${tv});"
    local clip_sql=""
    [ "$clip" != "off" ] && clip_sql="SET pac_clip_support = ${clip};"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE drivers(driver_id INTEGER, daily_earnings INTEGER);
INSERT INTO drivers
SELECT i,
    CASE
        WHEN hash(i*17+7) % 100 < 2  THEN ((hash(i*31+7) % 3000) + 2000)
        WHEN hash(i*17+7) % 100 < 5  THEN ((hash(i*31+7) % 1000) + 500)
        ELSE ((hash(i*31+7) % 400) + 100)
    END
FROM generate_series(1, ${n}) t(i);
${insert}
ALTER TABLE drivers ADD PAC_KEY(driver_id);
ALTER TABLE drivers SET PU;
SET pac_mi = ${MI};
SET pac_seed = ${seed};
${clip_sql}
SELECT SUM(daily_earnings) FROM drivers WHERE driver_id <= ${filt} OR driver_id = 0;
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

SELECT truth, printf('%.0f',AVG(v)) AS mean, printf('%.0f',STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;

-- Best-threshold classifier: search 50 evenly spaced thresholds between min and max result.
-- For each threshold, classify "in" if result > threshold, "out" otherwise. Report max accuracy.
WITH ths AS (SELECT UNNEST(generate_series(
    (SELECT (MIN(v))::BIGINT FROM raw WHERE v IS NOT NULL),
    (SELECT (MAX(v))::BIGINT FROM raw WHERE v IS NOT NULL),
    GREATEST(1, ((SELECT MAX(v)-MIN(v) FROM raw WHERE v IS NOT NULL)/50)::BIGINT)
)) AS t),
accs AS (
    SELECT t, 100.0*SUM(CASE
        WHEN truth='in' AND v > t THEN 1 WHEN truth='out' AND v <= t THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM raw, ths WHERE v IS NOT NULL GROUP BY t
)
SELECT printf('%.1f%%', MAX(acc)) AS best_accuracy FROM accs;
SQL
    echo ""
}

echo "###############################################"
echo "  REAL-WORLD SKEWED DATA: Power-law earnings"
echo "###############################################"
echo ""
echo "Background: power-law distributed driver earnings (fixed across trials)"
echo "95% earn \$100-\$500, 3% earn \$500-\$1500, 2% earn \$2K-\$5K"
echo "pac_mi = ${MI}, ${NT} trials per scenario"
echo ""

# --- Test 1: $50K outlier, various filters and clips ---
echo "## Extreme outlier (\$50,000 = 100x normal)"
echo ""
for FILT in 3 50 999; do
    for CLIP in off 2 5; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_skewed in $seed 1000 50000 $FILT $CLIP)" >> "$IN_F"
            echo "out,$(run_skewed out $seed 1000 50000 $FILT $CLIP)" >> "$OUT_F"
        done
        analyze "tv=50000 filt<=${FILT} clip=${CLIP}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

# --- Test 2: $5K outlier (moderate — same range as top 2% of background) ---
echo "## Moderate outlier (\$5,000 = within high-earner range)"
echo ""
for FILT in 3 50 999; do
    for CLIP in off 2; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_skewed in $seed 1000 5000 $FILT $CLIP)" >> "$IN_F"
            echo "out,$(run_skewed out $seed 1000 5000 $FILT $CLIP)" >> "$OUT_F"
        done
        analyze "tv=5000 filt<=${FILT} clip=${CLIP}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

# --- Test 3: $3K natural outlier (high earner, indistinguishable from background) ---
echo "## Natural outlier (\$3,000 = typical high earner)"
echo ""
for FILT in 3 50; do
    for CLIP in off 2; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_skewed in $seed 1000 3000 $FILT $CLIP)" >> "$IN_F"
            echo "out,$(run_skewed out $seed 1000 3000 $FILT $CLIP)" >> "$OUT_F"
        done
        analyze "tv=3000 filt<=${FILT} clip=${CLIP}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

echo "###############################################"
echo "  DONE"
echo "###############################################"
