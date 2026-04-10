#!/usr/bin/env bash
# Clip parameter sweep: systematic evaluation of clipping parameters,
# mean±t*sigma clipping, composition attacks, and combined approaches.
#
# METHODOLOGY:
# - N=1000 background users with uniform acctbal in [1, 10000]
# - Target user injected with value TV=999999 (100x outlier)
# - For each trial: run query with target ("in") and without ("out"), using a fresh pac_seed
# - Best-threshold accuracy: search over 50 evenly-spaced thresholds between min and max
#   observed result. For each threshold, classify as "in" if result > threshold, "out"
#   otherwise. Report the threshold that maximizes classification accuracy.
# - 50% accuracy = random guessing (no information leakage)
# - 100% accuracy = perfect MIA (complete privacy breach)
# - Composition attack (Exp 5a): run N queries per trial, average results, then classify.
#   Noise decreases at 1/sqrt(N) but signal stays constant — tests whether clipping
#   prevents the attacker from accumulating advantage across queries.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N=1000        # background users
TV=999999     # target outlier value
MI=0.0078125  # pac_mi = 1/128
NT=30         # trials per scenario

# ============================================================================
# Core query functions
# ============================================================================

# Standard PAC query (with optional pac_clip_support)
run_sum() {
    local cond=$1 seed=$2 filt=$3 clip=$4
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    local clip_sql=""
    [ "$clip" != "off" ] && clip_sql="SET pac_clip_support = ${clip};"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = ${MI};
SET pac_seed = ${seed};
${clip_sql}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filt} OR user_id = 0;
SQL
}

# Sigma-based pre-clip: aggregate per PU, iteratively clip to mean±t*sigma, then PAC query
run_sum_srini() {
    local cond=$1 seed=$2 filt=$3 t_sigma=$4 clip=${5:-off}
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    local clip_sql=""
    [ "$clip" != "off" ] && clip_sql="SET pac_clip_support = ${clip};"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) t(i);
${insert}

-- Pre-aggregate per PU
CREATE TABLE pu_totals AS SELECT user_id, SUM(acctbal)::INTEGER AS total FROM users GROUP BY user_id;

-- Iterative mean±t*sigma clipping (3 rounds)
UPDATE pu_totals SET total = LEAST(GREATEST(total::BIGINT,
  (SELECT (AVG(total) - ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals)),
  (SELECT (AVG(total) + ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals))::INTEGER;
UPDATE pu_totals SET total = LEAST(GREATEST(total::BIGINT,
  (SELECT (AVG(total) - ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals)),
  (SELECT (AVG(total) + ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals))::INTEGER;
UPDATE pu_totals SET total = LEAST(GREATEST(total::BIGINT,
  (SELECT (AVG(total) - ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals)),
  (SELECT (AVG(total) + ${t_sigma} * STDDEV(total))::BIGINT FROM pu_totals))::INTEGER;

-- Replace users with clipped values
DELETE FROM users;
INSERT INTO users SELECT user_id, total FROM pu_totals;
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = ${MI};
SET pac_seed = ${seed};
${clip_sql}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filt} OR user_id = 0;
SQL
}

# ============================================================================
# Analysis function: takes two files (in/out), computes stats + accuracy
# ============================================================================
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

-- Stats
SELECT truth, printf('%.0f',AVG(v)) AS mean, printf('%.0f',STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth ORDER BY truth;

-- Best-threshold classifier (search 50 thresholds)
WITH thresholds AS (
    SELECT UNNEST(generate_series(
        (SELECT (MIN(v))::BIGINT FROM raw WHERE v IS NOT NULL),
        (SELECT (MAX(v))::BIGINT FROM raw WHERE v IS NOT NULL),
        GREATEST(1, ((SELECT MAX(v)-MIN(v) FROM raw WHERE v IS NOT NULL)/50)::BIGINT)
    )) AS t
),
accs AS (
    SELECT t, 100.0*SUM(CASE
        WHEN truth='in' AND v > t THEN 1
        WHEN truth='out' AND v <= t THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM raw, thresholds WHERE v IS NOT NULL GROUP BY t
)
SELECT printf('%.1f%%', MAX(acc)) AS best_accuracy FROM accs;
SQL
    echo ""
}

# ============================================================================
# EXPERIMENT 1: pac_clip_support sweep
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 1: pac_clip_support (b) sweep"
echo "###############################################"
echo ""

for FILT in 3 50 999; do
    for CLIP in off 2 5 10 20 40 60; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_sum in $seed $FILT $CLIP)" >> "$IN_F"
            echo "out,$(run_sum out $seed $FILT $CLIP)" >> "$OUT_F"
        done
        analyze "filt<=${FILT} clip=${CLIP}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

# ============================================================================
# EXPERIMENT 3: Sigma-based iterative mean±t*sigma clipping
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 3: Sigma-based mean±t*sigma clipping"
echo "###############################################"
echo ""

for FILT in 3 50 999; do
    for T in 2 3 4 5; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_sum_srini in $seed $FILT $T)" >> "$IN_F"
            echo "out,$(run_sum_srini out $seed $FILT $T)" >> "$OUT_F"
        done
        analyze "sigma t=${T} filt<=${FILT}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

# ============================================================================
# EXPERIMENT 4: Combined (sigma pre-clip + level-clip)
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 4: Combined (sigma + level)"
echo "###############################################"
echo ""

for FILT in 3 999; do
    for T in 2 3 4; do
        for CLIP in 2 5 10; do
            IN_F=$(mktemp); OUT_F=$(mktemp)
            for seed in $(seq 1 $NT); do
                echo "in,$(run_sum_srini in $seed $FILT $T $CLIP)" >> "$IN_F"
                echo "out,$(run_sum_srini out $seed $FILT $T $CLIP)" >> "$OUT_F"
            done
            analyze "Combined t=${T} clip=${CLIP} filt<=${FILT}" "$IN_F" "$OUT_F"
            rm -f "$IN_F" "$OUT_F"
        done
    done
done

# ============================================================================
# EXPERIMENT 5a: Composition attack — many queries, with vs without clip
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 5a: Composition (up to 500 queries)"
echo "###############################################"
echo ""

FILT=3
for CLIP in off 2; do
    echo "--- clip=${CLIP} ---"

    # Run all 500 queries per trial upfront, then analyze at different NQ cutoffs
    IN_F=$(mktemp); OUT_F=$(mktemp)
    MAX_NQ=500
    for trial in $(seq 1 $NT); do
        for q in $(seq 1 $MAX_NQ); do
            s=$((trial * 10000 + q))
            echo "in,${trial},${q},$(run_sum in $s $FILT $CLIP)" >> "$IN_F"
            echo "out,${trial},${q},$(run_sum out $s $FILT $CLIP)" >> "$OUT_F"
        done
        echo "  trial ${trial}/${NT} done (clip=${CLIP})" >&2
    done

    # Analyze at different NQ cutoffs
    for NQ in 1 5 10 25 50 100 200 500; do
        echo "=== clip=${CLIP} NQ=${NQ} filt<=${FILT} ==="
        $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth,
       TRY_CAST(split_part(c,',',2) AS INTEGER) AS trial,
       TRY_CAST(split_part(c,',',3) AS INTEGER) AS qid,
       TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',4) != '';

-- Running average up to NQ queries
WITH avgs AS (
    SELECT truth, trial, AVG(v) AS avg_v
    FROM raw WHERE qid <= ${NQ} AND v IS NOT NULL
    GROUP BY truth, trial
)
SELECT truth, printf('%.0f', AVG(avg_v)) AS mean, printf('%.0f', STDDEV(avg_v)) AS std, COUNT(*) AS n
FROM avgs GROUP BY truth ORDER BY truth;

-- Best threshold on averaged values
WITH avgs AS (
    SELECT truth, trial, AVG(v) AS v
    FROM raw WHERE qid <= ${NQ} AND v IS NOT NULL
    GROUP BY truth, trial
),
thresholds AS (
    SELECT UNNEST(generate_series(
        (SELECT (MIN(v))::BIGINT FROM avgs),
        (SELECT (MAX(v))::BIGINT FROM avgs),
        GREATEST(1, ((SELECT MAX(v)-MIN(v) FROM avgs)/50)::BIGINT)
    )) AS t
),
accs AS (
    SELECT t, 100.0*SUM(CASE
        WHEN truth='in' AND v > t THEN 1
        WHEN truth='out' AND v <= t THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM avgs, thresholds GROUP BY t
)
SELECT printf('%.1f%%', MAX(acc)) AS best_accuracy FROM accs;
SQL
        echo ""
    done
    rm -f "$IN_F" "$OUT_F"
done

# ============================================================================
# EXPERIMENT 5b: Adaptive cross-filter (adaptive edge probing)
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 5b: Cross-filter differential"
echo "###############################################"
echo ""

for CLIP in off 2; do
    IN_F=$(mktemp); OUT_F=$(mktemp)
    for trial in $(seq 1 $NT); do
        s1=$((trial * 100 + 1)); s2=$((trial * 100 + 2))
        a_in=$(run_sum in $s1 3 $CLIP)
        b_in=$(run_sum in $s2 999 $CLIP)
        a_out=$(run_sum out $s1 3 $CLIP)
        b_out=$(run_sum out $s2 999 $CLIP)
        echo "in,${a_in},${b_in}" >> "$IN_F"
        echo "out,${a_out},${b_out}" >> "$OUT_F"
    done

    echo "=== Cross-filter clip=${CLIP} ==="
    $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth,
       TRY_CAST(split_part(c,',',2) AS DOUBLE) AS narrow,
       TRY_CAST(split_part(c,',',3) AS DOUBLE) AS wide
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
);

-- Stats per query type
SELECT 'narrow' AS query, truth,
    printf('%.0f',AVG(narrow)) AS mean, printf('%.0f',STDDEV(narrow)) AS std
FROM raw GROUP BY truth ORDER BY truth;

SELECT 'wide' AS query, truth,
    printf('%.0f',AVG(wide)) AS mean, printf('%.0f',STDDEV(wide)) AS std
FROM raw GROUP BY truth ORDER BY truth;

SELECT 'diff' AS query, truth,
    printf('%.0f',AVG(narrow-wide)) AS mean, printf('%.0f',STDDEV(narrow-wide)) AS std
FROM raw GROUP BY truth ORDER BY truth;

-- Best accuracy per signal: narrow alone, wide alone, differential
WITH ths AS (SELECT UNNEST(generate_series(-5000000,5000000,100000)) AS t)
SELECT 'narrow' AS signal, printf('%.1f%%', MAX(100.0*SUM(CASE
    WHEN truth='in' AND narrow > t THEN 1 WHEN truth='out' AND narrow <= t THEN 1
    ELSE 0 END)::DOUBLE / COUNT(*))) AS best_acc
FROM raw, ths GROUP BY t
UNION ALL
SELECT 'wide', printf('%.1f%%', MAX(100.0*SUM(CASE
    WHEN truth='in' AND wide > t THEN 1 WHEN truth='out' AND wide <= t THEN 1
    ELSE 0 END)::DOUBLE / COUNT(*)))
FROM raw, ths GROUP BY t
UNION ALL
SELECT 'diff(narrow-wide)', printf('%.1f%%', MAX(100.0*SUM(CASE
    WHEN truth='in' AND (narrow-wide) > t THEN 1 WHEN truth='out' AND (narrow-wide) <= t THEN 1
    ELSE 0 END)::DOUBLE / COUNT(*)))
FROM raw, ths GROUP BY t;
SQL
    echo ""
    rm -f "$IN_F" "$OUT_F"
done

# ============================================================================
# EXPERIMENT 5c: Adaptive filter sweep
# ============================================================================
echo "###############################################"
echo "  EXPERIMENT 5c: Filter sweep (attacker probes)"
echo "###############################################"
echo ""

for CLIP in off 2; do
    for FILT in 2 3 4 5 10 20 50 100 500 999; do
        IN_F=$(mktemp); OUT_F=$(mktemp)
        for seed in $(seq 1 $NT); do
            echo "in,$(run_sum in $seed $FILT $CLIP)" >> "$IN_F"
            echo "out,$(run_sum out $seed $FILT $CLIP)" >> "$OUT_F"
        done
        analyze "filter_sweep clip=${CLIP} filt<=${FILT}" "$IN_F" "$OUT_F"
        rm -f "$IN_F" "$OUT_F"
    done
done

echo "###############################################"
echo "  DONE"
echo "###############################################"
