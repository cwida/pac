#!/usr/bin/env bash
# Clipping experiment: does pre-PAC outlier clipping reduce attack success + improve utility?
#
# For each clipping threshold t in {1, 2, 3, 5, inf}, clips data at μ ± t·σ
# (recursive until convergence), then runs PAC and measures attack accuracy + utility.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N=1000; TV=999999; FILT=3; NTRIALS=30
CLIP_ITERS=20
T_VALUES="1 2 3 5 inf"

echo "============================================================"
echo "  CLIPPING EXPERIMENT"
echo "  Does pre-PAC outlier clipping reduce attack accuracy?"
echo "============================================================"
echo "  N=$N users, target=$TV, filter<=$FILT, $NTRIALS trials"
echo "  Clipping thresholds (t): $T_VALUES"
echo "============================================================"
echo ""

# Generate SQL for recursive clipping: CLIP_ITERS rounds of UPDATE at μ ± t·σ.
# Each round recomputes μ,σ from the current data. The WHERE clause ensures
# convergence — once all values are within bounds, subsequent rounds are no-ops.
gen_clip_sql() {
    local t=$1 sql="" i
    for i in $(seq 1 $CLIP_ITERS); do
        sql+="UPDATE users SET acctbal = LEAST(GREATEST(acctbal,
    (SELECT (AVG(acctbal) - ${t} * STDDEV_POP(acctbal))::INTEGER FROM users)),
    (SELECT (AVG(acctbal) + ${t} * STDDEV_POP(acctbal))::INTEGER FROM users))
WHERE acctbal < (SELECT (AVG(acctbal) - ${t} * STDDEV_POP(acctbal))::INTEGER FROM users)
   OR acctbal > (SELECT (AVG(acctbal) + ${t} * STDDEV_POP(acctbal))::INTEGER FROM users);
"
    done
    printf '%s' "$sql"
}

# Ground truth: no clipping, no PAC noise
run_true_unclipped() {
    local cond=$1 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    $DUCKDB -noheader -list <<SQL
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# Ground truth: clipped, no PAC noise
run_true_clipped() {
    local cond=$1 t_clip=$2 insert="" clip_sql=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    [ "$t_clip" != "inf" ] && clip_sql=$(gen_clip_sql "$t_clip")
    $DUCKDB -noheader -list <<SQL
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
${clip_sql}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# PAC-noised query after clipping
run_clipped_pac() {
    local cond=$1 seed=$2 t_clip=$3 insert="" clip_sql=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    [ "$t_clip" != "inf" ] && clip_sql=$(gen_clip_sql "$t_clip")
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
${clip_sql}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# --- Compute unclipped ground truths ---
echo "Computing unclipped ground truths..."
TRUE_IN=$(run_true_unclipped in | tr -d '[:space:]')
TRUE_OUT=$(run_true_unclipped out | tr -d '[:space:]')
echo "  Unclipped: in=$TRUE_IN  out=$TRUE_OUT  diff=$((TRUE_IN - TRUE_OUT))"

# --- Collect results ---
RESULTS_F=$(mktemp); GROUND_F=$(mktemp)

for t in $T_VALUES; do
    echo ""
    echo "--- t=$t ---"

    # Clipped ground truths (deterministic, compute once per t)
    CLIP_IN=$(run_true_clipped in "$t" | tr -d '[:space:]')
    CLIP_OUT=$(run_true_clipped out "$t" | tr -d '[:space:]')
    echo "  Clipped truth: in=$CLIP_IN  out=$CLIP_OUT  diff=$((CLIP_IN - CLIP_OUT))"
    echo "${t},in,${CLIP_IN}" >> "$GROUND_F"
    echo "${t},out,${CLIP_OUT}" >> "$GROUND_F"

    # Run PAC trials
    for seed in $(seq 1 $NTRIALS); do
        v_in=$(run_clipped_pac in "$seed" "$t" | tr -d '[:space:]')
        v_out=$(run_clipped_pac out "$seed" "$t" | tr -d '[:space:]')
        echo "${t},in,${seed},${v_in}" >> "$RESULTS_F"
        echo "${t},out,${seed},${v_out}" >> "$RESULTS_F"
        printf "."
    done
    echo " done"
done

echo ""
echo "============================================================"
echo "  RESULTS"
echo "============================================================"
echo ""

# --- Analysis ---
$DUCKDB -markdown <<SQL
-- Load PAC trial results
CREATE TABLE results AS
SELECT split_part(c,',',1) AS t_val,
       split_part(c,',',2) AS truth,
       split_part(c,',',3)::INT AS trial,
       TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (SELECT column0 AS c FROM read_csv('${RESULTS_F}', columns={'column0':'VARCHAR'}, header=false))
WHERE split_part(c,',',4) != '';

-- Load clipped ground truths
CREATE TABLE ground AS
SELECT split_part(c,',',1) AS t_val,
       split_part(c,',',2) AS truth,
       split_part(c,',',3)::DOUBLE AS clipped_true
FROM (SELECT column0 AS c FROM read_csv('${GROUND_F}', columns={'column0':'VARCHAR'}, header=false));

-- Main summary table
WITH attack AS (
    SELECT r.t_val,
        -- Variance classifier: |v - background| > 200000
        100.0 * SUM(CASE
            WHEN r.truth='in'  AND ABS(r.v - ${TRUE_OUT}) > 200000 THEN 1
            WHEN r.truth='out' AND ABS(r.v - ${TRUE_OUT}) <= 200000 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS var_acc,
        -- Midpoint classifier: v > (clipped_in + clipped_out) / 2
        100.0 * SUM(CASE
            WHEN r.truth='in'  AND r.v > (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            WHEN r.truth='out' AND r.v <= (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS mid_acc,
        STDDEV(CASE WHEN r.truth='in' THEN r.v END) AS std_in,
        STDDEV(CASE WHEN r.truth='out' THEN r.v END) AS std_out
    FROM results r
    JOIN ground g_in ON r.t_val = g_in.t_val AND g_in.truth = 'in'
    JOIN ground g_out ON r.t_val = g_out.t_val AND g_out.truth = 'out'
    WHERE r.v IS NOT NULL
    GROUP BY r.t_val, g_in.clipped_true, g_out.clipped_true
),
utility AS (
    SELECT r.t_val,
        AVG(CASE WHEN r.truth='in'
            THEN ABS(r.v - ${TRUE_IN}::DOUBLE) / NULLIF(ABS(${TRUE_IN}::DOUBLE), 0) * 100
            ELSE ABS(r.v - ${TRUE_OUT}::DOUBLE) / NULLIF(ABS(${TRUE_OUT}::DOUBLE), 0) * 100
        END) AS mape
    FROM results r
    WHERE r.v IS NOT NULL
    GROUP BY r.t_val
),
bias AS (
    SELECT g.t_val,
        MAX(CASE WHEN g.truth='in'  THEN ABS(g.clipped_true - ${TRUE_IN}::DOUBLE) END) AS bias_in,
        MAX(CASE WHEN g.truth='out' THEN ABS(g.clipped_true - ${TRUE_OUT}::DOUBLE) END) AS bias_out
    FROM ground g
    GROUP BY g.t_val
)
SELECT a.t_val AS t,
    printf('%.1f%%', a.var_acc) AS attack_acc_200k,
    printf('%.1f%%', a.mid_acc) AS attack_acc_mid,
    printf('%.1f%%', ut.mape) AS mape_vs_true,
    printf('%.0f', a.std_in) AS noise_std_in,
    printf('%.0f', a.std_out) AS noise_std_out,
    printf('%.0f', b.bias_in) AS clip_bias_in,
    printf('%.0f', b.bias_out) AS clip_bias_out
FROM attack a
JOIN utility ut ON a.t_val = ut.t_val
JOIN bias b ON a.t_val = b.t_val
ORDER BY CASE a.t_val WHEN 'inf' THEN 999 ELSE a.t_val::INT END;

-- Detailed per-condition stats
SELECT r.t_val AS t, r.truth,
    printf('%.0f', AVG(r.v)) AS mean_pac,
    printf('%.0f', STDDEV(r.v)) AS std_pac,
    printf('%.0f', g.clipped_true) AS truth_clipped,
    COUNT(*) AS n
FROM results r
JOIN ground g ON r.t_val = g.t_val AND r.truth = g.truth
WHERE r.v IS NOT NULL
GROUP BY r.t_val, r.truth, g.clipped_true
ORDER BY CASE r.t_val WHEN 'inf' THEN 999 ELSE r.t_val::INT END, r.truth;
SQL

rm -f "$RESULTS_F" "$GROUND_F"

echo ""
echo "============================================================"
echo "  INTERPRETATION"
echo "============================================================"
echo "  attack_acc_200k:  variance classifier (|v - bg| > 200k), 50% = random"
echo "  attack_acc_mid:   midpoint classifier (optimal threshold), 50% = random"
echo "  mape_vs_true:     mean |noised - unclipped_truth| / |unclipped_truth|"
echo "  clip_bias_in/out: |clipped_truth - unclipped_truth|"
echo "  Unclipped truths: in=$TRUE_IN  out=$TRUE_OUT"
echo "============================================================"
