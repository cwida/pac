#!/usr/bin/env bash
# Output clipping v2: clip values at QUERY TIME using pre-computed baseline bounds,
# BEFORE PAC computes sensitivity.
#
# Pipeline per query:
#   1. Data is stored unmodified (billionaire's 999999 stays as-is)
#   2. At query time, clamp each value to [μ-tσ, μ+tσ] (baseline stats, single pass)
#   3. PAC computes sensitivity from the CLIPPED range → noise ∝ 2tσ
#   4. Return noised result
#
# Key property: bounds are identical for in/out (derived from baseline, not current data),
# so PAC calibrates the SAME noise regardless of membership. No side-channel from
# differing sensitivities.
#
# Simulated by: UPDATE + PAC_KEY in the same session. The UPDATE models the query-time
# clamping; PAC then sees the clipped range for sensitivity.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N=1000; TV=999999; FILT=3; NTRIALS=30
T_VALUES="1 2 3 5 inf"

echo "============================================================"
echo "  OUTPUT CLIPPING v2"
echo "  Clip at query time, BEFORE PAC sensitivity computation"
echo "============================================================"
echo "  N=$N users, target=$TV, filter<=$FILT, $NTRIALS trials"
echo "  Clipping thresholds (t): $T_VALUES"
echo "============================================================"
echo ""

# --- Baseline column stats (from N users, NO target) ---
read MU SIGMA <<< "$($DUCKDB -noheader -csv -separator ' ' <<SQL
SELECT ROUND(AVG((hash(i*31+7)%10000+1)::DOUBLE))::INTEGER AS mu,
       ROUND(STDDEV_POP((hash(i*31+7)%10000+1)::DOUBLE))::INTEGER AS sigma
FROM generate_series(1,$N) s(i);
SQL
)"
MU=$(echo "$MU" | tr -d '[:space:]')
SIGMA=$(echo "$SIGMA" | tr -d '[:space:]')
echo "Baseline column stats: μ=$MU, σ=$SIGMA"

# --- Ground truths (no PAC noise) ---
run_true() {
    local cond=$1 t_clip=$2 insert="" clip_sql=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    if [ "$t_clip" != "inf" ]; then
        local LO=$(( MU - t_clip * SIGMA ))
        local HI=$(( MU + t_clip * SIGMA ))
        clip_sql="UPDATE users SET acctbal = LEAST(GREATEST(acctbal, ${LO}), ${HI})
WHERE acctbal < ${LO} OR acctbal > ${HI};"
    fi
    $DUCKDB -noheader -list <<SQL
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
${clip_sql}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

# --- PAC query with single-pass output clipping ---
run_clipped_pac() {
    local cond=$1 seed=$2 t_clip=$3 insert="" clip_sql=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    if [ "$t_clip" != "inf" ]; then
        local LO=$(( MU - t_clip * SIGMA ))
        local HI=$(( MU + t_clip * SIGMA ))
        clip_sql="UPDATE users SET acctbal = LEAST(GREATEST(acctbal, ${LO}), ${HI})
WHERE acctbal < ${LO} OR acctbal > ${HI};"
    fi
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

# --- Compute ground truths ---
echo "Computing unclipped ground truths..."
TRUE_IN=$(run_true in inf | tr -d '[:space:]')
TRUE_OUT=$(run_true out inf | tr -d '[:space:]')
echo "  Unclipped: in=$TRUE_IN  out=$TRUE_OUT  diff=$((TRUE_IN - TRUE_OUT))"

# --- Collect results ---
RESULTS_F=$(mktemp); GROUND_F=$(mktemp)

for t in $T_VALUES; do
    echo ""
    LO="−∞"; HI="+∞"
    if [ "$t" != "inf" ]; then
        LO=$(( MU - t * SIGMA ))
        HI=$(( MU + t * SIGMA ))
    fi
    echo "--- t=$t  bounds=[$LO, $HI] ---"

    # Clipped ground truths (deterministic)
    CLIP_IN=$(run_true in "$t" | tr -d '[:space:]')
    CLIP_OUT=$(run_true out "$t" | tr -d '[:space:]')
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
CREATE TABLE results AS
SELECT split_part(c,',',1) AS t_val,
       split_part(c,',',2) AS truth,
       split_part(c,',',3)::INT AS trial,
       TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (SELECT column0 AS c FROM read_csv('${RESULTS_F}', columns={'column0':'VARCHAR'}, header=false))
WHERE split_part(c,',',4) != '';

CREATE TABLE ground AS
SELECT split_part(c,',',1) AS t_val,
       split_part(c,',',2) AS truth,
       split_part(c,',',3)::DOUBLE AS clipped_true
FROM (SELECT column0 AS c FROM read_csv('${GROUND_F}', columns={'column0':'VARCHAR'}, header=false));

-- Main summary table
WITH attack AS (
    SELECT r.t_val,
        100.0 * SUM(CASE
            WHEN r.truth='in'  AND ABS(r.v - ${TRUE_OUT}) > 200000 THEN 1
            WHEN r.truth='out' AND ABS(r.v - ${TRUE_OUT}) <= 200000 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS var_acc,
        100.0 * SUM(CASE
            WHEN r.truth='in'  AND r.v > (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            WHEN r.truth='out' AND r.v <= (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS mid_acc,
        STDDEV(CASE WHEN r.truth='in' THEN r.v END) AS std_in,
        STDDEV(CASE WHEN r.truth='out' THEN r.v END) AS std_out
    FROM results r
    JOIN ground g_in  ON r.t_val = g_in.t_val  AND g_in.truth  = 'in'
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
    printf('%.1fx', a.std_in / NULLIF(a.std_out, 0)) AS std_ratio,
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
echo "  Single-pass clip at [μ-tσ, μ+tσ] using baseline stats"
echo "  μ=$MU, σ=$SIGMA (from $N background users)"
echo "  PAC sees clipped range → sensitivity = 2tσ for both in/out"
echo "  std_ratio: noise_std_in / noise_std_out (1.0 = no side-channel)"
echo "  Unclipped truths: in=$TRUE_IN  out=$TRUE_OUT"
echo "============================================================"
