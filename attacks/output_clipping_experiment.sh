#!/usr/bin/env bash
# Output clipping experiment: clip the PAC query RESULT (not input data) at bounds
# derived from baseline column statistics.
#
# Unlike input clipping (which modifies stored values before PAC), output clipping
# leaves the data untouched. After PAC returns a noised result, we clamp it to
# [n·(μ - t·σ), n·(μ + t·σ)] where μ,σ are pre-computed column stats and n is the
# expected number of users in the filter.
#
# Key property: if the billionaire is NOT in the filter, the result is already
# within bounds and nothing changes. Clipping only fires when an outlier inflates
# the result beyond the expected range.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N=1000; TV=999999; FILT=3; NTRIALS=30
T_VALUES="1 2 3 5 inf"

echo "============================================================"
echo "  OUTPUT CLIPPING EXPERIMENT"
echo "  Clip the PAC result post-hoc at n·(μ ± t·σ)"
echo "============================================================"
echo "  N=$N users, target=$TV, filter<=$FILT, $NTRIALS trials"
echo "  Clipping thresholds (t): $T_VALUES"
echo "============================================================"
echo ""

# --- Baseline column statistics (from N users, NO target) ---
# These represent the "known" column distribution used for clipping bounds.
read MU SIGMA <<< "$($DUCKDB -noheader -csv -separator ' ' <<SQL
SELECT ROUND(AVG((hash(i*31+7)%10000+1)::DOUBLE))::INTEGER AS mu,
       ROUND(STDDEV_POP((hash(i*31+7)%10000+1)::DOUBLE))::INTEGER AS sigma
FROM generate_series(1,$N) s(i);
SQL
)"
# n_base = number of users matching the filter WITHOUT the target
N_BASE=$($DUCKDB -noheader -list <<SQL
SELECT COUNT(*) FROM generate_series(1,$N) s(i) WHERE i <= $FILT;
SQL
)
N_BASE=$(echo "$N_BASE" | tr -d '[:space:]')
MU=$(echo "$MU" | tr -d '[:space:]')
SIGMA=$(echo "$SIGMA" | tr -d '[:space:]')
echo "Baseline column stats: μ=$MU, σ=$SIGMA, n_base=$N_BASE"
echo ""

# --- Ground truths (no PAC noise) ---
run_true() {
    local cond=$1 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    $DUCKDB -noheader -list <<SQL
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

TRUE_IN=$(run_true in | tr -d '[:space:]')
TRUE_OUT=$(run_true out | tr -d '[:space:]')
echo "Unclipped ground truths: in=$TRUE_IN  out=$TRUE_OUT  diff=$((TRUE_IN - TRUE_OUT))"

# --- Run PAC trials (ONCE — same raw results reused for all t values) ---
# This is the exact same query as the original attack, no data modification.
run_pac() {
    local cond=$1 seed=$2 insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${TV});"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N}) s(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

echo ""
echo "Running $NTRIALS PAC trials (in + out)..."
RESULTS_F=$(mktemp)
for seed in $(seq 1 $NTRIALS); do
    v_in=$(run_pac in "$seed" | tr -d '[:space:]')
    v_out=$(run_pac out "$seed" | tr -d '[:space:]')
    echo "in,${seed},${v_in}" >> "$RESULTS_F"
    echo "out,${seed},${v_out}" >> "$RESULTS_F"
    printf "."
done
echo " done"

echo ""
echo "============================================================"
echo "  RESULTS"
echo "============================================================"
echo ""

# --- Analysis: for each t, apply output clipping and compute metrics ---
$DUCKDB -markdown <<SQL
-- Load raw PAC results
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth,
       split_part(c,',',2)::INT AS trial,
       TRY_CAST(split_part(c,',',3) AS DOUBLE) AS v
FROM (SELECT column0 AS c FROM read_csv('${RESULTS_F}', columns={'column0':'VARCHAR'}, header=false))
WHERE split_part(c,',',3) != '';

-- t values to test
CREATE TABLE t_vals AS
SELECT * FROM (VALUES (1),(2),(3),(5)) t(t_val);

-- Clipping bounds per t: [n_base * (μ - t*σ), n_base * (μ + t*σ)]
-- For t=inf, no clipping (pass-through)
CREATE TABLE clipped AS
SELECT t.t_val, r.truth, r.trial, r.v AS raw_v,
    LEAST(GREATEST(r.v,
        ${N_BASE}.0 * (${MU}.0 - t.t_val * ${SIGMA}.0)),
        ${N_BASE}.0 * (${MU}.0 + t.t_val * ${SIGMA}.0)) AS v,
    ${N_BASE}.0 * (${MU}.0 - t.t_val * ${SIGMA}.0) AS lo,
    ${N_BASE}.0 * (${MU}.0 + t.t_val * ${SIGMA}.0) AS hi
FROM raw r CROSS JOIN t_vals t
WHERE r.v IS NOT NULL
UNION ALL
SELECT 999 AS t_val, r.truth, r.trial, r.v AS raw_v,
    r.v AS v,  -- inf = no clipping
    NULL AS lo, NULL AS hi
FROM raw r
WHERE r.v IS NOT NULL;

-- Clipped ground truths per t
CREATE TABLE ground AS
SELECT t.t_val,
    'in' AS truth,
    LEAST(GREATEST(${TRUE_IN}::DOUBLE,
        ${N_BASE}.0 * (${MU}.0 - t.t_val * ${SIGMA}.0)),
        ${N_BASE}.0 * (${MU}.0 + t.t_val * ${SIGMA}.0)) AS clipped_true
FROM t_vals t
UNION ALL
SELECT t.t_val,
    'out' AS truth,
    LEAST(GREATEST(${TRUE_OUT}::DOUBLE,
        ${N_BASE}.0 * (${MU}.0 - t.t_val * ${SIGMA}.0)),
        ${N_BASE}.0 * (${MU}.0 + t.t_val * ${SIGMA}.0)) AS clipped_true
FROM t_vals t
UNION ALL
SELECT 999, 'in', ${TRUE_IN}::DOUBLE
UNION ALL
SELECT 999, 'out', ${TRUE_OUT}::DOUBLE;

-- Show clipping bounds
SELECT t_val AS t,
    printf('%.0f', ${N_BASE}.0 * (${MU}.0 - t_val * ${SIGMA}.0)) AS lo,
    printf('%.0f', ${N_BASE}.0 * (${MU}.0 + t_val * ${SIGMA}.0)) AS hi,
    printf('%.0f', ${N_BASE}.0 * 2 * t_val * ${SIGMA}.0) AS width
FROM t_vals
ORDER BY t_val;

-- Main summary table
WITH attack AS (
    SELECT c.t_val,
        -- Variance classifier: |v - background| > 200000
        100.0 * SUM(CASE
            WHEN c.truth='in'  AND ABS(c.v - ${TRUE_OUT}) > 200000 THEN 1
            WHEN c.truth='out' AND ABS(c.v - ${TRUE_OUT}) <= 200000 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS var_acc,
        -- Midpoint classifier: v > (clipped_in_truth + clipped_out_truth) / 2
        100.0 * SUM(CASE
            WHEN c.truth='in'  AND c.v > (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            WHEN c.truth='out' AND c.v <= (g_in.clipped_true + g_out.clipped_true) / 2.0 THEN 1
            ELSE 0 END)::DOUBLE / COUNT(*) AS mid_acc,
        STDDEV(CASE WHEN c.truth='in' THEN c.v END) AS std_in,
        STDDEV(CASE WHEN c.truth='out' THEN c.v END) AS std_out,
        -- Fraction of results that hit a clip bound
        100.0 * SUM(CASE WHEN c.truth='in' AND (c.v = c.lo OR c.v = c.hi) THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN c.truth='in' THEN 1 ELSE 0 END), 0) AS pct_clipped_in,
        100.0 * SUM(CASE WHEN c.truth='out' AND (c.v = c.lo OR c.v = c.hi) THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN c.truth='out' THEN 1 ELSE 0 END), 0) AS pct_clipped_out
    FROM clipped c
    JOIN ground g_in  ON c.t_val = g_in.t_val  AND g_in.truth  = 'in'
    JOIN ground g_out ON c.t_val = g_out.t_val AND g_out.truth = 'out'
    GROUP BY c.t_val, g_in.clipped_true, g_out.clipped_true
),
utility AS (
    SELECT c.t_val,
        AVG(CASE WHEN c.truth='in'
            THEN ABS(c.v - ${TRUE_IN}::DOUBLE) / NULLIF(ABS(${TRUE_IN}::DOUBLE), 0) * 100
            ELSE ABS(c.v - ${TRUE_OUT}::DOUBLE) / NULLIF(ABS(${TRUE_OUT}::DOUBLE), 0) * 100
        END) AS mape
    FROM clipped c
    GROUP BY c.t_val
),
bias AS (
    SELECT g.t_val,
        MAX(CASE WHEN g.truth='in'  THEN ABS(g.clipped_true - ${TRUE_IN}::DOUBLE) END) AS bias_in,
        MAX(CASE WHEN g.truth='out' THEN ABS(g.clipped_true - ${TRUE_OUT}::DOUBLE) END) AS bias_out
    FROM ground g
    GROUP BY g.t_val
)
SELECT CASE WHEN a.t_val = 999 THEN 'inf' ELSE a.t_val::VARCHAR END AS t,
    printf('%.1f%%', a.var_acc) AS attack_acc_200k,
    printf('%.1f%%', a.mid_acc) AS attack_acc_mid,
    printf('%.1f%%', ut.mape) AS mape_vs_true,
    printf('%.0f', a.std_in) AS noise_std_in,
    printf('%.0f', a.std_out) AS noise_std_out,
    printf('%.0f', b.bias_in) AS clip_bias_in,
    printf('%.0f', b.bias_out) AS clip_bias_out,
    printf('%.0f%%', a.pct_clipped_in) AS pct_clip_in,
    printf('%.0f%%', a.pct_clipped_out) AS pct_clip_out
FROM attack a
JOIN utility ut ON a.t_val = ut.t_val
JOIN bias b ON a.t_val = b.t_val
ORDER BY CASE WHEN a.t_val = 999 THEN 999 ELSE a.t_val END;

-- Detailed per-condition stats
SELECT CASE WHEN c.t_val = 999 THEN 'inf' ELSE c.t_val::VARCHAR END AS t,
    c.truth,
    printf('%.0f', AVG(c.v)) AS mean_clipped,
    printf('%.0f', STDDEV(c.v)) AS std_clipped,
    printf('%.0f', AVG(c.raw_v)) AS mean_raw,
    printf('%.0f', STDDEV(c.raw_v)) AS std_raw,
    printf('%.0f', g.clipped_true) AS truth_clipped,
    COUNT(*) AS n
FROM clipped c
JOIN ground g ON c.t_val = g.t_val AND c.truth = g.truth
GROUP BY c.t_val, c.truth, g.clipped_true
ORDER BY CASE WHEN c.t_val = 999 THEN 999 ELSE c.t_val END, c.truth;
SQL

rm -f "$RESULTS_F"

echo ""
echo "============================================================"
echo "  INTERPRETATION"
echo "============================================================"
echo "  Output clipping: CLAMP(pac_result, n·(μ-tσ), n·(μ+tσ))"
echo "  Bounds use baseline stats (μ=$MU, σ=$SIGMA, n=$N_BASE)"
echo "  attack_acc_200k:  variance classifier, 50% = random"
echo "  attack_acc_mid:   midpoint classifier, 50% = random"
echo "  pct_clip_in/out:  fraction of results hitting a bound"
echo "  Unclipped truths: in=$TRUE_IN  out=$TRUE_OUT"
echo "============================================================"
