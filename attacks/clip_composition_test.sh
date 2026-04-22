#!/usr/bin/env bash
# Composition attack: does averaging many queries break clipping?
#
# METHODOLOGY:
# Run N independent queries (each with a different pac_seed) on the same data.
# The attacker averages the N results. Noise decreases at 1/sqrt(N) but the
# outlier signal stays constant. With enough queries, noise → 0 and the signal
# should be detectable.
#
# For each NQ (number of queries), we compute the average over those queries
# per trial, then find the best classification threshold across trials.
# 50% = random, 100% = perfect attack.
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

N=1000; TV=999999; MI=0.0078125; FILT=3; NT=15

run_sum() {
    local cond=$1 seed=$2 clip=$3
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
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILT} OR user_id = 0;
SQL
}

MAX_NQ=100  # max queries per trial

for CLIP in off 2; do
    echo "==================================================="
    echo "  COMPOSITION ATTACK: clip=${CLIP}, filt<=${FILT}"
    echo "  ${NT} trials x ${MAX_NQ} queries each"
    echo "==================================================="
    echo ""

    # Collect all queries upfront
    IN_F=$(mktemp); OUT_F=$(mktemp)
    for trial in $(seq 1 $NT); do
        for q in $(seq 1 $MAX_NQ); do
            s=$((trial * 10000 + q))
            echo "in,${trial},${q},$(run_sum in $s $CLIP)" >> "$IN_F"
            echo "out,${trial},${q},$(run_sum out $s $CLIP)" >> "$OUT_F"
        done
        echo "  trial ${trial}/${NT} done" >&2
    done

    # Analyze at different NQ cutoffs
    for NQ in 1 5 10 25 50 100; do
        echo "--- NQ=${NQ} ---"
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

-- Average first NQ queries per trial
WITH avgs AS (
    SELECT truth, trial, AVG(v) AS v
    FROM raw WHERE qid <= ${NQ} AND v IS NOT NULL
    GROUP BY truth, trial
)
SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std, COUNT(*) AS n
FROM avgs GROUP BY truth ORDER BY truth;

-- Best threshold classifier on averaged values
WITH avgs AS (
    SELECT truth, trial, AVG(v) AS v
    FROM raw WHERE qid <= ${NQ} AND v IS NOT NULL
    GROUP BY truth, trial
),
ths AS (SELECT UNNEST(generate_series(
    (SELECT (MIN(v))::BIGINT FROM avgs),
    (SELECT (MAX(v))::BIGINT FROM avgs),
    GREATEST(1, ((SELECT MAX(v)-MIN(v) FROM avgs)/50)::BIGINT)
)) AS t),
accs AS (
    SELECT t, 100.0*SUM(CASE
        WHEN truth='in' AND v > t THEN 1 WHEN truth='out' AND v <= t THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*) AS acc
    FROM avgs, ths GROUP BY t
)
SELECT printf('%.1f%%', MAX(acc)) AS best_accuracy FROM accs;
SQL
        echo ""
    done
    rm -f "$IN_F" "$OUT_F"
done
