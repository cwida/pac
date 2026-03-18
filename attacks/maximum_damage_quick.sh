#!/usr/bin/env bash
# Quick version of maximum_damage.sh — reduced trials for fast iteration
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"

run_sum() {
    local cond=$1 seed=$2 n_users=$3 target_val=$4 filter=$5
    local insert=""
    [ "$cond" = "in" ] && insert="INSERT INTO users VALUES (0, ${target_val});"
    $DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${n_users}) t(i);
${insert}
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${filter} OR user_id = 0;
SQL
}

echo "============================================================"
echo "  MAXIMUM DAMAGE MIA ATTACK (quick: 30 trials)"
echo "============================================================"
echo ""

# ATTACK 1: Extreme outlier + tiny filter + 5 composed queries
echo "=== Extreme outlier, filter<=3, 5 queries composed ==="
N=1000; TV=999999; FILT=3; NTRIALS=30; NQUERIES=5

FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILT}) t(i);" | tr -d '[:space:]')
echo "Background SUM (filter<=$FILT) = $FBG"

IN_F=$(mktemp); OUT_F=$(mktemp)
for trial in $(seq 1 $NTRIALS); do
    for q in $(seq 1 $NQUERIES); do
        s=$((trial * 1000 + q))
        echo "in,${trial},${q},$(run_sum in $s $N $TV $FILT)" >> "$IN_F"
        echo "out,${trial},${q},$(run_sum out $s $N $TV $FILT)" >> "$OUT_F"
    done
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, split_part(c,',',2)::INT AS trial,
    split_part(c,',',3)::INT AS qid, TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',4) != '';

-- Progressive accuracy
WITH cum AS (
    SELECT truth, trial, qid,
        AVG(v) OVER (PARTITION BY truth, trial ORDER BY qid) AS ravg
    FROM raw WHERE v IS NOT NULL
)
SELECT qid AS n_queries,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ravg > ${FBG} + ${TV}/2.0 THEN 1
        WHEN truth='out' AND ravg <= ${FBG} + ${TV}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM cum GROUP BY qid ORDER BY qid;

-- Majority vote
WITH votes AS (
    SELECT truth, trial,
        SUM(CASE WHEN v > ${FBG} + ${TV}/2.0 THEN 1 ELSE 0 END) AS yes, COUNT(*) AS total
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'Majority vote (5 queries)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes > total/2.0 THEN 1
        WHEN truth='out' AND yes <= total/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM votes;

-- Noise stats
SELECT truth, printf('%.0f',AVG(v)) AS mean, printf('%.0f',STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ATTACK 2: Variance-based classifier
echo "=== Variance-based classifier ==="
N=1000; TV=999999; FILT=3; NTRIALS=30

IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NTRIALS); do
    echo "in,$(run_sum in $seed $N $TV $FILT)" >> "$IN_F"
    echo "out,$(run_sum out $seed $N $TV $FILT)" >> "$OUT_F"
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

SELECT truth, printf('%.0f', AVG(v)) AS mean, printf('%.0f', STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth;

-- Standard threshold
SELECT 'Standard threshold' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > ${FBG} + ${TV}/2.0 THEN 1
        WHEN truth='out' AND v <= ${FBG} + ${TV}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;

-- Variance classifier
SELECT threshold,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ABS(v - ${FBG}) > threshold THEN 1
        WHEN truth='out' AND ABS(v - ${FBG}) <= threshold THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw, (VALUES (50000),(100000),(200000),(500000),(1000000)) thresholds(threshold)
WHERE v IS NOT NULL
GROUP BY threshold ORDER BY threshold;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ATTACK 3: 10K users + extreme outlier
echo "=== 10K users, filter<=2, target=9999999 ==="
N=10000; TV=9999999; FILT=2; NTRIALS=30

FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILT}) t(i);" | tr -d '[:space:]')

IN_F=$(mktemp); OUT_F=$(mktemp)
for seed in $(seq 1 $NTRIALS); do
    echo "in,$(run_sum in $seed $N $TV $FILT)" >> "$IN_F"
    echo "out,$(run_sum out $seed $N $TV $FILT)" >> "$OUT_F"
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, TRY_CAST(split_part(c,',',2) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',2) != '';

SELECT '10K users, filter<=2, target=9999999' AS test,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND v > ${FBG} + ${TV}/2.0 THEN 1
        WHEN truth='out' AND v <= ${FBG} + ${TV}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE v IS NOT NULL;

SELECT truth, printf('%.0f',AVG(v)) AS mean, printf('%.0f',STDDEV(v)) AS std, COUNT(*) AS n
FROM raw WHERE v IS NOT NULL GROUP BY truth;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ATTACK 4: 10K users + composed queries
echo "=== 10K users + 5 composed queries ==="
N=10000; TV=9999999; FILT=2; NTRIALS=30; NQUERIES=5

FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILT}) t(i);" | tr -d '[:space:]')

IN_F=$(mktemp); OUT_F=$(mktemp)
for trial in $(seq 1 $NTRIALS); do
    for q in $(seq 1 $NQUERIES); do
        s=$((trial * 1000 + q))
        echo "in,${trial},${q},$(run_sum in $s $N $TV $FILT)" >> "$IN_F"
        echo "out,${trial},${q},$(run_sum out $s $N $TV $FILT)" >> "$OUT_F"
    done
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(c,',',1) AS truth, split_part(c,',',2)::INT AS trial,
    split_part(c,',',3)::INT AS qid, TRY_CAST(split_part(c,',',4) AS DOUBLE) AS v
FROM (
    SELECT column0 AS c FROM read_csv('${IN_F}',columns={'column0':'VARCHAR'},header=false)
    UNION ALL
    SELECT column0 FROM read_csv('${OUT_F}',columns={'column0':'VARCHAR'},header=false)
) WHERE split_part(c,',',4) != '';

WITH cum AS (
    SELECT truth, trial, qid,
        AVG(v) OVER (PARTITION BY truth, trial ORDER BY qid) AS ravg
    FROM raw WHERE v IS NOT NULL
)
SELECT qid AS n_queries,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND ravg > ${FBG} + ${TV}/2.0 THEN 1
        WHEN truth='out' AND ravg <= ${FBG} + ${TV}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM cum GROUP BY qid ORDER BY qid;

WITH votes AS (
    SELECT truth, trial,
        SUM(CASE WHEN v > ${FBG} + ${TV}/2.0 THEN 1 ELSE 0 END) AS yes, COUNT(*) AS total
    FROM raw WHERE v IS NOT NULL GROUP BY truth, trial
)
SELECT 'FINAL: 10K users, majority vote 5 queries' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes > total/2.0 THEN 1
        WHEN truth='out' AND yes <= total/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM votes;
SQL

rm -f "$IN_F" "$OUT_F"

echo ""
echo "============================================================"
echo "  RESULTS vs PAC BOUNDS"
echo "============================================================"
echo "  Random guess:         50%"
echo "  PAC bound (MI=1/128): ~53% per query"
echo "  5 queries composed:   total MI = 5/128 ~ 0.04"
echo "============================================================"
