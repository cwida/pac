#!/usr/bin/env bash
# =============================================================================
# MIA v5: Cross-Query Composition Attack
# =============================================================================
# Run DIFFERENT queries against the SAME database and combine evidence.
#
# Attack vectors:
#   A) Multiple aggregate types: SUM, COUNT, AVG on same data
#   B) Different WHERE filters on same data (overlapping slices)
#   C) Combined: many diverse queries, accumulate log-likelihood ratios
#   D) COUNT(*) with different filters (most controlled signal)
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
N_USERS=1000
TARGET_VALUE=50000
NUM_TRIALS=500

echo "============================================================"
echo " MIA v5: Cross-Query Composition Attack"
echo "============================================================"
echo ""

# ============================================================
# Helper: run a batch of different queries in ONE DuckDB session
# against the same database, collect all results
# ============================================================

# ATTACK A: Multiple aggregate types in separate queries (same DB)
echo "=== ATTACK A: Different Aggregates (SUM, COUNT, AVG) ==="
echo "Each aggregate gives independent noisy info about the target."
echo ""

IN_F=$(mktemp); OUT_F=$(mktemp)

for seed in $(seq 1 $NUM_TRIALS); do
    # TARGET IN: run 3 queries in same session
    $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$IN_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT 'in' || ',' || ${seed} || ',' || SUM(acctbal) || ',' || COUNT(*) || ',' || AVG(acctbal) FROM users;
SQL

    # TARGET OUT
    $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$OUT_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT 'out' || ',' || ${seed} || ',' || SUM(acctbal) || ',' || COUNT(*) || ',' || AVG(acctbal) FROM users;
SQL
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT
    split_part(column0,',',1) AS truth,
    split_part(column0,',',2)::INT AS seed,
    TRY_CAST(split_part(column0,',',3) AS DOUBLE) AS pac_sum,
    TRY_CAST(split_part(column0,',',4) AS DOUBLE) AS pac_count,
    TRY_CAST(split_part(column0,',',5) AS DOUBLE) AS pac_avg
FROM read_csv('${IN_F}', columns={'column0':'VARCHAR'}, header=false)
UNION ALL
SELECT
    split_part(column0,',',1) AS truth,
    split_part(column0,',',2)::INT AS seed,
    TRY_CAST(split_part(column0,',',3) AS DOUBLE) AS pac_sum,
    TRY_CAST(split_part(column0,',',4) AS DOUBLE) AS pac_count,
    TRY_CAST(split_part(column0,',',5) AS DOUBLE) AS pac_avg
FROM read_csv('${OUT_F}', columns={'column0':'VARCHAR'}, header=false);

-- Background stats (exact, known to attacker)
-- bg_sum=4876582, bg_count=1000, bg_avg=4876.582
-- with target: sum=4926582, count=1001, avg=4921.66

-- Attack using SUM alone
SELECT 'SUM only' AS method,
    printf('%.1f%%', 100.0*SUM(CASE WHEN
        (truth='in' AND pac_sum > 4876582+25000) OR
        (truth='out' AND pac_sum <= 4876582+25000)
        THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE pac_sum IS NOT NULL;

-- Attack using COUNT alone (target adds 1 to count)
SELECT 'COUNT only' AS method,
    printf('%.1f%%', 100.0*SUM(CASE WHEN
        (truth='in' AND pac_count > 1000.5) OR
        (truth='out' AND pac_count <= 1000.5)
        THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE pac_count IS NOT NULL;

-- Attack using AVG alone
-- bg_avg = 4876582/1000 = 4876.582
-- with_target_avg = 4926582/1001 = 4921.66
SELECT 'AVG only' AS method,
    printf('%.1f%%', 100.0*SUM(CASE WHEN
        (truth='in' AND pac_avg > (4876.582+4921.66)/2) OR
        (truth='out' AND pac_avg <= (4876.582+4921.66)/2)
        THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE pac_avg IS NOT NULL;

-- COMBINED: majority vote across all 3
SELECT 'Majority vote (3 aggs)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE WHEN
        (truth='in' AND (
            (CASE WHEN pac_sum > 4876582+25000 THEN 1 ELSE 0 END) +
            (CASE WHEN pac_count > 1000.5 THEN 1 ELSE 0 END) +
            (CASE WHEN pac_avg > (4876.582+4921.66)/2 THEN 1 ELSE 0 END)
        ) >= 2) OR
        (truth='out' AND (
            (CASE WHEN pac_sum > 4876582+25000 THEN 1 ELSE 0 END) +
            (CASE WHEN pac_count > 1000.5 THEN 1 ELSE 0 END) +
            (CASE WHEN pac_avg > (4876.582+4921.66)/2 THEN 1 ELSE 0 END)
        ) < 2)
        THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy
FROM raw WHERE pac_sum IS NOT NULL AND pac_count IS NOT NULL AND pac_avg IS NOT NULL;

-- NOTE: SUM, COUNT, AVG in same query share the same secret world j*
-- and p-tracking state, so they are NOT independent!
SELECT 'NOTE: These 3 aggs share the same query -> same secret world -> correlated' AS info;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# ATTACK B: Different WHERE filters in SEPARATE queries (different seeds)
# Each query uses a different pac_seed -> different secret world
# ============================================================
echo "=== ATTACK B: Different WHERE filters, separate queries ==="
echo "Each query has a different seed -> independent noise."
echo "Combine evidence via log-likelihood ratio accumulation."
echo ""

IN_F=$(mktemp); OUT_F=$(mktemp)

for seed in $(seq 1 $NUM_TRIALS); do
    # 5 queries with different filters, each gets seed*10+offset for different pac_seed
    for offset in 1 2 3 4 5; do
        qseed=$((seed * 10 + offset))
        case $offset in
            1) where_clause="WHERE user_id <= 200 OR user_id = 0" ;;
            2) where_clause="WHERE user_id > 200 AND user_id <= 400" ;;
            3) where_clause="WHERE user_id > 400 AND user_id <= 600" ;;
            4) where_clause="WHERE user_id > 600 AND user_id <= 800" ;;
            5) where_clause="WHERE user_id > 800 OR user_id = 0" ;;
        esac

        # Target IN
        $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$IN_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${qseed};
SELECT 'in,${seed},${offset},' || SUM(acctbal) FROM users ${where_clause};
SQL

        # Target OUT
        $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$OUT_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${qseed};
SELECT 'out,${seed},${offset},' || SUM(acctbal) FROM users ${where_clause};
SQL
    done
done

# Compute exact sums per partition
BG1=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,200) t(i);" | tr -d '[:space:]')
BG2=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(201,400) t(i);" | tr -d '[:space:]')
BG3=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(401,600) t(i);" | tr -d '[:space:]')
BG4=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(601,800) t(i);" | tr -d '[:space:]')
BG5=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(801,1000) t(i);" | tr -d '[:space:]')

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT
    split_part(column0,',',1) AS truth,
    split_part(column0,',',2)::INT AS trial,
    split_part(column0,',',3)::INT AS qid,
    TRY_CAST(split_part(column0,',',4) AS DOUBLE) AS pac_sum
FROM read_csv('${IN_F}', columns={'column0':'VARCHAR'}, header=false)
WHERE split_part(column0,',',4) != ''
UNION ALL
SELECT
    split_part(column0,',',1),
    split_part(column0,',',2)::INT,
    split_part(column0,',',3)::INT,
    TRY_CAST(split_part(column0,',',4) AS DOUBLE)
FROM read_csv('${OUT_F}', columns={'column0':'VARCHAR'}, header=false)
WHERE split_part(column0,',',4) != '';

-- Expected sums: target (id=0) appears in filter 1 (<=200) and filter 5 (>800 OR =0)
-- Filter 1: bg=${BG1}, with target = ${BG1} + ${TARGET_VALUE}
-- Filter 2: bg=${BG2}, no target (id 0 not in 201-400)
-- Filter 3: bg=${BG3}, no target
-- Filter 4: bg=${BG4}, no target
-- Filter 5: bg=${BG5}, with target = ${BG5} + ${TARGET_VALUE}

-- Per-query accuracy
SELECT 'Q' || qid AS query,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN qid IN (1,5) THEN  -- target appears in these partitions
            CASE WHEN (truth='in' AND pac_sum > (CASE WHEN qid=1 THEN ${BG1} ELSE ${BG5} END) + ${TARGET_VALUE}/2.0)
                   OR (truth='out' AND pac_sum <= (CASE WHEN qid=1 THEN ${BG1} ELSE ${BG5} END) + ${TARGET_VALUE}/2.0)
                 THEN 1 ELSE 0 END
        ELSE  -- target doesn't appear: should be identical, always "out"
            CASE WHEN truth='out' THEN 1 ELSE 0 END  -- irrelevant query
        END)::DOUBLE / COUNT(*)) AS accuracy,
    COUNT(*) AS n
FROM raw
WHERE pac_sum IS NOT NULL
GROUP BY qid ORDER BY qid;

-- Combined: for each trial, use only queries 1 and 5 (where target appears)
-- Average their "votes" (both should lean positive if target is in)
WITH votes AS (
    SELECT truth, trial,
        SUM(CASE
            WHEN qid=1 AND pac_sum > ${BG1} + ${TARGET_VALUE}/2.0 THEN 1
            WHEN qid=5 AND pac_sum > ${BG5} + ${TARGET_VALUE}/2.0 THEN 1
            ELSE 0
        END) AS yes_votes,
        SUM(CASE WHEN qid IN (1,5) AND pac_sum IS NOT NULL THEN 1 ELSE 0 END) AS total_votes
    FROM raw
    WHERE qid IN (1,5)
    GROUP BY truth, trial
)
SELECT 'Combined Q1+Q5 (majority)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes_votes > total_votes/2.0 THEN 1
        WHEN truth='out' AND yes_votes <= total_votes/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy,
    COUNT(*) AS n_trials
FROM votes WHERE total_votes > 0;

-- Use "negative evidence" from filters 2,3,4 where target should NOT appear
-- If SUM is suspiciously high in these partitions, it's just noise
-- But if SUM is consistently higher in Q1,Q5 for "in" trials, that's signal
WITH per_trial AS (
    SELECT truth, trial,
        AVG(CASE WHEN qid=1 THEN pac_sum END) AS q1,
        AVG(CASE WHEN qid=2 THEN pac_sum END) AS q2,
        AVG(CASE WHEN qid=3 THEN pac_sum END) AS q3,
        AVG(CASE WHEN qid=4 THEN pac_sum END) AS q4,
        AVG(CASE WHEN qid=5 THEN pac_sum END) AS q5
    FROM raw WHERE pac_sum IS NOT NULL
    GROUP BY truth, trial
),
scored AS (
    SELECT truth, trial,
        -- Score: q1 and q5 should be higher when target is in
        -- Subtract q2,q3,q4 as "control" baselines
        COALESCE(q1 - ${BG1},0) + COALESCE(q5 - ${BG5},0)
        - (COALESCE(q2 - ${BG2},0) + COALESCE(q3 - ${BG3},0) + COALESCE(q4 - ${BG4},0)) / 3.0
        AS score
    FROM per_trial
)
SELECT 'Differential score (target vs control partitions)' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND score > ${TARGET_VALUE} THEN 1
        WHEN truth='out' AND score <= ${TARGET_VALUE} THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy,
    COUNT(*) AS n
FROM scored;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# ATTACK C: Many independent queries with different seeds
#           against same small filtered group
# ============================================================
echo "=== ATTACK C: 10 independent queries on filter<=5, combine ==="
echo ""

IN_F=$(mktemp); OUT_F=$(mktemp)
FBG5=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,5) t(i);" | tr -d '[:space:]')

for trial in $(seq 1 $NUM_TRIALS); do
    for q in $(seq 1 10); do
        qseed=$((trial * 100 + q))
        # Target IN
        $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$IN_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${qseed};
SELECT 'in,${trial},${q},' || SUM(acctbal) FROM users WHERE user_id <= 5 OR user_id = 0;
SQL

        # Target OUT
        $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$OUT_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${qseed};
SELECT 'out,${trial},${q},' || SUM(acctbal) FROM users WHERE user_id <= 5;
SQL
    done
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT
    split_part(column0,',',1) AS truth,
    split_part(column0,',',2)::INT AS trial,
    split_part(column0,',',3)::INT AS qid,
    TRY_CAST(split_part(column0,',',4) AS DOUBLE) AS pac_sum
FROM read_csv('${IN_F}', columns={'column0':'VARCHAR'}, header=false)
WHERE split_part(column0,',',4) != ''
UNION ALL
SELECT split_part(column0,',',1), split_part(column0,',',2)::INT,
    split_part(column0,',',3)::INT, TRY_CAST(split_part(column0,',',4) AS DOUBLE)
FROM read_csv('${OUT_F}', columns={'column0':'VARCHAR'}, header=false)
WHERE split_part(column0,',',4) != '';

-- Single query accuracy (baseline)
WITH single AS (
    SELECT truth, pac_sum,
        CASE WHEN pac_sum > ${FBG5} + ${TARGET_VALUE}/2.0 THEN 'in' ELSE 'out' END AS guess
    FROM raw WHERE qid = 1 AND pac_sum IS NOT NULL
)
SELECT '1 query' AS method,
    printf('%.1f%%', 100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy
FROM single;

-- Average of K queries, then threshold
WITH per_trial AS (
    SELECT truth, trial, AVG(pac_sum) AS avg_sum, COUNT(*) AS nq
    FROM raw WHERE pac_sum IS NOT NULL
    GROUP BY truth, trial
)
SELECT 'Avg of ' || nq || ' queries' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND avg_sum > ${FBG5} + ${TARGET_VALUE}/2.0 THEN 1
        WHEN truth='out' AND avg_sum <= ${FBG5} + ${TARGET_VALUE}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy,
    printf('%.0f', STDDEV(avg_sum) FILTER (WHERE truth='in')) AS noise_std,
    COUNT(*) AS n
FROM per_trial;

-- Majority vote of K queries
WITH votes AS (
    SELECT truth, trial,
        SUM(CASE WHEN pac_sum > ${FBG5} + ${TARGET_VALUE}/2.0 THEN 1 ELSE 0 END) AS yes,
        COUNT(*) AS total
    FROM raw WHERE pac_sum IS NOT NULL
    GROUP BY truth, trial
)
SELECT 'Majority vote of ' || MAX(total) || ' queries' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND yes > total/2.0 THEN 1
        WHEN truth='out' AND yes <= total/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy,
    COUNT(*) AS n
FROM votes;

-- Progressive: how does accuracy improve as we add more queries?
WITH cum AS (
    SELECT truth, trial, qid, pac_sum,
        AVG(pac_sum) OVER (PARTITION BY truth, trial ORDER BY qid) AS running_avg
    FROM raw WHERE pac_sum IS NOT NULL
)
SELECT qid AS n_queries,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND running_avg > ${FBG5} + ${TARGET_VALUE}/2.0 THEN 1
        WHEN truth='out' AND running_avg <= ${FBG5} + ${TARGET_VALUE}/2.0 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS cumulative_accuracy
FROM cum
GROUP BY qid ORDER BY qid;

SQL

rm -f "$IN_F" "$OUT_F"
echo ""

# ============================================================
# ATTACK D: COUNT queries on narrow filters (target adds exactly 1)
# ============================================================
echo "=== ATTACK D: COUNT(*) on narrow filters ==="
echo "Target adds exactly 1 to count. Check if PAC noise hides it."
echo ""

IN_F=$(mktemp); OUT_F=$(mktemp)

for seed in $(seq 1 $NUM_TRIALS); do
    $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$IN_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT 'in,' || COUNT(*) FROM users WHERE user_id <= 5 OR user_id = 0;
SQL

    $DUCKDB -noheader -list 2>/dev/null <<SQL >> "$OUT_F"
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT 'out,' || COUNT(*) FROM users WHERE user_id <= 5;
SQL
done

$DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT split_part(column0,',',1) AS truth, TRY_CAST(split_part(column0,',',2) AS DOUBLE) AS pac_count
FROM read_csv('${IN_F}', columns={'column0':'VARCHAR'}, header=false)
UNION ALL
SELECT split_part(column0,',',1), TRY_CAST(split_part(column0,',',2) AS DOUBLE)
FROM read_csv('${OUT_F}', columns={'column0':'VARCHAR'}, header=false);

-- COUNT: target in => 6, target out => 5, threshold = 5.5
SELECT 'COUNT filter<=5' AS method,
    printf('%.1f%%', 100.0*SUM(CASE
        WHEN truth='in' AND pac_count > 5.5 THEN 1
        WHEN truth='out' AND pac_count <= 5.5 THEN 1
        ELSE 0 END)::DOUBLE / COUNT(*)) AS accuracy,
    printf('%.0f', AVG(pac_count) FILTER (WHERE truth='in')) AS mean_in,
    printf('%.0f', AVG(pac_count) FILTER (WHERE truth='out')) AS mean_out,
    printf('%.1f', STDDEV(pac_count) FILTER (WHERE truth='in')) AS std_in
FROM raw WHERE pac_count IS NOT NULL;
SQL

rm -f "$IN_F" "$OUT_F"
echo ""

echo "============================================================"
echo " SUMMARY"
echo "============================================================"
echo "PAC bound: ~53% (MI=1/128) per query"
echo "Cross-query composition: total MI <= d * B for d queries"
echo "  => 10 queries at MI=1/128 each => total MI = 10/128"
echo "  => theoretical bound should still be well under 84%"
echo "============================================================"
