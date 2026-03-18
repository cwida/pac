#!/usr/bin/env bash
# =============================================================================
# MIA v4: NULL Side-Channel Attack
# =============================================================================
# Key discovery from v3: PAC returns NULLs probabilistically based on the
# accumulator's popcount. With fewer PUs in a group, more bits of the
# accumulator stay 0, increasing P(NULL).
#
# The formula: P(NULL) = popcount(~accumulator) / (64 * correction)
#
# ATTACK: When the target IS in the DB, the accumulator has more bits set
# (one more PU contributing), so P(NULL) is LOWER. The attacker simply
# observes whether the query returns NULL or not!
#
# This is a pure side-channel: no analysis of the noised VALUE needed.
# =============================================================================
set -euo pipefail

DUCKDB="/home/ila/Code/pac/build/release/duckdb"
PAC_EXT="/home/ila/Code/pac/build/release/extension/pac/pac.duckdb_extension"
N_USERS=1000
TARGET_VALUE=50000

echo "============================================================"
echo " MIA v4: NULL Side-Channel Attack"
echo "============================================================"
echo ""
echo "Hypothesis: PAC returns NULL more often when target is ABSENT"
echo "because fewer PUs => sparser accumulator => higher P(NULL)."
echo ""

# ============================================================
# Test various filter sizes - measure NULL rate
# ============================================================

for FILTER in 1 2 3 5 10 20; do
    NULL_IN=0; NON_NULL_IN=0
    NULL_OUT=0; NON_NULL_OUT=0
    NTRIALS=500

    for seed in $(seq 1 $NTRIALS); do
        # Target IN
        result=$($DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILTER} OR user_id = 0;
SQL
        )
        if [ -z "$result" ] || [ "$result" = "" ]; then
            NULL_IN=$((NULL_IN + 1))
        else
            NON_NULL_IN=$((NON_NULL_IN + 1))
        fi

        # Target OUT
        result=$($DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILTER};
SQL
        )
        if [ -z "$result" ] || [ "$result" = "" ]; then
            NULL_OUT=$((NULL_OUT + 1))
        else
            NON_NULL_OUT=$((NON_NULL_OUT + 1))
        fi
    done

    # NULL-based MIA: if NOT NULL => guess "in", if NULL => guess "out"
    # TP = non_null when target in, TN = null when target out
    TP=$NON_NULL_IN
    FP=$NON_NULL_OUT
    FN=$NULL_IN
    TN=$NULL_OUT
    ACCURACY=$(echo "scale=1; ($TP + $TN) * 100 / ($TP + $FP + $FN + $TN)" | bc)
    NULL_RATE_IN=$(echo "scale=1; $NULL_IN * 100 / $NTRIALS" | bc)
    NULL_RATE_OUT=$(echo "scale=1; $NULL_OUT * 100 / $NTRIALS" | bc)

    echo "filter<=$FILTER: NULL_rate(in)=${NULL_RATE_IN}% NULL_rate(out)=${NULL_RATE_OUT}% | NULL-MIA accuracy=${ACCURACY}%"
done

echo ""
echo "============================================================"
echo " COMBINED ATTACK: NULL + Value Threshold"
echo "============================================================"
echo "Decision rule:"
echo "  1. If query returns NULL => guess 'out'"
echo "  2. If query returns a value => use threshold test"
echo ""

for FILTER in 2 5; do
    IN_F=$(mktemp); OUT_F=$(mktemp)
    NTRIALS=500

    for seed in $(seq 1 $NTRIALS); do
        result=$($DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
INSERT INTO users VALUES (0, ${TARGET_VALUE});
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILTER} OR user_id = 0;
SQL
        )
        [ -z "$result" ] && result="NULL"
        echo "in,$result" >> "$IN_F"

        result=$($DUCKDB -noheader -list 2>/dev/null <<SQL
LOAD '${PAC_EXT}';
CREATE TABLE users(user_id INTEGER, acctbal INTEGER);
INSERT INTO users SELECT i, ((hash(i*31+7)%10000)+1)::INTEGER FROM generate_series(1,${N_USERS}) t(i);
ALTER TABLE users ADD PAC_KEY(user_id);
ALTER TABLE users SET PU;
SET pac_mi = 0.0078125;
SET pac_seed = ${seed};
SELECT SUM(acctbal) FROM users WHERE user_id <= ${FILTER};
SQL
        )
        [ -z "$result" ] && result="NULL"
        echo "out,$result" >> "$OUT_F"
    done

    FBG=$($DUCKDB -noheader -list -c "SELECT SUM((hash(i*31+7)%10000+1)::INTEGER) FROM generate_series(1,${FILTER}) t(i);" | tr -d '[:space:]')

    $DUCKDB -markdown <<SQL
CREATE TABLE raw AS
SELECT * FROM read_csv('${IN_F}', columns={'truth':'VARCHAR','val':'VARCHAR'}, header=false)
UNION ALL
SELECT * FROM read_csv('${OUT_F}', columns={'truth':'VARCHAR','val':'VARCHAR'}, header=false);

-- Combined attack: NULL => out, value > threshold => in
CREATE TABLE decisions AS
SELECT truth,
    CASE
        WHEN val = 'NULL' THEN 'out'
        WHEN TRY_CAST(val AS DOUBLE) > ${FBG} + ${TARGET_VALUE}/2.0 THEN 'in'
        ELSE 'out'
    END AS guess
FROM raw;

SELECT 'Combined(filter<=${FILTER})' AS attack,
    printf('%.1f%%', 100.0*SUM(CASE WHEN truth=guess THEN 1 ELSE 0 END)::DOUBLE/COUNT(*)) AS accuracy,
    printf('%.1f%%', 100.0*SUM(CASE WHEN truth='in' AND guess='in' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN truth='in' THEN 1 ELSE 0 END),0)) AS TPR,
    printf('%.1f%%', 100.0*SUM(CASE WHEN truth='out' AND guess='in' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN truth='out' THEN 1 ELSE 0 END),0)) AS FPR,
    COUNT(*) AS n
FROM decisions;

-- Confusion matrix
SELECT truth AS actual, guess AS predicted, COUNT(*) AS cnt
FROM decisions GROUP BY truth, guess ORDER BY truth, guess;

SQL

    rm -f "$IN_F" "$OUT_F"
    echo ""
done

echo "============================================================"
echo "PAC theoretical bound: ~53% (MI=1/128)"
echo "If combined attack > 53%, the NULL channel leaks extra info!"
echo "============================================================"
