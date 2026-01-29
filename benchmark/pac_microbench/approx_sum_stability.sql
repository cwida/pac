-- Test numeric accuracy of approximate pac_sum vs exact SUM
-- pac_sum with hash(1) and mi=0, divide by 2 since hash(1) has all bits set

-- Create test table with various distributions
CREATE OR REPLACE TABLE accuracy_test AS
WITH uniform_tiny   AS (SELECT 'uniform_tinyint' AS dist, (i % 127)::TINYINT AS val FROM range(1000000) t(i)),
     uniform_small  AS (SELECT 'uniform_smallint' AS dist, (i % 32767)::SMALLINT AS val FROM range(1000000) t(i)),
     uniform_int    AS (SELECT 'uniform_int' AS dist, (i % 2147483647)::INTEGER AS val FROM range(1000000) t(i)),
     uniform_bigint AS (SELECT 'uniform_bigint' AS dist, i::BIGINT AS val FROM range(1000000) t(i)),
     zipf_like      AS (SELECT 'zipf_like' AS dist, (1000000.0 / (i + 1))::BIGINT AS val FROM range(1000000) t(i)),
     exponential    AS (SELECT 'exponential' AS dist, (EXP(random() * 10))::BIGINT AS val FROM range(1000000) t(i)),
     bimodal        AS (SELECT 'bimodal' AS dist, CASE WHEN i % 2 = 0 THEN (i % 100)::BIGINT ELSE (1000000 + i % 100)::BIGINT END AS val FROM range(1000000) t(i)),
     sparse_large   AS (SELECT 'sparse_large' AS dist, CASE WHEN i % 1000 = 0 THEN 1000000::BIGINT ELSE 0::BIGINT END AS val FROM range(1000000) t(i)),
     negative_mixed AS (SELECT 'negative_mixed' AS dist, CASE WHEN i % 2 = 0 THEN i::BIGINT ELSE -i::BIGINT END AS val FROM range(1000000) t(i)),
     all_same       AS (SELECT 'all_same' AS dist, 42::BIGINT AS val FROM range(1000000) t(i))
SELECT * FROM uniform_tiny
UNION ALL SELECT * FROM uniform_small
UNION ALL SELECT * FROM uniform_int
UNION ALL SELECT * FROM uniform_bigint
UNION ALL SELECT * FROM zipf_like
UNION ALL SELECT * FROM exponential
UNION ALL SELECT * FROM bimodal
UNION ALL SELECT * FROM sparse_large
UNION ALL SELECT * FROM negative_mixed
UNION ALL SELECT * FROM all_same;

-- Compare exact SUM vs approximate pac_sum
SELECT dist AS distribution, COUNT(*) AS n_rows, SUM(val) AS exact_sum,
      (pac_sum(val, hash(1), 0) / 2)::BIGINT AS approx_sum, -- pac_sum with hash(1) has all bits set, so divide by 2
      ROUND(100.0 * ABS(SUM(val) - (pac_sum(val, hash(1), 0) / 2)) / NULLIF(ABS(SUM(val)), 0), 6) AS error_pct,
      ROUND((pac_sum(val, hash(1), 0) / 2) / NULLIF(SUM(val)::DOUBLE, 0), 6) AS ratio
FROM accuracy_test
GROUP BY dist
ORDER BY error_pct DESC;

-- Detailed stats per distribution
SELECT dist AS distribution, MIN(val) AS min_val, MAX(val) AS max_val, AVG(val)::BIGINT AS avg_val, STDDEV(val)::BIGINT AS stddev_val
FROM accuracy_test
GROUP BY dist
ORDER BY dist;
