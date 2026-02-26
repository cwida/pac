--var:SAMPLES = 128
--var:INDEX_COLS = ['o_orderpriority']
--var:OUTPUT_COLS = ['order_count']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TEMP TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(128)
), random_values AS MATERIALIZED (
    SELECT
        sample_numbers.sample_id,
        orders.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN orders ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
ORDER BY sample_id, row_id;
--end SAMPLE_STEP--

--begin PREPARE_STEP--

PREPARE run_query AS
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    customer,
    (SELECT * FROM orders
        JOIN random_samples AS rs ON rs.row_id = orders.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample) AS orders
WHERE
    c_custkey = o_custkey
    AND o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);
