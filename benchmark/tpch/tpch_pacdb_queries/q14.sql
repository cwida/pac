--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['promo_revenue']

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
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part,
    orders,
    customer,
    random_samples AS rs
WHERE
    rs.row_id = orders.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
    AND o_custkey = c_custkey
    AND o_orderkey = l_orderkey
    AND l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);
