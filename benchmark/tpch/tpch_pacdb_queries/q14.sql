--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['promo_revenue']

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
    random_samples_orders AS rs
WHERE
    rs.row_id = orders.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
    AND o_custkey = c_custkey
    AND o_orderkey = l_orderkey
    AND l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);

EXECUTE run_query(sample := 0);
