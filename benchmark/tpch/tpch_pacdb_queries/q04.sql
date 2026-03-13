--var:SAMPLES = 128
--var:INDEX_COLS = ['o_orderpriority']
--var:OUTPUT_COLS = ['order_count']

PREPARE run_query AS
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    customer,
    (SELECT * FROM orders
        JOIN random_samples_orders AS rs ON rs.row_id = orders.rowid
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

EXECUTE run_query(sample := 0);
