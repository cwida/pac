--var:SAMPLES = 128
--var:INDEX_COLS = ['l_shipmode']
--var:OUTPUT_COLS = ['high_line_count', 'low_line_count']

PREPARE run_query AS
SELECT
    l_shipmode,
    sum(
        CASE WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN
            1
        ELSE
            0
        END) AS high_line_count,
    sum(
        CASE WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN
            1
        ELSE
            0
        END) AS low_line_count
FROM
    customer,
    lineitem,
    (SELECT * FROM orders
        JOIN random_samples_orders AS rs ON rs.row_id = orders.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample) AS orders
WHERE
    o_custkey = c_custkey
    AND o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS date)
    AND l_receiptdate < CAST('1995-01-01' AS date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

EXECUTE run_query(sample := 0);
