--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']

PREPARE run_query AS
WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem,
        orders,
        customer,
        random_samples AS rs
    WHERE
        rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample
        AND o_custkey = c_custkey
        AND o_orderkey = l_orderkey
        AND l_shipdate >= CAST('1996-01-01' AS date)
        AND l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        supplier_no
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM revenue)
ORDER BY
    s_suppkey;

EXECUTE run_query(sample := 0);
