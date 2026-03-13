--var:SAMPLES = 128
--var:INDEX_COLS = ['n_name']
--var:OUTPUT_COLS = ['revenue']

PREPARE run_query AS
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    (SELECT * FROM customer
              JOIN random_samples AS rs ON rs.row_id = customer.rowid
              AND rs.random_binary = TRUE
              AND rs.sample_id = $sample) as customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;

EXECUTE run_query(sample := 0);
