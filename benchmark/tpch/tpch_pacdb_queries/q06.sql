--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['revenue']

PREPARE run_query AS
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
JOIN (
    SELECT * FROM customer
    JOIN random_samples AS rs ON rs.row_id = customer.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
) AS customer ON orders.o_custkey = customer.c_custkey
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;

EXECUTE run_query(sample := 0);
