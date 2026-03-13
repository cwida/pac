-- Modified TPC-H q1 to run on 50% subsamples


--var:SAMPLES = 128
--var:INDEX_COLS = ['l_returnflag', 'l_linestatus']
--var:OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']


PREPARE run_query AS
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
JOIN customer ON orders.o_custkey = customer.c_custkey
JOIN random_samples AS rs
    ON rs.row_id = customer.rowid
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

EXECUTE run_query(sample := 0);
