--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['avg_yearly']

PREPARE run_query AS
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part,
    orders,
    customer,
    random_samples AS rs
WHERE
    rs.row_id = customer.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
    AND o_custkey = c_custkey
    AND o_orderkey = l_orderkey
    AND p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey);

EXECUTE run_query(sample := 0);
