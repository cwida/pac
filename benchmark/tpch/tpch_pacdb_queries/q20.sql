--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['s_name', 's_address']

PREPARE run_query AS
SELECT
    s_name,
    s_address,
    1.0 as column_0
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'forest%')
                AND ps_availqty > (
                    SELECT
                        0.5 * sum(l_quantity)
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
                        AND l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date)))
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
        ORDER BY
            s_name;

EXECUTE run_query(sample := 0);
