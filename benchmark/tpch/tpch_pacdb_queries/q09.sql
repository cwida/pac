--var:SAMPLES = 128
--var:INDEX_COLS = ['nation', 'o_year']
--var:OUTPUT_COLS = ['sum_profit']

PREPARE run_query AS
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation,
        customer,
        random_samples AS rs
    WHERE
        rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample
        AND o_custkey = c_custkey
        AND s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

EXECUTE run_query(sample := 0);
