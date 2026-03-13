--var:SAMPLES = 128
--var:INDEX_COLS = ['c_count']
--var:OUTPUT_COLS = ['custdist']

PREPARE run_query AS
SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey)
    FROM
        (SELECT * FROM customer
        JOIN random_samples AS rs ON rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample) AS customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;

EXECUTE run_query(sample := 0);
