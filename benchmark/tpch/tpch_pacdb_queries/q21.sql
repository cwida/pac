--var:SAMPLES = 128
--var:INDEX_COLS = ['s_name']
--var:OUTPUT_COLS = ['numwait']

PREPARE run_query AS
WITH lineitem_sampled AS (
    SELECT l_orderkey,
        l_suppkey,
        s_name,
        is_late,
        is_orderstatus_f,
        is_nation_saudi_arabia
    FROM lineitem_enhanced l
    JOIN random_samples rs ON l.c_rowid = rs.row_id AND rs.random_binary = TRUE AND rs.sample_id = $sample
)
SELECT
    l.s_name,
    COUNT(*) AS numwait
FROM
    lineitem_sampled l
WHERE
    l.is_late = TRUE
    AND l.is_orderstatus_f = TRUE
    AND l.is_nation_saudi_arabia = TRUE
    AND EXISTS (
        SELECT 1
        FROM lineitem_enhanced l2
        WHERE l2.l_orderkey = l.l_orderkey
          AND l2.l_suppkey <> l.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem_enhanced l3
        WHERE l3.l_orderkey = l.l_orderkey
          AND l3.l_suppkey <> l.l_suppkey
          AND l3.is_late = TRUE
    )
GROUP BY
    l.s_name
ORDER BY
    numwait DESC,
    l.s_name;

EXECUTE run_query(sample := 0);
