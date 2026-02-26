--var:SAMPLES = 128
--var:INDEX_COLS = ['s_name']
--var:OUTPUT_COLS = ['numwait']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;
DROP TABLE IF EXISTS lineitem_enhanced;
DROP INDEX IF EXISTS idx_lineitem_enhanced_order_supp;

CREATE TEMP TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(128)
), random_values AS MATERIALIZED (
    SELECT
        sample_numbers.sample_id,
        customer.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN customer ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id
FROM random_values
WHERE random_binary = TRUE
ORDER BY sample_id, row_id;

CREATE TEMP TABLE lineitem_enhanced AS
SELECT l.l_orderkey,
    l.l_suppkey,
    l.l_linenumber,
    c.rowid as c_rowid,
    s.s_name as s_name,
    (l.l_receiptdate > l.l_commitdate) AS is_late,
    (o.o_orderstatus = 'F') AS is_orderstatus_f,
    (n.n_name = 'SAUDI ARABIA') AS is_nation_saudi_arabia
FROM lineitem l
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
ORDER BY l.l_orderkey, l.l_linenumber;

CREATE INDEX idx_lineitem_enhanced_order_supp ON lineitem_enhanced(l_orderkey, l_suppkey);
--end SAMPLE_STEP--


--begin PREPARE_STEP--

PREPARE run_query AS
WITH lineitem_sampled AS (
    SELECT l_orderkey,
        l_suppkey,
        s_name,
        is_late,
        is_orderstatus_f,
        is_nation_saudi_arabia
    FROM lineitem_enhanced l
    JOIN random_samples rs ON l.c_rowid = rs.row_id AND rs.sample_id = $sample
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
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);
