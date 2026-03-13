--var:SAMPLES = 128
--var:INDEX_COLS = ['cntrycode']
--var:OUTPUT_COLS = ['numcust', 'totacctbal']

PREPARE run_query AS
WITH customer_sample AS (
    SELECT * FROM customer
    JOIN random_samples AS rs ON rs.row_id = customer.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
)
SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer_sample
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT
                avg(c_acctbal)
            FROM
                customer_sample
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    o_custkey = c_custkey)) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;

EXECUTE run_query(sample := 0);
