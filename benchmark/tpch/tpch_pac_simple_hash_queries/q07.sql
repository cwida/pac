SELECT supp_nation, cust_nation, l_year, pac_noised_sum(pac_hash(hash(l_orderkey)), volume) AS revenue
  FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year,
               l_orderkey, l_extendedprice * (1 - l_discount) AS volume
          FROM supplier JOIN lineitem ON s_suppkey = l_suppkey
                        JOIN orders ON l_orderkey = o_orderkey
                        JOIN customer ON o_custkey = c_custkey
                        JOIN nation n1 ON s_nationkey = n1.n_nationkey
                        JOIN nation n2 ON c_nationkey = n2.n_nationkey
         WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
           AND l_shipdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1996-12-31' AS date)) AS shipping
 GROUP BY ALL
 ORDER BY ALL;
