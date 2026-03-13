SELECT nation, o_year, pac_noised_sum(pac_hash(hash(l_orderkey)), amount) AS sum_profit
  FROM (SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, l_orderkey,
               l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
          FROM part JOIN lineitem ON p_partkey = l_partkey
                    JOIN supplier ON l_suppkey = s_suppkey
                    JOIN partsupp ON l_suppkey = ps_suppkey AND l_partkey = ps_partkey
                    JOIN orders ON l_orderkey = o_orderkey
                    JOIN nation ON s_nationkey = n_nationkey
         WHERE p_name LIKE '%green%') AS profit
 GROUP BY ALL
 ORDER BY nation, o_year DESC;
