SELECT o_year, (pac_noised_sum(pac_pu, (CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END))
               / pac_noised_sum(pac_pu, volume)) AS mkt_share
  FROM (SELECT EXTRACT(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation, 
               pac_hash(hash(c_custkey)) AS pac_pu
          FROM part JOIN lineitem ON p_partkey = l_partkey
                    JOIN orders ON l_orderkey = o_orderkey 
                    JOIN customer ON o_custkey = c_custkey
                    JOIN nation n1 ON c_nationkey = n1.n_nationkey
                    JOIN region ON n1.n_regionkey = r_regionkey
                    JOIN supplier ON l_suppkey = s_suppkey 
                    JOIN nation n2 ON s_nationkey = n2.n_nationkey
         WHERE r_name = 'AMERICA' AND p_type = 'ECONOMY ANODIZED STEEL' AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS all_nations
 GROUP BY ALL 
 ORDER BY ALL;
