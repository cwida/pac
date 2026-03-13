SELECT n_name, pac_noised_sum(pac_hash(hash(l_orderkey)), l_extendedprice * (1 - l_discount)) AS revenue
  FROM customer JOIN orders ON c_custkey = o_custkey
                JOIN lineitem ON o_orderkey = l_orderkey
                JOIN supplier ON l_suppkey = s_suppkey
                JOIN nation ON s_nationkey = n_nationkey AND c_nationkey = s_nationkey 
                JOIN region ON n_regionkey = r_regionkey
 WHERE r_name = 'ASIA'
   AND o_orderdate >= CAST('1994-01-01' AS date) AND o_orderdate < CAST('1995-01-01' AS date)
 GROUP BY ALL
 ORDER BY revenue DESC;
