SELECT s_name, s_address
  FROM supplier JOIN nation ON s_nationkey = n_nationkey 
 WHERE s_suppkey IN (SELECT ps_suppkey 
                       FROM partsupp
                      WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
                        AND ps_availqty * 2 > (SELECT pac_noised_sum(pac_hash(hash(l_orderkey)), l_quantity)
                                                 FROM lineitem 
                                                WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey 
                                                  AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'))
   AND n_name = 'CANADA'
 ORDER BY s_name;
