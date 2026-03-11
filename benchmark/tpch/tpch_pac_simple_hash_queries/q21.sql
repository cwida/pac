SELECT s_name, pac_noised_count(pac_hash(hash(l_orderkey))) AS numwait
  FROM supplier JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
                JOIN orders ON l1.l_orderkey = o_orderkey
                JOIN nation ON s_nationkey = n_nationkey
  WHERE o_orderstatus = 'F' AND n_name = 'SAUDI ARABIA'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (FROM lineitem l2 
               WHERE l2.l_orderkey = l1.l_orderkey 
                 AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (FROM lineitem l3 
                   WHERE l3.l_orderkey = l1.l_orderkey
                     AND l3.l_suppkey <> l1.l_suppkey
                     AND l3.l_receiptdate > l3.l_commitdate)
 GROUP BY ALL
 ORDER BY numwait DESC, s_name
 LIMIT 100;
