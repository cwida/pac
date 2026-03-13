SELECT o_orderpriority, pac_noised_count(pac_hash(hash(o_orderkey)), 1) AS order_count
  FROM orders
 WHERE o_orderdate >= CAST('1993-07-01' AS date) AND o_orderdate < CAST('1993-10-01' AS date)
   AND EXISTS (FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
 GROUP BY ALL
 ORDER BY ALL;
