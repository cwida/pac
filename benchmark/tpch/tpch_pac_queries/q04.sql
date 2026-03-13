SELECT o_orderpriority, pac_noised_count(pac_hash(hash(o_custkey))) AS order_count
  FROM orders
 WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate <  DATE '1993-10-01'
   AND EXISTS (FROM lineitem
              WHERE l_orderkey = orders.o_orderkey AND l_commitdate < l_receiptdate)
 GROUP BY ALL 
 ORDER BY ALL;
