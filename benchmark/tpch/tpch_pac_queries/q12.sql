SELECT l_shipmode, pac_noised_sum(pac_hash(hash(o_custkey)), CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
                   pac_noised_sum(pac_hash(hash(o_custkey)), CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
  FROM orders JOIN lineitem ON o_orderkey = l_orderkey
 WHERE l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_shipmode IN ('MAIL', 'SHIP')
   AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'
 GROUP BY ALL 
 ORDER BY ALL;
