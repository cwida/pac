SELECT l_shipmode, 
       pac_noised_sum(pac_hash(hash(l_orderkey)), CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
       pac_noised_sum(pac_hash(hash(l_orderkey)), CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
  FROM orders JOIN lineitem ON o_orderkey = l_orderkey
 WHERE l_shipmode IN ('MAIL', 'SHIP')
   AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
   AND l_receiptdate >= CAST('1994-01-01' AS date) AND l_receiptdate < CAST('1995-01-01' AS date)
 GROUP BY ALL
 ORDER BY ALL;
