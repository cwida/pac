SELECT 100.0 * pac_noised_sum(pac_hash(hash(l_orderkey)), CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
             / pac_noised_sum(pac_hash(hash(l_orderkey)), l_extendedprice * (1 - l_discount)) AS promo_revenue
  FROM lineitem JOIN part ON l_partkey = p_partkey 
 WHERE l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01';
