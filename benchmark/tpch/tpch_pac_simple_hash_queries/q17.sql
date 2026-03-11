SELECT pac_noised_sum(pac_hash(hash(l_orderkey)), l_extendedprice) / 7.0 AS avg_yearly
  FROM lineitem JOIN part ON lineitem.l_partkey = part.p_partkey
 WHERE part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX' AND 
       lineitem.l_quantity*5 < (SELECT pac_noised_div(pac_sum(pac_hash(hash(l_sub.l_orderkey)), l_sub.l_quantity), 
                                                      pac_count(pac_hash(hash(l_sub.l_orderkey)), l_sub.l_quantity)) 
                                  FROM lineitem AS l_sub
                                 WHERE l_sub.l_partkey = part.p_partkey);
