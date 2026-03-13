-- q17: 64-possible-worlds-semantics with lambda expressions. 
-- the pac_filter makes a probabilistic choice over 64 booleans
-- the outer list_transform-lambda computes the expression, for all 64 possible worlds
-- the inner list_transform just casts. This is needed to make the original expression (l_quantity < 0.2 * x) work safely 
SELECT pac_noised_sum(pac_pu, l_extendedprice) / 7.0 AS avg_yearly,
  FROM (SELECT pac_select_lt(
                 pac_hash(hash(o_custkey)),
                 lineitem.l_quantity*5, 
                 (SELECT pac_div(pac_sum(pac_hash(hash(o_sub.o_custkey)), l_sub.l_quantity),
                                 pac_count(pac_hash(hash(o_sub.o_custkey)), l_sub.l_quantity))
                    FROM lineitem AS l_sub JOIN orders AS o_sub ON l_sub.l_orderkey = o_sub.o_orderkey
                   WHERE l_sub.l_partkey = part.p_partkey)) AS pac_pu,
               l_extendedprice,
          FROM lineitem JOIN part ON lineitem.l_partkey = part.p_partkey JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
         WHERE part.p_brand = 'Brand#23' AND part.p_container = 'MED BOX')
 WHERE pac_pu <> 0;
