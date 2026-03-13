SELECT l_returnflag, l_linestatus,
       pac_noised_sum(pac_pu, l_quantity) AS sum_qty,
       pac_noised_sum(pac_pu, l_extendedprice) AS sum_base_price,
       pac_noised_sum(pac_pu, l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       pac_noised_sum(pac_pu, l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       pac_noised_sum(pac_pu, l_quantity) / pac_noised_count(pac_pu, l_quantity) AS avg_qty,
       pac_noised_sum(pac_pu, l_extendedprice) / pac_noised_count(pac_pu, l_extendedprice) AS avg_price,
       pac_noised_sum(pac_pu, l_discount) / pac_noised_count(pac_pu, l_discount) AS avg_disc,
       pac_noised_count(pac_pu) AS count_order
  FROM (SELECT l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax,
               pac_hash(hash(l_orderkey)) AS pac_pu
          FROM lineitem
         WHERE l_shipdate <= CAST('1998-09-02' AS date)) sales
 GROUP BY ALL
 ORDER BY ALL;
