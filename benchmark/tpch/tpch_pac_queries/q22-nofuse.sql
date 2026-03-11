SELECT cntrycode, pac_noised_count(pac_hash(hash(c_custkey))) AS numcust, 
                  pac_noised_sum(pac_hash(hash(c_custkey)), c_acctbal) AS totacctbal
  FROM (SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal, c_custkey
          FROM customer
         WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
           AND pac_filter(
                 list_transform(
                    list_transform(
                     (SELECT list_transform(
                               list_zip(pac_sum(pac_hash(hash(c_custkey)), c_acctbal),
                                        pac_count(pac_hash(hash(c_custkey)), c_acctbal)),
                               lambda x: x[1] / x[2])
                        FROM customer
                       WHERE c_acctbal > 0.00
                         AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')), 
                   lambda x: CAST(x AS DECIMAL(18,2))),
                     lambda y: c_acctbal > y))
       AND NOT EXISTS (FROM orders WHERE o_custkey = customer.c_custkey)) AS custsale 
 GROUP BY ALL
 ORDER BY ALL;
