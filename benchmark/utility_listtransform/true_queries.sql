-- Q1: 1 ratio
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q2: 2 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q3: 3 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q4: 4 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q5: 5 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q6: 6 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * l_tax) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q7: 7 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_tax)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q8: 8 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (l_discount + l_tax)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q9: 9 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (l_discount + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount - l_tax)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';

-- Q10: 10 ratios
SELECT CAST(100.0 * SUM(l_extendedprice * (1 - l_discount)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * l_discount * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount) * l_tax) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (l_discount + l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 - l_discount - l_tax)) / SUM(l_extendedprice) + 100.0 * SUM(l_extendedprice * (1 + l_discount)) / SUM(l_extendedprice) AS DOUBLE) AS result
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= DATE '1998-01-16';
