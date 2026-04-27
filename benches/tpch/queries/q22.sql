SELECT
    substr(c.c_phone, 1, 2) AS cntrycode,
    count(*) AS numcust,
    sum(c.c_acctbal) AS totalacctbal
FROM customer c
WHERE substr(c.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND c.c_acctbal > (
      SELECT avg(c2.c_acctbal)
      FROM customer c2
      WHERE c2.c_acctbal > 0.0
        AND substr(c2.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM orders o
      WHERE o.o_custkey = c.c_custkey
  )
GROUP BY substr(c.c_phone, 1, 2)
ORDER BY cntrycode;

