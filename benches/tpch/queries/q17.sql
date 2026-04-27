SELECT
    sum(l.l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem l
JOIN part p ON p.p_partkey = l.l_partkey
WHERE p.p_brand = 'Brand#23'
  AND p.p_container = 'MED BOX'
  AND l.l_quantity < (
      SELECT 0.2 * avg(l2.l_quantity)
      FROM lineitem l2
      WHERE l2.l_partkey = p.p_partkey
  );

