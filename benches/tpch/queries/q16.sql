SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    count(DISTINCT ps.ps_suppkey) AS supplier_count
FROM partsupp ps
JOIN part p ON p.p_partkey = ps.ps_partkey
WHERE p.p_brand <> 'Brand#45'
  AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps.ps_suppkey NOT IN (
      SELECT s.s_suppkey
      FROM supplier s
      WHERE s.s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p.p_brand, p.p_type, p.p_size
ORDER BY supplier_count DESC, p.p_brand, p.p_type, p.p_size
LIMIT 100;

