SELECT
    100.0 * sum(CASE WHEN p.p_type LIKE 'PROMO%'
                     THEN l.l_extendedprice * (1.0 - l.l_discount)
                     ELSE 0.0 END)
        / sum(l.l_extendedprice * (1.0 - l.l_discount)) AS promo_revenue
FROM lineitem l
JOIN part p ON p.p_partkey = l.l_partkey
WHERE l.l_shipdate >= '1995-09-01'
  AND l.l_shipdate < '1995-10-01';

