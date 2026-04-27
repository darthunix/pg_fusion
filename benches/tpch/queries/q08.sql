SELECT
    substr(o.o_orderdate, 1, 4) AS order_year,
    sum(CASE WHEN n2.n_name = 'BRAZIL'
             THEN l.l_extendedprice * (1.0 - l.l_discount)
             ELSE 0.0 END)
        / sum(l.l_extendedprice * (1.0 - l.l_discount)) AS market_share
FROM part p
JOIN lineitem l ON p.p_partkey = l.l_partkey
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
JOIN nation n1 ON n1.n_nationkey = c.c_nationkey
JOIN region r ON r.r_regionkey = n1.n_regionkey
JOIN nation n2 ON n2.n_nationkey = s.s_nationkey
WHERE r.r_name = 'AMERICA'
  AND o.o_orderdate >= '1995-01-01'
  AND o.o_orderdate <= '1996-12-31'
  AND p.p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY substr(o.o_orderdate, 1, 4)
ORDER BY order_year;

