SELECT
    n.n_name,
    substr(o.o_orderdate, 1, 4) AS order_year,
    sum(l.l_extendedprice * (1.0 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS profit
FROM part p
JOIN lineitem l ON p.p_partkey = l.l_partkey
JOIN partsupp ps ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN nation n ON n.n_nationkey = s.s_nationkey
WHERE p.p_name LIKE '%green%'
GROUP BY n.n_name, substr(o.o_orderdate, 1, 4)
ORDER BY n.n_name, order_year DESC
LIMIT 100;

