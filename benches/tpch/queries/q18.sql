SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    sum(l.l_quantity) AS quantity
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderkey IN (
    SELECT l2.l_orderkey
    FROM lineitem l2
    GROUP BY l2.l_orderkey
    HAVING sum(l2.l_quantity) > 300.0
)
GROUP BY c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice
ORDER BY o.o_totalprice DESC, o.o_orderdate
LIMIT 100;

