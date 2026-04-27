SELECT
    ps.ps_partkey,
    sum(ps.ps_supplycost * ps.ps_availqty) AS value
FROM partsupp ps
JOIN supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN nation n ON n.n_nationkey = s.s_nationkey
WHERE n.n_name = 'GERMANY'
GROUP BY ps.ps_partkey
HAVING sum(ps.ps_supplycost * ps.ps_availqty) > (
    SELECT sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
    FROM partsupp ps2
    JOIN supplier s2 ON s2.s_suppkey = ps2.ps_suppkey
    JOIN nation n2 ON n2.n_nationkey = s2.s_nationkey
    WHERE n2.n_name = 'GERMANY'
)
ORDER BY value DESC
LIMIT 100;

