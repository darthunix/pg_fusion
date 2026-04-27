SELECT
    order_count,
    count(*) AS custdist
FROM (
    SELECT
        c.c_custkey,
        count(o.o_orderkey) AS order_count
    FROM customer c
    LEFT JOIN orders o
      ON c.c_custkey = o.o_custkey
     AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) counts
GROUP BY order_count
ORDER BY custdist DESC, order_count DESC
LIMIT 100;

