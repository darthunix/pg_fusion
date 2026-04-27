WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1.0 - l_discount)) AS total_revenue
    FROM lineitem
    WHERE l_shipdate >= '1996-01-01'
      AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey
)
SELECT
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone,
    revenue.total_revenue
FROM supplier s
JOIN revenue ON s.s_suppkey = revenue.supplier_no
WHERE revenue.total_revenue = (
    SELECT max(total_revenue)
    FROM revenue
)
ORDER BY s.s_suppkey;

