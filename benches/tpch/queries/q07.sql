SELECT
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_shipdate,
    sum(shipping.volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        l.l_shipdate,
        l.l_extendedprice * (1.0 - l.l_discount) AS volume
    FROM supplier s
    JOIN lineitem l ON s.s_suppkey = l.l_suppkey
    JOIN orders o ON o.o_orderkey = l.l_orderkey
    JOIN customer c ON c.c_custkey = o.o_custkey
    JOIN nation n1 ON n1.n_nationkey = s.s_nationkey
    JOIN nation n2 ON n2.n_nationkey = c.c_nationkey
    WHERE (
          (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
       OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    )
      AND l.l_shipdate >= '1995-01-01'
      AND l.l_shipdate <= '1996-12-31'
) shipping
GROUP BY shipping.supp_nation, shipping.cust_nation, shipping.l_shipdate
ORDER BY shipping.supp_nation, shipping.cust_nation, shipping.l_shipdate
LIMIT 100;

