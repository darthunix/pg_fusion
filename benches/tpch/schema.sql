DROP SCHEMA IF EXISTS __TPCH_SCHEMA__ CASCADE;
CREATE SCHEMA __TPCH_SCHEMA__;
SET search_path TO __TPCH_SCHEMA__, public;

CREATE TABLE region (
    r_regionkey integer NOT NULL,
    r_name text NOT NULL,
    r_comment text NOT NULL
);

CREATE TABLE nation (
    n_nationkey integer NOT NULL,
    n_name text NOT NULL,
    n_regionkey integer NOT NULL,
    n_comment text NOT NULL
);

CREATE TABLE supplier (
    s_suppkey integer NOT NULL,
    s_name text NOT NULL,
    s_address text NOT NULL,
    s_nationkey integer NOT NULL,
    s_phone text NOT NULL,
    s_acctbal double precision NOT NULL,
    s_comment text NOT NULL
);

CREATE TABLE part (
    p_partkey integer NOT NULL,
    p_name text NOT NULL,
    p_mfgr text NOT NULL,
    p_brand text NOT NULL,
    p_type text NOT NULL,
    p_size integer NOT NULL,
    p_container text NOT NULL,
    p_retailprice double precision NOT NULL,
    p_comment text NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey integer NOT NULL,
    ps_suppkey integer NOT NULL,
    ps_availqty integer NOT NULL,
    ps_supplycost double precision NOT NULL,
    ps_comment text NOT NULL
);

CREATE TABLE customer (
    c_custkey integer NOT NULL,
    c_name text NOT NULL,
    c_address text NOT NULL,
    c_nationkey integer NOT NULL,
    c_phone text NOT NULL,
    c_acctbal double precision NOT NULL,
    c_mktsegment text NOT NULL,
    c_comment text NOT NULL
);

CREATE TABLE orders (
    o_orderkey integer NOT NULL,
    o_custkey integer NOT NULL,
    o_orderstatus text NOT NULL,
    o_totalprice double precision NOT NULL,
    o_orderdate text NOT NULL,
    o_orderpriority text NOT NULL,
    o_clerk text NOT NULL,
    o_shippriority integer NOT NULL,
    o_comment text NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey integer NOT NULL,
    l_partkey integer NOT NULL,
    l_suppkey integer NOT NULL,
    l_linenumber integer NOT NULL,
    l_quantity double precision NOT NULL,
    l_extendedprice double precision NOT NULL,
    l_discount double precision NOT NULL,
    l_tax double precision NOT NULL,
    l_returnflag text NOT NULL,
    l_linestatus text NOT NULL,
    l_shipdate text NOT NULL,
    l_commitdate text NOT NULL,
    l_receiptdate text NOT NULL,
    l_shipinstruct text NOT NULL,
    l_shipmode text NOT NULL,
    l_comment text NOT NULL
);

