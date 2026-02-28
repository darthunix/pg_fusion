\set ON_ERROR_STOP on
\pset pager off
\timing on

SET client_min_messages = debug1;
SET log_min_messages = debug1;
SET log_min_error_statement = error;
SET statement_timeout = 0;
CREATE EXTENSION IF NOT EXISTS pg_fusion;
ANALYZE public.bench_scan;
SELECT count(*) AS rows_cnt, pg_size_pretty(pg_relation_size('public.bench_scan')) AS rel_size FROM public.bench_scan;

SET pg_fusion.enable = off;
SELECT sum(id) AS sum_off_1 FROM public.bench_scan;
SELECT sum(id) AS sum_off_2 FROM public.bench_scan;

SET pg_fusion.enable = on;
SELECT sum(id) AS sum_on_1 FROM public.bench_scan;
SELECT sum(id) AS sum_on_2 FROM public.bench_scan;
SELECT sum(id) AS sum_on_filter FROM public.bench_scan WHERE id >= 0;
