\set ON_ERROR_STOP on
\pset pager off
\timing on
\o /Users/darthunix/git/pg_fusion/results/psql_profile.log

SET client_min_messages = debug1;
SET log_min_messages = debug1;
SET log_min_error_statement = error;
SET statement_timeout = 0;
CREATE EXTENSION IF NOT EXISTS pg_fusion;

DO $$
BEGIN
  IF to_regclass('public.bench_scan') IS NULL THEN
    CREATE TABLE public.bench_scan AS
    SELECT gs AS id,
           (gs % 1000) AS k,
           repeat(md5(gs::text), 2) AS payload
    FROM generate_series(1, 2000000) gs;
  END IF;
END
$$;
ANALYZE public.bench_scan;
SELECT count(*) AS rows_cnt, pg_size_pretty(pg_relation_size('public.bench_scan')) AS rel_size FROM public.bench_scan;

SET pg_fusion.enable = off;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, SUMMARY ON) SELECT sum(id) FROM public.bench_scan;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, SUMMARY ON) SELECT sum(id) FROM public.bench_scan;

SET pg_fusion.enable = on;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, SUMMARY ON) SELECT sum(id) FROM public.bench_scan;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, SUMMARY ON) SELECT sum(id) FROM public.bench_scan;
EXPLAIN (ANALYZE, BUFFERS, TIMING ON, SUMMARY ON) SELECT sum(id) FROM public.bench_scan WHERE id >= 0;

\o
