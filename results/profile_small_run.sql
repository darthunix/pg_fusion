\set ON_ERROR_STOP on
\pset pager off
\timing on
\o /Users/darthunix/git/pg_fusion/results/psql_profile_small.log

SET client_min_messages = warning;
SET log_min_messages = debug1;
SET log_min_error_statement = error;
SET statement_timeout = 0;
CREATE EXTENSION IF NOT EXISTS pg_fusion;
DO $$ BEGIN RAISE WARNING 'profile_small_1772084524'; END $$;

DO $$
BEGIN
  IF to_regclass('public.bench_scan_small') IS NULL THEN
    CREATE TABLE public.bench_scan_small AS
    SELECT gs AS id,
           (gs % 1000) AS k,
           repeat(md5(gs::text), 2) AS payload
    FROM generate_series(1, 300000) gs;
  END IF;
END
$$;
ANALYZE public.bench_scan_small;
SELECT count(*) AS rows_cnt, pg_size_pretty(pg_relation_size('public.bench_scan_small')) AS rel_size FROM public.bench_scan_small;

SET pg_fusion.enable = off;
SELECT sum(id) AS sum_off FROM public.bench_scan_small;

SET pg_fusion.enable = on;
SELECT sum(id) AS sum_on FROM public.bench_scan_small;

\o
