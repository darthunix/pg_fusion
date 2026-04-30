use control_transport::{AcquireError, BackendSlotLease};
use postgres::{Client, SimpleQueryMessage, Transaction};
use std::thread;
use std::time::{Duration, Instant};

use crate::shmem::attach_control_region;

const WORKER_START_TIMEOUT: Duration = Duration::from_secs(5);
const SMOKE_TEST_ADVISORY_LOCK: i64 = 0x5047_4655_5349_4f4e;

fn ensure_shared_preload(client: &mut Client) {
    let preload = simple_query_first_column_client(client, "SHOW shared_preload_libraries")
        .expect("SHOW shared_preload_libraries must return one row");
    assert!(
        preload
            .split(',')
            .map(str::trim)
            .any(|lib| lib == "pg_fusion"),
        "pg_fusion must be in shared_preload_libraries, got: {preload}"
    );
}

fn wait_for_worker() {
    let region = attach_control_region();
    let deadline = Instant::now() + WORKER_START_TIMEOUT;
    loop {
        match BackendSlotLease::acquire(&region) {
            Ok(mut lease) => {
                lease.release();
                return;
            }
            Err(AcquireError::WorkerOffline | AcquireError::Empty) => {}
            Err(err) => panic!("control transport readiness probe failed: {err}"),
        }
        assert!(
            Instant::now() < deadline,
            "pg_fusion background worker did not publish an online control generation within {:?}",
            WORKER_START_TIMEOUT
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn smoke_client() -> Client {
    wait_for_worker();
    let (client, _session_id) = pgrx_tests::client().expect("connect to pgrx test cluster");
    client
}

fn smoke_transaction(client: &mut Client) -> Transaction<'_> {
    ensure_shared_preload(client);
    let mut tx = client.transaction().expect("start smoke-test transaction");
    tx.batch_execute(&format!(
        "\
        SELECT pg_advisory_xact_lock({SMOKE_TEST_ADVISORY_LOCK});
        SET LOCAL pg_fusion.enable = on
        "
    ))
    .expect("initialize pg_fusion smoke-test session state");
    tx
}

fn batch_execute_pg_fusion_disabled(tx: &mut Transaction<'_>, sql: &str) {
    tx.batch_execute("SET LOCAL pg_fusion.enable = off")
        .expect("disable pg_fusion during fixture setup");
    tx.batch_execute(sql)
        .expect("fixture setup with pg_fusion disabled must succeed");
    tx.batch_execute("SET LOCAL pg_fusion.enable = on")
        .expect("re-enable pg_fusion after fixture setup");
}

fn simple_query_first_column_client(client: &mut Client, sql: &str) -> Option<String> {
    client
        .simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
}

fn simple_query_first_column_tx(tx: &mut Transaction<'_>, sql: &str) -> Option<String> {
    tx.simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
}

fn simple_query_first_column_rows_tx(tx: &mut Transaction<'_>, sql: &str) -> Vec<String> {
    tx.simple_query(sql)
        .expect("simple query must succeed")
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

pub(crate) fn simple_select_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let one: i64 = simple_query_first_column_tx(&mut tx, "SELECT 1::bigint AS one")
        .expect("simple smoke select must return one row")
        .parse()
        .expect("simple smoke select must return one bigint value");
    assert_eq!(one, 1);
}

pub(crate) fn explain_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let explain =
        simple_query_first_column_tx(&mut tx, "EXPLAIN (FORMAT JSON) SELECT 1::bigint AS one")
            .expect("smoke EXPLAIN must return one row");
    assert!(
        explain.contains("\"Plan\""),
        "unexpected EXPLAIN JSON: {explain}"
    );
    let verbose_simple =
        simple_query_first_column_rows_tx(&mut tx, "EXPLAIN (VERBOSE) SELECT 1::bigint AS one")
            .join("\n");
    assert!(
        verbose_simple.contains("Custom Scan (PgFusionScan)"),
        "verbose EXPLAIN should render pg_fusion custom scan: {verbose_simple}"
    );

    reset_heap_fixture(&mut tx, "pg_fusion_explain_smoke");
    let text_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN SELECT * FROM pg_fusion_explain_smoke WHERE id = 1",
    )
    .join("\n");
    assert!(
        !text_explain.contains("pg_fusion:"),
        "text EXPLAIN should not render pg_fusion property label: {text_explain}"
    );
    assert!(
        text_explain.contains("PostgreSQL Scan:"),
        "text EXPLAIN should render the DataFusion leaf on a separate line: {text_explain}"
    );
    assert!(
        !text_explain.contains("PostgreSQL Plan:"),
        "text EXPLAIN should not render a redundant nested plan header: {text_explain}"
    );
    let verbose_heap_explain = simple_query_first_column_rows_tx(
        &mut tx,
        "EXPLAIN (VERBOSE) SELECT * FROM pg_fusion_explain_smoke WHERE id = 1",
    )
    .join("\n");
    assert!(
        verbose_heap_explain.contains("PostgreSQL Scan:")
            && verbose_heap_explain.contains("PgFusion Producers:"),
        "verbose heap EXPLAIN should render pg_fusion scan details: {verbose_heap_explain}"
    );
}

fn reset_heap_fixture(tx: &mut Transaction<'_>, table_name: &str) {
    tx.batch_execute(&format!(
        "CREATE TEMP TABLE {table_name} (id bigint NOT NULL, payload text NOT NULL)"
    ))
    .expect("create temp heap table must succeed");
    tx.batch_execute(&format!(
        "INSERT INTO {table_name} (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three')"
    ))
    .expect("insert temp heap fixture rows must succeed");
}

pub(crate) fn heap_select_single_row_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_single_row_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id = 2"),
    )
    .expect("single-row heap select must return one row")
    .parse()
    .expect("single-row heap select must return one bigint value");
    assert_eq!(id, 2);
}

pub(crate) fn heap_select_filtered_row_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_filtered_row_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id > 2"),
    )
    .expect("filtered heap select must return one row")
    .parse()
    .expect("filtered heap select must return one bigint value");
    assert_eq!(id, 3);
}

pub(crate) fn heap_avg_full_scan_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_avg_full_scan_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "CREATE TEMP TABLE {table_name} AS \
         SELECT generate_series(1, 50000)::bigint AS a"
        ),
    );

    let avg: f64 =
        simple_query_first_column_tx(&mut tx, &format!("SELECT avg(a) FROM {table_name}"))
            .expect("heap avg full scan must return one row")
            .parse()
            .expect("heap avg full scan must return a numeric value");
    assert!(
        (avg - 25000.5).abs() < 0.001,
        "heap avg full scan returned {avg}, expected 25000.5"
    );
}

pub(crate) fn heap_varlena_full_scan_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_heap_varlena_full_scan_smoke";
    batch_execute_pg_fusion_disabled(
        &mut tx,
        &format!(
            "\
        CREATE TEMP TABLE {table_name} AS
        SELECT
            g::bigint AS id,
            repeat(md5(g::text), 16) AS payload
        FROM generate_series(1, 5000) AS g
        "
        ),
    );

    let summary = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT concat(count(payload), ',', sum(id)) FROM {table_name}"),
    )
    .expect("heap varlena full scan must return one row");
    assert_eq!(summary, "5000,12502500");
}

pub(crate) fn heap_join_two_tables_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let left_table = "pg_temp.pgf_heap_join_left";
    let right_table = "pg_temp.pgf_heap_join_right";

    tx.batch_execute(&format!(
        "\
        CREATE TEMP TABLE {left_table} (
            id bigint NOT NULL,
            payload text NOT NULL
        );
        CREATE TEMP TABLE {right_table} (
            left_id bigint NOT NULL,
            score bigint NOT NULL,
            marker text NOT NULL
        );
        INSERT INTO {left_table} (id, payload)
        VALUES (1, 'one'), (2, 'two'), (3, 'three');
        INSERT INTO {right_table} (left_id, score, marker)
        VALUES (2, 20, 'dos'), (3, 30, 'tres'), (4, 40, 'cuatro');
        "
    ))
    .expect("create and populate temp heap join fixture must succeed");

    let joined_count: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "SELECT count(*)::bigint \
             FROM {left_table} AS l \
             JOIN {right_table} AS r ON l.id = r.left_id"
        ),
    )
    .expect("heap join count must return one row")
    .parse()
    .expect("heap join count must return one bigint value");
    assert_eq!(joined_count, 2);

    let score: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!(
            "SELECT r.score::bigint \
             FROM {left_table} AS l \
             JOIN {right_table} AS r ON l.id = r.left_id \
             WHERE l.payload = 'two'"
        ),
    )
    .expect("filtered heap join must return one row")
    .parse()
    .expect("filtered heap join must return one bigint value");
    assert_eq!(score, 20);
}

pub(crate) fn heap_multi_use_cte_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    batch_execute_pg_fusion_disabled(
        &mut tx,
        "\
        CREATE TEMP TABLE pgf_cte_lineitem (
            l_suppkey bigint NOT NULL,
            l_extendedprice double precision NOT NULL,
            l_discount double precision NOT NULL,
            l_shipdate text NOT NULL
        );
        CREATE TEMP TABLE pgf_cte_supplier (
            s_suppkey bigint NOT NULL,
            s_name text NOT NULL
        );
        INSERT INTO pgf_cte_supplier VALUES
            (1, 'supplier#1'),
            (2, 'supplier#2');
        INSERT INTO pgf_cte_lineitem VALUES
            (1, 100.0, 0.10, '1996-01-10'),
            (1, 200.0, 0.00, '1996-02-10'),
            (2, 500.0, 0.10, '1996-01-20'),
            (2, 100.0, 0.00, '1996-03-20'),
            (2, 1000.0, 0.00, '1996-06-01')
        ",
    );

    let supplier = simple_query_first_column_tx(
        &mut tx,
        "\
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                sum(l_extendedprice * (1.0 - l_discount)) AS total_revenue
            FROM pgf_cte_lineitem
            WHERE l_shipdate >= '1996-01-01'
              AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        )
        SELECT s.s_name
        FROM pgf_cte_supplier s
        JOIN revenue ON s.s_suppkey = revenue.supplier_no
        WHERE revenue.total_revenue = (SELECT max(total_revenue) FROM revenue)
        ORDER BY s.s_suppkey
        ",
    )
    .expect("multi-use CTE query must return one row");
    assert_eq!(supplier, "supplier#2");

    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        "\
        EXPLAIN
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                sum(l_extendedprice * (1.0 - l_discount)) AS total_revenue
            FROM pgf_cte_lineitem
            WHERE l_shipdate >= '1996-01-01'
              AND l_shipdate < '1996-04-01'
            GROUP BY l_suppkey
        )
        SELECT s.s_name
        FROM pgf_cte_supplier s
        JOIN revenue ON s.s_suppkey = revenue.supplier_no
        WHERE revenue.total_revenue = (SELECT max(total_revenue) FROM revenue)
        ",
    )
    .join("\n");
    assert!(
        explain.contains("CteScanExec: revenue"),
        "EXPLAIN should expose materialized multi-use CTE reads: {explain}"
    );
}

pub(crate) fn heap_parallel_scan_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let table_name = "public.pgf_parallel_scan_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 20000) AS g;
            ANALYZE {table_name};
            "
        ))
        .expect("create committed heap table for parallel scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 2;
        SET LOCAL pg_fusion.scan_timing_detail = on
        ",
    )
    .expect("enable dynamic scan workers");
    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"),
    )
    .join("\n");
    assert!(
        explain.contains("PgFusion Producers: planned=3 (leader + 2 workers)")
            && explain.contains("strategy=ctid_range"),
        "EXPLAIN should show planned dynamic scan producers: {explain}"
    );
    let analyze_explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!(
            "EXPLAIN ANALYZE SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"
        ),
    )
    .join("\n");
    assert!(
        analyze_explain.contains(
            "PgFusion Producers: planned=3 (leader + 2 workers), actual=3 (leader + 2 workers)"
        ),
        "EXPLAIN ANALYZE should show actual dynamic scan producers: {analyze_explain}"
    );
    simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
        .expect("reset metrics before parallel scan");
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 100 AND 20000"),
    )
    .expect("parallel heap scan must return one row")
    .parse()
    .expect("parallel heap scan sum must be an integer");
    assert_eq!(sum, 20000 * 20001 / 2 - 99 * 100 / 2);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_fill_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_slot_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_pages_sent_total'), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("parallel scan metrics summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    assert!(
        parts[0] > 0 && parts[1] > 0 && parts[2] > 0,
        "parallel detailed scan metrics must be positive: {summary}"
    );
    assert!(
        parts[1] * 4 > parts[0] * 3,
        "detailed scan drain timing should cover all dynamic scan producers: {summary}"
    );
    tx.commit()
        .expect("commit parallel scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name}"))
        .expect("drop committed heap table for parallel scan smoke");
}

pub(crate) fn heap_parallel_scan_search_path_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let schema_name = "pgf_parallel_search_path_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP SCHEMA IF EXISTS {schema_name} CASCADE;
            CREATE SCHEMA {schema_name};
            CREATE TABLE {schema_name}.items AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 20000) AS g;
            ANALYZE {schema_name}.items;
            "
        ))
        .expect("create committed search_path fixture for parallel scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(&format!(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 2;
        SET LOCAL search_path = {schema_name}, public
        "
    ))
    .expect("enable dynamic scan workers with non-public search_path");
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        "SELECT sum(id)::bigint FROM items WHERE id BETWEEN 100 AND 20000",
    )
    .expect("parallel heap scan through search_path must return one row")
    .parse()
    .expect("parallel heap scan sum must be an integer");
    assert_eq!(sum, 20000 * 20001 / 2 - 99 * 100 / 2);
    tx.commit()
        .expect("commit parallel search_path scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .expect("drop search_path fixture for parallel scan smoke");
}

pub(crate) fn heap_parallel_worker_budget_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let tables = [
        "public.pgf_worker_budget_a",
        "public.pgf_worker_budget_b",
        "public.pgf_worker_budget_c",
        "public.pgf_worker_budget_d",
    ];
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {a};
            DROP TABLE IF EXISTS {b};
            DROP TABLE IF EXISTS {c};
            DROP TABLE IF EXISTS {d};
            CREATE TABLE {a} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 8000) AS g;
            CREATE TABLE {b} AS SELECT * FROM {a};
            CREATE TABLE {c} AS SELECT * FROM {a};
            CREATE TABLE {d} AS SELECT * FROM {a};
            ANALYZE {a};
            ANALYZE {b};
            ANALYZE {c};
            ANALYZE {d};
            ",
            a = tables[0],
            b = tables[1],
            c = tables[2],
            d = tables[3],
        ))
        .expect("create committed heap tables for worker budget smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '30s';
        SET LOCAL max_parallel_workers_per_gather = 32
        ",
    )
    .expect("request more dynamic scan workers than a small cluster can launch per leaf");
    let count: i64 = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT count(*)::bigint
        FROM public.pgf_worker_budget_a a
        JOIN public.pgf_worker_budget_b b ON a.id = b.id
        JOIN public.pgf_worker_budget_c c ON a.id = c.id
        JOIN public.pgf_worker_budget_d d ON a.id = d.id
        WHERE a.id BETWEEN 100 AND 8000
        ",
    )
    .expect("multi-scan worker budget query must return one row")
    .parse()
    .expect("multi-scan worker budget count must be an integer");
    assert_eq!(count, 8000 - 99);
    tx.commit()
        .expect("commit worker budget smoke transaction before cleanup");
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {a};
            DROP TABLE IF EXISTS {b};
            DROP TABLE IF EXISTS {c};
            DROP TABLE IF EXISTS {d};
            ",
            a = tables[0],
            b = tables[1],
            c = tables[2],
            d = tables[3],
        ))
        .expect("drop committed heap tables for worker budget smoke");
}

pub(crate) fn heap_leader_only_scan_smoke() {
    let mut client = smoke_client();
    ensure_shared_preload(&mut client);
    let table_name = "public.pgf_leader_only_scan_smoke";
    client
        .batch_execute(&format!(
            "\
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS
            SELECT g::bigint AS id, (g * 10)::bigint AS payload
            FROM generate_series(1, 1000) AS g;
            ANALYZE {table_name};
            "
        ))
        .expect("create committed heap table for leader-only scan smoke");

    let mut tx = smoke_transaction(&mut client);
    tx.batch_execute(
        "\
        SET LOCAL statement_timeout = '20s';
        SET LOCAL max_parallel_workers_per_gather = 0
        ",
    )
    .expect("disable dynamic scan workers through PostgreSQL parallel worker GUC");
    let explain = simple_query_first_column_rows_tx(
        &mut tx,
        &format!("EXPLAIN SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 10 AND 1000"),
    )
    .join("\n");
    assert!(
        explain.contains("PgFusion Producers: planned=1 (leader-only)")
            && explain.contains("reason=worker_budget_zero"),
        "EXPLAIN should show leader-only scan producer reason: {explain}"
    );
    let sum: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT sum(id)::bigint FROM {table_name} WHERE id BETWEEN 10 AND 1000"),
    )
    .expect("leader-only heap scan must return one row")
    .parse()
    .expect("leader-only heap scan sum must be an integer");
    assert_eq!(sum, 1000 * 1001 / 2 - 9 * 10 / 2);
    tx.commit()
        .expect("commit leader-only scan smoke transaction before cleanup");
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table_name}"))
        .expect("drop committed heap table for leader-only scan smoke");
}

pub(crate) fn metrics_smoke() {
    let mut client = smoke_client();
    let mut tx = smoke_transaction(&mut client);
    let table_name = "pg_temp.pgf_metrics_smoke";
    reset_heap_fixture(&mut tx, table_name);

    let before_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("metrics reset must return an epoch")
            .parse()
            .expect("metrics reset epoch must be an integer");

    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id = 1"),
    )
    .expect("metrics smoke query must return one row")
    .parse()
    .expect("metrics smoke query must return one bigint value");
    assert_eq!(id, 1);

    let summary = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_pages_sent_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_bytes_sent_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fetch_calls_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_rows_encoded_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_slot_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_snapshot_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_overflow_copy_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_retry_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_retry_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_pre_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_post_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_overflow_encode_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_emit_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_unclassified_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_batch_send_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_batch_delivery_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_idle_sleep_total'), 0), ',',
            count(*) FILTER (WHERE metric IN (
                'scan_batch_send_ns',
                'scan_batch_send_total',
                'scan_batch_delivery_ns',
                'scan_batch_delivery_total',
                'scan_idle_sleep_ns',
                'scan_idle_sleep_total'
            )), ',',
            coalesce(max(value) FILTER (WHERE metric = 'result_pages_read_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'backend_rows_returned_total'), 0), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("metrics summary must return one row");
    let parts = summary
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 21);
    assert!(
        parts[0] > 0,
        "scan_pages_sent_total must be positive: {summary}"
    );
    assert!(
        parts[1] > 0,
        "scan_bytes_sent_total must be positive: {summary}"
    );
    assert!(
        parts[2] > 0,
        "scan_fetch_calls_total must be positive: {summary}"
    );
    assert!(
        parts[3] > 0,
        "scan_rows_encoded_total must be positive: {summary}"
    );
    assert_eq!(
        parts[4], 0,
        "scan_slot_drain_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[5], 0,
        "scan_page_snapshot_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[6], 0,
        "scan_overflow_copy_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[7], 0,
        "scan_page_retry_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[8], 0,
        "scan_page_retry_total must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[9], 0,
        "scan_fill_pre_drain_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[10], 0,
        "scan_fill_post_drain_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[11], 0,
        "scan_fill_overflow_encode_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[12], 0,
        "scan_fill_emit_ns must stay zero without detailed timing: {summary}"
    );
    assert_eq!(
        parts[13], 0,
        "scan_fill_unclassified_ns must stay zero without detailed timing: {summary}"
    );
    assert!(
        parts[14] > 0,
        "scan_batch_send_total must be positive: {summary}"
    );
    assert!(
        parts[15] > 0,
        "scan_batch_delivery_total must be positive: {summary}"
    );
    assert_eq!(
        parts[17], 6,
        "all worker scan-thread metric rows must be present: {summary}"
    );
    assert!(
        parts[18] > 0,
        "result_pages_read_total must be positive: {summary}"
    );
    assert!(
        parts[19] > 0,
        "backend_rows_returned_total must be positive: {summary}"
    );
    assert_eq!(parts[20], before_epoch);

    let detail_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("detailed metrics reset must return an epoch")
            .parse()
            .expect("detailed metrics reset epoch must be an integer");
    tx.batch_execute("SET LOCAL pg_fusion.scan_timing_detail = on")
        .expect("enable detailed scan timing");
    let id: i64 = simple_query_first_column_tx(
        &mut tx,
        &format!("SELECT id::bigint FROM {table_name} WHERE id = 2"),
    )
    .expect("detailed metrics smoke query must return one row")
    .parse()
    .expect("detailed metrics smoke query must return one bigint value");
    assert_eq!(id, 2);

    let detailed = simple_query_first_column_tx(
        &mut tx,
        "\
        SELECT concat(
            coalesce(max(value) FILTER (WHERE metric = 'scan_fetch_calls_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_rows_encoded_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_slot_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_snapshot_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_overflow_copy_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_retry_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_page_retry_total'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_pre_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_post_drain_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_overflow_encode_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_emit_ns'), 0), ',',
            coalesce(max(value) FILTER (WHERE metric = 'scan_fill_unclassified_ns'), 0), ',',
            count(*) FILTER (WHERE metric IN (
                'scan_fill_pre_drain_ns',
                'scan_fill_post_drain_ns',
                'scan_fill_overflow_encode_ns',
                'scan_fill_emit_ns',
                'scan_fill_unclassified_ns'
            )), ',',
            coalesce(max(reset_epoch), 0)
        )
        FROM pg_fusion_metrics()
        ",
    )
    .expect("detailed metrics summary must return one row");
    let detailed_parts = detailed
        .split(',')
        .map(|part| part.parse::<i64>().expect("metric value must be integer"))
        .collect::<Vec<_>>();
    assert_eq!(detailed_parts.len(), 14);
    assert!(
        detailed_parts[0] > 0,
        "scan_fetch_calls_total must be positive with detailed timing: {detailed}"
    );
    assert!(
        detailed_parts[1] > 0,
        "scan_rows_encoded_total must be positive with detailed timing: {detailed}"
    );
    assert!(
        detailed_parts[2] > 0,
        "scan_slot_drain_ns must be positive with detailed timing: {detailed}"
    );
    assert_eq!(
        detailed_parts[12], 5,
        "all scan fill residual metric rows must be present: {detailed}"
    );
    assert_eq!(detailed_parts[13], detail_epoch);

    let after_epoch: i64 =
        simple_query_first_column_tx(&mut tx, "SELECT pg_fusion_metrics_reset()")
            .expect("second metrics reset must return an epoch")
            .parse()
            .expect("second metrics reset epoch must be an integer");
    assert!(after_epoch > detail_epoch);
}
