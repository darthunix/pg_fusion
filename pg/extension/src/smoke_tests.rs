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
