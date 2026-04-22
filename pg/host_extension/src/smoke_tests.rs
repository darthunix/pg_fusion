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
            .any(|lib| lib == "pg_fusion_host"),
        "pg_fusion_host must be in shared_preload_libraries, got: {preload}"
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
