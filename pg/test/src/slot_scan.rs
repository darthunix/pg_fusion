use pgrx::prelude::*;
use scan_sql::{compile_scan, CompileScanInput, LimitLowering, PgRelation};
use slot_scan::{
    explain_scan, prepare_scan, ScanError, ScanExplainOptions, ScanOptions, ScanPlanKind,
    SinkError, SlotSink, SlotSinkAction, SlotSinkContext, SlotSinkMethods,
};
use std::ffi::{c_void, CString};

const SLOT_SCAN_TABLE: &str = "slot_scan_runtime_test";
const SLOT_SCAN_INDEX_TABLE: &str = "slot_scan_index_test";
const SLOT_SCAN_PARTITIONED_TABLE: &str = "slot_scan_partitioned_test";

#[derive(Default)]
struct CountSink {
    rows_seen: usize,
    init_called: bool,
    finish_called: bool,
    abort_called: bool,
    tuple_desc_natts: Option<usize>,
    first_type_oid: Option<pg_sys::Oid>,
    init_parallel_capable: Option<bool>,
    init_planned_workers: Option<usize>,
    init_plan_kind: Option<ScanPlanKind>,
}

#[derive(Default)]
struct FailingSink {
    init_called: usize,
    consume_called: usize,
    finish_called: usize,
    abort_called: usize,
    fail_init_with_pg_error: bool,
    fail_consume_with_pg_error_at: Option<usize>,
    fail_consume_with_sink_error_at: Option<usize>,
    fail_finish_with_pg_error: bool,
    fail_abort_with_pg_error: bool,
}

static COUNT_SINK_METHODS: SlotSinkMethods = SlotSinkMethods {
    init: Some(count_sink_init),
    consume_slot: count_sink_consume_slot,
    finish: Some(count_sink_finish),
    abort: Some(count_sink_abort),
};

static FAILING_SINK_METHODS: SlotSinkMethods = SlotSinkMethods {
    init: Some(failing_sink_init),
    consume_slot: failing_sink_consume_slot,
    finish: Some(failing_sink_finish),
    abort: Some(failing_sink_abort),
};

unsafe fn count_sink_init(
    ctx: &mut SlotSinkContext,
    private: *mut c_void,
    tuple_desc: pg_sys::TupleDesc,
) -> Result<(), SinkError> {
    let state = &mut *(private as *mut CountSink);
    state.init_called = true;
    state.init_parallel_capable = Some(ctx.parallel_capable());
    state.init_planned_workers = Some(ctx.planned_workers());
    state.init_plan_kind = Some(ctx.plan_kind());

    let natts = (*tuple_desc).natts as usize;
    state.tuple_desc_natts = Some(natts);
    if natts > 0 {
        let attrs = (*tuple_desc).attrs.as_slice(natts);
        state.first_type_oid = Some(attrs[0].atttypid);
    }
    Ok(())
}

unsafe fn count_sink_consume_slot(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
    _slot: *mut pg_sys::TupleTableSlot,
) -> Result<SlotSinkAction, SinkError> {
    let state = &mut *(private as *mut CountSink);
    state.rows_seen += 1;
    Ok(SlotSinkAction::Continue)
}

unsafe fn count_sink_finish(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
) -> Result<(), SinkError> {
    let state = &mut *(private as *mut CountSink);
    state.finish_called = true;
    Ok(())
}

unsafe fn count_sink_abort(_ctx: &mut SlotSinkContext, private: *mut c_void) {
    let state = &mut *(private as *mut CountSink);
    state.abort_called = true;
}

unsafe fn failing_sink_init(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
    _tuple_desc: pg_sys::TupleDesc,
) -> Result<(), SinkError> {
    let state = &mut *(private as *mut FailingSink);
    state.init_called += 1;
    if state.fail_init_with_pg_error {
        pgrx::error!("slot_scan failing sink init pg error");
    }
    Ok(())
}

unsafe fn failing_sink_consume_slot(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
    _slot: *mut pg_sys::TupleTableSlot,
) -> Result<SlotSinkAction, SinkError> {
    let state = &mut *(private as *mut FailingSink);
    state.consume_called += 1;
    if state.fail_consume_with_pg_error_at == Some(state.consume_called) {
        pgrx::error!("slot_scan failing sink consume pg error");
    }
    if state.fail_consume_with_sink_error_at == Some(state.consume_called) {
        return Err(SinkError::new("slot_scan failing sink consume error"));
    }
    Ok(SlotSinkAction::Continue)
}

unsafe fn failing_sink_finish(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
) -> Result<(), SinkError> {
    let state = &mut *(private as *mut FailingSink);
    state.finish_called += 1;
    if state.fail_finish_with_pg_error {
        pgrx::error!("slot_scan failing sink finish pg error");
    }
    Ok(())
}

unsafe fn failing_sink_abort(_ctx: &mut SlotSinkContext, private: *mut c_void) {
    let state = &mut *(private as *mut FailingSink);
    state.abort_called += 1;
    if state.fail_abort_with_pg_error {
        pgrx::error!("slot_scan failing sink abort pg error");
    }
}

fn reset_slot_scan_table(rows: usize) {
    Spi::run(&format!("DROP TABLE IF EXISTS {SLOT_SCAN_TABLE}")).unwrap();
    Spi::run(&format!(
        "CREATE TABLE {SLOT_SCAN_TABLE} AS \
         SELECT g AS id, md5(g::text) AS payload \
         FROM generate_series(1, {}) AS g",
        rows
    ))
    .unwrap();
    Spi::run(&format!("ANALYZE {SLOT_SCAN_TABLE}")).unwrap();
}

fn reset_slot_scan_index_table(rows: usize) {
    Spi::run(&format!("DROP TABLE IF EXISTS {SLOT_SCAN_INDEX_TABLE}")).unwrap();
    Spi::run(&format!(
        "CREATE TABLE {SLOT_SCAN_INDEX_TABLE} AS \
         SELECT g AS id, md5(g::text) AS payload \
         FROM generate_series(1, {}) AS g",
        rows
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE INDEX {SLOT_SCAN_INDEX_TABLE}_id_idx ON {SLOT_SCAN_INDEX_TABLE} (id)"
    ))
    .unwrap();
    Spi::run(&format!("ANALYZE {SLOT_SCAN_INDEX_TABLE}")).unwrap();
}

fn reset_slot_scan_partitioned_table(rows: usize) {
    Spi::run(&format!(
        "DROP TABLE IF EXISTS {SLOT_SCAN_PARTITIONED_TABLE} CASCADE"
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE TABLE {SLOT_SCAN_PARTITIONED_TABLE} (id int, payload text) PARTITION BY RANGE (id)"
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE TABLE {SLOT_SCAN_PARTITIONED_TABLE}_p1 PARTITION OF {SLOT_SCAN_PARTITIONED_TABLE} \
         FOR VALUES FROM (1) TO ({})",
        (rows / 2) + 1
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE TABLE {SLOT_SCAN_PARTITIONED_TABLE}_p2 PARTITION OF {SLOT_SCAN_PARTITIONED_TABLE} \
         FOR VALUES FROM ({}) TO ({})",
        (rows / 2) + 1,
        rows + 1
    ))
    .unwrap();
    Spi::run(&format!(
        "INSERT INTO {SLOT_SCAN_PARTITIONED_TABLE} \
         SELECT g AS id, md5(g::text) AS payload \
         FROM generate_series(1, {}) AS g",
        rows
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE INDEX {SLOT_SCAN_PARTITIONED_TABLE}_p1_id_idx ON {SLOT_SCAN_PARTITIONED_TABLE}_p1 (id)"
    ))
    .unwrap();
    Spi::run(&format!(
        "CREATE INDEX {SLOT_SCAN_PARTITIONED_TABLE}_p2_id_idx ON {SLOT_SCAN_PARTITIONED_TABLE}_p2 (id)"
    ))
    .unwrap();
    Spi::run(&format!("ANALYZE {SLOT_SCAN_PARTITIONED_TABLE}")).unwrap();
}

fn enable_parallel_scan_gucs() {
    Spi::run("SET LOCAL debug_parallel_query = on").unwrap();
    Spi::run("SET LOCAL max_parallel_workers = 2").unwrap();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 2").unwrap();
    Spi::run("SET LOCAL min_parallel_table_scan_size = 0").unwrap();
    Spi::run("SET LOCAL parallel_setup_cost = 0").unwrap();
    Spi::run("SET LOCAL parallel_tuple_cost = 0").unwrap();
    Spi::run("SET LOCAL enable_indexscan = on").unwrap();
    Spi::run("SET LOCAL enable_bitmapscan = on").unwrap();
}

fn enable_fast_plan_gucs() {
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();
    Spi::run("SET LOCAL cursor_tuple_fraction = 0.000001").unwrap();
    Spi::run("SET LOCAL enable_indexscan = on").unwrap();
    Spi::run("SET LOCAL enable_indexonlyscan = off").unwrap();
    Spi::run("SET LOCAL enable_bitmapscan = off").unwrap();
}

struct LatestSnapshotGuard;

impl LatestSnapshotGuard {
    unsafe fn acquire() -> Self {
        let snapshot = pg_sys::GetLatestSnapshot();
        pg_sys::PushActiveSnapshot(snapshot);
        Self
    }
}

impl Drop for LatestSnapshotGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::PopActiveSnapshot();
        }
    }
}

struct RegisteredSnapshot(pg_sys::Snapshot);

impl RegisteredSnapshot {
    unsafe fn capture_active() -> Self {
        let snapshot = pg_sys::GetActiveSnapshot();
        assert!(!snapshot.is_null(), "expected an active snapshot");
        Self(pg_sys::RegisterSnapshot(snapshot))
    }

    fn as_ptr(&self) -> pg_sys::Snapshot {
        self.0
    }
}

impl Drop for RegisteredSnapshot {
    fn drop(&mut self) {
        unsafe {
            pg_sys::UnregisterSnapshot(self.0);
        }
    }
}

fn count_rows_with_snapshot(qualified: &str, snapshot: pg_sys::Snapshot) -> i64 {
    let sql = CString::new(format!("SELECT 1 FROM {qualified}")).unwrap();

    Spi::connect(|_| unsafe {
        let plan = pg_sys::SPI_prepare(sql.as_ptr(), 0, std::ptr::null_mut());
        assert!(!plan.is_null(), "SPI_prepare returned null");

        let exec_rc = pg_sys::SPI_execute_snapshot(
            plan,
            std::ptr::null_mut(),
            std::ptr::null(),
            snapshot,
            std::ptr::null_mut(),
            true,
            false,
            0,
        );
        assert_eq!(
            exec_rc,
            pg_sys::SPI_OK_SELECT as i32,
            "unexpected SPI_execute_snapshot status: {exec_rc}",
        );

        let processed = pg_sys::SPI_processed as i64;
        let tuptable = pg_sys::SPI_tuptable;
        if !tuptable.is_null() {
            pg_sys::SPI_freetuptable(tuptable);
        }

        let free_rc = pg_sys::SPI_freeplan(plan);
        assert_eq!(free_rc, 0, "unexpected SPI_freeplan status: {free_rc}");

        processed
    })
}

pub fn slot_scan_prepare_and_run_smoke() {
    reset_slot_scan_table(32);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id, payload FROM {SLOT_SCAN_TABLE} WHERE id <= 10"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen, 10);
    assert_eq!(sink.rows_seen, 10);
    assert!(sink.init_called);
    assert!(sink.finish_called);
    assert!(!sink.abort_called);
    assert_eq!(sink.tuple_desc_natts, Some(2));
}

pub fn slot_scan_explain_renders_postgres_plan() {
    reset_slot_scan_table(32);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let rendered = explain_scan(
        &format!("SELECT id, payload FROM {SLOT_SCAN_TABLE} WHERE id <= 10"),
        ScanOptions::default(),
        ScanExplainOptions::default(),
    )
    .expect("explain_scan");

    assert!(
        rendered.contains("Seq Scan") || rendered.contains("Index Scan"),
        "unexpected PostgreSQL scan explain: {rendered}"
    );
}

pub fn slot_scan_parallel_plan_metadata_smoke() {
    reset_slot_scan_table(100_000);
    enable_parallel_scan_gucs();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE} WHERE id > 0"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert!(
        stats.parallel_capable,
        "expected a parallel-capable run-time plan"
    );
    assert!(
        stats.planned_workers > 0,
        "expected planner to choose at least one worker"
    );
    assert_eq!(sink.init_parallel_capable, Some(stats.parallel_capable));
    assert_eq!(sink.init_planned_workers, Some(stats.planned_workers));
    assert_eq!(sink.init_plan_kind, Some(stats.plan_kind));
}

pub fn slot_scan_local_row_cap_smoke() {
    reset_slot_scan_table(256);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions {
            planner_fetch_hint: None,
            local_row_cap: Some(17),
            ..ScanOptions::default()
        },
    )
    .expect("prepare_scan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen, 17);
    assert!(stats.hit_local_row_cap);
    assert_eq!(sink.rows_seen, 17);
}

pub fn slot_scan_accepts_tid_range_scan() {
    reset_slot_scan_table(100);
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();
    Spi::run("SET LOCAL enable_seqscan = off").unwrap();
    Spi::run("SET LOCAL enable_tidscan = on").unwrap();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!(
            "SELECT id FROM {SLOT_SCAN_TABLE} \
             WHERE ctid >= '(0,1)'::tid AND ctid < '(1000,1)'::tid"
        ),
        ScanOptions::default(),
    )
    .expect("prepare_scan must accept TidRangeScan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run TidRangeScan");

    assert_eq!(stats.rows_seen, 100);
    assert_eq!(sink.rows_seen, 100);
    assert_eq!(stats.plan_kind, ScanPlanKind::TidRangeScan);
    assert_eq!(sink.init_plan_kind, Some(ScanPlanKind::TidRangeScan));
}

pub fn slot_scan_rejects_limit_node() {
    reset_slot_scan_table(32);

    let err = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE} LIMIT 5"),
        ScanOptions::default(),
    )
    .expect_err("LIMIT should be rejected");

    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(message.contains("LIMIT"), "unexpected message: {message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_accepts_scan_sql_external_hint_and_rejects_sql_clause() {
    reset_slot_scan_table(32);
    let schema = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("payload", arrow_schema::DataType::Utf8, true),
    ]);
    let relation = PgRelation::new(None::<String>, SLOT_SCAN_TABLE);
    let filters = Vec::new();

    let external_hint = compile_scan(CompileScanInput {
        relation: &relation,
        schema: &schema,
        identifier_max_bytes: (pg_sys::NAMEDATALEN as usize).saturating_sub(1),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: Some(5),
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile_scan external hint");
    assert_eq!(external_hint.requested_limit, Some(5));
    assert_eq!(external_hint.sql_limit, None);
    assert!(!external_hint.sql.contains("LIMIT"));

    prepare_scan(&external_hint.sql, ScanOptions::default())
        .expect("slot_scan should accept SQL without LIMIT");

    let sql_limit = compile_scan(CompileScanInput {
        relation: &relation,
        schema: &schema,
        identifier_max_bytes: (pg_sys::NAMEDATALEN as usize).saturating_sub(1),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: Some(5),
        limit_lowering: LimitLowering::SqlClause,
    })
    .expect("compile_scan sql limit");
    assert_eq!(sql_limit.sql_limit, Some(5));
    assert!(sql_limit.sql.contains("LIMIT 5"));

    let err = prepare_scan(&sql_limit.sql, ScanOptions::default())
        .expect_err("slot_scan should reject SQL LIMIT");
    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(message.contains("LIMIT"), "unexpected message: {message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_rejects_join_plan() {
    reset_slot_scan_table(16);

    let err = prepare_scan(
        &format!(
            "SELECT a.id FROM {0} a JOIN {0} b ON a.id = b.id",
            SLOT_SCAN_TABLE
        ),
        ScanOptions::default(),
    )
    .expect_err("join should be rejected");

    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(message.contains("join"), "unexpected message: {message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_rejects_subplans() {
    reset_slot_scan_table(16);

    let err = prepare_scan(
        &format!(
            "SELECT id FROM {SLOT_SCAN_TABLE} WHERE id = (SELECT min(id) FROM {SLOT_SCAN_TABLE})"
        ),
        ScanOptions::default(),
    )
    .expect_err("subplans should be rejected");

    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(
                message.contains("subplans") || message.contains("init plans"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_rejects_modifying_cte() {
    reset_slot_scan_table(16);

    let err = prepare_scan(
        &format!(
            "WITH deleted AS (DELETE FROM {SLOT_SCAN_TABLE} WHERE id = 1 RETURNING 1) \
             SELECT id FROM {SLOT_SCAN_TABLE}"
        ),
        ScanOptions::default(),
    )
    .expect_err("modifying CTE should be rejected");

    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(
                message.contains("modifying CTE"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_prepare_catches_postgres_errors() {
    let err = prepare_scan(
        "SELECT * FROM missing_slot_scan_relation",
        ScanOptions::default(),
    )
    .expect_err("missing table should surface as ScanError");

    match err {
        ScanError::Postgres(message) => {
            assert!(
                message.contains("missing_slot_scan_relation")
                    || message.contains("does not exist"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

pub fn slot_scan_run_uses_saved_plan_and_aborts_on_error() {
    reset_slot_scan_table(32);

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    Spi::run(&format!("DROP TABLE {SLOT_SCAN_TABLE}")).unwrap();

    let mut sink = CountSink::default();
    let err = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect_err("run should fail after dropping the prepared relation");

    match err {
        ScanError::Postgres(_) => {}
        other => panic!("unexpected error: {other:?}"),
    }

    assert!(!sink.init_called);
    assert!(!sink.finish_called);
    assert!(sink.abort_called);
}

pub fn slot_scan_run_converts_init_pg_error_and_aborts_once() {
    reset_slot_scan_table(8);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = FailingSink {
        fail_init_with_pg_error: true,
        ..Default::default()
    };
    let err = prepared
        .run(SlotSink::new(&FAILING_SINK_METHODS, &mut sink))
        .expect_err("init callback pg error should surface as ScanError");

    match err {
        ScanError::Postgres(message) => {
            assert!(
                message.contains("slot_scan failing sink init pg error"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(sink.init_called, 1);
    assert_eq!(sink.consume_called, 0);
    assert_eq!(sink.finish_called, 0);
    assert_eq!(sink.abort_called, 1);
}

pub fn slot_scan_run_converts_consume_pg_error_and_aborts_once() {
    reset_slot_scan_table(8);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = FailingSink {
        fail_consume_with_pg_error_at: Some(1),
        ..Default::default()
    };
    let err = prepared
        .run(SlotSink::new(&FAILING_SINK_METHODS, &mut sink))
        .expect_err("consume callback pg error should surface as ScanError");

    match err {
        ScanError::Postgres(message) => {
            assert!(
                message.contains("slot_scan failing sink consume pg error"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(sink.init_called, 1);
    assert_eq!(sink.consume_called, 1);
    assert_eq!(sink.finish_called, 0);
    assert_eq!(sink.abort_called, 1);
}

pub fn slot_scan_run_converts_finish_pg_error_and_aborts_once() {
    reset_slot_scan_table(8);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = FailingSink {
        fail_finish_with_pg_error: true,
        ..Default::default()
    };
    let err = prepared
        .run(SlotSink::new(&FAILING_SINK_METHODS, &mut sink))
        .expect_err("finish callback pg error should surface as ScanError");

    match err {
        ScanError::Postgres(message) => {
            assert!(
                message.contains("slot_scan failing sink finish pg error"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(sink.init_called, 1);
    assert!(sink.consume_called > 0);
    assert_eq!(sink.finish_called, 1);
    assert_eq!(sink.abort_called, 1);
}

pub fn slot_scan_run_preserves_primary_error_when_abort_raises_pg_error() {
    reset_slot_scan_table(8);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = FailingSink {
        fail_consume_with_sink_error_at: Some(1),
        fail_abort_with_pg_error: true,
        ..Default::default()
    };
    let err = prepared
        .run(SlotSink::new(&FAILING_SINK_METHODS, &mut sink))
        .expect_err("primary sink error should survive abort pg error");

    match err {
        ScanError::Sink(message) => {
            assert_eq!(
                message.to_string(),
                "slot_scan failing sink consume error",
                "abort pg error must not replace the primary sink error"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(sink.init_called, 1);
    assert_eq!(sink.consume_called, 1);
    assert_eq!(sink.finish_called, 0);
    assert_eq!(sink.abort_called, 1);
}

pub fn slot_scan_run_revalidates_across_search_path_changes() {
    Spi::run("DROP SCHEMA IF EXISTS slot_scan_a CASCADE").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS slot_scan_b CASCADE").unwrap();
    Spi::run("CREATE SCHEMA slot_scan_a").unwrap();
    Spi::run("CREATE SCHEMA slot_scan_b").unwrap();
    Spi::run("CREATE TABLE slot_scan_a.drift_t (id int4)").unwrap();
    Spi::run("CREATE TABLE slot_scan_b.drift_t (id int8)").unwrap();
    Spi::run("INSERT INTO slot_scan_a.drift_t VALUES (1), (2), (3)").unwrap();
    Spi::run("INSERT INTO slot_scan_b.drift_t VALUES (10), (20), (30), (40), (50), (60), (70)")
        .unwrap();
    let snapshot = unsafe { LatestSnapshotGuard::acquire() };

    Spi::run("SET LOCAL search_path = slot_scan_a").unwrap();
    let prepared = prepare_scan("SELECT id FROM drift_t", ScanOptions::default()).unwrap();
    Spi::run("SET LOCAL search_path = slot_scan_b").unwrap();

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen, 7);
    assert_eq!(sink.rows_seen, 7);
    assert_eq!(sink.first_type_oid, Some(pg_sys::INT8OID));

    drop(snapshot);
    Spi::run("DROP SCHEMA slot_scan_a CASCADE").unwrap();
    Spi::run("DROP SCHEMA slot_scan_b CASCADE").unwrap();
}

pub fn slot_scan_run_rejects_replanned_limit_shape() {
    Spi::run("DROP SCHEMA IF EXISTS slot_scan_a CASCADE").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS slot_scan_b CASCADE").unwrap();
    Spi::run("CREATE SCHEMA slot_scan_a").unwrap();
    Spi::run("CREATE SCHEMA slot_scan_b").unwrap();
    Spi::run("CREATE TABLE slot_scan_a.drift_t (id int4)").unwrap();
    Spi::run("INSERT INTO slot_scan_a.drift_t VALUES (1), (2), (3)").unwrap();
    Spi::run("CREATE TABLE slot_scan_b.source_t (id int4)").unwrap();
    Spi::run("INSERT INTO slot_scan_b.source_t VALUES (10), (20), (30)").unwrap();
    Spi::run("CREATE VIEW slot_scan_b.drift_t AS SELECT id FROM slot_scan_b.source_t LIMIT 1")
        .unwrap();

    Spi::run("SET LOCAL search_path = slot_scan_a").unwrap();
    let prepared = prepare_scan("SELECT id FROM drift_t", ScanOptions::default()).unwrap();
    Spi::run("SET LOCAL search_path = slot_scan_b").unwrap();

    let mut sink = CountSink::default();
    let err = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect_err("replanned LIMIT shape should be rejected");

    match err {
        ScanError::UnsupportedPlan(message) => {
            assert!(message.contains("LIMIT"), "unexpected message: {message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    assert!(!sink.init_called);
    assert!(!sink.finish_called);
    assert!(sink.abort_called);

    Spi::run("DROP SCHEMA slot_scan_a CASCADE").unwrap();
    Spi::run("DROP SCHEMA slot_scan_b CASCADE").unwrap();
}

pub fn slot_scan_reuses_active_snapshot_for_read_only_cursor() {
    reset_slot_scan_table(3);
    let _active_snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT id FROM {SLOT_SCAN_TABLE}"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let snapshot = unsafe { RegisteredSnapshot::capture_active() };
    Spi::run(&format!(
        "INSERT INTO {SLOT_SCAN_TABLE} VALUES (999, 'inserted-after-snapshot')"
    ))
    .unwrap();

    let expected_rows = count_rows_with_snapshot(SLOT_SCAN_TABLE, snapshot.as_ptr());
    assert_eq!(expected_rows, 3);

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen as i64, expected_rows);
    assert_eq!(sink.rows_seen as i64, expected_rows);
}

pub fn slot_scan_planner_fetch_hint_reports_plan_kind() {
    reset_slot_scan_index_table(100_000);
    enable_fast_plan_gucs();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT payload FROM {SLOT_SCAN_INDEX_TABLE} WHERE id > 50000"),
        ScanOptions {
            planner_fetch_hint: Some(1),
            local_row_cap: None,
            ..ScanOptions::default()
        },
    )
    .expect("prepare_scan with planner hint");
    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run with planner hint");

    assert_ne!(stats.plan_kind, ScanPlanKind::Unknown);
    assert_eq!(sink.init_plan_kind, Some(stats.plan_kind));
}

pub fn slot_scan_planner_fetch_hint_is_independent_from_local_cap() {
    reset_slot_scan_index_table(50_000);
    enable_fast_plan_gucs();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let sql = format!("SELECT payload FROM {SLOT_SCAN_INDEX_TABLE} WHERE id > 25000");

    let prepared = prepare_scan(
        &sql,
        ScanOptions {
            planner_fetch_hint: Some(1),
            local_row_cap: None,
            ..ScanOptions::default()
        },
    )
    .expect("prepare_scan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen, 25_000);
    assert_eq!(sink.rows_seen, 25_000);
    assert!(!stats.hit_local_row_cap);
    assert_ne!(stats.plan_kind, ScanPlanKind::Unknown);
}

pub fn slot_scan_append_merges_uniform_child_plan_kind() {
    reset_slot_scan_partitioned_table(100_000);
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();
    Spi::run("SET LOCAL enable_seqscan = off").unwrap();
    Spi::run("SET LOCAL enable_indexscan = on").unwrap();
    Spi::run("SET LOCAL enable_indexonlyscan = off").unwrap();
    Spi::run("SET LOCAL enable_bitmapscan = off").unwrap();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };

    let prepared = prepare_scan(
        &format!("SELECT payload FROM {SLOT_SCAN_PARTITIONED_TABLE} WHERE id > 0"),
        ScanOptions::default(),
    )
    .expect("prepare_scan");

    let mut sink = CountSink::default();
    let stats = prepared
        .run(SlotSink::new(&COUNT_SINK_METHODS, &mut sink))
        .expect("run");

    assert_eq!(stats.rows_seen, 100_000);
    assert_eq!(sink.rows_seen, 100_000);
    assert_eq!(stats.plan_kind, ScanPlanKind::IndexScan);
    assert_eq!(sink.init_plan_kind, Some(ScanPlanKind::IndexScan));
}
