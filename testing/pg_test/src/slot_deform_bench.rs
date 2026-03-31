use anyhow::Result as AnyResult;
use anyhow::{bail, Context};
use arrow_schema::{DataType, Field, Schema};
use layout::{init_block, LayoutPlan};
use pgrx::prelude::*;
use serde_json::{json, Value};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use std::hint::black_box;
use std::ptr;
use std::sync::Arc;
use std::time::Instant;

const DEFAULT_ROWS_PER_PAGE: usize = 64;
const DEFAULT_PAYLOAD_CAPACITY_BYTES: usize = 8192 - 20;
const FIXED_RELATION: &str = "pg_temp.slot_deform_fixed_src";
const MIXED_RELATION: &str = "pg_temp.slot_deform_mixed_src";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchmarkProfile {
    Fixed,
    Mixed,
}

impl BenchmarkProfile {
    fn parse(raw: &str) -> AnyResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "fixed" => Ok(Self::Fixed),
            "mixed" => Ok(Self::Mixed),
            other => bail!("unknown benchmark profile: {other}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::Mixed => "mixed",
        }
    }

    fn relation_name(self) -> &'static str {
        match self {
            Self::Fixed => FIXED_RELATION,
            Self::Mixed => MIXED_RELATION,
        }
    }

    fn schema(self) -> Arc<Schema> {
        match self {
            Self::Fixed => Arc::new(Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("i", DataType::Int32, true),
                Field::new("l", DataType::Int64, true),
                Field::new("d", DataType::Float64, true),
                Field::new("u", DataType::FixedSizeBinary(16), true),
            ])),
            Self::Mixed => Arc::new(Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("i", DataType::Int32, true),
                Field::new("l", DataType::Int64, true),
                Field::new("d", DataType::Float64, true),
                Field::new("u", DataType::FixedSizeBinary(16), true),
                Field::new("t", DataType::Utf8View, true),
                Field::new("bytes", DataType::BinaryView, true),
            ])),
        }
    }
}

struct SnapshotGuard {
    snapshot: pg_sys::Snapshot,
}

impl SnapshotGuard {
    unsafe fn acquire() -> Self {
        let snapshot = unsafe { pg_sys::GetLatestSnapshot() };
        unsafe { pg_sys::PushActiveSnapshot(snapshot) };
        Self { snapshot }
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        unsafe { pg_sys::PopActiveSnapshot() };
    }
}

struct RelationScan {
    relation: pg_sys::Relation,
    scan: pg_sys::TableScanDesc,
    slot: *mut pg_sys::TupleTableSlot,
    _snapshot: SnapshotGuard,
}

impl RelationScan {
    fn open(qualified: &str) -> AnyResult<Self> {
        let relation = unsafe { relation_open_by_name(qualified) };
        let snapshot = unsafe { SnapshotGuard::acquire() };
        let slot = unsafe { pg_sys::table_slot_create(relation, ptr::null_mut()) };
        if slot.is_null() {
            unsafe {
                pg_sys::relation_close(relation, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
            bail!("table_slot_create returned null");
        }
        let scan =
            unsafe { pg_sys::table_beginscan(relation, snapshot.snapshot, 0, ptr::null_mut()) };
        if scan.is_null() {
            unsafe {
                pg_sys::ExecDropSingleTupleTableSlot(slot);
                pg_sys::relation_close(relation, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
            bail!("table_beginscan returned null");
        }
        Ok(Self {
            relation,
            scan,
            slot,
            _snapshot: snapshot,
        })
    }

    fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { (*self.relation).rd_att }
    }

    fn next_slot(&mut self) -> bool {
        unsafe {
            pg_sys::table_scan_getnextslot(
                self.scan,
                pg_sys::ScanDirection::ForwardScanDirection,
                self.slot,
            )
        }
    }

    fn slot(&self) -> *mut pg_sys::TupleTableSlot {
        self.slot
    }
}

impl Drop for RelationScan {
    fn drop(&mut self) {
        unsafe {
            if !self.scan.is_null() {
                pg_sys::table_endscan(self.scan);
            }
            if !self.slot.is_null() {
                pg_sys::ExecDropSingleTupleTableSlot(self.slot);
            }
            if !self.relation.is_null() {
                pg_sys::relation_close(self.relation, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct BaselineMetrics {
    rows_total: usize,
    elapsed_ns: u128,
}

#[derive(Clone, Copy, Debug, Default)]
struct ArrowMetrics {
    rows_total: usize,
    pages_total: usize,
    elapsed_ns: u128,
    block_size_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    rows_per_page: usize,
    payload_capacity_bytes: usize,
}

impl BenchConfig {
    fn new(rows_per_page: usize, payload_capacity_bytes: usize) -> AnyResult<Self> {
        if rows_per_page == 0 {
            bail!("rows_per_page must be greater than zero");
        }
        if payload_capacity_bytes == 0 {
            bail!("payload_capacity_bytes must be greater than zero");
        }
        Ok(Self {
            rows_per_page,
            payload_capacity_bytes,
        })
    }
}

unsafe fn relation_open_by_name(qualified: &str) -> pg_sys::Relation {
    let sql = format!("SELECT '{}'::regclass::oid", qualified);
    let relid: pg_sys::Oid = Spi::get_one(&sql).unwrap().unwrap();
    pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
}

fn lookup_relation_oid(qualified: &str) -> AnyResult<Option<pg_sys::Oid>> {
    let sql = format!("SELECT to_regclass('{qualified}')::oid::int4");
    let relid: Option<i32> = Spi::get_one(&sql).context("lookup relation oid")?;
    Ok(relid.map(|oid| pg_sys::Oid::from(oid as u32)))
}

fn ensure_prepared_relation(profile: BenchmarkProfile) -> AnyResult<()> {
    if lookup_relation_oid(profile.relation_name())?.is_some() {
        Ok(())
    } else {
        bail!(
            "source relation {} is not prepared; call tests.slot_deform_bench_prepare('{}', rows) first",
            profile.relation_name(),
            profile.name(),
        );
    }
}

fn setup_profile_table(profile: BenchmarkProfile, rows: usize) -> AnyResult<()> {
    let rows = i64::try_from(rows).context("rows does not fit into i64")?;
    let relation = profile.relation_name();
    let uuid_expr = "(
        substr(md5(g::text), 1, 8) || '-' ||
        substr(md5(g::text), 9, 4) || '-' ||
        substr(md5(g::text), 13, 4) || '-' ||
        substr(md5(g::text), 17, 4) || '-' ||
        substr(md5(g::text), 21, 12)
    )::uuid";

    Spi::run(&format!("DROP TABLE IF EXISTS {relation}")).context("drop temp table")?;
    match profile {
        BenchmarkProfile::Fixed => {
            Spi::run(&format!(
                "CREATE TEMP TABLE {relation} (
                    b boolean,
                    i int4,
                    l int8,
                    d double precision,
                    u uuid
                )"
            ))
            .context("create fixed temp table")?;
            Spi::run(&format!(
                "INSERT INTO {relation}
                 SELECT
                    CASE WHEN g % 7 = 0 THEN NULL ELSE (g % 2 = 0) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE g::int4 END,
                    CASE WHEN g % 13 = 0 THEN NULL ELSE (g * 10)::int8 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE g::float8 / 10.0 END,
                    CASE WHEN g % 19 = 0 THEN NULL ELSE {uuid_expr} END
                 FROM generate_series(1, {rows}) AS g"
            ))
            .context("populate fixed temp table")?;
        }
        BenchmarkProfile::Mixed => {
            Spi::run(&format!(
                "CREATE TEMP TABLE {relation} (
                    b boolean,
                    i int4,
                    l int8,
                    d double precision,
                    u uuid,
                    t text,
                    bytes bytea
                )"
            ))
            .context("create mixed temp table")?;
            Spi::run(&format!(
                "INSERT INTO {relation}
                 SELECT
                    CASE WHEN g % 7 = 0 THEN NULL ELSE (g % 2 = 0) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE g::int4 END,
                    CASE WHEN g % 13 = 0 THEN NULL ELSE (g * 10)::int8 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE g::float8 / 10.0 END,
                    CASE WHEN g % 19 = 0 THEN NULL ELSE {uuid_expr} END,
                    CASE
                        WHEN g % 5 = 0 THEN NULL
                        WHEN g % 5 = 1 THEN ''
                        WHEN g % 5 = 2 THEN 'short'
                        WHEN g % 5 = 3 THEN repeat('x', 48)
                        ELSE repeat(md5(g::text), 2)
                    END,
                    CASE
                        WHEN g % 6 = 0 THEN NULL
                        WHEN g % 6 = 1 THEN decode('', 'hex')
                        WHEN g % 6 = 2 THEN decode('0001', 'hex')
                        WHEN g % 6 = 3 THEN convert_to(repeat('y', 48), 'UTF8')
                        ELSE decode(md5(g::text), 'hex')
                    END
                 FROM generate_series(1, {rows}) AS g"
            ))
            .context("populate mixed temp table")?;
        }
    }
    unsafe {
        pg_sys::CommandCounterIncrement();
    }
    Ok(())
}

fn run_baseline_bench(profile: BenchmarkProfile, iterations: usize) -> AnyResult<BaselineMetrics> {
    let relation = profile.relation_name();
    let start = Instant::now();
    let mut rows_total = 0usize;

    for _ in 0..iterations {
        let mut scan = RelationScan::open(relation)?;
        let natts = usize::try_from(unsafe { (*scan.tuple_desc()).natts })
            .context("natts does not fit into usize")?;
        while scan.next_slot() {
            unsafe {
                pg_sys::slot_getallattrs(scan.slot());
                let slot = &*scan.slot();
                black_box(slot.tts_nvalid);
                if natts > 0 {
                    black_box(slot.tts_values);
                    black_box(slot.tts_isnull);
                }
            }
            rows_total += 1;
        }
    }

    Ok(BaselineMetrics {
        rows_total,
        elapsed_ns: start.elapsed().as_nanos(),
    })
}

fn baseline_metrics_json(
    profile: BenchmarkProfile,
    iterations: usize,
    baseline: BaselineMetrics,
) -> Value {
    let baseline_secs = if baseline.elapsed_ns > 0 {
        baseline.elapsed_ns as f64 / 1_000_000_000.0
    } else {
        f64::MIN_POSITIVE
    };
    let baseline_rows_per_sec = baseline.rows_total as f64 / baseline_secs;

    json!({
        "profile": profile.name(),
        "iterations": iterations,
        "rows_total": baseline.rows_total,
        "elapsed_ns": baseline.elapsed_ns,
        "rows_per_sec": baseline_rows_per_sec,
    })
}

fn finish_page(encoder: PageBatchEncoder<'_>, pages_total: &mut usize) -> AnyResult<()> {
    let encoded = encoder.finish()?;
    if encoded.row_count > 0 {
        *pages_total += 1;
    }
    black_box(encoded.row_count);
    black_box(encoded.payload_len);
    Ok(())
}

fn run_arrow_bench(
    profile: BenchmarkProfile,
    iterations: usize,
    config: BenchConfig,
) -> AnyResult<ArrowMetrics> {
    let relation = profile.relation_name();
    let plan = LayoutPlan::from_arrow_schema(
        profile.schema().as_ref(),
        u32::try_from(config.rows_per_page).context("rows per page does not fit into u32")?,
        u32::try_from(config.payload_capacity_bytes)
            .context("payload capacity does not fit into u32")?,
    )?;
    let payload_len = usize::try_from(plan.block_size()).context("block size does not fit")?;
    let mut payload = vec![0_u8; payload_len];

    let start = Instant::now();
    let mut rows_total = 0usize;
    let mut pages_total = 0usize;

    for _ in 0..iterations {
        let mut scan = RelationScan::open(relation)?;
        init_block(&mut payload, &plan)?;
        let mut encoder = unsafe { PageBatchEncoder::new(scan.tuple_desc(), &mut payload)? };

        while scan.next_slot() {
            loop {
                match encoder.append_slot(scan.slot())? {
                    AppendStatus::Appended => {
                        rows_total += 1;
                        break;
                    }
                    AppendStatus::Full => {
                        finish_page(encoder, &mut pages_total)?;
                        init_block(&mut payload, &plan)?;
                        encoder =
                            unsafe { PageBatchEncoder::new(scan.tuple_desc(), &mut payload)? };
                    }
                }
            }
        }

        finish_page(encoder, &mut pages_total)?;
    }

    Ok(ArrowMetrics {
        rows_total,
        pages_total,
        elapsed_ns: start.elapsed().as_nanos(),
        block_size_bytes: payload_len,
    })
}

fn arrow_metrics_json(
    profile: BenchmarkProfile,
    iterations: usize,
    config: BenchConfig,
    arrow: ArrowMetrics,
) -> Value {
    let arrow_secs = if arrow.elapsed_ns > 0 {
        arrow.elapsed_ns as f64 / 1_000_000_000.0
    } else {
        f64::MIN_POSITIVE
    };
    let arrow_rows_per_sec = arrow.rows_total as f64 / arrow_secs;
    let pages_per_sec = arrow.pages_total as f64 / arrow_secs;
    let avg_rows_per_page = if arrow.pages_total > 0 {
        arrow.rows_total as f64 / arrow.pages_total as f64
    } else {
        0.0
    };

    json!({
        "profile": profile.name(),
        "iterations": iterations,
        "rows_per_page": config.rows_per_page,
        "payload_capacity_bytes": config.payload_capacity_bytes,
        "rows_total": arrow.rows_total,
        "pages_total": arrow.pages_total,
        "elapsed_ns": arrow.elapsed_ns,
        "rows_per_sec": arrow_rows_per_sec,
        "pages_per_sec": pages_per_sec,
        "avg_rows_per_page": avg_rows_per_page,
        "block_size_bytes": arrow.block_size_bytes,
    })
}

fn metrics_json(
    profile: BenchmarkProfile,
    rows: usize,
    iterations: usize,
    config: BenchConfig,
    baseline: BaselineMetrics,
    arrow: ArrowMetrics,
) -> Value {
    let baseline_secs = if baseline.elapsed_ns > 0 {
        baseline.elapsed_ns as f64 / 1_000_000_000.0
    } else {
        f64::MIN_POSITIVE
    };
    let arrow_secs = if arrow.elapsed_ns > 0 {
        arrow.elapsed_ns as f64 / 1_000_000_000.0
    } else {
        f64::MIN_POSITIVE
    };
    let baseline_rows_per_sec = baseline.rows_total as f64 / baseline_secs;
    let arrow_rows_per_sec = arrow.rows_total as f64 / arrow_secs;
    let pages_per_sec = arrow.pages_total as f64 / arrow_secs;
    let avg_rows_per_page = if arrow.pages_total > 0 {
        arrow.rows_total as f64 / arrow.pages_total as f64
    } else {
        0.0
    };

    json!({
        "profile": profile.name(),
        "rows": rows,
        "iterations": iterations,
        "rows_per_page": config.rows_per_page,
        "payload_capacity_bytes": config.payload_capacity_bytes,
        "baseline": {
            "rows_total": baseline.rows_total,
            "elapsed_ns": baseline.elapsed_ns,
            "rows_per_sec": baseline_rows_per_sec,
        },
        "arrow": {
            "rows_total": arrow.rows_total,
            "pages_total": arrow.pages_total,
            "elapsed_ns": arrow.elapsed_ns,
            "rows_per_sec": arrow_rows_per_sec,
            "pages_per_sec": pages_per_sec,
            "avg_rows_per_page": avg_rows_per_page,
            "block_size_bytes": arrow.block_size_bytes,
        },
        "ratio": {
            "arrow_vs_baseline": arrow_rows_per_sec / baseline_rows_per_sec,
            "slowdown_vs_baseline": baseline_rows_per_sec / arrow_rows_per_sec,
        }
    })
}

fn run_slot_deform_vs_page_encode_bench_impl(
    profile: BenchmarkProfile,
    rows: usize,
    iterations: usize,
    config: BenchConfig,
) -> AnyResult<Value> {
    if rows == 0 {
        bail!("rows must be greater than zero");
    }
    if iterations == 0 {
        bail!("iterations must be greater than zero");
    }

    setup_profile_table(profile, rows)?;
    let baseline = run_baseline_bench(profile, iterations)?;
    let arrow = run_arrow_bench(profile, iterations, config)?;

    if baseline.rows_total != arrow.rows_total {
        bail!(
            "row count mismatch between baseline ({}) and arrow ({})",
            baseline.rows_total,
            arrow.rows_total
        );
    }

    Ok(metrics_json(
        profile, rows, iterations, config, baseline, arrow,
    ))
}

fn run_slot_deform_bench_prepare_impl(profile: BenchmarkProfile, rows: usize) -> AnyResult<Value> {
    if rows == 0 {
        bail!("rows must be greater than zero");
    }
    setup_profile_table(profile, rows)?;
    Ok(json!({
        "profile": profile.name(),
        "rows": rows,
        "relation": profile.relation_name(),
    }))
}

fn run_slot_deform_baseline_bench_impl(
    profile: BenchmarkProfile,
    iterations: usize,
) -> AnyResult<Value> {
    if iterations == 0 {
        bail!("iterations must be greater than zero");
    }
    ensure_prepared_relation(profile)?;
    let baseline = run_baseline_bench(profile, iterations)?;
    Ok(baseline_metrics_json(profile, iterations, baseline))
}

fn run_slot_deform_arrow_bench_impl(
    profile: BenchmarkProfile,
    iterations: usize,
    config: BenchConfig,
) -> AnyResult<Value> {
    if iterations == 0 {
        bail!("iterations must be greater than zero");
    }
    ensure_prepared_relation(profile)?;
    let arrow = run_arrow_bench(profile, iterations, config)?;
    Ok(arrow_metrics_json(profile, iterations, config, arrow))
}

pub(crate) fn slot_deform_bench_prepare(profile: String, rows: i32) -> pgrx::JsonB {
    match (|| -> AnyResult<Value> {
        let profile = BenchmarkProfile::parse(&profile)?;
        let rows = usize::try_from(rows).context("rows must be non-negative")?;
        run_slot_deform_bench_prepare_impl(profile, rows)
    })() {
        Ok(metrics) => pgrx::JsonB(metrics),
        Err(error) => pgrx::error!("{}", error),
    }
}

pub(crate) fn slot_deform_baseline_bench(profile: String, iterations: i32) -> pgrx::JsonB {
    match (|| -> AnyResult<Value> {
        let profile = BenchmarkProfile::parse(&profile)?;
        let iterations = usize::try_from(iterations).context("iterations must be non-negative")?;
        run_slot_deform_baseline_bench_impl(profile, iterations)
    })() {
        Ok(metrics) => pgrx::JsonB(metrics),
        Err(error) => pgrx::error!("{}", error),
    }
}

pub(crate) fn slot_deform_arrow_bench(
    profile: String,
    iterations: i32,
    rows_per_page: i32,
    payload_capacity_bytes: i32,
) -> pgrx::JsonB {
    match (|| -> AnyResult<Value> {
        let profile = BenchmarkProfile::parse(&profile)?;
        let iterations = usize::try_from(iterations).context("iterations must be non-negative")?;
        let rows_per_page =
            usize::try_from(rows_per_page).context("rows_per_page must be non-negative")?;
        let payload_capacity_bytes = usize::try_from(payload_capacity_bytes)
            .context("payload_capacity_bytes must be non-negative")?;
        let config = BenchConfig::new(rows_per_page, payload_capacity_bytes)?;
        run_slot_deform_arrow_bench_impl(profile, iterations, config)
    })() {
        Ok(metrics) => pgrx::JsonB(metrics),
        Err(error) => pgrx::error!("{}", error),
    }
}

pub(crate) fn slot_deform_vs_page_encode_bench(
    profile: String,
    rows: i32,
    iterations: i32,
    rows_per_page: i32,
    payload_capacity_bytes: i32,
) -> pgrx::JsonB {
    match (|| -> AnyResult<Value> {
        let profile = BenchmarkProfile::parse(&profile)?;
        let rows = usize::try_from(rows).context("rows must be non-negative")?;
        let iterations = usize::try_from(iterations).context("iterations must be non-negative")?;
        let rows_per_page =
            usize::try_from(rows_per_page).context("rows_per_page must be non-negative")?;
        let payload_capacity_bytes = usize::try_from(payload_capacity_bytes)
            .context("payload_capacity_bytes must be non-negative")?;
        let config = BenchConfig::new(rows_per_page, payload_capacity_bytes)?;
        run_slot_deform_vs_page_encode_bench_impl(profile, rows, iterations, config)
    })() {
        Ok(metrics) => pgrx::JsonB(metrics),
        Err(error) => pgrx::error!("{}", error),
    }
}

fn smoke_assertions(metrics: &Value, rows: usize, iterations: usize) {
    let expected_rows = u64::try_from(rows.saturating_mul(iterations)).expect("expected rows");
    assert_eq!(metrics["rows"].as_u64(), Some(rows as u64));
    assert_eq!(metrics["iterations"].as_u64(), Some(iterations as u64));
    assert_eq!(
        metrics["baseline"]["rows_total"].as_u64(),
        Some(expected_rows)
    );
    assert_eq!(metrics["arrow"]["rows_total"].as_u64(), Some(expected_rows));
    assert!(metrics["arrow"]["pages_total"].as_u64().unwrap_or(0) > 0);
}

pub(crate) fn slot_deform_vs_page_encode_bench_fixed_smoke() {
    let rows = 256usize;
    let iterations = 1usize;
    let config =
        BenchConfig::new(DEFAULT_ROWS_PER_PAGE, DEFAULT_PAYLOAD_CAPACITY_BYTES).expect("config");
    let metrics = run_slot_deform_vs_page_encode_bench_impl(
        BenchmarkProfile::Fixed,
        rows,
        iterations,
        config,
    )
    .expect("fixed profile benchmark");
    smoke_assertions(&metrics, rows, iterations);
}

pub(crate) fn slot_deform_vs_page_encode_bench_mixed_smoke() {
    let rows = 256usize;
    let iterations = 1usize;
    let config =
        BenchConfig::new(DEFAULT_ROWS_PER_PAGE, DEFAULT_PAYLOAD_CAPACITY_BYTES).expect("config");
    let metrics = run_slot_deform_vs_page_encode_bench_impl(
        BenchmarkProfile::Mixed,
        rows,
        iterations,
        config,
    )
    .expect("mixed profile benchmark");
    smoke_assertions(&metrics, rows, iterations);
}

pub(crate) fn slot_deform_vs_page_encode_bench_large_page_smoke() {
    let rows = 1024usize;
    let iterations = 1usize;
    let small_config =
        BenchConfig::new(DEFAULT_ROWS_PER_PAGE, DEFAULT_PAYLOAD_CAPACITY_BYTES).expect("config");
    let large_config = BenchConfig::new(4096, 1024 * 1024 - 20).expect("config");
    let small = run_slot_deform_vs_page_encode_bench_impl(
        BenchmarkProfile::Fixed,
        rows,
        iterations,
        small_config,
    )
    .expect("small-page benchmark");
    let large = run_slot_deform_vs_page_encode_bench_impl(
        BenchmarkProfile::Fixed,
        rows,
        iterations,
        large_config,
    )
    .expect("large-page benchmark");
    smoke_assertions(&small, rows, iterations);
    smoke_assertions(&large, rows, iterations);
    let small_pages = small["arrow"]["pages_total"].as_u64().expect("small pages");
    let large_pages = large["arrow"]["pages_total"].as_u64().expect("large pages");
    assert!(
        large_pages < small_pages,
        "expected fewer pages for large-page config"
    );
}

pub(crate) fn slot_deform_split_bench_smoke() {
    let rows = 256usize;
    let iterations = 1usize;
    let config =
        BenchConfig::new(DEFAULT_ROWS_PER_PAGE, DEFAULT_PAYLOAD_CAPACITY_BYTES).expect("config");

    let prepared = run_slot_deform_bench_prepare_impl(BenchmarkProfile::Mixed, rows)
        .expect("prepare benchmark table");
    assert_eq!(prepared["rows"].as_u64(), Some(rows as u64));

    let baseline = run_slot_deform_baseline_bench_impl(BenchmarkProfile::Mixed, iterations)
        .expect("baseline-only benchmark");
    assert_eq!(
        baseline["rows_total"].as_u64(),
        Some((rows * iterations) as u64)
    );

    let arrow = run_slot_deform_arrow_bench_impl(BenchmarkProfile::Mixed, iterations, config)
        .expect("arrow-only benchmark");
    assert_eq!(
        arrow["rows_total"].as_u64(),
        Some((rows * iterations) as u64)
    );
    assert!(arrow["pages_total"].as_u64().unwrap_or(0) > 0);
}
