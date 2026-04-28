use arrow_layout::{init_block, ColumnSpec, LayoutPlan, TypeTag};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use row_encoder::{AppendStatus, CellRef, PageRowEncoder, RowEncodeError, RowSource};
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const BLOCK_SIZE: u32 = 65_516;

#[derive(Clone, Debug)]
enum BenchCell {
    Int32(i32),
    Float64(f64),
    Utf8(String),
}

impl BenchCell {
    fn as_ref(&self) -> CellRef<'_> {
        match self {
            Self::Int32(value) => CellRef::Int32(*value),
            Self::Float64(value) => CellRef::Float64(*value),
            Self::Utf8(value) => CellRef::Utf8(value.as_bytes()),
        }
    }
}

#[derive(Clone, Debug)]
struct BenchTable {
    name: &'static str,
    specs: Vec<ColumnSpec>,
    rows: Vec<Vec<BenchCell>>,
}

impl BenchTable {
    fn plan(&self) -> LayoutPlan {
        largest_fitting_plan(&self.specs, BLOCK_SIZE, self.rows.len())
    }
}

struct BenchRow<'a> {
    cells: &'a [BenchCell],
}

impl RowSource for BenchRow<'_> {
    type Error = RowEncodeError;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        f(self.cells[index].as_ref())
    }
}

fn largest_fitting_plan(specs: &[ColumnSpec], block_size: u32, row_hint: usize) -> LayoutPlan {
    let mut low = 1_u32;
    let mut high = block_size.min(u32::try_from(row_hint.max(1)).expect("row hint"));
    let mut best = LayoutPlan::new(specs, 1, block_size).expect("single row must fit");
    while low <= high {
        let mid = low + ((high - low) / 2);
        match LayoutPlan::new(specs, mid, block_size) {
            Ok(plan) => {
                best = plan;
                low = mid.saturating_add(1);
            }
            Err(_) => {
                high = mid.saturating_sub(1);
            }
        }
    }
    best
}

fn measure_append_pages(tables: &[BenchTable], iterations: u64) -> Duration {
    let plans = tables.iter().map(BenchTable::plan).collect::<Vec<_>>();
    let max_payload_len = plans
        .iter()
        .map(|plan| usize::try_from(plan.block_size()).expect("block size"))
        .max()
        .unwrap_or(BLOCK_SIZE as usize);
    let mut payload = vec![0_u8; max_payload_len];
    let mut offsets = vec![0usize; tables.len()];
    let mut total = Duration::ZERO;
    let mut rows_total = 0usize;
    let mut pages_total = 0usize;

    for _ in 0..iterations {
        for (index, (table, plan)) in tables.iter().zip(plans.iter()).enumerate() {
            let (rows, elapsed) =
                encode_one_page(table, plan, &mut offsets[index], &mut payload, true);
            rows_total += rows;
            pages_total += 1;
            total += elapsed;
        }
    }

    black_box(rows_total);
    black_box(pages_total);
    total
}

fn estimated_page_rows(table: &BenchTable) -> usize {
    let plan = table.plan();
    let mut payload = vec![0_u8; usize::try_from(plan.block_size()).expect("block size")];
    let mut offset = 0usize;
    encode_one_page(table, &plan, &mut offset, &mut payload, false).0
}

fn encode_one_page(
    table: &BenchTable,
    plan: &LayoutPlan,
    offset: &mut usize,
    payload: &mut [u8],
    timed: bool,
) -> (usize, Duration) {
    assert!(!table.rows.is_empty(), "benchmark table has no rows");
    init_block(payload, plan).expect("init block");
    let mut encoder = PageRowEncoder::new(payload).expect("encoder");
    let mut rows = 0usize;
    let max_rows = usize::try_from(plan.max_rows()).expect("max rows");
    let start = timed.then(Instant::now);

    while rows < max_rows {
        let row_index = *offset % table.rows.len();
        let mut source = BenchRow {
            cells: &table.rows[row_index],
        };
        match encoder
            .append_row(black_box(&mut source))
            .expect("append row")
        {
            AppendStatus::Appended => {
                *offset += 1;
                rows += 1;
            }
            AppendStatus::Full => break,
        }
    }

    let elapsed = start.map_or(Duration::ZERO, |start| start.elapsed());
    let encoded = encoder.finish().expect("finish page");
    assert_eq!(encoded.row_count, rows);
    black_box(&payload[..encoded.payload_len]);
    black_box(encoded.payload_len);
    (rows, elapsed)
}

fn int32() -> ColumnSpec {
    ColumnSpec::new(TypeTag::Int32, false)
}

fn float64() -> ColumnSpec {
    ColumnSpec::new(TypeTag::Float64, false)
}

fn utf8() -> ColumnSpec {
    ColumnSpec::new(TypeTag::Utf8View, false)
}

fn synthetic_tables() -> Vec<BenchTable> {
    vec![
        synthetic_lineitem(60_175),
        synthetic_orders(2_395),
        synthetic_customer(1_500),
        synthetic_supplier(100),
        synthetic_nation(),
        synthetic_region(),
    ]
}

fn synthetic_lineitem(rows: usize) -> BenchTable {
    BenchTable {
        name: "synthetic_lineitem",
        specs: vec![int32(), int32(), float64(), float64()],
        rows: (0..rows)
            .map(|row| {
                vec![
                    BenchCell::Int32(row as i32 + 1),
                    BenchCell::Int32((row % 100) as i32 + 1),
                    BenchCell::Float64(10_000.0 + row as f64),
                    BenchCell::Float64((row % 10) as f64 / 100.0),
                ]
            })
            .collect(),
    }
}

fn synthetic_orders(rows: usize) -> BenchTable {
    BenchTable {
        name: "synthetic_orders",
        specs: vec![int32(), int32()],
        rows: (0..rows)
            .map(|row| {
                vec![
                    BenchCell::Int32(row as i32 + 1),
                    BenchCell::Int32((row % 1_500) as i32 + 1),
                ]
            })
            .collect(),
    }
}

fn synthetic_customer(rows: usize) -> BenchTable {
    BenchTable {
        name: "synthetic_customer",
        specs: vec![int32(), int32()],
        rows: (0..rows)
            .map(|row| {
                vec![
                    BenchCell::Int32(row as i32 + 1),
                    BenchCell::Int32((row % 25) as i32),
                ]
            })
            .collect(),
    }
}

fn synthetic_supplier(rows: usize) -> BenchTable {
    BenchTable {
        name: "synthetic_supplier",
        specs: vec![int32(), int32()],
        rows: (0..rows)
            .map(|row| {
                vec![
                    BenchCell::Int32(row as i32 + 1),
                    BenchCell::Int32((row % 25) as i32),
                ]
            })
            .collect(),
    }
}

fn synthetic_nation() -> BenchTable {
    const NAMES: [&str; 25] = [
        "ALGERIA",
        "ARGENTINA",
        "BRAZIL",
        "CANADA",
        "EGYPT",
        "ETHIOPIA",
        "FRANCE",
        "GERMANY",
        "INDIA",
        "INDONESIA",
        "IRAN",
        "IRAQ",
        "JAPAN",
        "JORDAN",
        "KENYA",
        "MOROCCO",
        "MOZAMBIQUE",
        "PERU",
        "CHINA",
        "ROMANIA",
        "SAUDI ARABIA",
        "VIETNAM",
        "RUSSIA",
        "UNITED KINGDOM",
        "UNITED STATES",
    ];
    BenchTable {
        name: "synthetic_nation",
        specs: vec![int32(), utf8(), int32()],
        rows: NAMES
            .iter()
            .enumerate()
            .map(|(row, name)| {
                vec![
                    BenchCell::Int32(row as i32),
                    BenchCell::Utf8((*name).to_owned()),
                    BenchCell::Int32((row % 5) as i32),
                ]
            })
            .collect(),
    }
}

fn synthetic_region() -> BenchTable {
    BenchTable {
        name: "synthetic_region",
        specs: vec![int32()],
        rows: vec![vec![BenchCell::Int32(2)]],
    }
}

fn live_tables(dir: &Path) -> csv::Result<Vec<BenchTable>> {
    Ok(vec![
        live_lineitem(dir)?,
        live_orders(dir)?,
        live_customer(dir)?,
        live_supplier(dir)?,
        live_nation(dir)?,
        live_region(dir)?,
    ])
}

fn live_lineitem(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("lineitem.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(vec![
            BenchCell::Int32(parse_i32(&record[0])),
            BenchCell::Int32(parse_i32(&record[2])),
            BenchCell::Float64(parse_f64(&record[5])),
            BenchCell::Float64(parse_f64(&record[6])),
        ]);
    }
    Ok(BenchTable {
        name: "live_lineitem",
        specs: vec![int32(), int32(), float64(), float64()],
        rows,
    })
}

fn live_orders(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("orders.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let order_date = &record[4];
        if order_date >= "1994-01-01" && order_date < "1995-01-01" {
            rows.push(vec![
                BenchCell::Int32(parse_i32(&record[0])),
                BenchCell::Int32(parse_i32(&record[1])),
            ]);
        }
    }
    Ok(BenchTable {
        name: "live_orders",
        specs: vec![int32(), int32()],
        rows,
    })
}

fn live_customer(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("customer.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(vec![
            BenchCell::Int32(parse_i32(&record[0])),
            BenchCell::Int32(parse_i32(&record[3])),
        ]);
    }
    Ok(BenchTable {
        name: "live_customer",
        specs: vec![int32(), int32()],
        rows,
    })
}

fn live_supplier(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("supplier.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(vec![
            BenchCell::Int32(parse_i32(&record[0])),
            BenchCell::Int32(parse_i32(&record[3])),
        ]);
    }
    Ok(BenchTable {
        name: "live_supplier",
        specs: vec![int32(), int32()],
        rows,
    })
}

fn live_nation(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("nation.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(vec![
            BenchCell::Int32(parse_i32(&record[0])),
            BenchCell::Utf8(record[1].to_owned()),
            BenchCell::Int32(parse_i32(&record[2])),
        ]);
    }
    Ok(BenchTable {
        name: "live_nation",
        specs: vec![int32(), utf8(), int32()],
        rows,
    })
}

fn live_region(dir: &Path) -> csv::Result<BenchTable> {
    let mut reader = csv::Reader::from_path(dir.join("region.csv"))?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        if &record[1] == "ASIA" {
            rows.push(vec![BenchCell::Int32(parse_i32(&record[0]))]);
        }
    }
    Ok(BenchTable {
        name: "live_region",
        specs: vec![int32()],
        rows,
    })
}

fn parse_i32(raw: &str) -> i32 {
    raw.parse().expect("i32")
}

fn parse_f64(raw: &str) -> f64 {
    raw.parse().expect("f64")
}

fn default_tpch_dir() -> PathBuf {
    workspace_root().join("benches/tpch/data/sf_0_01")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn resolve_data_dir(path: PathBuf) -> PathBuf {
    if path.is_absolute() || path.exists() {
        path
    } else {
        workspace_root().join(path)
    }
}

fn discover_live_dir() -> Option<PathBuf> {
    env::var_os("PG_FUSION_TPCH_DIR")
        .map(PathBuf::from)
        .map(resolve_data_dir)
        .or_else(|| {
            let default = default_tpch_dir();
            default.exists().then_some(default)
        })
}

fn bench_q05_encode(c: &mut Criterion) {
    let synthetic = synthetic_tables();
    let synthetic_rows = synthetic.iter().map(estimated_page_rows).sum::<usize>();
    let mut group = c.benchmark_group("q05_encode_append_only");
    group.throughput(Throughput::Elements(synthetic_rows as u64));
    group.bench_function("synthetic_all_scans_64k", |b| {
        b.iter_custom(|iters| measure_append_pages(&synthetic, iters))
    });

    if let Some(dir) = discover_live_dir() {
        match live_tables(&dir) {
            Ok(live) => {
                let live_rows = live.iter().map(estimated_page_rows).sum::<usize>();
                group.throughput(Throughput::Elements(live_rows as u64));
                group.bench_function("live_all_scans_64k", |b| {
                    b.iter_custom(|iters| measure_append_pages(&live, iters))
                });
                for table in &live {
                    group.throughput(Throughput::Elements(estimated_page_rows(table) as u64));
                    group.bench_function(table.name, |b| {
                        b.iter_custom(|iters| {
                            measure_append_pages(std::slice::from_ref(table), iters)
                        })
                    });
                }
            }
            Err(error) => eprintln!("skipping live q05 fixture at {}: {error}", dir.display()),
        }
    } else {
        eprintln!(
            "skipping live q05 fixture: PG_FUSION_TPCH_DIR is unset and default data is absent"
        );
    }

    group.finish();
}

criterion_group!(benches, bench_q05_encode);
criterion_main!(benches);
