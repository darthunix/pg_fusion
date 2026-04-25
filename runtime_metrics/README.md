# runtime_metrics

`runtime_metrics` stores low-overhead runtime counters, timers, and page handoff
stamps for the active `pg_fusion` runtime. The crate is intentionally small and
has no PostgreSQL dependency: the extension allocates one shared-memory region,
initializes it during `_PG_init`, then passes cheap `RuntimeMetrics` handles to
the backend and worker code paths.

The crate does not instrument control rings directly. Page senders stamp the
`PageDescriptor` when an issued page is published, and receivers measure the
elapsed monotonic time when that descriptor arrives. This keeps the first version
focused on the data-plane delays that matter for scan/result latency without
wrapping every transport operation.

## Region Lifecycle

Use `RuntimeMetricsConfig` to size the shared region. `page_count` must match the
page pool, because the crate stores one page stamp per page id.

```rust
use std::ptr::NonNull;
use runtime_metrics::{RuntimeMetrics, RuntimeMetricsConfig};

let config = RuntimeMetricsConfig::new(page_count)?;
let layout = RuntimeMetrics::layout(config)?;

// Allocate `layout.size` bytes with `layout.align` alignment in shared memory.
let base: NonNull<u8> = shmem_base;

// Postmaster initialization path.
let metrics = unsafe { RuntimeMetrics::init_in_place(base, layout.size, config)? };

// Backend/worker attach path.
let metrics = unsafe { RuntimeMetrics::attach(base, layout.size)? };
```

`RuntimeMetrics::default()` is a no-op handle. It is useful in tests and in
runtime code that can run outside the PostgreSQL extension.

## Recording Values

Counters and timers are updated with relaxed atomics and saturating arithmetic.
The values are cumulative until `reset()` is called.

```rust
use runtime_metrics::{MetricId, PageDirection};

metrics.increment(MetricId::BackendExecCallsTotal);

let start = metrics.now_ns();
do_backend_work();
metrics.add_elapsed(MetricId::BackendTotalNs, start);

metrics.stamp_page(PageDirection::BackendToWorker, descriptor, payload_len);
if let Some(observation) = metrics.observe_page(PageDirection::BackendToWorker, descriptor) {
    metrics.add(MetricId::ScanB2wWaitNs, observation.wait_ns);
    metrics.increment(MetricId::ScanB2wWaitTotal);
}
```

`reset()` clears all metric values, clears page stamps, and advances
`reset_epoch`. Receivers ignore stamps from older epochs. Reset is intended for
manual experiments; concurrent increments can race with a reset and are not a
transactional snapshot boundary.

## Reading Values

The crate exposes raw `MetricValue` rows through `snapshot()`. The extension maps
those rows to SQL via `pg_fusion_metrics()`:

```sql
SELECT component, metric, kind, unit, value, reset_epoch
FROM pg_fusion_metrics()
ORDER BY component, metric;
```

Naming conventions:

- `*_ns` is accumulated monotonic time in nanoseconds.
- `*_total` is an event count or cumulative counter.
- `*_bytes_sent_total` is payload bytes written into shared page slots.

Timer totals can overlap. For example, `backend_total_ns` and `worker_total_ns`
often cover the same wall-clock interval from different processes, so their sum
is not expected to equal `query_total_ns`.

To derive averages, divide a timer by its matching count:

```sql
SELECT
  sum(value) FILTER (WHERE metric = 'scan_b2w_wait_ns')::numeric
  / nullif(sum(value) FILTER (WHERE metric = 'scan_b2w_wait_total'), 0)
    AS avg_scan_backend_to_worker_wait_ns
FROM pg_fusion_metrics();
```

## Metric Reference

| Metric | Meaning |
| --- | --- |
| `query_total_ns` | Wall-clock time for a `PgFusionScan` execution from backend begin until EOF/end is recorded. Excludes PostgreSQL planning and extension load time. |
| `backend_total_ns` | Accumulated time spent inside backend-side pg_fusion execution callbacks and scan-driving work. Includes useful backend work and backend-side waiting/polling. |
| `backend_exec_calls_total` | Number of PostgreSQL custom scan `Exec` calls handled by pg_fusion, including the final call that returns EOF. |
| `backend_rows_returned_total` | Number of result rows returned from pg_fusion back into PostgreSQL executor slots. |
| `backend_wait_latch_ns` | Time the backend spent in short latch waits after an execution loop made no progress. This usually means it was waiting for worker/control/result progress. |
| `backend_wait_latch_total` | Number of backend latch waits included in `backend_wait_latch_ns`. |
| `scan_page_fill_ns` | Backend time spent filling successful scan pages from PostgreSQL scan output into Arrow page payloads. This includes cursor/slot draining, tuple encoding, page layout, and estimator work for emitted pages. |
| `scan_pages_sent_total` | Number of scan data pages sent from backend to worker. Terminal scan close/control frames are not counted here. |
| `scan_bytes_sent_total` | Payload bytes sent in scan data pages from backend to worker. This is page payload length, not necessarily useful row bytes. |
| `scan_b2w_wait_ns` | Time from backend stamping a scan page after send until the worker scan thread observes the same page descriptor. This approximates backend-to-worker data-plane handoff latency. |
| `scan_b2w_wait_total` | Number of scan page observations included in `scan_b2w_wait_ns`. |
| `scan_page_read_ns` | Worker-side time spent accepting/importing a scan page frame into the scan flow and forwarding the resulting `RecordBatch` toward DataFusion. |
| `scan_pages_read_total` | Number of scan pages read by the worker scan path. |
| `worker_total_ns` | Worker-side wall-clock time spent executing one physical plan after it becomes ready, including time blocked on scan input. It overlaps with backend scan time. |
| `worker_physical_plan_ns` | Time spent converting the logical plan into a DataFusion physical plan. |
| `worker_physical_plan_total` | Number of physical planning operations included in `worker_physical_plan_ns`. |
| `worker_result_page_fill_ns` | Worker time spent encoding DataFusion output `RecordBatch` rows into successful result pages. |
| `worker_result_pages_total` | Number of result data pages sent from worker to backend. Terminal result close/control frames are not counted here. |
| `worker_result_bytes_sent_total` | Payload bytes sent in result data pages from worker to backend. |
| `result_w2b_wait_ns` | Time from worker stamping a result page after send until the backend observes the same page descriptor. This approximates worker-to-backend data-plane handoff latency. |
| `result_w2b_wait_total` | Number of result page observations included in `result_w2b_wait_ns`. |
| `result_page_read_ns` | Backend-side time spent accepting/importing a result page frame into result ingress. |
| `result_pages_read_total` | Number of result pages read by the backend result path. |

## Interpreting Common Patterns

If `scan_page_fill_ns` dominates and `scan_pages_sent_total` is small, the
backend is likely spending most latency inside PostgreSQL scanning and page
encoding before the worker receives data. A selective query without an exact
limit can still need to scan to EOF before emitting a partial page.

If `scan_b2w_wait_ns` or `result_w2b_wait_ns` dominates, the page has already
been produced and the delay is in data-plane handoff or receiver scheduling.

If `worker_total_ns` is high but `worker_result_page_fill_ns` and physical
planning are low, the worker may mostly be waiting on scan input. Compare it
with `scan_page_fill_ns` before treating it as worker CPU time.
