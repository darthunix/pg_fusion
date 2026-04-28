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

Worker scan threads also record the local delivery path after a backend page is
visible: idle polling sleeps, page import, and the blocking send into
DataFusion's scan channel. These timers help distinguish backend-to-worker page
handoff from DataFusion-side backpressure.

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

## Detailed Scan Timing

`scan_page_fill_ns` is a coarse backend timer. It includes PostgreSQL cursor
execution, callback dispatch, slot deform/detoast, Arrow writes, page
initialization, and page finalization. To split that time, enable detailed scan
timing in the session:

```sql
SELECT pg_fusion_metrics_reset();
SET pg_fusion.scan_timing_detail = on;

SELECT a FROM t2 WHERE a = 1;

SELECT metric, value
FROM pg_fusion_metrics()
WHERE metric LIKE 'scan_%'
ORDER BY metric;
```

Detailed timing measures the row callback on every returned slot. Keep it off
for normal runs; the measurement itself adds overhead to fast scans. When
dynamic scan workers are used, the query-time flag is propagated to each scan
producer so scan detail metrics cover the leader and worker producers.

Interpretation:

- `scan_postgres_read_ns` is PostgreSQL executor/heap/filter time outside the
  pg_fusion row callback.
- `scan_arrow_encode_ns` is callback time: slot deform/detoast plus Arrow page
  writes.
- `scan_fill_pre_drain_ns`, `scan_fill_post_drain_ns`,
  `scan_fill_overflow_encode_ns`, and `scan_fill_emit_ns` split Rust-side
  page-fill bookkeeping around the PostgreSQL drain and page emission paths.
- `scan_fill_unclassified_ns` is the remaining successful-page fill time after
  all published page-fill buckets are subtracted.
- `scan_postgres_read_ns >> scan_arrow_encode_ns` points at PostgreSQL scan
  cost; the opposite points at serialization cost.

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
| `scan_page_prepare_ns` | Backend time spent estimating page shape, building the Arrow layout, initializing the block, and constructing the scan page encoder for emitted pages. |
| `scan_page_finish_ns` | Backend time spent finalizing emitted scan pages and feeding their encoded size back into the row estimator. |
| `scan_page_snapshot_ns` | Detailed-only overhead around the registered PostgreSQL snapshot wrapper for emitted scan pages. This is `scan_page_fill_ns` minus the inner page-fill body. |
| `scan_slot_drain_ns` | Detailed-only wall-clock time around one PostgreSQL slot drain call. Compare with `scan_postgres_read_ns + scan_arrow_encode_ns` to estimate direct receiver/SPI wrapper overhead. |
| `scan_overflow_copy_ns` | Detailed-only time spent copying an overflowing PostgreSQL slot into the pending overflow tuple when a variable-width row does not fit the current Arrow page. |
| `scan_page_retry_ns` | Detailed-only time spent in page-fill attempts that did not emit a page and instead forced an estimator retry/backoff. |
| `scan_page_retry_total` | Number of page-fill retry/backoff attempts included in `scan_page_retry_ns`. |
| `scan_fill_pre_drain_ns` | Detailed-only Rust-side bookkeeping before a PostgreSQL drain call, such as row budget selection and callback setup, for emitted scan pages. |
| `scan_fill_post_drain_ns` | Detailed-only Rust-side bookkeeping after a PostgreSQL drain call returns, before page finalization or retry handling, for emitted scan pages. |
| `scan_fill_overflow_encode_ns` | Detailed-only time spent encoding a pending overflow row into a later emitted page, outside the normal drain callback path. |
| `scan_fill_emit_ns` | Detailed-only time spent updating scan page counters and constructing the emitted page status after page finalization. |
| `scan_fill_unclassified_ns` | Detailed-only remainder of successful scan page fill time after the published page-fill buckets are subtracted. This is a control metric and should stay small. |
| `scan_fetch_calls_total` | Number of PostgreSQL cursor drain calls issued by the backend scan page source. |
| `scan_rows_encoded_total` | Number of PostgreSQL rows encoded into emitted backend-to-worker scan pages. |
| `scan_postgres_read_ns` | Detailed-only time spent in `PortalRunFetch` outside the pg_fusion row callback. This approximates PostgreSQL executor/heap/filter time. |
| `scan_arrow_encode_ns` | Detailed-only time spent inside the pg_fusion row callback, including slot deform/detoast and Arrow page writes. |
| `scan_full_pages_total` | Number of scan pages emitted because the current Arrow page became full. |
| `scan_eof_pages_total` | Number of partial scan pages emitted only after PostgreSQL reached EOF. |
| `scan_pages_sent_total` | Number of scan data pages sent from backend to worker. Terminal scan close/control frames are not counted here. |
| `scan_bytes_sent_total` | Payload bytes sent in scan data pages from backend to worker. This is page payload length, not necessarily useful row bytes. |
| `scan_b2w_wait_ns` | Time from backend stamping a scan page after send until the worker scan thread observes the same page descriptor. This approximates backend-to-worker data-plane handoff latency. |
| `scan_b2w_wait_total` | Number of scan page observations included in `scan_b2w_wait_ns`. |
| `scan_page_read_ns` | Worker-side time spent accepting/importing a scan page frame into the scan flow before any DataFusion channel send. |
| `scan_pages_read_total` | Number of scan pages read by the worker scan path. |
| `scan_batch_send_ns` | Worker scan-thread time spent inside `tx.send(Ok(batch))` when handing scan batches to DataFusion. High values indicate channel backpressure or uneven DataFusion polling. |
| `scan_batch_send_total` | Number of scan batch sends included in `scan_batch_send_ns`. |
| `scan_batch_delivery_ns` | Worker scan-thread time from reading a backend scan frame from the ring until the resulting batch send into DataFusion returns. This includes page import plus `scan_batch_send_ns`. |
| `scan_batch_delivery_total` | Number of scan batch deliveries included in `scan_batch_delivery_ns`. |
| `scan_idle_sleep_ns` | Worker scan-thread time spent sleeping after polling all scan producers and finding no frame ready. The value is measured around the sleep call, so scheduler delay is included. |
| `scan_idle_sleep_total` | Number of idle sleeps included in `scan_idle_sleep_ns`. |
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

If `scan_eof_pages_total` is positive and `scan_full_pages_total` is zero, scan
pages are reaching the worker only after PostgreSQL exhausts the cursor. For a
highly selective query this can explain high latency even when only one row is
returned.

When `scan_timing_detail` is enabled, use these derived values to explain the
non-PostgreSQL parts of `scan_page_fill_ns`:

```sql
WITH m AS (
  SELECT metric, max(value) AS value
  FROM pg_fusion_metrics()
  GROUP BY metric
)
SELECT
  coalesce(max(value) FILTER (WHERE metric = 'scan_slot_drain_ns'), 0)
    - coalesce(max(value) FILTER (WHERE metric = 'scan_postgres_read_ns'), 0)
    - coalesce(max(value) FILTER (WHERE metric = 'scan_arrow_encode_ns'), 0)
    AS scan_drain_wrapper_ns,
  coalesce(max(value) FILTER (WHERE metric = 'scan_fill_pre_drain_ns'), 0)
    AS scan_fill_pre_drain_ns,
  coalesce(max(value) FILTER (WHERE metric = 'scan_fill_post_drain_ns'), 0)
    AS scan_fill_post_drain_ns,
  coalesce(max(value) FILTER (WHERE metric = 'scan_fill_overflow_encode_ns'), 0)
    AS scan_fill_overflow_encode_ns,
  coalesce(max(value) FILTER (WHERE metric = 'scan_fill_emit_ns'), 0)
    AS scan_fill_emit_ns,
  coalesce(max(value) FILTER (WHERE metric = 'scan_fill_unclassified_ns'), 0)
    AS scan_fill_unclassified_ns
FROM m;
```

`scan_drain_wrapper_ns` points at `PortalRunFetch` receiver/SPI wrapper
overhead outside PostgreSQL executor work and Arrow encoding. The `scan_fill_*`
metrics split the old residual page-fill bookkeeping bucket; a large
`scan_fill_unclassified_ns` means the fill path still needs finer metrics.
`scan_overflow_copy_ns` is nested inside the drain callback path, so inspect it
separately instead of subtracting it again from the residual.

If `scan_b2w_wait_ns` or `result_w2b_wait_ns` dominates, the page has already
been produced and the delay is in data-plane handoff or receiver scheduling.

If `scan_b2w_wait_ns` is high but `scan_batch_send_ns` is low, the worker scan
thread is not reading frames promptly. Check `scan_idle_sleep_ns` and scheduler
or producer wakeup behavior. If `scan_batch_send_ns` is high, the worker scan
thread is blocked handing batches to DataFusion, usually because the downstream
physical plan is not polling that scan stream fast enough.

`scan_batch_delivery_ns` should be read with `scan_page_read_ns` and
`scan_batch_send_ns`: delivery minus send approximates worker-local frame
decode/import overhead, while send isolates DataFusion channel backpressure.

If `worker_total_ns` is high but `worker_result_page_fill_ns` and physical
planning are low, the worker may mostly be waiting on scan input. Compare it
with `scan_page_fill_ns` before treating it as worker CPU time.
