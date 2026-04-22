use control_transport::{AcquireError, BackendSlotLease};
use pgrx::prelude::*;
use std::thread;
use std::time::{Duration, Instant};

use crate::shmem::attach_control_region;

const WORKER_START_TIMEOUT: Duration = Duration::from_secs(5);

fn ensure_shared_preload() {
    let preload = Spi::get_one::<String>("SHOW shared_preload_libraries")
        .expect("SHOW shared_preload_libraries must succeed")
        .unwrap_or_default();
    assert!(
        preload
            .split(',')
            .map(str::trim)
            .any(|lib| lib == "pg_fusion_host"),
        "pg_fusion_host must be in shared_preload_libraries, got: {preload}"
    );
}

fn wait_for_worker() {
    ensure_shared_preload();
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

fn enable_pg_fusion() {
    wait_for_worker();
    Spi::run("SET LOCAL pg_fusion.enable = on").expect("enable pg_fusion");
}

pub(crate) fn simple_select_smoke() {
    enable_pg_fusion();
    Spi::run("SELECT 1::bigint AS one").expect("simple smoke select must succeed");
}

pub(crate) fn explain_smoke() {
    enable_pg_fusion();
    Spi::run("EXPLAIN (FORMAT JSON) SELECT 1::bigint AS one").expect("smoke EXPLAIN must succeed");
}
