use std::alloc::{Layout, LayoutError};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug)]
pub struct TelemetryLayout {
    pub layout: Layout,
}

pub fn telemetry_layout(num_conns: usize) -> Result<TelemetryLayout, LayoutError> {
    let layout = Layout::array::<ConnTelemetry>(num_conns)?;
    Ok(TelemetryLayout {
        layout: layout.pad_to_align(),
    })
}

pub unsafe fn telemetry_ptr(base: *mut u8, _layout: TelemetryLayout) -> *mut ConnTelemetry {
    base as *mut ConnTelemetry
}

static TELEMETRY_BASE: AtomicPtr<ConnTelemetry> = AtomicPtr::new(std::ptr::null_mut());
static TELEMETRY_LEN: OnceLock<usize> = OnceLock::new();
static PROBE_ENABLED: OnceLock<bool> = OnceLock::new();

#[repr(C, align(64))]
pub struct ConnTelemetry {
    pub epoch: AtomicU64,
    pub backend_wait_count: AtomicU64,
    pub backend_wait_ns: AtomicU64,
    pub backend_wait_timeout_count: AtomicU64,
    pub backend_heap_serve_count: AtomicU64,
    pub backend_heap_serve_ns: AtomicU64,
    pub backend_result_read_count: AtomicU64,
    pub backend_result_read_ns: AtomicU64,
    pub backend_page_publish_seq: AtomicU64,
    pub backend_page_publish_ns: AtomicU64,
    pub backend_to_worker_page_count: AtomicU64,
    pub backend_to_worker_page_ns: AtomicU64,
    pub worker_poll_wait_count: AtomicU64,
    pub worker_poll_wait_ns: AtomicU64,
    pub worker_heap_msg_count: AtomicU64,
    pub worker_heap_msg_ns: AtomicU64,
    pub worker_heap_copy_count: AtomicU64,
    pub worker_heap_copy_ns: AtomicU64,
    pub worker_heap_tx_send_count: AtomicU64,
    pub worker_heap_tx_send_ns: AtomicU64,
    pub worker_request_next_count: AtomicU64,
    pub worker_request_next_ns: AtomicU64,
    pub worker_heap_request_seq: AtomicU64,
    pub worker_heap_request_ns: AtomicU64,
    pub worker_to_backend_req_count: AtomicU64,
    pub worker_to_backend_req_ns: AtomicU64,
    pub worker_pgscan_wait_count: AtomicU64,
    pub worker_pgscan_wait_ns: AtomicU64,
    pub worker_pgscan_decode_count: AtomicU64,
    pub worker_pgscan_decode_ns: AtomicU64,
    pub worker_result_write_count: AtomicU64,
    pub worker_result_write_ns: AtomicU64,
    pub worker_result_signal_seq: AtomicU64,
    pub worker_result_signal_ns: AtomicU64,
    pub worker_to_backend_result_count: AtomicU64,
    pub worker_to_backend_result_ns: AtomicU64,
}

impl ConnTelemetry {
    #[inline]
    fn clear_counter(counter: &AtomicU64) {
        counter.store(0, Ordering::Relaxed);
    }

    pub fn reset_for_new_epoch(&self) -> u64 {
        let next_epoch = self.epoch.load(Ordering::Relaxed).saturating_add(1).max(1);
        Self::clear_counter(&self.backend_wait_count);
        Self::clear_counter(&self.backend_wait_ns);
        Self::clear_counter(&self.backend_wait_timeout_count);
        Self::clear_counter(&self.backend_heap_serve_count);
        Self::clear_counter(&self.backend_heap_serve_ns);
        Self::clear_counter(&self.backend_result_read_count);
        Self::clear_counter(&self.backend_result_read_ns);
        Self::clear_counter(&self.backend_page_publish_seq);
        Self::clear_counter(&self.backend_page_publish_ns);
        Self::clear_counter(&self.backend_to_worker_page_count);
        Self::clear_counter(&self.backend_to_worker_page_ns);
        Self::clear_counter(&self.worker_poll_wait_count);
        Self::clear_counter(&self.worker_poll_wait_ns);
        Self::clear_counter(&self.worker_heap_msg_count);
        Self::clear_counter(&self.worker_heap_msg_ns);
        Self::clear_counter(&self.worker_heap_copy_count);
        Self::clear_counter(&self.worker_heap_copy_ns);
        Self::clear_counter(&self.worker_heap_tx_send_count);
        Self::clear_counter(&self.worker_heap_tx_send_ns);
        Self::clear_counter(&self.worker_request_next_count);
        Self::clear_counter(&self.worker_request_next_ns);
        Self::clear_counter(&self.worker_heap_request_seq);
        Self::clear_counter(&self.worker_heap_request_ns);
        Self::clear_counter(&self.worker_to_backend_req_count);
        Self::clear_counter(&self.worker_to_backend_req_ns);
        Self::clear_counter(&self.worker_pgscan_wait_count);
        Self::clear_counter(&self.worker_pgscan_wait_ns);
        Self::clear_counter(&self.worker_pgscan_decode_count);
        Self::clear_counter(&self.worker_pgscan_decode_ns);
        Self::clear_counter(&self.worker_result_write_count);
        Self::clear_counter(&self.worker_result_write_ns);
        Self::clear_counter(&self.worker_result_signal_seq);
        Self::clear_counter(&self.worker_result_signal_ns);
        Self::clear_counter(&self.worker_to_backend_result_count);
        Self::clear_counter(&self.worker_to_backend_result_ns);
        self.epoch.store(next_epoch, Ordering::Release);
        next_epoch
    }

    #[inline]
    fn record_pair(count: &AtomicU64, total_ns: &AtomicU64, delta_ns: u64) {
        count.fetch_add(1, Ordering::Relaxed);
        total_ns.fetch_add(delta_ns, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_backend_wait(&self, delta_ns: u64, timed_out: bool) {
        Self::record_pair(&self.backend_wait_count, &self.backend_wait_ns, delta_ns);
        if timed_out {
            self.backend_wait_timeout_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_backend_heap_serve(&self, delta_ns: u64) {
        Self::record_pair(
            &self.backend_heap_serve_count,
            &self.backend_heap_serve_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_backend_result_read(&self, delta_ns: u64) {
        Self::record_pair(
            &self.backend_result_read_count,
            &self.backend_result_read_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn publish_backend_page(&self, now_ns: u64) {
        self.backend_page_publish_ns
            .store(now_ns, Ordering::Relaxed);
        let next_seq = self
            .backend_page_publish_seq
            .load(Ordering::Relaxed)
            .saturating_add(1);
        self.backend_page_publish_seq
            .store(next_seq, Ordering::Release);
    }

    #[inline]
    pub fn observe_backend_page_publish(&self, last_seen_seq: &mut u64, now_ns: u64) {
        let seq = self.backend_page_publish_seq.load(Ordering::Acquire);
        if seq == 0 || seq == *last_seen_seq {
            return;
        }
        let sent_ns = self.backend_page_publish_ns.load(Ordering::Relaxed);
        if sent_ns > 0 {
            Self::record_pair(
                &self.backend_to_worker_page_count,
                &self.backend_to_worker_page_ns,
                now_ns.saturating_sub(sent_ns),
            );
        }
        *last_seen_seq = seq;
    }

    #[inline]
    pub fn record_worker_poll_wait(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_poll_wait_count,
            &self.worker_poll_wait_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_heap_msg(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_heap_msg_count,
            &self.worker_heap_msg_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_heap_copy(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_heap_copy_count,
            &self.worker_heap_copy_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_heap_tx_send(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_heap_tx_send_count,
            &self.worker_heap_tx_send_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_request_next(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_request_next_count,
            &self.worker_request_next_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn publish_worker_heap_request(&self, now_ns: u64) {
        self.worker_heap_request_ns.store(now_ns, Ordering::Relaxed);
        let next_seq = self
            .worker_heap_request_seq
            .load(Ordering::Relaxed)
            .saturating_add(1);
        self.worker_heap_request_seq
            .store(next_seq, Ordering::Release);
    }

    #[inline]
    pub fn observe_worker_heap_request(&self, last_seen_seq: &mut u64, now_ns: u64) {
        let seq = self.worker_heap_request_seq.load(Ordering::Acquire);
        if seq == 0 || seq == *last_seen_seq {
            return;
        }
        let sent_ns = self.worker_heap_request_ns.load(Ordering::Relaxed);
        if sent_ns > 0 {
            Self::record_pair(
                &self.worker_to_backend_req_count,
                &self.worker_to_backend_req_ns,
                now_ns.saturating_sub(sent_ns),
            );
        }
        *last_seen_seq = seq;
    }

    #[inline]
    pub fn record_worker_pgscan_wait(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_pgscan_wait_count,
            &self.worker_pgscan_wait_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_pgscan_decode(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_pgscan_decode_count,
            &self.worker_pgscan_decode_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn record_worker_result_write(&self, delta_ns: u64) {
        Self::record_pair(
            &self.worker_result_write_count,
            &self.worker_result_write_ns,
            delta_ns,
        );
    }

    #[inline]
    pub fn publish_worker_result_signal(&self, now_ns: u64) {
        self.worker_result_signal_ns
            .store(now_ns, Ordering::Relaxed);
        let next_seq = self
            .worker_result_signal_seq
            .load(Ordering::Relaxed)
            .saturating_add(1);
        self.worker_result_signal_seq
            .store(next_seq, Ordering::Release);
    }

    #[inline]
    pub fn observe_worker_result_signal(&self, last_seen_seq: &mut u64, now_ns: u64) {
        let seq = self.worker_result_signal_seq.load(Ordering::Acquire);
        if seq == 0 || seq == *last_seen_seq {
            return;
        }
        let sent_ns = self.worker_result_signal_ns.load(Ordering::Relaxed);
        if sent_ns > 0 {
            Self::record_pair(
                &self.worker_to_backend_result_count,
                &self.worker_to_backend_result_ns,
                now_ns.saturating_sub(sent_ns),
            );
        }
        *last_seen_seq = seq;
    }

    pub fn snapshot(&self) -> ProbeSnapshot {
        ProbeSnapshot {
            epoch: self.epoch.load(Ordering::Acquire),
            backend_wait_count: self.backend_wait_count.load(Ordering::Relaxed),
            backend_wait_ns: self.backend_wait_ns.load(Ordering::Relaxed),
            backend_wait_timeout_count: self.backend_wait_timeout_count.load(Ordering::Relaxed),
            backend_heap_serve_count: self.backend_heap_serve_count.load(Ordering::Relaxed),
            backend_heap_serve_ns: self.backend_heap_serve_ns.load(Ordering::Relaxed),
            backend_result_read_count: self.backend_result_read_count.load(Ordering::Relaxed),
            backend_result_read_ns: self.backend_result_read_ns.load(Ordering::Relaxed),
            backend_to_worker_page_count: self.backend_to_worker_page_count.load(Ordering::Relaxed),
            backend_to_worker_page_ns: self.backend_to_worker_page_ns.load(Ordering::Relaxed),
            worker_poll_wait_count: self.worker_poll_wait_count.load(Ordering::Relaxed),
            worker_poll_wait_ns: self.worker_poll_wait_ns.load(Ordering::Relaxed),
            worker_heap_msg_count: self.worker_heap_msg_count.load(Ordering::Relaxed),
            worker_heap_msg_ns: self.worker_heap_msg_ns.load(Ordering::Relaxed),
            worker_heap_copy_count: self.worker_heap_copy_count.load(Ordering::Relaxed),
            worker_heap_copy_ns: self.worker_heap_copy_ns.load(Ordering::Relaxed),
            worker_heap_tx_send_count: self.worker_heap_tx_send_count.load(Ordering::Relaxed),
            worker_heap_tx_send_ns: self.worker_heap_tx_send_ns.load(Ordering::Relaxed),
            worker_request_next_count: self.worker_request_next_count.load(Ordering::Relaxed),
            worker_request_next_ns: self.worker_request_next_ns.load(Ordering::Relaxed),
            worker_to_backend_req_count: self.worker_to_backend_req_count.load(Ordering::Relaxed),
            worker_to_backend_req_ns: self.worker_to_backend_req_ns.load(Ordering::Relaxed),
            worker_pgscan_wait_count: self.worker_pgscan_wait_count.load(Ordering::Relaxed),
            worker_pgscan_wait_ns: self.worker_pgscan_wait_ns.load(Ordering::Relaxed),
            worker_pgscan_decode_count: self.worker_pgscan_decode_count.load(Ordering::Relaxed),
            worker_pgscan_decode_ns: self.worker_pgscan_decode_ns.load(Ordering::Relaxed),
            worker_result_write_count: self.worker_result_write_count.load(Ordering::Relaxed),
            worker_result_write_ns: self.worker_result_write_ns.load(Ordering::Relaxed),
            worker_to_backend_result_count: self
                .worker_to_backend_result_count
                .load(Ordering::Relaxed),
            worker_to_backend_result_ns: self.worker_to_backend_result_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProbeSnapshot {
    pub epoch: u64,
    pub backend_wait_count: u64,
    pub backend_wait_ns: u64,
    pub backend_wait_timeout_count: u64,
    pub backend_heap_serve_count: u64,
    pub backend_heap_serve_ns: u64,
    pub backend_result_read_count: u64,
    pub backend_result_read_ns: u64,
    pub backend_to_worker_page_count: u64,
    pub backend_to_worker_page_ns: u64,
    pub worker_poll_wait_count: u64,
    pub worker_poll_wait_ns: u64,
    pub worker_heap_msg_count: u64,
    pub worker_heap_msg_ns: u64,
    pub worker_heap_copy_count: u64,
    pub worker_heap_copy_ns: u64,
    pub worker_heap_tx_send_count: u64,
    pub worker_heap_tx_send_ns: u64,
    pub worker_request_next_count: u64,
    pub worker_request_next_ns: u64,
    pub worker_to_backend_req_count: u64,
    pub worker_to_backend_req_ns: u64,
    pub worker_pgscan_wait_count: u64,
    pub worker_pgscan_wait_ns: u64,
    pub worker_pgscan_decode_count: u64,
    pub worker_pgscan_decode_ns: u64,
    pub worker_result_write_count: u64,
    pub worker_result_write_ns: u64,
    pub worker_to_backend_result_count: u64,
    pub worker_to_backend_result_ns: u64,
}

impl ProbeSnapshot {
    pub fn total_ms(ns: u64) -> f64 {
        ns as f64 / 1_000_000.0
    }

    pub fn avg_us(total_ns: u64, count: u64) -> f64 {
        if count == 0 {
            0.0
        } else {
            (total_ns as f64 / count as f64) / 1_000.0
        }
    }
}

pub fn set_telemetry(base: *mut u8, num_conns: usize) {
    TELEMETRY_BASE.store(base.cast::<ConnTelemetry>(), Ordering::Release);
    let _ = TELEMETRY_LEN.set(num_conns);
}

pub fn try_conn_telemetry(conn_id: usize) -> Option<&'static ConnTelemetry> {
    let base = TELEMETRY_BASE.load(Ordering::Acquire);
    let len = TELEMETRY_LEN.get().copied().unwrap_or(0);
    if base.is_null() || conn_id >= len {
        return None;
    }
    Some(unsafe { &*base.add(conn_id) })
}

pub fn conn_telemetry(conn_id: usize) -> &'static ConnTelemetry {
    try_conn_telemetry(conn_id).expect("connection telemetry not initialized")
}

#[inline]
pub fn probe_enabled() -> bool {
    *PROBE_ENABLED.get_or_init(|| {
        std::env::var("PG_FUSION_PROBE")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false)
    })
}

#[inline]
pub fn monotonic_now_ns() -> u64 {
    unsafe {
        let mut ts = std::mem::zeroed::<libc::timespec>();
        #[cfg(target_os = "macos")]
        let clock_id = libc::CLOCK_UPTIME_RAW;
        #[cfg(not(target_os = "macos"))]
        let clock_id = libc::CLOCK_MONOTONIC;
        let rc = libc::clock_gettime(clock_id, &mut ts);
        if rc != 0 {
            return 0;
        }
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }
}
