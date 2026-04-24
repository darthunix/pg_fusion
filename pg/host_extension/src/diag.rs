use std::cell::RefCell;
use std::ffi::c_void;

use pgrx::pg_sys;

#[derive(Clone, Copy, Default)]
pub(crate) struct WatchSnapshot {
    pub query_desc: *mut pg_sys::QueryDesc,
    pub estate: *mut pg_sys::EState,
    pub custom_scan: *mut pg_sys::CustomScanState,
    pub es_query_cxt: pg_sys::MemoryContext,
    pub scan_slot: *mut pg_sys::TupleTableSlot,
    pub scan_tupdesc: pg_sys::TupleDesc,
    pub scan_mintuple: pg_sys::MinimalTuple,
    pub result_slot: *mut pg_sys::TupleTableSlot,
    pub result_tupdesc: pg_sys::TupleDesc,
    pub project_slot: *mut pg_sys::TupleTableSlot,
    pub project_tupdesc: pg_sys::TupleDesc,
    pub queued_tuple: pg_sys::MinimalTuple,
    pub ingress_per_tuple_cxt: pg_sys::MemoryContext,
    pub ingress_queue_cxt: pg_sys::MemoryContext,
}

#[repr(C)]
struct RegisteredMemoryContextCallback {
    callback: pg_sys::MemoryContextCallback,
    label: &'static str,
    context: pg_sys::MemoryContext,
}

thread_local! {
    static WATCH: RefCell<WatchSnapshot> = RefCell::new(WatchSnapshot::default());
}

pub(crate) unsafe fn update_executor_watch(
    query_desc: *mut pg_sys::QueryDesc,
    estate: *mut pg_sys::EState,
    custom_scan: *mut pg_sys::CustomScanState,
) {
    WATCH.with(|watch| {
        let mut watch = watch.borrow_mut();
        watch.query_desc = query_desc;
        watch.estate = estate;
        watch.custom_scan = custom_scan;
        watch.es_query_cxt = if estate.is_null() {
            std::ptr::null_mut()
        } else {
            (*estate).es_query_cxt
        };
    });
}

pub(crate) unsafe fn update_slot_watch(
    scan_slot: *mut pg_sys::TupleTableSlot,
    result_slot: *mut pg_sys::TupleTableSlot,
) {
    WATCH.with(|watch| {
        let mut watch = watch.borrow_mut();
        watch.scan_slot = scan_slot;
        watch.scan_tupdesc = if scan_slot.is_null() {
            std::ptr::null_mut()
        } else {
            (*scan_slot).tts_tupleDescriptor
        };
        watch.scan_mintuple = scan_mintuple(scan_slot);
        watch.result_slot = result_slot;
        watch.result_tupdesc = if result_slot.is_null() {
            std::ptr::null_mut()
        } else {
            (*result_slot).tts_tupleDescriptor
        };
    });
}

pub(crate) unsafe fn update_result_ingress_watch(
    project_slot: *mut pg_sys::TupleTableSlot,
    queued_tuple: pg_sys::MinimalTuple,
    per_tuple_cxt: pg_sys::MemoryContext,
    queue_cxt: pg_sys::MemoryContext,
) {
    WATCH.with(|watch| {
        let mut watch = watch.borrow_mut();
        watch.project_slot = project_slot;
        watch.project_tupdesc = if project_slot.is_null() {
            std::ptr::null_mut()
        } else {
            (*project_slot).tts_tupleDescriptor
        };
        watch.queued_tuple = queued_tuple;
        watch.ingress_per_tuple_cxt = per_tuple_cxt;
        watch.ingress_queue_cxt = queue_cxt;
    });
}

pub(crate) fn watch_snapshot() -> String {
    WATCH.with(|watch| format_watch(*watch.borrow()))
}

pub(crate) fn clear_watch() {
    WATCH.with(|watch| *watch.borrow_mut() = WatchSnapshot::default());
}

pub(crate) unsafe fn log_live_watch(label: &str) {
    let watch = WATCH.with(|watch| *watch.borrow());
    backend_diag(format!("{label} {}", format_watch(watch)));

    if !watch.scan_slot.is_null() {
        backend_diag(live_chunk_line("scan_slot", watch.scan_slot.cast()));
    }
    if !watch.scan_mintuple.is_null() {
        backend_diag(live_chunk_line("scan_mintuple", watch.scan_mintuple.cast()));
    }
    if !watch.scan_tupdesc.is_null() {
        backend_diag(live_tuple_desc_line("scan_tupdesc", watch.scan_tupdesc));
    }
    if !watch.result_slot.is_null() {
        backend_diag(live_chunk_line("result_slot", watch.result_slot.cast()));
    }
    if !watch.result_tupdesc.is_null() {
        backend_diag(live_tuple_desc_line("result_tupdesc", watch.result_tupdesc));
    }
    if !watch.project_slot.is_null() {
        backend_diag(live_chunk_line("project_slot", watch.project_slot.cast()));
    }
    if !watch.project_tupdesc.is_null() {
        backend_diag(live_tuple_desc_line(
            "project_tupdesc",
            watch.project_tupdesc,
        ));
    }
    if !watch.queued_tuple.is_null() {
        backend_diag(live_chunk_line("queued_tuple", watch.queued_tuple.cast()));
    }
}

pub(crate) unsafe fn register_context_callback(
    label: &'static str,
    context: pg_sys::MemoryContext,
) {
    if context.is_null() {
        return;
    }

    let mut record = Box::new(RegisteredMemoryContextCallback {
        callback: std::mem::zeroed(),
        label,
        context,
    });
    let arg = (&mut *record as *mut RegisteredMemoryContextCallback).cast::<c_void>();
    record.callback.func = Some(memory_context_callback);
    record.callback.arg = arg;
    record.callback.next = std::ptr::null_mut();
    pg_sys::MemoryContextRegisterResetCallback(context, &mut record.callback);
    backend_diag(format!(
        "registered memory context callback label={} context={:p} {}",
        label,
        context,
        watch_snapshot()
    ));
    let _ = Box::into_raw(record);
}

fn scan_mintuple(slot: *mut pg_sys::TupleTableSlot) -> pg_sys::MinimalTuple {
    if slot.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        if (*slot).tts_ops == &raw const pg_sys::TTSOpsMinimalTuple {
            let mslot = slot.cast::<pg_sys::MinimalTupleTableSlot>();
            (*mslot).mintuple
        } else {
            std::ptr::null_mut()
        }
    }
}

fn format_watch(watch: WatchSnapshot) -> String {
    format!(
        "query_desc={:p} estate={:p} custom_scan={:p} current_mcxt={:p} es_query_cxt={:p} scan_slot={:p} scan_tupdesc={:p} scan_mintuple={:p} result_slot={:p} result_tupdesc={:p} project_slot={:p} project_tupdesc={:p} queued_tuple={:p} ingress_per_tuple_cxt={:p} ingress_queue_cxt={:p}",
        watch.query_desc,
        watch.estate,
        watch.custom_scan,
        unsafe { pg_sys::CurrentMemoryContext },
        watch.es_query_cxt,
        watch.scan_slot,
        watch.scan_tupdesc,
        watch.scan_mintuple,
        watch.result_slot,
        watch.result_tupdesc,
        watch.project_slot,
        watch.project_tupdesc,
        watch.queued_tuple,
        watch.ingress_per_tuple_cxt,
        watch.ingress_queue_cxt,
    )
}

unsafe fn live_chunk_line(label: &str, ptr: *mut c_void) -> String {
    format!(
        "{} ptr={:p} chunk_ctx={:p} chunk_space={}",
        label,
        ptr,
        pg_sys::GetMemoryChunkContext(ptr),
        pg_sys::GetMemoryChunkSpace(ptr),
    )
}

unsafe fn live_tuple_desc_line(label: &str, desc: pg_sys::TupleDesc) -> String {
    format!(
        "{} ptr={:p} tdrefcount={} chunk_ctx={:p} chunk_space={}",
        label,
        desc,
        (*desc).tdrefcount,
        pg_sys::GetMemoryChunkContext(desc.cast()),
        pg_sys::GetMemoryChunkSpace(desc.cast()),
    )
}

pub(crate) fn backend_diag(message: String) {
    backend_diag_write_file(&message);
    #[cfg(test)]
    {
        let _ = message;
    }
}

fn backend_diag_write_file(message: &str) {
    #[cfg(not(test))]
    {
        use std::fs::OpenOptions;
        use std::io::Write;

        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/pg_fusion_backend.log")
        {
            let _ = writeln!(file, "pid={} {}", std::process::id(), message);
        }
    }
    #[cfg(test)]
    {
        let _ = message;
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn memory_context_callback(arg: *mut c_void) {
    let record = &*(arg.cast::<RegisteredMemoryContextCallback>());
    let message = format!(
        "memory context callback label={} context={:p} {}",
        record.label,
        record.context,
        watch_snapshot()
    );
    backend_diag_write_file(&message);
}
