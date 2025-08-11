use crate::worker::treiber_stack;
use anyhow::Result as AnyResult;
use libc::c_long;
use pgrx::pg_sys::{
    error, fetch_search_path_array, get_namespace_oid, get_relname_relid, palloc0,
    CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState, ExplainPropertyText,
    ExplainState, InvalidOid, List, ListCell, MyLatch, MyProcNumber, Node, NodeTag, Oid,
    ParamExternData, ParamListInfo, RegisterCustomScanMethods, ResetLatch, TupleTableSlot,
    WaitLatch, PG_WAIT_EXTENSION, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::{check_for_interrupts, pg_guard, warning};
use rmp::decode::{read_array_len, read_bin_len};
use smallvec::SmallVec;
use std::cell::OnceCell;
use std::ffi::c_char;
use std::time::Duration;

thread_local! {
    static SCAN_METHODS: CustomScanMethods = CustomScanMethods {
        CustomName: b"DataFusionScan\0".as_ptr() as *const c_char,
        CreateCustomScanState: Some(create_df_scan_state),
    };
    static EXEC_METHODS: CustomExecMethods = CustomExecMethods {
        CustomName: b"DataFusionScan\0".as_ptr() as *const c_char,
        BeginCustomScan: Some(begin_df_scan),
        ExecCustomScan: Some(exec_df_scan),
        EndCustomScan: Some(end_df_scan),
        ReScanCustomScan: None,
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(explain_df_scan),
    };
}

#[pg_guard]
#[no_mangle]
pub(crate) extern "C" fn init_datafusion_methods() {
    unsafe {
        RegisterCustomScanMethods(scan_methods());
    }
}

pub(crate) fn scan_methods() -> *const CustomScanMethods {
    SCAN_METHODS.with(|m| &*m as *const CustomScanMethods)
}

pub(crate) fn exec_methods() -> *const CustomExecMethods {
    EXEC_METHODS.with(|m| &*m as *const CustomExecMethods)
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn create_df_scan_state(cscan: *mut CustomScan) -> *mut Node {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn begin_df_scan(node: *mut CustomScanState, estate: *mut EState, eflags: i32) {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn exec_df_scan(node: *mut CustomScanState) -> *mut TupleTableSlot {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn end_df_scan(node: *mut CustomScanState) {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn explain_df_scan(
    _node: *mut CustomScanState,
    _ancestors: *mut List,
    es: *mut ExplainState,
) {
    todo!()
}

fn wait_latch(timeout: Option<Duration>) {
    let timeout: c_long = timeout
        .map(|t| t.as_millis().try_into().unwrap())
        .unwrap_or(0);
    let events = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
    let rc = unsafe {
        let rc = WaitLatch(MyLatch, events as i32, timeout, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        rc
    };
    check_for_interrupts!();
    if rc & WL_TIMEOUT as i32 != 0 {
        error!("Waiting latch timeout exceeded");
    } else if rc & WL_POSTMASTER_DEATH as i32 != 0 {
        panic!("Postmaster is dead");
    }
}

pub(crate) struct PgNode {
    size: usize,
    node: *mut Node,
}

impl PgNode {
    pub(crate) fn empty(data_size: usize) -> Self {
        let size = std::mem::size_of::<Node>() + data_size;
        let buf_ptr = unsafe { palloc0(size) };
        PgNode {
            size,
            node: buf_ptr as *mut Node,
        }
    }

    pub(crate) fn set_tag(&mut self, tag: NodeTag) {
        unsafe {
            (*self.node).type_ = tag;
        }
    }

    pub(crate) fn set_data(&mut self, data: &[u8]) {
        assert!(data.len() <= self.size - std::mem::size_of::<Node>());
        unsafe {
            let buf = std::slice::from_raw_parts_mut(self.node as *mut u8, self.size);
            buf[std::mem::size_of::<Node>()..].copy_from_slice(data);
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        unsafe {
            let buf = std::slice::from_raw_parts(self.node as *const u8, self.size);
            &buf[std::mem::size_of::<Node>()..]
        }
    }

    pub(crate) fn mut_node(&self) -> *mut Node {
        self.node
    }
}

fn list_nth(list: *mut List, n: i32) -> *mut ListCell {
    assert_ne!(list, std::ptr::null_mut());
    unsafe {
        assert!(n >= 0 && n < (*list).length);
        (*list).elements.offset(n as isize)
    }
}

/// RAII guard that acquires a free connection id from the global Treiber stack
/// and returns it back on drop.
struct ConnectionHandle(u32);

impl ConnectionHandle {
    fn acquire() -> AnyResult<Self> {
        match treiber_stack().allocate() {
            Ok(id) => Ok(Self(id)),
            Err(e) => anyhow::bail!("No free connection slots: {e:?}"),
        }
    }

    #[inline]
    fn id(&self) -> u32 {
        self.0
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        if let Err(e) = treiber_stack().release(self.0) {
            warning!("Failed to release connection slot {}: {:?}", self.0, e);
        }
    }
}

// Thread-local, one-per-backend handle that returns to the stack on thread exit.
thread_local! {
    static CONNECTION_HANDLE: OnceCell<ConnectionHandle> = OnceCell::new();
}

/// Ensure the per-backend ConnectionHandle is initialized exactly once.
fn acuire_connection() -> AnyResult<()> {
    CONNECTION_HANDLE.with(|cell| {
        if cell.get().is_none() {
            let handle = ConnectionHandle::acquire()?;
            // Ignore error if already set by a race in the same thread (unlikely)
            let _ = cell.set(handle);
        }
        Ok(())
    })
}

/// Get the current backend's connection id, initializing lazily on first use.
fn connection_id() -> AnyResult<u32> {
    CONNECTION_HANDLE.with(|cell| {
        if cell.get().is_none() {
            let handle = ConnectionHandle::acquire()?;
            let _ = cell.set(handle);
        }
        Ok(cell.get().unwrap().id())
    })
}
