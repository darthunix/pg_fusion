use std::collections::VecDeque;

use arrow_schema::SchemaRef;
use issuance::{IssuancePool, IssueEvent, IssuedOwnedFrame, IssuedRx};
use pgrx::pg_sys;
use pgrx::PgMemoryContexts;
use pool::PagePool;
use slot_import::{ArrowSlotProjector, ProjectError};
use thiserror::Error;
use transfer::PageRx;

#[derive(Debug, Error)]
pub(crate) enum ResultIngressError {
    #[error("issued result frame failed: {0}")]
    Issued(#[from] issuance::IssuedRxError),
    #[error("result projector configuration failed: {0}")]
    ProjectConfig(#[from] slot_import::ConfigError),
    #[error("result projection failed: {0}")]
    Project(#[from] ProjectError),
    #[error("failed to materialize a minimal tuple from one projected row")]
    CopyMinimalTuple,
}

pub(crate) struct ResultIngress {
    rx: IssuedRx,
    _per_tuple_memory: PgMemoryContexts,
    queued_tuple_memory: PgMemoryContexts,
    projector: ArrowSlotProjector,
    project_slot: *mut pg_sys::TupleTableSlot,
    queued: VecDeque<OwnedMinimalTuple>,
    stream_closed: bool,
    execution_completed: bool,
}

impl ResultIngress {
    pub(crate) unsafe fn new(
        transport_schema: SchemaRef,
        tuple_desc: pg_sys::TupleDesc,
        page_pool: PagePool,
        issuance_pool: IssuancePool,
    ) -> Result<Self, ResultIngressError> {
        let per_tuple_memory = PgMemoryContexts::new("pg_fusion_result_ingress");
        let queued_tuple_memory = PgMemoryContexts::new("pg_fusion_result_queue");
        let projector =
            ArrowSlotProjector::new(transport_schema, tuple_desc, per_tuple_memory.value())?;
        let project_slot =
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsVirtual);
        Ok(Self {
            rx: IssuedRx::new(PageRx::new(page_pool), issuance_pool),
            _per_tuple_memory: per_tuple_memory,
            queued_tuple_memory,
            projector,
            project_slot,
            queued: VecDeque::new(),
            stream_closed: false,
            execution_completed: false,
        })
    }

    pub(crate) fn accept_frame(
        &mut self,
        frame: &IssuedOwnedFrame,
    ) -> Result<(), ResultIngressError> {
        match self.rx.accept(frame)? {
            IssueEvent::Page(page) => {
                let mut cursor = self.projector.open_owned_page(page)?;
                while let Some(slot) = unsafe { cursor.next_into_slot(self.project_slot) }? {
                    let tuple = unsafe {
                        PgMemoryContexts::For(self.queued_tuple_memory.value())
                            .switch_to(|_| pg_sys::ExecCopySlotMinimalTuple(slot))
                    };
                    if tuple.is_null() {
                        return Err(ResultIngressError::CopyMinimalTuple);
                    }
                    self.queued.push_back(OwnedMinimalTuple::new(tuple));
                }
            }
            IssueEvent::Closed => {
                self.stream_closed = true;
            }
        }
        Ok(())
    }

    pub(crate) fn mark_execution_complete(&mut self) {
        self.execution_completed = true;
    }

    pub(crate) fn store_next_into(
        &mut self,
        scan_slot: *mut pg_sys::TupleTableSlot,
    ) -> Option<*mut pg_sys::TupleTableSlot> {
        let tuple = self.queued.pop_front()?;
        let tuple = tuple.into_raw();
        unsafe { Some(pg_sys::ExecStoreMinimalTuple(tuple, scan_slot, true)) }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.stream_closed && self.execution_completed && self.queued.is_empty()
    }
}

impl Drop for ResultIngress {
    fn drop(&mut self) {
        unsafe {
            if !self.project_slot.is_null() {
                pg_sys::ExecDropSingleTupleTableSlot(self.project_slot);
            }
        }
    }
}

struct OwnedMinimalTuple {
    ptr: pg_sys::MinimalTuple,
}

impl OwnedMinimalTuple {
    fn new(ptr: pg_sys::MinimalTuple) -> Self {
        Self { ptr }
    }

    fn into_raw(mut self) -> pg_sys::MinimalTuple {
        let ptr = self.ptr;
        self.ptr = std::ptr::null_mut();
        ptr
    }
}

impl Drop for OwnedMinimalTuple {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { pg_sys::heap_free_minimal_tuple(self.ptr) };
        }
    }
}
