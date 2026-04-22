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
        result_diag(format!(
            "result_ingress init tuple_desc={:p} per_tuple_mcxt={:p} queue_mcxt={:p} project_slot={}",
            tuple_desc,
            per_tuple_memory.value(),
            queued_tuple_memory.value(),
            slot_snapshot(project_slot),
        ));
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
                result_diag(format!(
                    "result_ingress accept page queued_len_before={} project_slot={}",
                    self.queued.len(),
                    slot_snapshot(self.project_slot),
                ));
                let mut cursor = self.projector.open_owned_page(page)?;
                while let Some(slot) = unsafe { cursor.next_into_slot(self.project_slot) }? {
                    result_diag(format!(
                        "result_ingress projected one virtual row project_slot={} slot={}",
                        slot_snapshot(self.project_slot),
                        slot_snapshot(slot),
                    ));
                    let tuple = unsafe {
                        PgMemoryContexts::For(self.queued_tuple_memory.value())
                            .switch_to(|_| pg_sys::ExecCopySlotMinimalTuple(slot))
                    };
                    if tuple.is_null() {
                        return Err(ResultIngressError::CopyMinimalTuple);
                    }
                    result_diag(format!(
                        "result_ingress copied minimal tuple {} from project_slot={}",
                        minimal_tuple_snapshot(tuple),
                        slot_snapshot(self.project_slot),
                    ));
                    self.queued.push_back(OwnedMinimalTuple::new(tuple));
                    result_diag(format!(
                        "result_ingress queued minimal tuple queued_len_after={} tuple={}",
                        self.queued.len(),
                        minimal_tuple_snapshot(tuple),
                    ));
                }
            }
            IssueEvent::Closed => {
                self.stream_closed = true;
                result_diag(format!(
                    "result_ingress observed stream close queued_len={} execution_completed={}",
                    self.queued.len(),
                    self.execution_completed,
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn mark_execution_complete(&mut self) {
        self.execution_completed = true;
        result_diag(format!(
            "result_ingress marked execution complete queued_len={} stream_closed={}",
            self.queued.len(),
            self.stream_closed,
        ));
    }

    pub(crate) fn store_next_into(
        &mut self,
        scan_slot: *mut pg_sys::TupleTableSlot,
    ) -> Option<*mut pg_sys::TupleTableSlot> {
        result_diag(format!(
            "result_ingress store_next_into start queued_len={} scan_slot={}",
            self.queued.len(),
            slot_snapshot(scan_slot),
        ));
        let tuple = self.queued.pop_front()?;
        let tuple_ptr = tuple.ptr;
        result_diag(format!(
            "result_ingress store_next_into dequeued tuple={} scan_slot_before={}",
            minimal_tuple_snapshot(tuple_ptr),
            slot_snapshot(scan_slot),
        ));
        let tuple = tuple.into_raw();
        let stored = unsafe { pg_sys::ExecStoreMinimalTuple(tuple, scan_slot, true) };
        result_diag(format!(
            "result_ingress store_next_into stored tuple={} scan_slot_after={}",
            minimal_tuple_snapshot(tuple),
            slot_snapshot(scan_slot),
        ));
        Some(stored)
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.stream_closed && self.execution_completed && self.queued.is_empty()
    }
}

impl Drop for ResultIngress {
    fn drop(&mut self) {
        result_diag(format!(
            "result_ingress drop queued_len={} stream_closed={} execution_completed={} project_slot={}",
            self.queued.len(),
            self.stream_closed,
            self.execution_completed,
            slot_snapshot(self.project_slot),
        ));
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
            result_diag(format!(
                "result_ingress dropping owned minimal tuple {}",
                minimal_tuple_snapshot(self.ptr),
            ));
            unsafe { pg_sys::heap_free_minimal_tuple(self.ptr) };
        }
    }
}

fn result_diag(message: String) {
    result_diag_write_file(&message);
    #[cfg(not(test))]
    {
        pgrx::warning!("{message}");
    }
    #[cfg(test)]
    {
        let _ = message;
    }
}

fn result_diag_write_file(message: &str) {
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

fn slot_snapshot(slot: *mut pg_sys::TupleTableSlot) -> String {
    if slot.is_null() {
        return "slot=null".to_string();
    }

    unsafe {
        let flags = (*slot).tts_flags as u32;
        let should_free = (flags & pg_sys::TTS_FLAG_SHOULDFREE) != 0;
        let empty = (flags & pg_sys::TTS_FLAG_EMPTY) != 0;
        let ops = if (*slot).tts_ops == &raw const pg_sys::TTSOpsMinimalTuple {
            "minimal"
        } else if (*slot).tts_ops == &raw const pg_sys::TTSOpsVirtual {
            "virtual"
        } else {
            "other"
        };
        let slot_specific = if (*slot).tts_ops == &raw const pg_sys::TTSOpsMinimalTuple {
            let mslot = slot.cast::<pg_sys::MinimalTupleTableSlot>();
            format!(" mintuple={:p}", (*mslot).mintuple)
        } else if (*slot).tts_ops == &raw const pg_sys::TTSOpsVirtual {
            let vslot = slot.cast::<pg_sys::VirtualTupleTableSlot>();
            format!(" data={:p}", (*vslot).data)
        } else {
            String::new()
        };
        format!(
            "slot={:p} ops={} flags=0x{:x} should_free={} empty={} nvalid={} tupdesc={:p} mcxt={:p}{}",
            slot,
            ops,
            flags,
            should_free,
            empty,
            (*slot).tts_nvalid,
            (*slot).tts_tupleDescriptor,
            (*slot).tts_mcxt,
            slot_specific,
        )
    }
}

fn minimal_tuple_snapshot(tuple: pg_sys::MinimalTuple) -> String {
    if tuple.is_null() {
        return "tuple=null".to_string();
    }

    unsafe { format!("tuple={:p} t_len={}", tuple, (*tuple).t_len) }
}

#[cfg(feature = "pg_test")]
#[allow(dead_code)]
pub(crate) mod debug_repro {
    use super::*;
    use std::alloc::{alloc_zeroed, dealloc, Layout};
    use std::pin::Pin;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
    use datafusion_common::Result as DFResult;
    use futures::Stream;
    use issuance::{IssuanceConfig, IssuancePool, IssuedTx};
    use pool::{PagePool, PagePoolConfig};
    use transfer::PageTx;
    use worker_runtime::{ResultPageProducer, ResultPageProducerConfig, ResultPageStep};

    #[derive(Debug)]
    struct TestStream {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Stream for TestStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.batches.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Ok(self.batches.remove(0))))
            }
        }
    }

    impl RecordBatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    struct OwnedRegion {
        base: NonNull<u8>,
        layout: Layout,
    }

    impl OwnedRegion {
        fn from_layout(layout: Layout) -> Self {
            let ptr = unsafe { alloc_zeroed(layout) };
            let base = NonNull::new(ptr).expect("allocation must succeed");
            Self { base, layout }
        }
    }

    impl Drop for OwnedRegion {
        fn drop(&mut self) {
            unsafe { dealloc(self.base.as_ptr(), self.layout) };
        }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn single_page_result_ingress_roundtrip(
        tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["two"])) as ArrayRef,
            ],
        )
        .map_err(|err| err.to_string())?;

        let stream: SendableRecordBatchStream = Box::pin(TestStream {
            schema: Arc::clone(&schema),
            batches: vec![batch],
        });

        let page_cfg = PagePoolConfig::new(8192, 4).map_err(|err| err.to_string())?;
        let page_layout = PagePool::layout(page_cfg).map_err(|err| err.to_string())?;
        let page_region = OwnedRegion::from_layout(
            Layout::from_size_align(page_layout.size, page_layout.align).unwrap(),
        );
        let page_pool = PagePool::init_in_place(page_region.base, page_layout.size, page_cfg)
            .map_err(|err| err.to_string())?;

        let issuance_cfg = IssuanceConfig::new(4).map_err(|err| err.to_string())?;
        let issuance_layout = IssuancePool::layout(issuance_cfg).map_err(|err| err.to_string())?;
        let issuance_region = OwnedRegion::from_layout(
            Layout::from_size_align(issuance_layout.size, issuance_layout.align).unwrap(),
        );
        let issuance_pool =
            IssuancePool::init_in_place(issuance_region.base, issuance_layout.size, issuance_cfg)
                .map_err(|err| err.to_string())?;

        let page_tx = PageTx::new(page_pool);
        let payload_capacity = u32::try_from(page_tx.payload_capacity())
            .map_err(|_| "payload capacity exceeds u32".to_string())?;
        let tx = IssuedTx::new(page_tx, issuance_pool);

        let mut producer = ResultPageProducer::new(
            stream,
            tx,
            payload_capacity,
            ResultPageProducerConfig::default(),
        )
        .map_err(|err| err.to_string())?;
        let transport_schema = producer.transport_schema();
        let mut ingress =
            ResultIngress::new(transport_schema, tuple_desc, page_pool, issuance_pool)
                .map_err(|err| err.to_string())?;
        let scan_slot =
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsMinimalTuple);
        if scan_slot.is_null() {
            return Err("MakeSingleTupleTableSlot(TTSOpsMinimalTuple) returned null".to_string());
        }

        while let Some(step) = producer.next_step().map_err(|err| err.to_string())? {
            match step {
                ResultPageStep::OutboundPage(outbound) => {
                    let frame = outbound.frame();
                    ingress
                        .accept_frame(&frame)
                        .map_err(|err| err.to_string())?;
                    outbound.mark_sent();
                }
                ResultPageStep::CloseFrame(frame) => {
                    ingress
                        .accept_frame(&frame)
                        .map_err(|err| err.to_string())?;
                }
            }
        }
        ingress.mark_execution_complete();

        let slot = ingress
            .store_next_into(scan_slot)
            .ok_or_else(|| "expected one stored tuple".to_string())?;
        if slot.is_null() {
            return Err("ExecStoreMinimalTuple returned null slot".to_string());
        }
        result_diag(format!(
            "result_ingress debug_repro clearing stored scan slot {}",
            slot_snapshot(scan_slot),
        ));
        pg_sys::ExecClearTuple(scan_slot);
        pg_sys::ExecDropSingleTupleTableSlot(scan_slot);
        drop(ingress);
        Ok(())
    }
}
