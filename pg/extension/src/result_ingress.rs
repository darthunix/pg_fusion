use arrow_schema::SchemaRef;
use issuance::{IssuancePool, IssueEvent, IssuedOwnedFrame, IssuedRx};
use pgrx::pg_sys;
use pgrx::PgMemoryContexts;
use pool::PagePool;
use slot_import::{ArrowSlotProjector, OwnedPageSlotCursor, ProjectError};
use thiserror::Error;
use transfer::PageRx;

use crate::diag;
use backend_service::DiagnosticLogLevel;

#[derive(Debug, Error)]
pub(crate) enum ResultIngressError {
    #[error("issued result frame failed: {0}")]
    Issued(#[from] issuance::IssuedRxError),
    #[error("result projector configuration failed: {0}")]
    ProjectConfig(#[from] slot_import::ConfigError),
    #[error("result projection failed: {0}")]
    Project(#[from] ProjectError),
    #[error("received a result page while the previous result page is still active")]
    ActivePageBusy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcceptedResultFrame {
    Page,
    Closed,
}

pub(crate) struct ResultIngress {
    rx: IssuedRx,
    per_tuple_memory: PgMemoryContexts,
    projector: ArrowSlotProjector,
    active_page: Option<OwnedPageSlotCursor>,
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
        let projector =
            ArrowSlotProjector::new(transport_schema, tuple_desc, per_tuple_memory.value())?;
        result_diag(|| {
            format!(
                "result_ingress init tuple_desc={:p} per_tuple_mcxt={:p}",
                tuple_desc,
                per_tuple_memory.value(),
            )
        });
        Ok(Self {
            rx: IssuedRx::new(PageRx::new(page_pool), issuance_pool),
            per_tuple_memory,
            projector,
            active_page: None,
            stream_closed: false,
            execution_completed: false,
        })
    }

    pub(crate) fn accept_frame(
        &mut self,
        frame: &IssuedOwnedFrame,
    ) -> Result<AcceptedResultFrame, ResultIngressError> {
        match self.rx.accept(frame)? {
            IssueEvent::Page(page) => {
                if self.active_page.is_some() {
                    return Err(ResultIngressError::ActivePageBusy);
                }
                result_diag(|| {
                    format!(
                        "result_ingress accept page active_page_before={}",
                        self.active_page.is_some(),
                    )
                });
                self.active_page = Some(self.projector.open_owned_cursor(page)?);
                result_diag(|| "result_ingress activated one result page".to_string());
                Ok(AcceptedResultFrame::Page)
            }
            IssueEvent::Closed => {
                self.stream_closed = true;
                result_diag(|| {
                    format!(
                        "result_ingress observed stream close active_page={} execution_completed={}",
                        self.active_page.is_some(),
                        self.execution_completed,
                    )
                });
                Ok(AcceptedResultFrame::Closed)
            }
        }
    }

    pub(crate) fn mark_execution_complete(&mut self) {
        self.execution_completed = true;
        result_diag(|| {
            format!(
                "result_ingress marked execution complete active_page={} stream_closed={}",
                self.active_page.is_some(),
                self.stream_closed,
            )
        });
    }

    pub(crate) fn store_next_into(
        &mut self,
        scan_slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<Option<*mut pg_sys::TupleTableSlot>, ResultIngressError> {
        result_diag(|| {
            format!(
                "result_ingress store_next_into start active_page={} scan_slot={}",
                self.active_page.is_some(),
                slot_snapshot(scan_slot),
            )
        });
        let Some(cursor) = self.active_page.as_mut() else {
            return Ok(None);
        };
        let stored = unsafe {
            self.projector
                .next_cursor_row_into_slot(cursor, scan_slot)?
        };
        if let Some(stored) = stored {
            result_diag(|| {
                format!(
                    "result_ingress store_next_into projected row scan_slot_after={}",
                    slot_snapshot(scan_slot),
                )
            });
            Ok(Some(stored))
        } else {
            self.active_page = None;
            result_diag(|| "result_ingress released exhausted result page".to_string());
            Ok(None)
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.stream_closed && self.execution_completed && self.active_page.is_none()
    }

    pub(crate) fn debug_project_slot(&self) -> *mut pg_sys::TupleTableSlot {
        std::ptr::null_mut()
    }

    pub(crate) fn debug_front_queued_tuple(&self) -> pg_sys::MinimalTuple {
        std::ptr::null_mut()
    }

    pub(crate) fn debug_contexts(&self) -> (pg_sys::MemoryContext, pg_sys::MemoryContext) {
        (self.per_tuple_memory.value(), std::ptr::null_mut())
    }
}

impl Drop for ResultIngress {
    fn drop(&mut self) {
        if crate::logging::backend_log_enabled(DiagnosticLogLevel::Trace) {
            let (per_tuple_cxt, _queue_cxt) = self.debug_contexts();
            unsafe {
                diag::update_result_ingress_watch(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    per_tuple_cxt,
                    std::ptr::null_mut(),
                );
                diag::log_live_watch("result_ingress drop live watch");
            }
        }
        result_diag(|| {
            format!(
                "result_ingress drop active_page={} stream_closed={} execution_completed={}",
                self.active_page.is_some(),
                self.stream_closed,
                self.execution_completed,
            )
        });
    }
}

fn result_diag(message: impl FnOnce() -> String) {
    crate::logging::write_backend_log(
        DiagnosticLogLevel::Trace,
        "backend",
        "extension::result_ingress",
        message,
    );
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
    use pgrx::varlena::rust_str_to_text_p;
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
    pub(crate) unsafe fn minimal_tuple_queue_context_ownership_repro(
        tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), String> {
        let mut queue_memory = PgMemoryContexts::new("pg_fusion_result_queue_repro");
        let scan_slot =
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsMinimalTuple);
        if scan_slot.is_null() {
            return Err("MakeSingleTupleTableSlot(TTSOpsMinimalTuple) returned null".to_string());
        }

        let tuple = unsafe {
            queue_memory.switch_to(|_| {
                let text = rust_str_to_text_p("two");
                let mut values = [
                    pg_sys::Datum::from(2_i64),
                    pg_sys::Datum::from(text.as_ptr()),
                ];
                let mut nulls = [false, false];
                pg_sys::heap_form_minimal_tuple(tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr())
            })
        };
        if tuple.is_null() {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err("heap_form_minimal_tuple returned null".to_string());
        }

        unsafe { pg_sys::ExecStoreMinimalTuple(tuple, scan_slot, true) };
        let flags = unsafe { (*scan_slot).tts_flags as u32 };
        if flags & pg_sys::TTS_FLAG_SHOULDFREE == 0 {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err("ExecStoreMinimalTuple did not mark scan slot shouldFree".to_string());
        }

        let stored = unsafe { (*scan_slot.cast::<pg_sys::MinimalTupleTableSlot>()).mintuple };
        if stored != tuple {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err("scan slot did not retain the expected minimal tuple".to_string());
        }
        let chunk_context = unsafe { pg_sys::GetMemoryChunkContext(stored.cast()) };
        if chunk_context != queue_memory.value() {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err(format!(
                "stored tuple context mismatch: got {:p}, expected {:p}",
                chunk_context,
                queue_memory.value()
            ));
        }

        unsafe {
            pg_sys::ExecClearTuple(scan_slot);
            pg_sys::ExecDropSingleTupleTableSlot(scan_slot);
        }
        Ok(())
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
                Arc::new(Int64Array::from(vec![2_i64, 3_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["two", "three"])) as ArrayRef,
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
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsVirtual);
        if scan_slot.is_null() {
            return Err("MakeSingleTupleTableSlot(TTSOpsVirtual) returned null".to_string());
        }

        while let Some(step) = producer.next_step().map_err(|err| err.to_string())? {
            match step {
                ResultPageStep::OutboundPage(outbound) => {
                    let frame = outbound.frame();
                    let accepted = ingress
                        .accept_frame(&frame)
                        .map_err(|err| err.to_string())?;
                    if accepted != AcceptedResultFrame::Page {
                        return Err("expected outbound result page".to_string());
                    }
                    outbound.mark_sent();
                    if issuance_pool.snapshot().leased_permits != 1 {
                        return Err(
                            "accepted result page should keep one issuance permit".to_string()
                        );
                    }
                }
                ResultPageStep::CloseFrame(frame) => {
                    let accepted = ingress
                        .accept_frame(&frame)
                        .map_err(|err| err.to_string())?;
                    if accepted != AcceptedResultFrame::Closed {
                        return Err("expected result close frame".to_string());
                    }
                }
            }
        }
        ingress.mark_execution_complete();

        let mut observed = Vec::new();
        while let Some(slot) = ingress
            .store_next_into(scan_slot)
            .map_err(|err| err.to_string())?
        {
            if slot.is_null() {
                return Err("next_cursor_row_into_slot returned null slot".to_string());
            }
            let flags = unsafe { (*slot).tts_flags as u32 };
            if flags & pg_sys::TTS_FLAG_SHOULDFREE != 0 {
                return Err("virtual result slot unexpectedly owns a minimal tuple".to_string());
            }
            let slot_ref = unsafe { &*slot };
            let values = unsafe {
                std::slice::from_raw_parts(slot_ref.tts_values, slot_ref.tts_nvalid as usize)
            };
            observed.push(values[0].value() as i64);
            if issuance_pool.snapshot().leased_permits != 1 {
                return Err(
                    "active result page permit was released before slot was cleared".to_string(),
                );
            }
        }
        if observed != [2_i64, 3_i64] {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err(format!("unexpected result ids: {observed:?}"));
        }
        if !ingress.is_complete() {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err("result ingress should be complete after final cursor drain".to_string());
        }
        if issuance_pool.snapshot().leased_permits != 0 {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(scan_slot) };
            return Err("exhausted result page should release issuance permit".to_string());
        }

        pg_sys::ExecDropSingleTupleTableSlot(scan_slot);
        drop(ingress);
        Ok(())
    }
}
