use crate::error::{NotifyError, RxError, TxError};
use crate::process::signal_pid_usr1;
use crate::CommitOutcome;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

pub(crate) const FRAME_PREFIX_LEN: usize = std::mem::size_of::<u32>();
pub(crate) const MIN_RING_CAPACITY: usize = FRAME_PREFIX_LEN + 1;

#[derive(Clone, Copy, Debug)]
pub(crate) struct FramedRingLayout {
    pub(crate) layout: std::alloc::Layout,
    pub(crate) head_offset: usize,
    pub(crate) tail_offset: usize,
    pub(crate) data_offset: usize,
    pub(crate) data_len: usize,
}

pub(crate) fn framed_ring_layout(capacity: usize) -> Result<FramedRingLayout, crate::ConfigError> {
    if capacity < MIN_RING_CAPACITY {
        return Err(crate::ConfigError::RingTooSmall {
            minimum: MIN_RING_CAPACITY,
            actual: capacity,
        });
    }
    if capacity > (u32::MAX as usize) {
        return Err(crate::ConfigError::RingTooLarge {
            maximum: u32::MAX as usize,
            actual: capacity,
        });
    }

    let head = std::alloc::Layout::new::<AtomicU32>();
    let tail = std::alloc::Layout::new::<AtomicU32>();
    let (ht, tail_offset) = head
        .extend(tail)
        .map_err(|_| crate::ConfigError::LayoutOverflow)?;
    let data = std::alloc::Layout::array::<u8>(capacity)
        .map_err(|_| crate::ConfigError::LayoutOverflow)?;
    let (combined, data_offset) = ht
        .extend(data)
        .map_err(|_| crate::ConfigError::LayoutOverflow)?;

    Ok(FramedRingLayout {
        layout: combined.pad_to_align(),
        head_offset: 0,
        tail_offset,
        data_offset,
        data_len: capacity,
    })
}

#[derive(Clone, Copy)]
pub(crate) struct FramedRing<'a> {
    head: &'a AtomicU32,
    tail: &'a AtomicU32,
    data: NonNull<u8>,
    capacity: NonZeroU32,
    _marker: PhantomData<&'a mut [u8]>,
}

#[derive(Clone, Copy)]
struct RingSnapshot {
    head: u32,
    tail: u32,
    capacity: u32,
}

impl RingSnapshot {
    fn is_empty(self) -> bool {
        self.head == self.tail
    }

    fn used_bytes(self) -> usize {
        if self.tail >= self.head {
            (self.tail - self.head) as usize
        } else {
            (self.capacity - (self.head - self.tail)) as usize
        }
    }

    fn available_bytes(self) -> usize {
        self.capacity as usize - self.used_bytes() - 1
    }
}

#[derive(Clone, Copy)]
struct PublishPlan {
    payload_start: u32,
    publish_tail: u32,
    prefix_index: u32,
    prefix_value: u32,
}

#[derive(Clone, Copy)]
struct ConsumePlan {
    len: u32,
    payload_start: u32,
    next_head: u32,
}

impl<'a> FramedRing<'a> {
    pub(crate) unsafe fn init_empty_in_place(base: *mut u8, layout: FramedRingLayout) {
        std::ptr::write(
            base.add(layout.head_offset).cast::<AtomicU32>(),
            AtomicU32::new(0),
        );
        std::ptr::write(
            base.add(layout.tail_offset).cast::<AtomicU32>(),
            AtomicU32::new(0),
        );
    }

    pub(crate) unsafe fn from_layout(base: *mut u8, layout: FramedRingLayout) -> Self {
        let head = &*(base.add(layout.head_offset) as *const AtomicU32);
        let tail = &*(base.add(layout.tail_offset) as *const AtomicU32);
        let data = NonNull::new_unchecked(base.add(layout.data_offset));
        Self {
            head,
            tail,
            data,
            capacity: NonZeroU32::new_unchecked(layout.data_len as u32),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn clear(&self) {
        let tail = self.tail.load(Ordering::Acquire);
        self.head.store(tail, Ordering::Release);
    }

    #[inline]
    pub(crate) fn has_pending_frame(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head >= self.capacity() || tail >= self.capacity() || head != tail
    }

    #[inline]
    pub(crate) fn capacity(&self) -> u32 {
        self.capacity.get()
    }

    fn build_publish_plan(
        &self,
        snapshot: RingSnapshot,
        payload_len: usize,
    ) -> Result<PublishPlan, TxError> {
        let max_payload = snapshot.capacity as usize - FRAME_PREFIX_LEN - 1;
        if payload_len > max_payload {
            return Err(TxError::PayloadTooLarge {
                actual: payload_len,
                maximum: max_payload,
            });
        }

        let required = FRAME_PREFIX_LEN + payload_len;
        let available = snapshot.available_bytes();
        if required > available {
            return Err(TxError::Full {
                required,
                available,
            });
        }

        Ok(PublishPlan {
            prefix_index: snapshot.tail,
            prefix_value: payload_len as u32,
            payload_start: wrap_index(snapshot.tail + FRAME_PREFIX_LEN as u32, snapshot.capacity),
            publish_tail: wrap_index(snapshot.tail + required as u32, snapshot.capacity),
        })
    }

    fn build_consume_plan(
        &self,
        snapshot: RingSnapshot,
        dst_len: usize,
    ) -> Result<ConsumePlan, RxError> {
        let len = read_u32_wrapped(self.data, snapshot.head, snapshot.capacity);
        let available = snapshot.used_bytes();
        let frame_len =
            FRAME_PREFIX_LEN
                .checked_add(len as usize)
                .ok_or(RxError::CorruptFrameLen {
                    len,
                    available,
                    capacity: snapshot.capacity,
                })?;
        if frame_len > available {
            return Err(RxError::CorruptFrameLen {
                len,
                available,
                capacity: snapshot.capacity,
            });
        }

        if len as usize > dst_len {
            return Err(RxError::BufferTooSmall {
                required: len as usize,
                available: dst_len,
            });
        }

        Ok(ConsumePlan {
            len,
            payload_start: wrap_index(snapshot.head + FRAME_PREFIX_LEN as u32, snapshot.capacity),
            next_head: wrap_index(snapshot.head + frame_len as u32, snapshot.capacity),
        })
    }

    pub(crate) fn send_frame(
        &mut self,
        ready_flag: &AtomicBool,
        peer_pid: &AtomicI32,
        payload: &[u8],
    ) -> Result<CommitOutcome, TxError> {
        let snapshot = self.head_tail_checked()?;
        let plan = self.build_publish_plan(snapshot, payload.len())?;
        write_u32_wrapped(
            self.data,
            plan.prefix_index,
            snapshot.capacity,
            plan.prefix_value,
        );
        write_bytes_wrapped(self.data, plan.payload_start, snapshot.capacity, payload);
        self.tail.store(plan.publish_tail, Ordering::Release);
        ready_flag.store(true, Ordering::Release);

        let outcome = match signal_peer(peer_pid) {
            Ok(true) => CommitOutcome::Notified,
            Ok(false) => CommitOutcome::PeerMissing,
            Err(err) => CommitOutcome::NotifyFailed(err),
        };
        Ok(outcome)
    }

    pub(crate) fn recv_frame_into(
        &mut self,
        ready_flag: &AtomicBool,
        dst: &mut [u8],
    ) -> Result<Option<usize>, RxError> {
        let snapshot = self.head_tail_checked_rx()?;
        if snapshot.is_empty() {
            return Ok(None);
        }

        let plan = self.build_consume_plan(snapshot, dst.len())?;
        read_bytes_wrapped(
            self.data,
            plan.payload_start,
            snapshot.capacity,
            &mut dst[..plan.len as usize],
        );

        self.head.store(plan.next_head, Ordering::Release);
        if plan.next_head == snapshot.tail {
            self.update_ready_after_consume(plan.next_head, ready_flag);
        }
        Ok(Some(plan.len as usize))
    }

    fn update_ready_after_consume(&self, next_head: u32, ready_flag: &AtomicBool) {
        let tail_after_head = self.tail.load(Ordering::Acquire);
        if tail_after_head != next_head {
            return;
        }

        ready_flag.store(false, Ordering::Release);

        let tail_after_clear = self.tail.load(Ordering::Acquire);
        if tail_after_clear != next_head {
            ready_flag.store(true, Ordering::Release);
        }
    }

    #[inline]
    fn head_tail_checked(&self) -> Result<RingSnapshot, TxError> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        let capacity = self.capacity();
        if head >= capacity || tail >= capacity {
            return Err(TxError::CorruptState {
                head,
                tail,
                capacity,
            });
        }
        Ok(RingSnapshot {
            head,
            tail,
            capacity,
        })
    }

    #[inline]
    fn head_tail_checked_rx(&self) -> Result<RingSnapshot, RxError> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        let capacity = self.capacity();
        if head >= capacity || tail >= capacity {
            return Err(RxError::CorruptState {
                head,
                tail,
                capacity,
            });
        }
        Ok(RingSnapshot {
            head,
            tail,
            capacity,
        })
    }
}

pub(crate) fn signal_peer(peer_pid: &AtomicI32) -> Result<bool, NotifyError> {
    signal_pid_usr1(peer_pid.load(Ordering::Acquire))
}

#[inline]
fn wrap_index(idx: u32, capacity: u32) -> u32 {
    if idx >= capacity {
        idx % capacity
    } else {
        idx
    }
}

fn write_u32_wrapped(data: NonNull<u8>, index: u32, capacity: u32, value: u32) {
    debug_assert!(index < capacity);
    let bytes = value.to_ne_bytes();
    write_bytes_wrapped(data, index, capacity, &bytes);
}

fn read_u32_wrapped(data: NonNull<u8>, index: u32, capacity: u32) -> u32 {
    debug_assert!(index < capacity);
    let mut bytes = [0u8; FRAME_PREFIX_LEN];
    read_bytes_wrapped(data, index, capacity, &mut bytes);
    u32::from_ne_bytes(bytes)
}

fn write_bytes_wrapped(data: NonNull<u8>, start: u32, capacity: u32, src: &[u8]) {
    debug_assert!(start < capacity);
    if src.is_empty() {
        return;
    }

    let first = src.len().min((capacity - start) as usize);
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), data.as_ptr().add(start as usize), first);
        if src.len() > first {
            std::ptr::copy_nonoverlapping(
                src.as_ptr().add(first),
                data.as_ptr(),
                src.len() - first,
            );
        }
    }
}

fn read_bytes_wrapped(data: NonNull<u8>, start: u32, capacity: u32, dst: &mut [u8]) {
    debug_assert!(start < capacity);
    if dst.is_empty() {
        return;
    }

    let first = dst.len().min((capacity - start) as usize);
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr().add(start as usize), dst.as_mut_ptr(), first);
        if dst.len() > first {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                dst.as_mut_ptr().add(first),
                dst.len() - first,
            );
        }
    }
}
