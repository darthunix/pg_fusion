use crate::error::{NotifyError, RxError, TxError};
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

pub(crate) const WRAP_SENTINEL: u32 = u32::MAX;
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

    #[cfg(test)]
    pub(crate) fn clear_with_hook<F>(&self, hook: F)
    where
        F: FnOnce(),
    {
        let tail = self.tail.load(Ordering::Acquire);
        self.head.store(tail, Ordering::Release);
        hook();
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

    pub(crate) fn try_reserve<'ring>(
        &'ring mut self,
        ready_flag: &'ring AtomicBool,
        peer_pid: &'ring AtomicI32,
        payload_len: usize,
    ) -> Result<crate::FrameWriter<'ring>, TxError> {
        let max_payload = self.capacity() as usize - FRAME_PREFIX_LEN - 1;
        if payload_len > max_payload {
            return Err(TxError::PayloadTooLarge {
                actual: payload_len,
                maximum: max_payload,
            });
        }

        let (head, tail) = self.head_tail_checked()?;
        let required = FRAME_PREFIX_LEN + payload_len;
        let available = self.available_bytes(head, tail);
        if required > available {
            return Err(TxError::Full {
                required,
                available,
            });
        }

        let capacity = self.capacity();
        let mut publish_tail;
        let payload_start;
        let payload_len_u32 = payload_len as u32;
        let tail_remaining = (capacity - tail) as usize;

        if head <= tail {
            if required <= tail_remaining {
                payload_start = wrap_index(tail + FRAME_PREFIX_LEN as u32, capacity);
                write_u32(self.data, tail, capacity, payload_len_u32);
                publish_tail = wrap_index(tail + required as u32, capacity);
            } else if required <= head.saturating_sub(1) as usize {
                if tail_remaining >= FRAME_PREFIX_LEN {
                    write_u32(self.data, tail, capacity, WRAP_SENTINEL);
                }
                write_u32(self.data, 0, capacity, payload_len_u32);
                payload_start = FRAME_PREFIX_LEN as u32;
                publish_tail = required as u32;
            } else {
                return Err(TxError::Full {
                    required,
                    available,
                });
            }
        } else {
            let contiguous = head - tail - 1;
            if required > contiguous as usize {
                return Err(TxError::Full {
                    required,
                    available: contiguous as usize,
                });
            }
            write_u32(self.data, tail, capacity, payload_len_u32);
            payload_start = tail + FRAME_PREFIX_LEN as u32;
            publish_tail = tail + required as u32;
        }

        publish_tail = wrap_index(publish_tail, capacity);
        Ok(crate::FrameWriter::new(
            self.data,
            self.tail,
            ready_flag,
            peer_pid,
            payload_start,
            payload_len_u32,
            publish_tail,
        ))
    }

    pub(crate) fn peek_frame(&self) -> Result<Option<&[u8]>, RxError> {
        let capacity = self.capacity();
        loop {
            let (head, tail) = self.head_tail_checked_rx()?;
            if head == tail {
                return Ok(None);
            }

            if tail < head {
                let tail_remaining = capacity - head;
                if tail_remaining < FRAME_PREFIX_LEN as u32 {
                    self.head.store(0, Ordering::Release);
                    continue;
                }
            }

            let len = read_u32(self.data, head, capacity);
            if len == WRAP_SENTINEL {
                self.head.store(0, Ordering::Release);
                continue;
            }

            let available = if tail >= head {
                (tail - head) as usize
            } else {
                (capacity - head) as usize
            };
            let frame_len =
                FRAME_PREFIX_LEN
                    .checked_add(len as usize)
                    .ok_or(RxError::CorruptFrameLen {
                        len,
                        available,
                        capacity,
                    })?;
            if frame_len > available {
                return Err(RxError::CorruptFrameLen {
                    len,
                    available,
                    capacity,
                });
            }

            let payload_start = head + FRAME_PREFIX_LEN as u32;
            let data_ptr = unsafe { self.data.as_ptr().add(payload_start as usize) };
            let payload =
                unsafe { std::slice::from_raw_parts(data_ptr as *const u8, len as usize) };
            return Ok(Some(payload));
        }
    }

    pub(crate) fn consume_frame(&mut self, ready_flag: &AtomicBool) -> Result<(), RxError> {
        self.consume_frame_inner(ready_flag, || {})
    }

    #[cfg(test)]
    pub(crate) fn consume_frame_with_post_clear_hook<F>(
        &mut self,
        ready_flag: &AtomicBool,
        hook: F,
    ) -> Result<(), RxError>
    where
        F: FnOnce(),
    {
        self.consume_frame_inner(ready_flag, hook)
    }

    fn consume_frame_inner<F>(
        &mut self,
        ready_flag: &AtomicBool,
        post_clear_hook: F,
    ) -> Result<(), RxError>
    where
        F: FnOnce(),
    {
        let capacity = self.capacity();
        let (mut head, tail) = self.head_tail_checked_rx()?;
        if head == tail {
            return Err(RxError::Empty);
        }

        if tail < head {
            let tail_remaining = capacity - head;
            let needs_wrap = tail_remaining < FRAME_PREFIX_LEN as u32
                || read_u32(self.data, head, capacity) == WRAP_SENTINEL;
            if needs_wrap {
                head = 0;
                self.head.store(0, Ordering::Release);
            }
        } else if read_u32(self.data, head, capacity) == WRAP_SENTINEL {
            head = 0;
            self.head.store(0, Ordering::Release);
        }

        let len = read_u32(self.data, head, capacity);
        if len == WRAP_SENTINEL {
            return Err(RxError::CorruptFrameLen {
                len,
                available: 0,
                capacity,
            });
        }

        let available = if tail >= head {
            (tail - head) as usize
        } else {
            (capacity - head) as usize
        };
        let frame_len =
            FRAME_PREFIX_LEN
                .checked_add(len as usize)
                .ok_or(RxError::CorruptFrameLen {
                    len,
                    available,
                    capacity,
                })?;
        if frame_len > available {
            return Err(RxError::CorruptFrameLen {
                len,
                available,
                capacity,
            });
        }

        let next_head = wrap_index(head + frame_len as u32, capacity);
        self.head.store(next_head, Ordering::Release);
        if next_head == tail {
            self.update_ready_after_consume(next_head, ready_flag, post_clear_hook);
        }
        Ok(())
    }

    fn update_ready_after_consume<F>(
        &self,
        next_head: u32,
        ready_flag: &AtomicBool,
        post_clear_hook: F,
    ) where
        F: FnOnce(),
    {
        let tail_after_head = self.tail.load(Ordering::Acquire);
        if tail_after_head != next_head {
            return;
        }

        ready_flag.store(false, Ordering::Release);
        post_clear_hook();

        let tail_after_clear = self.tail.load(Ordering::Acquire);
        if tail_after_clear != next_head {
            ready_flag.store(true, Ordering::Release);
        }
    }

    #[inline]
    fn head_tail_checked(&self) -> Result<(u32, u32), TxError> {
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
        Ok((head, tail))
    }

    #[inline]
    fn head_tail_checked_rx(&self) -> Result<(u32, u32), RxError> {
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
        Ok((head, tail))
    }

    #[inline]
    fn available_bytes(&self, head: u32, tail: u32) -> usize {
        let capacity = self.capacity();
        let used = if tail >= head {
            tail - head
        } else {
            capacity - (head - tail)
        };
        (capacity - used - 1) as usize
    }
}

pub(crate) fn signal_peer(peer_pid: &AtomicI32) -> Result<bool, NotifyError> {
    let pid = peer_pid.load(Ordering::Acquire);
    if pid <= 0 {
        return Ok(false);
    }

    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
        if rc == -1 {
            return Err(NotifyError::Signal(std::io::Error::last_os_error()));
        }
        Ok(true)
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(NotifyError::Signal(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "signals are unsupported on this platform",
        )))
    }
}

#[inline]
fn wrap_index(idx: u32, capacity: u32) -> u32 {
    if idx >= capacity {
        idx % capacity
    } else {
        idx
    }
}

fn write_u32(data: NonNull<u8>, index: u32, capacity: u32, value: u32) {
    debug_assert!(index < capacity);
    debug_assert!((capacity - index) as usize >= FRAME_PREFIX_LEN);
    let bytes = value.to_ne_bytes();
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            data.as_ptr().add(index as usize),
            FRAME_PREFIX_LEN,
        );
    }
}

fn read_u32(data: NonNull<u8>, index: u32, capacity: u32) -> u32 {
    debug_assert!(index < capacity);
    debug_assert!((capacity - index) as usize >= FRAME_PREFIX_LEN);
    let mut bytes = [0u8; FRAME_PREFIX_LEN];
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr().add(index as usize),
            bytes.as_mut_ptr(),
            FRAME_PREFIX_LEN,
        );
    }
    u32::from_ne_bytes(bytes)
}
