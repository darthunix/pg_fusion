use crate::buffer::LockFreeBuffer;
use anyhow::Result;
use futures::task::AtomicWaker;
use futures::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::signal::unix::{signal, SignalKind};

pub struct SharedState<'bytes> {
    size: usize,
    flags: &'bytes [AtomicBool],
    wakers: Vec<AtomicWaker>,
}

impl<'bytes> SharedState<'bytes> {
    pub fn new(flags: &'bytes [AtomicBool]) -> Self {
        let size = flags.len();
        let wakers = (0..size).map(|_| AtomicWaker::new()).collect::<Vec<_>>();
        Self {
            size,
            flags,
            wakers,
        }
    }
}

pub async fn signal_listener(state: Arc<SharedState<'_>>) {
    let mut signal = signal(SignalKind::user_defined1()).expect("failed to create signal");
    while signal.recv().await.is_some() {
        // Notify affected sockets that the signal has been received.
        let mut woke = 0usize;
        for i in 0..state.size {
            if state.flags[i].load(Ordering::Acquire) {
                state.wakers[i].wake();
                woke += 1;
                if tracing::enabled!(target: "executor::ipc", tracing::Level::TRACE) {
                    tracing::trace!(
                        target = "executor::ipc",
                        conn_id = i,
                        "signal_listener: woke socket"
                    );
                }
            }
        }
        if woke == 0 {
            if tracing::enabled!(target: "executor::ipc", tracing::Level::TRACE) {
                tracing::trace!(
                    target = "executor::ipc",
                    "signal_listener: signal received but no flags set"
                );
            }
        }
    }
}

pub struct Socket<'bytes> {
    id: usize,
    state: Arc<SharedState<'bytes>>,
    pub buffer: LockFreeBuffer<'bytes>,
}

impl<'bytes> Socket<'bytes> {
    pub fn new(id: usize, state: Arc<SharedState<'bytes>>, buffer: LockFreeBuffer<'bytes>) -> Self {
        assert!(id < state.size, "Socket ID out of bounds");
        Self { id, state, buffer }
    }

    pub fn signal(&self) {
        self.state.flags[self.id].store(true, Ordering::Release);
    }

    /// Construct a socket from a memory region described by `SocketLayout`
    /// and an existing `SharedState` (which owns the flags array).
    ///
    /// # Safety
    /// - `base` must be a valid pointer to a region of at least `layout.layout.size()` bytes
    ///   with alignment `layout.layout.align()`.
    /// - The memory must live at least as long as `'bytes` and not be aliased mutably elsewhere.
    pub unsafe fn from_layout_with_state(
        id: usize,
        state: Arc<SharedState<'bytes>>,
        base: *mut u8,
        layout: crate::layout::SocketLayout,
    ) -> Self {
        let buffer_base = crate::layout::socket_ptrs(base, layout);
        let buffer = LockFreeBuffer::from_layout(buffer_base, layout.buffer_layout);
        Socket::new(id, state, buffer)
    }
}

impl Future for Socket<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check if the socket was already signaled.
        if self.state.flags[self.id]
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return Poll::Ready(Ok(()));
        }

        // Register the waker for this socket.
        self.state.wakers[self.id].register(cx.waker());

        // Recheck in case a signal arrived between initial check and registration.
        // This avoids lost wakeups.
        if self.state.flags[self.id]
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::layout::lockfree_buffer_layout;
    use std::cell::UnsafeCell;
    use std::time::Duration;
    use tokio::task;
    use tokio::time::timeout;

    struct Memory {
        bytes: UnsafeCell<[u8; 8 + 13]>,
        flags: UnsafeCell<[AtomicBool; 1]>,
    }
    impl Memory {
        const fn new() -> Self {
            Self {
                flags: UnsafeCell::new([AtomicBool::new(false); 1]),
                bytes: UnsafeCell::new([0; 8 + 13]),
            }
        }
    }
    unsafe impl Sync for Memory {}

    #[tokio::test]
    async fn test_signal_before_poll() -> Result<()> {
        let flags = [AtomicBool::new(true)];
        let state = Arc::new(SharedState::new(&flags));

        // Allocate buffer via BufferLayout on the heap
        let layout = lockfree_buffer_layout(13).unwrap();
        unsafe {
            let base = std::alloc::alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let buffer = LockFreeBuffer::from_layout(base, layout);
            let socket = Socket::new(0, Arc::clone(&state), buffer);

            // As the flag is already set, .await should complete immediately.
            timeout(Duration::from_secs(1), socket).await??;
            std::alloc::dealloc(base, layout.layout);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_signal_after_poll() -> Result<()> {
        static BUFFER: Memory = Memory::new();

        let state = Arc::new(SharedState::new(unsafe { &*BUFFER.flags.get() }));
        let buffer = LockFreeBuffer::new(unsafe { &mut *BUFFER.bytes.get() });
        let socket = Socket::new(0, Arc::clone(&state), buffer);

        let handle = task::spawn(socket);

        // Wait for socket to register its waker.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set the flag.
        unsafe {
            (&mut *BUFFER.flags.get())[0].store(true, Ordering::Release);
        }
        // Manually wake the waker.
        state.wakers[0].wake();

        // Future should succeed.
        timeout(Duration::from_secs(1), handle).await???;
        Ok(())
    }
}
