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
        for i in 0..state.size {
            if state.flags[i].load(Ordering::Acquire) {
                state.wakers[i].wake();
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
