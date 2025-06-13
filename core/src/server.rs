use crate::buffer::LockFreeBuffer;
use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::ipc::Socket;
use crate::protocol::failure::prepare_error;
use crate::protocol::{consume_header, Direction};
use anyhow::Result;
use smol_str::format_smolstr;

pub struct Connection<'bytes> {
    state: StateMachine,
    recv_socket: Socket<'bytes>,
    send_buffer: LockFreeBuffer<'bytes>,
}

impl<'bytes> Connection<'bytes> {
    pub fn new(recv_socket: Socket<'bytes>, send_buffer: LockFreeBuffer<'bytes>) -> Self {
        Self {
            state: StateMachine::new(),
            recv_socket,
            send_buffer,
        }
    }

    pub async fn receive(&mut self, buffer: &mut [u8]) -> Result<usize> {
        (&mut self.recv_socket).await?;
        let bytes_read = self.recv_socket.buffer.pop(buffer);
        Ok(bytes_read)
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.send_buffer.push(data)?;
        Ok(())
    }

    pub async fn run(&mut self) {
        let header = match consume_header(&mut self.recv_socket.buffer) {
            Ok(header) => header,
            Err(e) => {
                self.handle_error(FusionError::InvalidHeader(e));
                return;
            }
        };
        if header.direction == Direction::ToBackend {
            return;
        }
        let action = match self.state.consume(&header.packet) {
            Ok(action) => action,
            Err(e) => {
                self.handle_error(FusionError::InvalidTransition(e.into()));
                return;
            }
        };
    }

    fn handle_error(&mut self, error: FusionError) {
        self.recv_socket.buffer.flush_read();
        let error_message = format_smolstr!("{error}");
        if let Err(e) = prepare_error(&mut self.send_buffer, &error_message) {
            // Double error, we can't do much about it.
            eprintln!("Failed to prepare error response: {e}");
        }
        // TODO: notify client about the error.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::ipc::SharedState;
    use std::cell::UnsafeCell;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    struct ConnMemory {
        rx: UnsafeCell<[u8; 8 + 13]>,
        tx: UnsafeCell<[u8; 8 + 13]>,
        flag: UnsafeCell<[AtomicBool; 1]>,
    }
    impl ConnMemory {
        const fn new() -> Self {
            Self {
                rx: UnsafeCell::new([0; 8 + 13]),
                tx: UnsafeCell::new([0; 8 + 13]),
                flag: UnsafeCell::new([AtomicBool::new(false); 1]),
            }
        }
    }
    unsafe impl Sync for ConnMemory {}

    #[tokio::test]
    async fn test_single_connection() -> Result<()> {
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flag.get() }));
        let recv_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.rx.get() });
        let send_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.tx.get() });
        let socket = Socket::new(0, Arc::clone(&state), recv_buffer);
        let mut conn = Connection::new(socket, send_buffer);
        tokio::spawn(async move {
            // TODO: Simulate sending data to the connection
            conn.run().await
        });
        Ok(())
    }
}
