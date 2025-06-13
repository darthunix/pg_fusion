use crate::error::FusionError;
use anyhow::{bail, Result};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LockFreeBuffer<'bytes> {
    head: &'bytes AtomicU32,
    tail: &'bytes AtomicU32,
    data: &'bytes mut [u8],
    cap: u32,
}

impl<'bytes> Write for LockFreeBuffer<'bytes> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self.push(buf) {
            Ok(_) => Ok(buf.len()),
            Err(_) => Err(IoError::new(ErrorKind::Other, "Failed to write to buffer")),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl<'bytes> Read for LockFreeBuffer<'bytes> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let bytes_read = self.pop(buf);
        Ok(bytes_read)
    }
}

impl<'bytes> LockFreeBuffer<'bytes> {
    pub fn new(mem: &'bytes mut [u8]) -> Self {
        assert!(mem.len() >= 8, "Buffer too small");

        let head_ptr = mem.as_ptr() as *const AtomicU32;
        let tail_ptr = unsafe { mem.as_ptr().add(4) } as *const AtomicU32;
        let data_ptr = unsafe { mem.as_mut_ptr().add(8) };

        let data_len = mem.len() - 8;
        let data = unsafe { std::slice::from_raw_parts_mut(data_ptr, data_len) };

        Self {
            head: unsafe { &*head_ptr },
            tail: unsafe { &*tail_ptr },
            data,
            cap: data_len as u32,
        }
    }

    #[inline(always)]
    fn wrap(&self, idx: u32) -> usize {
        (idx % self.cap) as usize
    }

    /// Writes bytes to the buffer and moves the tail.
    pub fn push(&mut self, input: &[u8]) -> Result<()> {
        let t = self.write(input)?;
        self.tail.store(t, Ordering::Release);
        Ok(())
    }

    /// Reads bytes from the buffer into `out` and moves the head.
    /// Returns the number of bytes that were read.
    pub fn pop(&mut self, out: &mut [u8]) -> usize {
        let (to_read, h) = self.read_bytes(out);
        self.head.store(h, Ordering::Release);
        to_read
    }

    /// Flushes the read position of the buffer.
    pub fn flush_read(&mut self) {
        self.head
            .store(self.tail.load(Ordering::Relaxed), Ordering::Release);
    }

    /// Reads bytes from the buffer into `out` without moving the head.
    /// Returns the number of bytes that can be read.
    pub fn peek(&self, out: &mut [u8]) -> usize {
        let (to_read, _) = self.read_bytes(out);
        to_read
    }

    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if tail >= head {
            (tail - head) as usize
        } else {
            (self.cap - (head - tail)) as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    /// Reads bytes from the buffer into `out`, returns how many bytes were read.
    /// If there are no bytes to read, returns 0.
    /// If there are not enough bytes to read, returns n where n is the number of bytes that
    /// were read.
    fn read_bytes(&self, out: &mut [u8]) -> (usize, u32) {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Relaxed);

        let available = if tail >= head {
            tail - head
        } else {
            self.cap - (head - tail)
        };

        if available == 0 {
            return (0, head);
        }

        let to_read = out.len().min(available as usize);
        let mut h = head;
        for b in out[..to_read].iter_mut() {
            *b = self.data[self.wrap(h)];
            h += 1;
        }

        (to_read, h)
    }

    /// Write bytes to the buffer without moving the tail.
    /// Returns the new tail position.
    pub fn write(&mut self, input: &[u8]) -> Result<u32> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);

        let used = if tail >= head {
            tail - head
        } else {
            self.cap - (head - tail)
        };

        let free = self.cap - used;
        if input.len() as u32 > free {
            bail!(FusionError::PayloadTooLarge(input.len()));
        }

        let mut t = tail;
        for &b in input {
            self.data[self.wrap(t)] = b;
            t += 1;
        }

        Ok(t)
    }

    /// Sets the tail position of the buffer.
    ///
    /// # Safety
    /// This is unsafe because it can lead to data corruption if not used correctly.
    pub unsafe fn set_tail(&mut self, tail: u32) {
        self.tail.store(tail, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_free_buffer() {
        let mut bytes = vec![0u8; 8 + 13];
        let mut buffer = LockFreeBuffer::new(&mut bytes);

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 13);
        assert!(buffer.is_empty());

        // Test pushing data into the buffer.
        buffer.push(b"Hello, ").unwrap();
        buffer.push(b"world!").unwrap();
        assert_eq!(buffer.len(), 13);
        // Test there is no more space in the buffer.
        assert!(buffer.push(b"1").is_err());

        // Test peeking data from the buffer.
        let mut out = vec![0u8; 5];
        let bytes_read = buffer.peek(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b"Hello");
        assert_eq!(buffer.len(), 13);

        // Test popping data from the buffer.
        let mut out = vec![0u8; 5];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b"Hello");
        assert_eq!(buffer.len(), 8);
        assert!(!buffer.is_empty());
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b", wor");
        assert_eq!(buffer.len(), 3);
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 3);
        assert_eq!(&out[..bytes_read], b"ld!");
        assert!(buffer.is_empty());
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 0);

        let bytes_read = buffer.peek(&mut out);
        assert_eq!(bytes_read, 0);

        // Test wrapping around the buffer.
        buffer.push(b"1234567890123").unwrap();
        assert_eq!(buffer.len(), 13);
        let mut out = vec![0u8; 10];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 10);
        assert_eq!(&out[..bytes_read], b"1234567890");
        assert_eq!(buffer.len(), 3);
        buffer.push(b"456").unwrap();
        assert_eq!(buffer.len(), 6);
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 6);
        assert_eq!(&out[..bytes_read], b"123456");
        assert_eq!(buffer.len(), 0);

        // Test writing data to the buffer.
        let t = buffer.write(b"Hello, world!").unwrap();
        assert_eq!(buffer.len(), 0);
        buffer.write(b"Peace").unwrap();
        unsafe { buffer.set_tail(t) };
        assert_eq!(buffer.len(), 13);
        let mut out = vec![0u8; 13];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 13);
        assert_eq!(&out[..bytes_read], b"Peace, world!");
    }
}
