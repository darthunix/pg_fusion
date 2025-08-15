use anyhow::{bail, Result};
use common::FusionError;
use protocol::Tape;
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LockFreeBuffer<'bytes> {
    head: &'bytes AtomicU32,
    tail: &'bytes AtomicU32,
    data: &'bytes mut [u8],
    uncommitted_tail: AtomicU32,
    cap: u32,
}

impl<'bytes> Write for LockFreeBuffer<'bytes> {
    /// Write bytes to the buffer without moving the tail.
    /// Returns the number of bytes written.
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.uncommitted_tail.load(Ordering::Relaxed);

        let used = if tail >= head {
            tail - head
        } else {
            self.cap - (head - tail)
        };

        let free = self.cap - used;
        // We should avoid an overflow: if the tail returns to the head position,
        // we can not distinguish between an empty buffer and a full buffer.
        if buf.len() as u32 >= free {
            return Err(IoError::new(
                ErrorKind::Other,
                FusionError::PayloadTooLarge(buf.len()),
            ));
        }

        let mut t = self.uncommitted_tail.load(Ordering::Relaxed);
        for &b in buf {
            self.data[self.wrap(t) as usize] = b;
            t = self.wrap(t + 1);
        }
        self.uncommitted_tail.store(t, Ordering::Release);

        Ok(buf.len())
    }

    /// Flush the buffer by committing the uncommitted tail position.
    fn flush(&mut self) -> IoResult<()> {
        let tail = self.uncommitted_tail.load(Ordering::Relaxed);
        self.tail.store(tail, Ordering::Release);
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
            uncommitted_tail: AtomicU32::new((unsafe { &*tail_ptr }).load(Ordering::Relaxed)),
            cap: data_len as u32,
        }
    }

    /// Construct a buffer view from a region laid out by `crate::layout::lockfree_buffer_layout`.
    ///
    /// # Safety
    /// - `base` must point to a memory region of at least `layout.layout.size()` bytes,
    ///   aligned to `layout.layout.align()`.
    /// - The memory must remain valid and uniquely borrowed for the lifetime `'bytes`.
    pub unsafe fn from_layout(base: *mut u8, layout: crate::layout::BufferLayout) -> Self {
        let (head_ptr, tail_ptr, data_ptr) = crate::layout::lockfree_buffer_ptrs(base, layout);
        let data_len = layout.data_len;
        let data = std::slice::from_raw_parts_mut(data_ptr, data_len);

        Self {
            head: &*head_ptr,
            tail: &*tail_ptr,
            data,
            uncommitted_tail: AtomicU32::new((&*tail_ptr).load(Ordering::Relaxed)),
            cap: data_len as u32,
        }
    }

    #[inline(always)]
    fn wrap(&self, idx: u32) -> u32 {
        idx % self.cap
    }

    /// Writes bytes to the buffer and moves the tail.
    pub fn push(&mut self, input: &[u8]) -> Result<()> {
        let _ = self.write(input)?;
        self.flush()?;
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

    fn len_impl(&self, tail: u32) -> usize {
        let head = self.head.load(Ordering::Acquire);
        if tail >= head {
            (tail - head) as usize
        } else {
            (self.cap - (head - tail)) as usize
        }
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
            *b = self.data[h as usize];
            h = self.wrap(h + 1);
        }

        (to_read, h)
    }
}

impl Tape for LockFreeBuffer<'_> {
    fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        self.len_impl(tail)
    }

    fn uncommitted_len(&self) -> usize {
        let tail = self.uncommitted_tail.load(Ordering::Relaxed);
        self.len_impl(tail) - self.len()
    }

    /// Reads bytes from the buffer into `out` without moving the head.
    /// Returns the number of bytes that can be read.
    fn peek(&self, out: &mut [u8]) -> usize {
        let (to_read, _) = self.read_bytes(out);
        to_read
    }

    fn rollback(&mut self) {
        let tail = self.tail.load(Ordering::Relaxed);
        self.uncommitted_tail.store(tail, Ordering::Relaxed);
    }

    fn rewind(&mut self, len: u32) -> Result<()> {
        if self.uncommitted_len() < len as usize {
            bail!(FusionError::PayloadTooLarge(len as usize));
        }
        let t = self.uncommitted_tail.load(Ordering::Relaxed);
        self.uncommitted_tail
            .store(self.wrap(t - len), Ordering::Relaxed);
        Ok(())
    }

    fn fast_forward(&mut self, len: u32) -> Result<()> {
        let used = self.uncommitted_len() as u32;
        let free = self.cap - used;
        if len >= free {
            bail!(FusionError::PayloadTooLarge(free as usize));
        }
        let t = self.uncommitted_tail.load(Ordering::Relaxed);
        self.uncommitted_tail
            .store(self.wrap(t + len), Ordering::Relaxed);
        Ok(())
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
        buffer.push(b"world").unwrap();
        assert_eq!(buffer.len(), 12);
        // Test there is no more space in the buffer.
        assert!(buffer.push(b"1").is_err());

        // Test peeking data from the buffer.
        let mut out = vec![0u8; 5];
        let bytes_read = buffer.peek(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b"Hello");
        assert_eq!(buffer.len(), 12);

        // Test popping data from the buffer.
        let mut out = vec![0u8; 5];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b"Hello");
        assert_eq!(buffer.len(), 7);
        assert!(!buffer.is_empty());
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 5);
        assert_eq!(&out[..bytes_read], b", wor");
        assert_eq!(buffer.len(), 2);
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 2);
        assert_eq!(&out[..bytes_read], b"ld");
        assert!(buffer.is_empty());
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 0);

        let bytes_read = buffer.peek(&mut out);
        assert_eq!(bytes_read, 0);

        // Test wrapping around the buffer.
        buffer.push(b"123456789012").unwrap();
        assert_eq!(buffer.len(), 12);
        let mut out = vec![0u8; 10];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 10);
        assert_eq!(&out[..bytes_read], b"1234567890");
        assert_eq!(buffer.len(), 2);
        buffer.push(b"3456").unwrap();
        assert_eq!(buffer.len(), 6);
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 6);
        assert_eq!(&out[..bytes_read], b"123456");
        assert_eq!(buffer.len(), 0);

        // Test writing data to the buffer.
        let len = buffer.write(b"Hello, world").unwrap();
        assert_eq!(len, b"Hello, world".len());
        assert_eq!(buffer.len(), 0);
        // Replace "World" with "Peace".
        buffer.rollback();
        buffer.write_all(b"Peace").unwrap();
        // Replace "w" with "W".
        buffer.fast_forward(2).unwrap();
        buffer.write_all(b"W").unwrap();
        // Replace "," with "!".
        buffer.rewind(3).unwrap();
        buffer.write_all(b"!").unwrap();
        // Go to the end of the message.
        buffer.fast_forward(6).unwrap();
        buffer.flush().unwrap();
        assert_eq!(buffer.len(), 12);
        let mut out = vec![0u8; 12];
        let bytes_read = buffer.pop(&mut out);
        assert_eq!(bytes_read, 12);
        assert_eq!(&out[..bytes_read], b"Peace! World");
    }
}
