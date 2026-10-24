//! Raw JSON counts and compressed output enforce independent frame allowances.
use std::io::{self, Write};

pub(super) struct BoundedSerialization<W> {
    writer: W,
    bytes: usize,
    limit: usize,
}

impl<W> BoundedSerialization<W> {
    pub(super) fn new(writer: W, limit: usize) -> Self {
        Self {
            writer,
            bytes: 0,
            limit,
        }
    }
    pub(super) fn into_parts(self) -> (usize, W) {
        (self.bytes, self.writer)
    }
}

impl<W: Write> Write for BoundedSerialization<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or(io::ErrorKind::FileTooLarge)?;
        self.writer.write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub(super) struct FrameBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl FrameBuffer {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}

impl Write for FrameBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or(io::ErrorKind::FileTooLarge)?;
        if next > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .max(64)
                .saturating_mul(2)
                .max(next)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::ErrorKind::OutOfMemory)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl From<FrameBuffer> for Vec<u8> {
    fn from(buffer: FrameBuffer) -> Self {
        buffer.bytes
    }
}
