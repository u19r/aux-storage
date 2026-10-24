use std::io::{self, Write};

/// Refuse a write before it grows the JSON buffer beyond its owner's bound.
pub(crate) struct BoundedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    rejected: Option<io::ErrorKind>,
}

impl BoundedJsonWriter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            rejected: None,
        }
    }

    pub(crate) fn require_capacity(&self) -> io::Result<()> {
        self.rejected
            .map_or(Ok(()), |kind| Err(io::Error::from(kind)))
    }

    fn reject(&mut self, kind: io::ErrorKind) -> io::Error {
        self.rejected = Some(kind);
        io::Error::from(kind)
    }
}

impl From<BoundedJsonWriter> for Vec<u8> {
    fn from(writer: BoundedJsonWriter) -> Self {
        writer.bytes
    }
}

impl AsRef<[u8]> for BoundedJsonWriter {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.require_capacity()?;
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(self.reject(io::ErrorKind::FileTooLarge));
        }
        let length = self.bytes.len() + bytes.len();
        if length > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(length)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| self.reject(io::ErrorKind::OutOfMemory))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.require_capacity()
    }
}

/// Measure the maintained JSON grammar without retaining an encoded copy.
pub(crate) struct BoundedJsonCounter {
    length: usize,
    limit: usize,
    rejected: bool,
}

impl BoundedJsonCounter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            length: 0,
            limit,
            rejected: false,
        }
    }
    pub(crate) fn length(&self) -> usize {
        self.length
    }

    pub(crate) fn require_capacity(&self) -> io::Result<()> {
        if self.rejected {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge));
        }
        Ok(())
    }
}

impl Write for BoundedJsonCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.require_capacity()?;
        if bytes.len() > self.limit - self.length {
            self.rejected = true;
            return Err(io::Error::from(io::ErrorKind::FileTooLarge));
        }
        self.length += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.require_capacity()
    }
}
