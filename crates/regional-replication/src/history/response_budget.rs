//! Count complete JSON responses without allocating an oversized encoded body.
use std::io::{self, Write};

use storage_types::{StorageError, StorageResult};
struct EncodedLength {
    length: usize,
    maximum: usize,
}

impl Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum - self.length {
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        self.length += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encoded_length(
    value: &impl serde::Serialize,
    maximum: usize,
) -> StorageResult<usize> {
    let mut counter = EncodedLength { length: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_err(|error| {
        if error.io_error_kind() == Some(io::ErrorKind::FileTooLarge) {
            response_too_large()
        } else {
            crate::native_record::corrupt_record()
        }
    })?;
    Ok(counter.length)
}
fn response_too_large() -> StorageError {
    StorageError::validation(
        "Canonical history response exceeds the configured source frame budget",
    )
}
