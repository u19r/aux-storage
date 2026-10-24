//! Retained JSON output preserves spelling and rejects allocation failure
//! irreversibly.
use std::io::{self, Write};

use serde::Serialize;
use storage_types::StorageResult;

use super::error::capacity_rejected;

#[derive(Default)]
pub(super) struct RetainedJsonOutput {
    bytes: Vec<u8>,
    rejected: bool,
}

impl RetainedJsonOutput {
    pub(super) fn encode(value: &impl Serialize) -> StorageResult<Vec<u8>> {
        let mut output = Self::default();
        let serialized = serde_json::to_writer(&mut output, value);
        if output.rejected {
            return Err(capacity_rejected());
        }
        serialized?;
        Ok(output.bytes)
    }
}

impl Write for RetainedJsonOutput {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if self.rejected {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        if self.bytes.try_reserve(input.len()).is_err() {
            self.rejected = true;
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.rejected {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        Ok(())
    }
}
