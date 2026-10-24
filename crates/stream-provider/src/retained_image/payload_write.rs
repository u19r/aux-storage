//! Compression output never grows past the native encoded-record allowance.
use std::io::{self, Write};

use storage_types::StorageResult;

use super::error::{capacity_rejected, invalid_image};
use crate::constants::{RETAINED_PAYLOAD_FORMAT, RETAINED_PAYLOAD_HEADER_BYTES};

pub(super) struct RetainedPayloadWrite {
    bytes: Vec<u8>,
    maximum: usize,
    rejected: bool,
}

impl RetainedPayloadWrite {
    pub(super) fn try_new(decoded_length: usize, maximum: usize) -> StorageResult<Self> {
        if maximum < RETAINED_PAYLOAD_HEADER_BYTES {
            return Err(capacity_rejected());
        }
        let length = u64::try_from(decoded_length).map_err(|_| invalid_image())?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(RETAINED_PAYLOAD_HEADER_BYTES)
            .map_err(|_| capacity_rejected())?;
        bytes.push(RETAINED_PAYLOAD_FORMAT);
        bytes.extend_from_slice(&length.to_be_bytes());
        Ok(Self {
            bytes,
            maximum,
            rejected: false,
        })
    }

    pub(super) fn require_capacity(&self) -> StorageResult<()> {
        if self.rejected {
            Err(capacity_rejected())
        } else {
            Ok(())
        }
    }
}

impl Write for RetainedPayloadWrite {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if self.rejected {
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        let Some(end) = self
            .bytes
            .len()
            .checked_add(input.len())
            .filter(|end| *end <= self.maximum)
        else {
            self.rejected = true;
            return Err(io::ErrorKind::FileTooLarge.into());
        };
        if end > self.bytes.capacity() {
            let target = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .min(self.maximum)
                .max(end);
            if self
                .bytes
                .try_reserve_exact(target - self.bytes.len())
                .is_err()
            {
                self.rejected = true;
                return Err(io::ErrorKind::OutOfMemory.into());
            }
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl From<RetainedPayloadWrite> for Vec<u8> {
    fn from(value: RetainedPayloadWrite) -> Self {
        value.bytes
    }
}
