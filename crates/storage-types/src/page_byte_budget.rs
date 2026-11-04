//! Byte pages admit one legal wide record so every continuation can progress.

use crate::{StorageError, StorageResult};

#[derive(Clone, Copy)]
pub struct PageByteBudget {
    remaining: usize,
    has_record: bool,
}

impl PageByteBudget {
    pub fn try_new(maximum: usize) -> StorageResult<Self> {
        if maximum == 0 {
            return Err(StorageError::internal("Page byte budget must be positive"));
        }
        Ok(Self {
            remaining: maximum,
            has_record: false,
        })
    }

    pub fn admit(&mut self, bytes: usize) -> bool {
        if self.has_record && bytes > self.remaining {
            return false;
        }
        self.remaining = self.remaining.saturating_sub(bytes);
        self.has_record = true;
        true
    }
}
