//! Image page progress is independent of logical evaluated-item accounting.

use storage_types::StorageResult;

use super::error::invalid_image;

#[derive(Clone, Copy)]
pub struct RetainedImagePageBudget {
    remaining: usize,
    has_image: bool,
}

impl RetainedImagePageBudget {
    pub fn try_new(maximum: usize) -> StorageResult<Self> {
        if maximum == 0 {
            return Err(invalid_image());
        }
        Ok(Self {
            remaining: maximum,
            has_image: false,
        })
    }

    /// One qualified row can progress beyond the work budget. Opaque/absent
    /// work is zero.
    pub fn admit(&mut self, work: usize) -> StorageResult<bool> {
        if self.has_image && work > self.remaining {
            return Ok(false);
        }
        self.remaining = self.remaining.saturating_sub(work);
        self.has_image = true;
        Ok(true)
    }
}
