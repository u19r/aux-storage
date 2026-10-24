use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::{StorageError, StorageResult};

/// Non-zero item and byte limits for one bounded provider page.
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PageLimits {
    pub max_items: NonZeroU32,
    pub max_bytes: NonZeroU32,
}

impl TryFrom<(usize, usize)> for PageLimits {
    type Error = StorageError;

    fn try_from((max_items, max_bytes): (usize, usize)) -> StorageResult<Self> {
        let max_items = u32::try_from(max_items)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| StorageError::validation("page item limit must be non-zero"))?;
        let max_bytes = u32::try_from(max_bytes)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| StorageError::validation("page byte limit must be non-zero"))?;
        Ok(Self {
            max_items,
            max_bytes,
        })
    }
}
