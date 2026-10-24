//! Primary-item mutation order, independent of stream positions.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::{ItemStreamVersion, StorageError, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemRevision(NonZeroU64);

impl ItemRevision {
    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub fn next(current: Self) -> StorageResult<Self> {
        let next = current
            .get()
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("item revision overflowed its counter"))?;
        Self::try_from(next)
    }

    /// Advance a durable revision without falling below its caller's floor.
    /// The floor may be zero when an existing item has no physical position.
    pub fn next_at_least(current: Option<Self>, floor: u64) -> StorageResult<Self> {
        let next = current
            .map(Self::next)
            .transpose()?
            .map(Self::get)
            .unwrap_or(0);
        Self::try_from(next.max(floor))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for ItemRevision {
    type Error = StorageError;

    fn try_from(value: u64) -> StorageResult<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| StorageError::internal("item revision must be a positive counter"))
    }
}

impl TryFrom<i64> for ItemRevision {
    type Error = StorageError;

    fn try_from(value: i64) -> StorageResult<Self> {
        Self::try_from(
            u64::try_from(value)
                .map_err(|_| StorageError::internal("item revision must be a positive counter"))?,
        )
    }
}

impl TryFrom<ItemRevision> for i64 {
    type Error = StorageError;

    fn try_from(value: ItemRevision) -> StorageResult<Self> {
        i64::try_from(value.get())
            .map_err(|_| StorageError::internal("item revision exceeds the SQL row range"))
    }
}

/// The two counters recorded by one item write. Neither counter implies the
/// other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemWriteVersions {
    pub item_revision: ItemRevision,
    pub item_stream_version: ItemStreamVersion,
}
