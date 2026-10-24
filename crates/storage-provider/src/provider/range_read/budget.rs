use serde::{Deserialize, Serialize};
use storage_types::{ItemKey, LogicalItemSize, StorageError, StorageResult};

/// Source evaluation limits, applied before response filtering or projection.
#[derive(Debug, Clone, Copy)]
pub struct RangeReadBudget {
    pub max_items: u32,
    pub max_evaluated_bytes: u64,
}

/// Evidence for the admitted source prefix. Lookahead never advances it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeReadProgress {
    pub evaluated_count: u32,
    pub evaluated_bytes: u64,
    pub last_evaluated_key: Option<ItemKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeAdmission {
    Admit,
    Stop,
}

impl RangeReadBudget {
    pub fn try_new(max_items: u32, max_evaluated_bytes: u64) -> StorageResult<Self> {
        if max_items == 0 || max_evaluated_bytes == 0 {
            return Err(StorageError::validation(
                "Range read limits must be positive",
            ));
        }
        Ok(Self {
            max_items,
            max_evaluated_bytes,
        })
    }

    pub fn admit(
        &self,
        progress: &mut RangeReadProgress,
        key: ItemKey,
        size: LogicalItemSize,
    ) -> StorageResult<RangeAdmission> {
        let count = progress
            .evaluated_count
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("range evaluated count overflow"))?;
        let bytes = progress
            .evaluated_bytes
            .checked_add(u64::from(size))
            .ok_or_else(|| StorageError::internal("range evaluated byte count overflow"))?;
        if count > self.max_items || bytes > self.max_evaluated_bytes {
            return Ok(RangeAdmission::Stop);
        }
        progress.evaluated_count = count;
        progress.evaluated_bytes = bytes;
        progress.last_evaluated_key = Some(key);
        Ok(RangeAdmission::Admit)
    }
}
