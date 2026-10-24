use storage_types::{ItemKey, StorageError, StorageResult, WireItem};

use super::RangeReadProgress;

/// A native range image paired with its independently decoded physical address.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeReadItem {
    pub key: ItemKey,
    pub item: WireItem,
}

/// Admitted source images and their evaluation evidence, before response
/// shaping.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeReadPage<T = WireItem> {
    pub items: Vec<T>,
    pub progress: RangeReadProgress,
    pub continuation: Option<String>,
}

impl<T> Default for RangeReadPage<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            progress: RangeReadProgress::default(),
            continuation: None,
        }
    }
}

impl<T> RangeReadPage<T> {
    pub fn finish_with_more(mut self) -> StorageResult<Self> {
        let key =
            self.progress.last_evaluated_key.as_ref().ok_or_else(|| {
                StorageError::internal("range continuation lacks an admitted item")
            })?;
        self.continuation = Some(key.next_page_token()?);
        self.finish()
    }

    pub fn finish(self) -> StorageResult<Self> {
        let count = u32::try_from(self.items.len()).map_err(|_| {
            StorageError::internal("range page returned more items than its count type")
        })?;
        if count != self.progress.evaluated_count {
            return Err(StorageError::internal(
                "range source item count must equal evaluated count",
            ));
        }
        let is_empty = count == 0;
        if is_empty != (self.progress.evaluated_bytes == 0)
            || is_empty != self.progress.last_evaluated_key.is_none()
        {
            return Err(StorageError::internal(
                "range page progress is inconsistent",
            ));
        }
        Ok(self)
    }

    /// Decode images without discarding source evaluation evidence.
    pub fn try_map<U>(
        self,
        map: impl FnMut(&T) -> StorageResult<U>,
    ) -> StorageResult<RangeReadPage<U>> {
        Ok(RangeReadPage {
            items: self.items.iter().map(map).collect::<StorageResult<_>>()?,
            progress: self.progress,
            continuation: self.continuation,
        })
    }
}
