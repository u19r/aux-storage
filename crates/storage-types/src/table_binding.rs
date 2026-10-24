//! Catalog metadata and its durable history identity from the same row.

use std::{ops::Deref, sync::Arc};

use crate::{StoredTableInfo, TableHistoryId};

/// The public metadata Arc can be borrowed by shared request preparation
/// without copying its schemas. The enclosing cache entry retains the native
/// identity.
#[derive(Debug, Clone)]
pub struct BoundTableInfo {
    pub history_id: TableHistoryId,
    pub info: Arc<StoredTableInfo>,
}

impl Deref for BoundTableInfo {
    type Target = StoredTableInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl From<(StoredTableInfo, uuid::Uuid)> for BoundTableInfo {
    fn from((info, id): (StoredTableInfo, uuid::Uuid)) -> Self {
        Self {
            history_id: id.into(),
            info: Arc::new(info),
        }
    }
}

impl std::borrow::Borrow<StoredTableInfo> for BoundTableInfo {
    fn borrow(&self) -> &StoredTableInfo {
        &self.info
    }
}

impl From<BoundTableInfo> for StoredTableInfo {
    fn from(bound: BoundTableInfo) -> Self {
        Arc::unwrap_or_clone(bound.info)
    }
}

impl std::ops::DerefMut for BoundTableInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.info)
    }
}
