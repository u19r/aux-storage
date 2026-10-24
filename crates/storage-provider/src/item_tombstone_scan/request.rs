use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use storage_types::{BoundTableInfo, KeyAttributes, StorageError, StorageResult};

use super::ItemTombstonePage;
use crate::{canonical_revision_key, primary_state::key::validate_primary_state_key};

/// The catalog binds an incarnation; only a schema-valid key can continue it.
pub struct ItemTombstoneScan {
    table: Arc<BoundTableInfo>,
    after: Option<String>,
    limit: NonZeroU32,
    max_bytes: NonZeroUsize,
}

impl ItemTombstoneScan {
    pub fn try_new(
        table: Arc<BoundTableInfo>,
        after: Option<KeyAttributes>,
        limit: NonZeroU32,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Self> {
        if limit.get() == u32::MAX {
            return Err(StorageError::validation(
                "tombstone scan needs room for one probe row",
            ));
        }
        let after = after
            .map(|key| {
                validate_primary_state_key(&table, &key)?;
                canonical_revision_key(&key)
            })
            .transpose()?;
        Ok(Self {
            table,
            after,
            limit,
            max_bytes,
        })
    }

    pub fn table(&self) -> &BoundTableInfo {
        &self.table
    }
    pub fn after(&self) -> Option<&str> {
        self.after.as_deref()
    }
    pub fn limit(&self) -> NonZeroU32 {
        self.limit
    }
    pub fn probe_limit(&self) -> NonZeroU32 {
        self.limit.saturating_add(1)
    }
    pub fn max_bytes(&self) -> NonZeroUsize {
        self.max_bytes
    }

    /// Stored identity is checked once before exposing a typed key. Reject
    /// duplicate names, noncanonical numbers and keys outside the schema.
    pub fn decode_key(&self, json: &str) -> StorageResult<KeyAttributes> {
        let key: KeyAttributes = serde_json::from_str(json)
            .map_err(|_| StorageError::internal("tombstone key is corrupt"))?;
        validate_primary_state_key(&self.table, &key)
            .map_err(|_| StorageError::internal("tombstone key does not match its schema"))?;
        if canonical_revision_key(&key)? != json {
            return Err(StorageError::internal("tombstone key is not canonical"));
        }
        Ok(key)
    }
}

pub trait ItemTombstoneScanAttempt: Send {
    fn scan_item_tombstones(
        &mut self,
        scan: &ItemTombstoneScan,
    ) -> impl Future<Output = StorageResult<ItemTombstonePage>> + Send;
}
