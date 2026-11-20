use std::{borrow::Borrow, ops::Deref, sync::Arc};

use super::WireItem;
use crate::{KeyAttributes, StorageError, StorageResult, StoredWireItem};

/// An image whose logical keys have been checked against its addressed item.
#[derive(Clone, Debug)]
pub struct VerifiedWireItem<I = WireItem> {
    item: I,
}

impl VerifiedWireItem {
    pub fn try_new(item: WireItem, key: &KeyAttributes) -> StorageResult<Self> {
        item.with_verified_keys(key).map(|item| Self { item })
    }

    pub fn try_from_stored(stored: StoredWireItem, key: &KeyAttributes) -> StorageResult<Self> {
        if !stored.item.has_verified_keys(key)? {
            return Err(StorageError::internal(
                "Stored item differs from its addressed key",
            ));
        }
        Ok(Self { item: stored.item })
    }
}

impl VerifiedWireItem<Arc<WireItem>> {
    pub fn try_from_shared(item: Arc<WireItem>, key: &KeyAttributes) -> StorageResult<Self> {
        let item = if item.has_complete_write_keys(key)? {
            item
        } else {
            let item = Arc::try_unwrap(item).unwrap_or_else(|item| (*item).clone());
            Arc::new(item.complete_write_keys(key)?)
        };
        Ok(Self { item })
    }

    /// Encode an image whose keys `AttributeMap::with_verified_keys` already
    /// verified and completed; skips the per-key scans of `try_from_shared`.
    pub fn encode_key_complete(attributes: &crate::AttributeMap) -> StorageResult<Self> {
        Ok(Self {
            item: Arc::new(WireItem::try_from(attributes)?),
        })
    }
}

impl<I: Borrow<WireItem>> Deref for VerifiedWireItem<I> {
    type Target = WireItem;

    fn deref(&self) -> &Self::Target {
        self.item.borrow()
    }
}

impl From<VerifiedWireItem> for WireItem {
    fn from(item: VerifiedWireItem) -> Self {
        item.item
    }
}
