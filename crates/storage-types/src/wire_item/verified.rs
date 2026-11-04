use std::ops::Deref;

use super::WireItem;
use crate::{KeyAttributes, StorageError, StorageResult, StoredWireItem};

/// An image whose logical keys have been checked against its addressed item.
#[derive(Clone, Debug)]
pub struct VerifiedWireItem {
    item: WireItem,
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

impl Deref for VerifiedWireItem {
    type Target = WireItem;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl From<VerifiedWireItem> for WireItem {
    fn from(item: VerifiedWireItem) -> Self {
        item.item
    }
}
