use std::ops::Deref;

use super::WireItem;
use crate::{KeyAttributes, StorageResult, StoredImageSource, StoredWireItem};

/// An image whose logical keys have been checked against its addressed item.
#[derive(Clone, Debug)]
pub struct VerifiedWireItem {
    item: WireItem,
    source: Option<StoredImageSource>,
}

impl VerifiedWireItem {
    pub fn try_new(item: WireItem, key: &KeyAttributes) -> StorageResult<Self> {
        item.with_verified_keys(key)
            .map(|item| Self { item, source: None })
    }

    pub fn try_from_stored(stored: StoredWireItem, key: &KeyAttributes) -> StorageResult<Self> {
        Ok(Self {
            item: stored.item.with_verified_keys(key)?,
            source: stored.source,
        })
    }

    pub fn stored_source(&self) -> Option<StoredImageSource> {
        self.source
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
