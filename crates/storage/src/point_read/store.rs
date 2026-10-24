use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{constants::POINT_READ_ITEM_OVERHEAD_BYTES, key_fingerprint};
use crate::types::{
    KeyAttributes, StorageError, StorageResult, TableName, TryFromWireItem, VecMap, WireItem,
};

#[derive(Debug, Clone)]
pub(super) enum PointReadEntry {
    Hit(Arc<PointReadImage>),
    Miss,
}

#[derive(Debug)]
pub enum PointReadLookup<'a> {
    Hit(&'a WireItem),
    Miss,
    NotRequested,
}

#[derive(Debug, Clone, Default)]
pub struct PointReadStore {
    inner: Arc<PointReadStoreData>,
}

#[derive(Debug, Default)]
struct PointReadStoreData {
    entries: VecMap<String, PointReadEntry>,
    _plan_budget: Option<OwnedSemaphorePermit>,
}

impl PointReadStore {
    pub(super) fn from_entries(
        entries: VecMap<String, PointReadEntry>,
        plan_budget: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner: Arc::new(PointReadStoreData {
                entries,
                _plan_budget: Some(plan_budget),
            }),
        }
    }

    pub fn lookup(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> crate::types::StorageResult<PointReadLookup<'_>> {
        let fingerprint = key_fingerprint(table_name, key)?;
        Ok(match self.inner.entries.get(&fingerprint) {
            Some(PointReadEntry::Hit(item)) => PointReadLookup::Hit(&item.item),
            Some(PointReadEntry::Miss) => PointReadLookup::Miss,
            None => PointReadLookup::NotRequested,
        })
    }

    pub fn get_decoded<T>(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> crate::types::StorageResult<Option<T>>
    where
        T: TryFromWireItem,
    {
        match self.lookup(table_name, key)? {
            PointReadLookup::Hit(item) => item.try_decode::<T>().map(Some),
            PointReadLookup::Miss | PointReadLookup::NotRequested => Ok(None),
        }
    }
}

#[derive(Debug)]
pub(super) struct PointReadImage {
    item: WireItem,
    _budget: OwnedSemaphorePermit,
}

impl PointReadEntry {
    pub(super) fn try_hit(item: WireItem, budget: &Arc<Semaphore>) -> StorageResult<Self> {
        let charge = item
            .payload_len()
            .checked_add(POINT_READ_ITEM_OVERHEAD_BYTES)
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or_else(|| StorageError::service_unavailable(1))?;
        let permit = Arc::clone(budget)
            .try_acquire_many_owned(charge)
            .map_err(|_| StorageError::service_unavailable(1))?;
        Ok(Self::Hit(Arc::new(PointReadImage {
            item,
            _budget: permit,
        })))
    }
}
