//! Fallible, deterministic inventories own transient retained-image graph
//! records.

use infrastructure_provider::record::DurableRecordKey;
use storage_types::StorageResult;

use super::error::capacity_rejected;

pub(super) struct OwnershipRecords<T> {
    records: Vec<(DurableRecordKey, T)>,
}

impl<T> Default for OwnershipRecords<T> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
        }
    }
}

impl<T> OwnershipRecords<T> {
    pub(super) fn len(&self) -> usize {
        self.records.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (&DurableRecordKey, &T)> {
        self.records.iter().map(|(key, value)| (key, value))
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &DurableRecordKey> {
        self.records.iter().map(|(key, _)| key)
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.records.iter().map(|(_, value)| value)
    }
    pub(super) fn get(&self, key: &DurableRecordKey) -> Option<&T> {
        self.records
            .binary_search_by(|(stored, _)| stored.cmp(key))
            .ok()
            .map(|slot| &self.records[slot].1)
    }
    pub(super) fn get_mut(&mut self, key: &DurableRecordKey) -> Option<&mut T> {
        self.records
            .binary_search_by(|(stored, _)| stored.cmp(key))
            .ok()
            .map(|slot| &mut self.records[slot].1)
    }
    pub(super) fn try_insert(
        &mut self,
        key: DurableRecordKey,
        value: T,
    ) -> StorageResult<Option<T>> {
        match self
            .records
            .binary_search_by(|(stored, _)| stored.cmp(&key))
        {
            Ok(slot) => Ok(Some(std::mem::replace(&mut self.records[slot].1, value))),
            Err(slot) => {
                self.records
                    .try_reserve(1)
                    .map_err(|_| capacity_rejected())?;
                self.records.insert(slot, (key, value));
                Ok(None)
            }
        }
    }
    pub(super) fn try_get_or_insert(
        &mut self,
        key: DurableRecordKey,
        value: impl FnOnce() -> T,
    ) -> StorageResult<&mut T> {
        let slot = match self
            .records
            .binary_search_by(|(stored, _)| stored.cmp(&key))
        {
            Ok(slot) => slot,
            Err(slot) => {
                self.records
                    .try_reserve(1)
                    .map_err(|_| capacity_rejected())?;
                self.records.insert(slot, (key, value()));
                slot
            }
        };
        Ok(&mut self.records[slot].1)
    }
    pub(super) fn remove(&mut self, key: &DurableRecordKey) -> Option<T> {
        self.records
            .binary_search_by(|(stored, _)| stored.cmp(key))
            .ok()
            .map(|slot| self.records.remove(slot).1)
    }
    pub(super) fn try_reserve(&mut self, count: usize) -> StorageResult<()> {
        self.records
            .try_reserve(count)
            .map_err(|_| capacity_rejected())
    }
}

impl<T> IntoIterator for OwnershipRecords<T> {
    type Item = (DurableRecordKey, T);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}
