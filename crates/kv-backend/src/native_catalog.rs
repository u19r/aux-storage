//! Native-root catalog observations, checked writer routes, and stream event
//! numbers.

#[derive(Default)]
pub(crate) struct NativeCatalogState {
    /// None means not read; Some(None) means the native key was missing.
    pub(crate) observation: Option<Option<storage_types::CatalogVersion>>,
    pub(crate) changed: bool,
    pub(crate) publication_staged: bool,
    /// Debug builds: a staged write reached a key that cached writers protect
    /// with a conflict range instead of the metadata version.
    protected_key_written: bool,
    pub(crate) writer_routes: Option<std::sync::Arc<stream_provider::SystemStreamLineageCatalog>>,
    next_event_number: u32,
}

impl NativeCatalogState {
    pub(crate) fn reserve_event_number(
        &mut self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> storage_types::StorageResult<u16> {
        settlement.require_open()?;
        let number = u16::try_from(self.next_event_number).map_err(|_| {
            settlement.reject();
            storage_types::StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
        })?;
        self.next_event_number += 1;
        Ok(number)
    }

    /// Debug builds record whether a write touched a table name lookup key,
    /// the system-stream scope key, or a lineage key. Cached writers conflict
    /// on exactly those keys, so a catalog change that writes none of them
    /// would not abort an in-flight cached write.
    pub(crate) fn observe_write_key(&mut self, key: &[u8]) {
        if cfg!(debug_assertions) && !self.protected_key_written {
            self.protected_key_written = key.first() == Some(&crate::constants::LOOKUP_FAMILY)
                || key == crate::constants::SYSTEM_STREAM_SCOPE_KEY
                || key.starts_with(crate::constants::SYSTEM_STREAM_LINEAGE_PREFIX);
        }
    }

    pub(crate) fn observe_write(&mut self, change: &crate::KvMutation) {
        if cfg!(debug_assertions) {
            match change {
                crate::KvMutation::Put { key, .. }
                | crate::KvMutation::PutUnchunked { key, .. }
                | crate::KvMutation::Delete { key }
                | crate::KvMutation::DeleteUnchunked { key } => self.observe_write_key(key),
                crate::KvMutation::DeleteRange { begin, .. }
                | crate::KvMutation::ClearUnchunkedRange { begin, .. } => {
                    self.observe_write_key(begin);
                }
            }
        }
    }

    /// Called once before an attempt commits.
    pub(crate) fn debug_assert_change_protected(&self) {
        debug_assert!(
            !self.changed || self.protected_key_written,
            "a catalog change must write a table name lookup key, the system-stream scope key, or \
             a lineage key in the same attempt"
        );
    }

    pub(crate) fn record_change(&mut self) {
        self.writer_routes = None;
        self.changed = true;
        self.observation = None;
    }
}

#[cfg(feature = "foundationdb")]
pub(crate) fn decode_foundationdb_version(
    value: &[u8],
) -> storage_types::StorageResult<storage_types::CatalogVersion> {
    let stamp = value.try_into().map_err(|_| {
        storage_types::StorageError::internal("Invalid FoundationDB catalog version")
    })?;
    Ok(storage_types::CatalogVersion::FoundationDb(stamp))
}
