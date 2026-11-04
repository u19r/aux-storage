//! Invocation identity survives native aborts independently of cached metadata.
use std::sync::{Arc, Mutex};

use storage_types::{StorageError, StorageResult, TableHistoryId, TableName};

use crate::{CatalogStamp, TableMetadataCache};

pub(crate) struct CatalogConfirmation<'a> {
    pub(crate) name: &'a TableName,
    pub(crate) prepared: Option<CatalogStamp>,
    pub(crate) observed: Option<CatalogStamp>,
}

/// IDs accepted by this invocation survive known-abort retries. Revision
/// observations can refresh, but a reusable name cannot retarget the request.
#[derive(Clone, Default)]
pub struct RequestTableBindings {
    tables: Arc<Mutex<Vec<(TableName, TableHistoryId)>>>,
}

impl RequestTableBindings {
    /// Pin every stamp already read in the native snapshot, even when stale
    /// preparation or another name rejects the attempt. Cache candidates do
    /// not select an ID; the authoritative observations do.
    pub(crate) fn confirm_observations<'a>(
        &self,
        cache: &TableMetadataCache,
        observations: impl IntoIterator<Item = CatalogConfirmation<'a>>,
    ) -> StorageResult<()> {
        let mut identity_error = None;
        let mut confirmation_error = None;
        for observation in observations {
            let confirmation =
                cache.confirm_stamp(observation.name, observation.prepared, observation.observed);
            if confirmation_error.is_none() {
                confirmation_error = confirmation.err();
            }
            if let Some(stamp) = observation.observed {
                let identity = self.observe(observation.name, stamp.history_id);
                if identity_error.is_none() {
                    identity_error = identity.err();
                }
            }
        }
        match identity_error.or(confirmation_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn pin_stamp(
        &self,
        name: &TableName,
        stamp: Option<CatalogStamp>,
    ) -> StorageResult<CatalogStamp> {
        let stamp = stamp.ok_or_else(|| StorageError::table_not_found(name))?;
        self.observe(name, stamp.history_id)?;
        Ok(stamp)
    }

    pub fn observe(&self, name: &TableName, history_id: TableHistoryId) -> StorageResult<()> {
        let mut tables = self
            .tables
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some((_, expected)) = tables.iter().find(|(bound, _)| bound == name) {
            return if *expected == history_id {
                Ok(())
            } else {
                Err(StorageError::table_not_found(name))
            };
        }
        tables.push((name.clone(), history_id));
        Ok(())
    }
}
