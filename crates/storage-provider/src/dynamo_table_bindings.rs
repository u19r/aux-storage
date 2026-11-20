//! Invocation identity survives native aborts independently of cached metadata.
use std::sync::{Arc, Mutex};

use storage_types::{BoundTableInfo, StorageError, StorageResult, TableHistoryId, TableName};

use crate::{CatalogStamp, TableMetadataCache};

pub(crate) struct CatalogConfirmation<'a> {
    pub(crate) name: &'a TableName,
    pub(crate) prepared: Option<CatalogStamp>,
    pub(crate) observed: Option<CatalogStamp>,
}

type BoundTables = Vec<(TableName, TableHistoryId)>;

/// IDs accepted by this invocation survive known-abort retries. Revision
/// observations can refresh, but a reusable name cannot retarget the request.
#[derive(Clone, Default)]
pub struct RequestTableBindings {
    tables: Arc<Mutex<BoundTables>>,
    provisional: Option<Arc<Mutex<BoundTables>>>,
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
        self.confirm_write_observations(cache, observations)?
            .into_result()
    }

    pub(crate) fn for_write_attempt(&self, policy: storage_types::ReadVersionPolicy) -> Self {
        Self {
            tables: Arc::clone(&self.tables),
            provisional: (policy == storage_types::ReadVersionPolicy::Cached)
                .then(|| Arc::new(Mutex::new(Vec::new()))),
        }
    }

    /// Only acknowledged native authority can promote a cached observation.
    pub(crate) fn promote(&self) {
        let Some(provisional) = &self.provisional else {
            return;
        };
        let mut tables = self
            .tables
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let candidates = provisional
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for (name, history_id) in candidates.iter() {
            if !tables.iter().any(|(bound, _)| bound == name) {
                tables.push((name.clone(), *history_id));
            }
        }
    }

    pub(crate) fn confirm_write_observations<'a>(
        &self,
        cache: &TableMetadataCache,
        observations: impl IntoIterator<Item = CatalogConfirmation<'a>>,
    ) -> StorageResult<crate::SemanticDecision<()>> {
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
        let has_identity_error = identity_error.is_some();
        match identity_error.or(confirmation_error) {
            Some(error) if has_identity_error => Ok(crate::SemanticDecision::Rejected(error)),
            Some(error) => Err(error),
            None => Ok(crate::SemanticDecision::Accepted(())),
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
        if let Some(provisional) = &self.provisional {
            let mut candidates = provisional
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if !candidates.iter().any(|(bound, _)| bound == name) {
                candidates.push((name.clone(), history_id));
            }
        } else {
            tables.push((name.clone(), history_id));
        }
        Ok(())
    }
}

/// The catalog authority of one Dynamo operation: the shared metadata cache
/// and the table identities this invocation bound, which survive known-abort
/// retries. A try's catalog publications are applied only after its attempt
/// answered `Ok`; after a missing relation the try's tables are invalidated;
/// any other failure keeps the cache.
#[derive(Clone)]
pub(crate) struct CatalogSession {
    cache: Arc<TableMetadataCache>,
    bindings: RequestTableBindings,
}

impl CatalogSession {
    pub(crate) fn new(cache: Arc<TableMetadataCache>) -> Self {
        Self {
            cache,
            bindings: RequestTableBindings::default(),
        }
    }

    pub(crate) fn cache(&self) -> &Arc<TableMetadataCache> {
        &self.cache
    }

    pub(crate) fn bindings(&self) -> &RequestTableBindings {
        &self.bindings
    }

    /// Resolve and pin one table inside the caller's attempt.
    pub(crate) async fn resolve_table<A: crate::dynamo_catalog::CatalogAttempt>(
        &self,
        attempt: &mut A,
        name: &TableName,
    ) -> StorageResult<(Arc<BoundTableInfo>, Option<crate::CachePublication>)> {
        self.bindings
            .resolve_table(attempt, &self.cache, name)
            .await
    }

    /// Settle one try: publish after `Ok`; after a missing relation drop the
    /// cache entries of `names` so the fresh try resolves them again.
    pub(crate) fn settle<'a, R, Publications>(
        &self,
        result: StorageResult<(R, Publications)>,
        names: impl IntoIterator<Item = &'a TableName>,
    ) -> StorageResult<R>
    where
        Publications: IntoIterator<Item = crate::CachePublication>,
    {
        match result {
            Ok((value, publications)) => {
                self.cache.publish(publications);
                Ok(value)
            }
            Err(error) => {
                crate::dynamo_read_execution::invalidate_on_missing_relation(
                    &self.cache,
                    &error,
                    names,
                );
                Err(error)
            }
        }
    }
}
