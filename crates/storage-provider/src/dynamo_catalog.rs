//! Authoritative table binding shared by SQL and KV Dynamo operations.
//!
//! A cache entry is only a candidate. Each native attempt checks its catalog
//! version or protected table stamp before use, including remote changes.
//! Publication happens after the native attempt succeeds.

use std::{
    collections::HashMap,
    future::Future,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use lru_ttl_cache::LruTtlCache;
use storage_types::{
    BoundTableInfo, CatalogVersion, Exact, StorageError, StorageResult, TableHistoryId, TableName,
};

use crate::{
    SemanticDecision, dynamo_table_bindings::RequestTableBindings, table_info_cache_config,
};

/// Catalog identity changes on table recreation; revision changes on every
/// schema, status, stream, GSI, or TTL configuration change.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CatalogStamp {
    pub history_id: TableHistoryId,
    pub revision: NonZeroU64,
}

impl RequestTableBindings {
    /// Resolve and pin an authoritative catalog observation before dependent
    /// I/O.
    pub async fn resolve_table<A: CatalogAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        name: &TableName,
    ) -> StorageResult<(Arc<BoundTableInfo>, Option<CachePublication>)> {
        let lookup = cache.lookup(name);
        let version = attempt.read_catalog_version().await?;
        let observed = lookup.observe_stamp(attempt, version).await?;
        let stamp = self.pin_stamp(name, observed)?;
        let (prepared, publication) = lookup.resolve(attempt, stamp, version).await?;
        Ok((prepared.table, publication))
    }
}

/// Candidate metadata for preparation; authority requires a current native
/// version match or a protected table stamp.
#[derive(Clone)]
pub(crate) struct PreparedTable {
    pub(crate) table: Arc<BoundTableInfo>,
    pub(crate) stamp: CatalogStamp,
    pub(crate) catalog_version: Option<CatalogVersion>,
}

impl PreparedTable {
    pub(crate) fn read_candidate(&self) -> crate::TableReadCandidate<'_> {
        crate::TableReadCandidate {
            table: &self.table,
            stamp: self.stamp,
            catalog_version: self.catalog_version,
        }
    }

    pub(crate) fn cache_update_for_version(
        &self,
        name: &TableName,
        version: Option<CatalogVersion>,
        generation: u64,
    ) -> Option<CachePublication> {
        (self.catalog_version != version).then(|| CachePublication {
            entry: CacheEntry::Table(
                name.clone(),
                Self {
                    catalog_version: version,
                    ..self.clone()
                },
            ),
            generation,
        })
    }
}

/// One bounded cache shared by the local Dynamo operation owner. TTL and
/// capacity bound memory; native catalog authority provides validity.
pub struct TableMetadataCache {
    entries: LruTtlCache<TableName, PreparedTable>,
    /// Bumped by every invalidation. Readers load it without a lock; writers
    /// (`publish`, `invalidate`) hold `routes` so that a publication check and
    /// its insert are atomic against an invalidation.
    generation: AtomicU64,
    routes: Mutex<CacheRoutes>,
}

impl TableMetadataCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: LruTtlCache::new(table_info_cache_config()),
            generation: AtomicU64::new(0),
            routes: Mutex::new(CacheRoutes::default()),
        }
    }

    /// A cache whose entries live for `ttl`. The cache measures TTL on the
    /// machine clock; a deterministic simulation passes a TTL longer than the
    /// run so that hits do not depend on real elapsed time.
    #[must_use]
    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            entries: LruTtlCache::new(table_info_cache_config().with_ttl(ttl)),
            generation: AtomicU64::new(0),
            routes: Mutex::new(CacheRoutes::default()),
        }
    }

    pub(crate) fn confirm_stamp(
        &self,
        name: &TableName,
        expected: Option<CatalogStamp>,
        actual: Option<CatalogStamp>,
    ) -> StorageResult<()> {
        if expected == actual {
            return Ok(());
        }
        self.invalidate(name);
        Err(storage_types::StorageError::logical_conflict(
            "prepared table metadata changed",
        ))
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn candidate(&self, name: &TableName) -> (Option<PreparedTable>, u64) {
        // Generation first: an invalidation removes the entry before it bumps
        // the generation, so a reader that sees a new generation sees no
        // entry removed by that invalidation.
        let generation = self.generation();
        (self.entries.get(name), generation)
    }

    fn lookup<'a>(&self, name: &'a TableName) -> CatalogLookup<'a> {
        let (candidate, generation) = self.candidate(name);
        CatalogLookup {
            name,
            candidate,
            generation,
        }
    }

    pub(crate) fn prepared_candidate(&self, name: &TableName) -> Option<PreparedTable> {
        self.candidate(name).0
    }

    /// Call only after the native attempt that loaded this table succeeded.
    /// A local catalog invalidation since the load rejects stale publication.
    pub fn publish(&self, publications: impl IntoIterator<Item = CachePublication>) {
        let mut publications = publications.into_iter();
        let Some(first) = publications.next() else {
            return;
        };
        let mut state = self
            .routes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for publication in std::iter::once(first).chain(publications) {
            if self.generation() == publication.generation {
                match publication.entry {
                    CacheEntry::Table(name, table) => {
                        self.entries.insert(name, table);
                    }
                    CacheEntry::Routes(version, routes) => {
                        state.routes = Some((version, routes));
                    }
                }
            }
        }
    }

    /// Acknowledged native metadata changes also invalidate topology-only
    /// candidates.
    pub fn invalidate_routes(&self) {
        let mut state = self
            .routes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.generation.fetch_add(1, Ordering::AcqRel);
        state.routes = None;
    }

    /// Invalidate after a local catalog mutation commits. Other processes
    /// are detected by the durable stamp check inside each attempt.
    pub fn invalidate(&self, name: &TableName) {
        let mut state = self
            .routes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.entries.remove(name);
        self.generation.fetch_add(1, Ordering::AcqRel);
        state.routes = None;
    }
}

impl Default for TableMetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Deferred cache write. Returning this from an attempt does not publish it;
/// the operation owner publishes it only after acknowledged settlement.
pub struct CachePublication {
    entry: CacheEntry,
    generation: u64,
}

enum CacheEntry {
    Table(TableName, PreparedTable),
    Routes(
        CatalogVersion,
        Arc<stream_provider::SystemStreamLineageCatalog>,
    ),
}

#[derive(Default)]
struct CacheRoutes {
    routes: Option<(
        CatalogVersion,
        Arc<stream_provider::SystemStreamLineageCatalog>,
    )>,
}

/// Select checked immutable routes once per native write attempt. An absent
/// token never authorizes reuse, including after this attempt changes metadata.
pub async fn prepare_writer_routes_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: Option<&TableMetadataCache>,
) -> StorageResult<Option<CachePublication>> {
    let version = attempt.read_catalog_version().await?;
    prepare_writer_routes_for_version(attempt, cache, version).await
}

pub(crate) async fn prepare_writer_routes_for_version<A: CatalogAttempt + ?Sized>(
    attempt: &mut A,
    cache: Option<&TableMetadataCache>,
    version: Option<CatalogVersion>,
) -> StorageResult<Option<CachePublication>> {
    let (candidate, generation) = cache
        .map(|cache| {
            let state = cache
                .routes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            (state.routes.clone(), cache.generation())
        })
        .unwrap_or((None, 0));
    if let Some((candidate_version, routes)) = candidate
        && Some(candidate_version) == version
    {
        attempt.protect_writer_routes()?;
        attempt.install_writer_routes(routes)?;
        return Ok(None);
    }
    let Some(routes) = attempt.read_writer_routes().await? else {
        return Ok(None);
    };
    attempt.install_writer_routes(Arc::clone(&routes))?;
    Ok(version
        .filter(|_| cache.is_some())
        .map(|version| CachePublication {
            entry: CacheEntry::Routes(version, routes),
            generation,
        }))
}

/// SQL/KV family lowerers implement these reads on their lent native attempt.
/// A write attempt protects catalog authority against concurrent mutation.
/// Readers use one snapshot; writers protect the catalog until settlement.
pub trait CatalogAttempt: Send {
    /// Return this attempt's protected native catalog version, if available.
    /// None always requires the protected table stamp path.
    fn read_catalog_version(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<CatalogVersion>>> + Send {
        async { Ok(None) }
    }

    /// PERF-FDB-1. Ask the engine to read this attempt's catalog version in
    /// the same native wave as the next data read. `true`: the next data wave
    /// carries it and `pipelined_catalog_version` returns it afterwards.
    /// `false` (default): nothing changes and the caller keeps the
    /// version-first order. No I/O. Call it immediately before the data read.
    ///
    /// `expected`: the version every candidate of the read was cached at.
    /// The engine must not decode pipelined data before comparing its
    /// observation with `expected`; on a difference it drops the raw data
    /// and fails the read without rejecting the attempt.
    fn pipeline_catalog_version(&mut self, expected: CatalogVersion) -> StorageResult<bool> {
        let _ = expected;
        Ok(false)
    }

    /// The catalog version that a data wave observed after
    /// `pipeline_catalog_version`: `Some(observation)` once a data wave
    /// carried it (`Some(None)`: the native key is absent), `None` when no
    /// data wave did. No I/O; it also answers after a failed data read, whose
    /// settlement no longer allows a native call.
    fn pipelined_catalog_version(&mut self) -> Option<Option<CatalogVersion>> {
        None
    }

    /// Protect the stamps of `names`, trusted from a cache validated at this
    /// attempt's catalog version, against a later change until settlement,
    /// without reading them. Required wherever a write attempt skips a stamp
    /// read: the catalog version itself is only a hint on engines that read
    /// it without a conflict (FoundationDB). No I/O.
    fn protect_catalog_stamps(&mut self, names: &[&TableName]) -> StorageResult<()> {
        let _ = names;
        Ok(())
    }

    /// As `protect_catalog_stamps`, for writer routes trusted from the cache.
    fn protect_writer_routes(&mut self) -> StorageResult<()> {
        Ok(())
    }

    fn read_writer_routes(
        &mut self,
    ) -> impl Future<
        Output = StorageResult<Option<Arc<stream_provider::SystemStreamLineageCatalog>>>,
    > + Send {
        async { Ok(None) }
    }

    fn install_writer_routes(
        &mut self,
        _routes: Arc<stream_provider::SystemStreamLineageCatalog>,
    ) -> StorageResult<()> {
        Ok(())
    }

    /// Persist a permanent external-cleanup requirement and change the catalog
    /// revision in this attempt. Capture/control installation conflicts with
    /// previously bound writers. Ordinary metadata updates cannot clear it.
    /// A record that is absent, deleted, of another incarnation or at another
    /// revision is a `StorageError::write_miss("catalog_barrier",
    /// MissExpectation::Revision)`; the caller decides the public answer after
    /// a fresh read.
    fn store_external_cleanup_barrier(
        &mut self,
        name: &TableName,
        expected: CatalogStamp,
    ) -> impl Future<Output = StorageResult<CatalogStamp>> + Send;

    fn read_catalog_stamp(
        &mut self,
        name: &TableName,
    ) -> impl Future<Output = StorageResult<Option<CatalogStamp>>> + Send;

    /// Declare, before any item lock of this attempt, that the attempt will
    /// change the catalog binding of `name`. Families without a catalog lock
    /// detect the change through their native conflict fence.
    fn lock_catalog_for_change(
        &mut self,
        name: &TableName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        let _ = name;
        async { Ok(()) }
    }

    /// Return exactly the requested incarnation and revision. A concurrent
    /// change must conflict or cause an error, never return a different table.
    fn read_table_at_stamp(
        &mut self,
        name: &TableName,
        stamp: CatalogStamp,
    ) -> impl Future<Output = StorageResult<Arc<BoundTableInfo>>> + Send;

    /// Batch independent name lookups on one native attempt.
    fn read_catalog_stamps(
        &mut self,
        names: &[TableName],
    ) -> impl Future<Output = StorageResult<Exact<Option<CatalogStamp>>>> + Send;

    /// Batch metadata loads once their names have yielded durable IDs.
    fn read_tables_at_stamps(
        &mut self,
        requests: &[(TableName, CatalogStamp)],
    ) -> impl Future<Output = StorageResult<Exact<Arc<BoundTableInfo>>>> + Send;
}

/// One authority observation covers warm and cold members of one native
/// attempt.
pub(crate) async fn observe_catalog_stamps<'a, A: CatalogAttempt + ?Sized>(
    attempt: &mut A,
    candidates: impl ExactSizeIterator<Item = (&'a TableName, Option<crate::TableReadCandidate<'a>>)>
    + Send,
    version: Option<CatalogVersion>,
) -> StorageResult<Exact<Option<CatalogStamp>>> {
    let mut stamps = vec![None; candidates.len()];
    let mut missing = Vec::new();
    let mut slots = Vec::new();
    let mut trusted = Vec::new();
    for (slot, (name, candidate)) in candidates.enumerate() {
        if let Some(candidate) = candidate
            && version.is_some()
            && candidate.catalog_version == version
        {
            stamps[slot] = Some(candidate.stamp);
            trusted.push(name);
        } else {
            missing.push(name.clone());
            slots.push(slot);
        }
    }
    if !trusted.is_empty() {
        attempt.protect_catalog_stamps(&trusted)?;
    }
    if !missing.is_empty() {
        let observed = attempt.read_catalog_stamps(&missing).await?;
        for (slot, stamp) in observed.zip(slots) {
            stamps[slot] = stamp;
        }
    }
    let expected = stamps.len();
    Exact::try_new(expected, stamps)
}

/// Resolve an entire request's table set in two native read batches. A name
/// lookup must precede the metadata-key read for KV's persisted numeric IDs.
pub(crate) async fn resolve_prepared_tables_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    names: &[TableName],
    bindings: &RequestTableBindings,
) -> StorageResult<Exact<StorageResult<(PreparedTable, Option<CachePublication>)>>> {
    if names.is_empty() {
        return Ok(Exact::empty());
    }
    let mut unique_slots = HashMap::with_capacity(names.len());
    let mut unique_names = Vec::new();
    let mut request_slots = Vec::with_capacity(names.len());
    for name in names {
        let slot = *unique_slots.entry(name).or_insert_with(|| {
            let slot = unique_names.len();
            unique_names.push(name.clone());
            slot
        });
        request_slots.push(slot);
    }
    let candidates = unique_names
        .iter()
        .map(|name| cache.candidate(name))
        .collect::<Vec<_>>();
    let version = attempt.read_catalog_version().await?;
    let stamps = observe_catalog_stamps(
        attempt,
        unique_names
            .iter()
            .zip(&candidates)
            .map(|(name, (candidate, _))| {
                (name, candidate.as_ref().map(PreparedTable::read_candidate))
            }),
        version,
    )
    .await?;
    let mut resolved = (0..unique_names.len()).map(|_| None).collect::<Vec<_>>();
    let mut loads = Vec::new();
    let mut load_slots = Vec::new();
    for (slot, ((name, stamp), (candidate, _))) in
        unique_names.iter().zip(stamps).zip(&candidates).enumerate()
    {
        let stamp = match bindings.pin_stamp(name, stamp) {
            Ok(stamp) => stamp,
            Err(error) => {
                resolved[slot] = Some(Err(error));
                continue;
            }
        };
        if let Some(candidate) = candidate
            && candidate.stamp == stamp
        {
            resolved[slot] = Some(Ok((
                PreparedTable {
                    table: Arc::clone(&candidate.table),
                    stamp,
                    catalog_version: version,
                },
                candidate.cache_update_for_version(name, version, candidates[slot].1),
            )));
        } else {
            loads.push((name.clone(), stamp));
            load_slots.push(slot);
        }
    }
    let tables = if loads.is_empty() {
        Exact::empty()
    } else {
        attempt.read_tables_at_stamps(&loads).await?
    };
    for ((slot, table), (_, stamp)) in load_slots.into_iter().zip(tables).zip(loads) {
        if table.history_id != stamp.history_id {
            return Err(StorageError::internal(
                "catalog table incarnation does not match stamp",
            ));
        }
        let publication = CachePublication {
            entry: CacheEntry::Table(
                unique_names[slot].clone(),
                PreparedTable {
                    stamp,
                    table: Arc::clone(&table),
                    catalog_version: version,
                },
            ),
            generation: candidates[slot].1,
        };
        resolved[slot] = Some(Ok((
            PreparedTable {
                table,
                stamp,
                catalog_version: version,
            },
            Some(publication),
        )));
    }
    let results = names
        .iter()
        .zip(request_slots)
        .map(|(name, slot)| {
            let result = resolved[slot]
                .as_mut()
                .ok_or_else(|| StorageError::internal("catalog batch left a table unresolved"))?;
            Ok(match result {
                Ok((table, publication)) => Ok((table.clone(), publication.take())),
                Err(_) => Err(StorageError::table_not_found(name)),
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Exact::try_new(names.len(), results)
}

pub async fn resolve_tables_batch_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    names: &[TableName],
    bindings: &RequestTableBindings,
) -> StorageResult<Exact<StorageResult<(Arc<BoundTableInfo>, Option<CachePublication>)>>> {
    Ok(
        resolve_prepared_tables_in_attempt(attempt, cache, names, bindings)
            .await?
            .map(|result| result.map(|(prepared, publication)| (prepared.table, publication))),
    )
}

/// Resolve a table inside the operation attempt. The returned publication
/// remains private until the native owner acknowledges the attempt.
pub(crate) async fn resolve_prepared_table_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
    bindings: &RequestTableBindings,
) -> StorageResult<SemanticDecision<(PreparedTable, Option<CachePublication>)>> {
    let lookup = cache.lookup(name);
    let version = attempt.read_catalog_version().await?;
    let stamp = lookup.observe_stamp(attempt, version).await?;
    let stamp = match bindings.pin_stamp(name, stamp) {
        Ok(stamp) => stamp,
        Err(error) => return Ok(SemanticDecision::Rejected(error)),
    };
    Ok(SemanticDecision::Accepted(
        lookup.resolve(attempt, stamp, version).await?,
    ))
}

/// Cache generation is captured before the stamp read. The ID binding owner
/// can observe that stamp before metadata I/O, while publication still rejects
/// an invalidation that raced with this lookup.
struct CatalogLookup<'a> {
    name: &'a TableName,
    candidate: Option<PreparedTable>,
    generation: u64,
}

impl CatalogLookup<'_> {
    async fn observe_stamp(
        &self,
        attempt: &mut impl CatalogAttempt,
        version: Option<CatalogVersion>,
    ) -> StorageResult<Option<CatalogStamp>> {
        if let Some(candidate) = &self.candidate
            && version.is_some()
            && candidate.catalog_version == version
        {
            attempt.protect_catalog_stamps(&[self.name])?;
            return Ok(Some(candidate.stamp));
        }
        attempt.read_catalog_stamp(self.name).await
    }

    async fn resolve(
        self,
        attempt: &mut impl CatalogAttempt,
        stamp: CatalogStamp,
        version: Option<CatalogVersion>,
    ) -> StorageResult<(PreparedTable, Option<CachePublication>)> {
        if let Some(candidate) = &self.candidate
            && candidate.stamp == stamp
        {
            return Ok((
                PreparedTable {
                    table: Arc::clone(&candidate.table),
                    stamp,
                    catalog_version: version,
                },
                candidate.cache_update_for_version(self.name, version, self.generation),
            ));
        }
        let table = attempt.read_table_at_stamp(self.name, stamp).await?;
        if table.history_id != stamp.history_id {
            return Err(StorageError::internal(
                "catalog table incarnation does not match stamp",
            ));
        }
        let publication = CachePublication {
            entry: CacheEntry::Table(
                self.name.clone(),
                PreparedTable {
                    stamp,
                    table: Arc::clone(&table),
                    catalog_version: version,
                },
            ),
            generation: self.generation,
        };
        Ok((
            PreparedTable {
                table,
                stamp,
                catalog_version: version,
            },
            Some(publication),
        ))
    }
}

pub async fn resolve_table_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
) -> StorageResult<(Arc<BoundTableInfo>, Option<CachePublication>)> {
    let (prepared, publication) =
        resolve_prepared_table_in_attempt(attempt, cache, name, &RequestTableBindings::default())
            .await?
            .into_result()?;
    Ok((prepared.table, publication))
}
