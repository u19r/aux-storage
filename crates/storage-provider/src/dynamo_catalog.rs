//! Authoritative table binding shared by SQL and KV Dynamo operations.
//!
//! A cache entry is only a candidate. Every native attempt checks its durable
//! stamp before using the entry, including when another process changed the
//! table. Publication happens after the native attempt succeeds.

use std::{
    collections::HashMap,
    future::Future,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};

use lru_ttl_cache::LruTtlCache;
use storage_common::TtlConfigRecord;
use storage_types::{BoundTableInfo, StorageError, StorageResult, TableHistoryId, TableName};

use crate::{SemanticDecision, TABLE_INFO_CACHE_TTL_SECONDS, table_info_cache_config};

/// Catalog identity changes on table recreation; revision changes on every
/// schema, status, stream, GSI, or TTL configuration change.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CatalogStamp {
    pub history_id: TableHistoryId,
    pub revision: NonZeroU64,
}

/// Candidate metadata for preparation; authority requires an in-attempt stamp
/// check.
#[derive(Clone)]
pub(crate) struct PreparedTable {
    pub(crate) table: Arc<BoundTableInfo>,
    pub(crate) stamp: CatalogStamp,
}

impl PreparedTable {
    pub(crate) async fn confirm<A: CatalogAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
    ) -> StorageResult<()> {
        let name = &self.table.info.table_name;
        cache.confirm_stamp(
            name,
            Some(self.stamp),
            attempt.read_catalog_stamp(name).await?,
        )
    }
}

#[derive(Clone)]
struct CachedTtlPolicy {
    history_id: TableHistoryId,
    config: Option<TtlConfigRecord>,
}

/// A cache miss carries the local invalidation generation observed before the
/// caller's native read. The caller may batch that read with other independent
/// prerequisites, then publish only if the generation still matches.
pub enum TtlPolicyLookup {
    Hit(Option<TtlConfigRecord>),
    Miss(u64),
}

/// One bounded cache shared by the local Dynamo operation owner. TTL and
/// capacity bound memory; the native stamp check, not TTL, provides validity.
pub struct TableMetadataCache {
    entries: LruTtlCache<TableName, PreparedTable>,
    ttl_policies: LruTtlCache<TableName, CachedTtlPolicy>,
    generation: Mutex<u64>,
}

impl TableMetadataCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: LruTtlCache::new(table_info_cache_config()),
            ttl_policies: LruTtlCache::new(table_info_cache_config()),
            generation: Mutex::new(0),
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
        Err(storage_types::StorageEnum::TransactionConflict {
            message: "prepared table metadata changed".to_owned(),
        }
        .into())
    }

    fn candidate(&self, name: &TableName) -> (Option<PreparedTable>, u64) {
        let generation = self
            .generation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        (self.entries.get(name), *generation)
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
        let generation = self
            .generation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for publication in std::iter::once(first).chain(publications) {
            if *generation == publication.generation {
                self.entries.insert(publication.name, publication.entry);
            }
        }
    }

    /// Invalidate after a local catalog mutation commits. Other processes
    /// are detected by the durable stamp check inside each attempt.
    pub fn invalidate(&self, name: &TableName) {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *generation = generation.wrapping_add(1);
        self.entries.remove(name);
        self.ttl_policies.remove(name);
    }

    /// TTL tracking tolerates a bounded stale policy across API nodes. Table
    /// binding still uses the protected durable stamp on every native attempt.
    pub fn lookup_ttl_policy(&self, table: &BoundTableInfo) -> TtlPolicyLookup {
        let name = &table.info.table_name;
        let generation = {
            let generation = self
                .generation
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(cached) = self.ttl_policies.get(name)
                && cached.history_id == table.history_id
            {
                return TtlPolicyLookup::Hit(cached.config);
            }
            *generation
        };
        TtlPolicyLookup::Miss(generation)
    }

    pub fn publish_ttl_policy(
        &self,
        table: &BoundTableInfo,
        config: Option<TtlConfigRecord>,
        generation: u64,
    ) {
        let current = self
            .generation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if *current == generation {
            self.ttl_policies.insert_with_ttl(
                table.info.table_name.clone(),
                CachedTtlPolicy {
                    history_id: table.history_id,
                    config,
                },
                Duration::from_secs(TABLE_INFO_CACHE_TTL_SECONDS),
            );
        }
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
    name: TableName,
    entry: PreparedTable,
    generation: u64,
}

/// SQL/KV family lowerers implement these reads on their lent native attempt.
/// A write attempt protects the stamp against concurrent catalog mutation.
/// Readers use one snapshot; writers protect the catalog until settlement.
pub trait CatalogAttempt: Send {
    /// Change only the catalog revision in this same attempt. Capture/control
    /// installation uses this barrier to conflict with previously bound
    /// writers.
    fn advance_catalog_stamp(
        &mut self,
        name: &TableName,
        expected: CatalogStamp,
    ) -> impl Future<Output = StorageResult<CatalogStamp>> + Send;

    fn read_catalog_stamp(
        &mut self,
        name: &TableName,
    ) -> impl Future<Output = StorageResult<Option<CatalogStamp>>> + Send;

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
    ) -> impl Future<Output = StorageResult<Vec<Option<CatalogStamp>>>> + Send;

    /// Batch metadata loads once their names have yielded durable IDs.
    fn read_tables_at_stamps(
        &mut self,
        requests: &[(TableName, CatalogStamp)],
    ) -> impl Future<Output = StorageResult<Vec<Arc<BoundTableInfo>>>> + Send;
}

/// Resolve an entire request's table set in two native read batches. A name
/// lookup must precede the metadata-key read for KV's persisted numeric IDs.
pub(crate) async fn resolve_prepared_tables_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    names: &[TableName],
) -> StorageResult<Vec<StorageResult<(PreparedTable, Option<CachePublication>)>>> {
    if names.is_empty() {
        return Ok(Vec::new());
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
    let stamps = attempt.read_catalog_stamps(&unique_names).await?;
    if stamps.len() != unique_names.len() {
        return Err(StorageError::internal(
            "catalog stamp batch returned the wrong count",
        ));
    }
    let mut resolved = (0..unique_names.len()).map(|_| None).collect::<Vec<_>>();
    let mut loads = Vec::new();
    let mut load_slots = Vec::new();
    for (slot, ((name, stamp), (candidate, _))) in
        unique_names.iter().zip(stamps).zip(&candidates).enumerate()
    {
        let Some(stamp) = stamp else {
            resolved[slot] = Some(Err(StorageError::table_not_found(name)));
            continue;
        };
        if let Some(candidate) = candidate
            && candidate.stamp == stamp
        {
            resolved[slot] = Some(Ok((
                PreparedTable {
                    table: Arc::clone(&candidate.table),
                    stamp,
                },
                None,
            )));
        } else {
            loads.push((name.clone(), stamp));
            load_slots.push(slot);
        }
    }
    let tables = if loads.is_empty() {
        Vec::new()
    } else {
        attempt.read_tables_at_stamps(&loads).await?
    };
    if tables.len() != loads.len() {
        return Err(StorageError::internal(
            "catalog metadata batch returned the wrong count",
        ));
    }
    for ((slot, table), (_, stamp)) in load_slots.into_iter().zip(tables).zip(loads) {
        if table.history_id != stamp.history_id {
            return Err(StorageError::internal(
                "catalog table incarnation does not match stamp",
            ));
        }
        let publication = CachePublication {
            name: unique_names[slot].clone(),
            entry: PreparedTable {
                stamp,
                table: Arc::clone(&table),
            },
            generation: candidates[slot].1,
        };
        resolved[slot] = Some(Ok((PreparedTable { table, stamp }, Some(publication))));
    }
    names
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
        .collect()
}

pub async fn resolve_tables_batch_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    names: &[TableName],
) -> StorageResult<Vec<StorageResult<(Arc<BoundTableInfo>, Option<CachePublication>)>>> {
    Ok(resolve_prepared_tables_in_attempt(attempt, cache, names)
        .await?
        .into_iter()
        .map(|result| result.map(|(prepared, publication)| (prepared.table, publication)))
        .collect())
}

/// Resolve a table inside the operation attempt. The returned publication
/// remains private until the native owner acknowledges the attempt.
pub(crate) async fn resolve_prepared_table_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
) -> StorageResult<SemanticDecision<(PreparedTable, Option<CachePublication>)>> {
    let (candidate, generation) = cache.candidate(name);
    let Some(stamp) = attempt.read_catalog_stamp(name).await? else {
        return Ok(SemanticDecision::Rejected(StorageError::table_not_found(
            name,
        )));
    };
    if let Some(candidate) = candidate
        && candidate.stamp == stamp
    {
        return Ok(SemanticDecision::Accepted((
            PreparedTable {
                table: candidate.table,
                stamp,
            },
            None,
        )));
    }
    let table = attempt.read_table_at_stamp(name, stamp).await?;
    if table.history_id != stamp.history_id {
        return Err(StorageError::internal(
            "catalog table incarnation does not match stamp",
        ));
    }
    let publication = CachePublication {
        name: name.clone(),
        entry: PreparedTable {
            stamp,
            table: Arc::clone(&table),
        },
        generation,
    };
    Ok(SemanticDecision::Accepted((
        PreparedTable { table, stamp },
        Some(publication),
    )))
}

pub async fn resolve_table_in_attempt<A: CatalogAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &TableName,
) -> StorageResult<(Arc<BoundTableInfo>, Option<CachePublication>)> {
    let (prepared, publication) = resolve_prepared_table_in_attempt(attempt, cache, name)
        .await?
        .into_result()?;
    Ok((prepared.table, publication))
}
