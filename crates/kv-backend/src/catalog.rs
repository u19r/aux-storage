//! Persisted KV table-name lookup and metadata layout, shared by both drivers.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, CatalogNamePage, CatalogStamp, CatalogStreamPage,
    CreateTableLayout, ListStreamsAttempt, ListTablesAttempt, TableMetadataCache,
    TableNameReservation, TableReclaimPhase, TableReclaimTarget, name_locked_until,
};
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, StoredTableInfo, TableHistoryId,
    TableName, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{
        INITIAL_REVISION, LOOKUP_FAMILY, METADATA_FAMILY, TABLE_ID_ALLOCATOR_KEY,
        TABLE_NAME_LOCK_FAMILY, TABLE_STREAM_FAMILY,
    },
};

/// The stored identity also owns physical index IDs.
/// Item and index lowerers reuse this one decoded identity during an attempt.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub struct KvTableIdentity {
    pub table_id: u32,
    pub revision: NonZeroU64,
    pub table_name: TableName,
    pub indexes: Vec<KvIndexIdentity>,
    pub index_id_high_water: u16,
    pub deleted: bool,
}

#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub struct KvIndexIdentity {
    pub index_id: u16,
    pub index_name: IndexName,
    pub index_kind: KvIndexKind,
}

// KV stores stable numeric index IDs in physical keys. SQL identifies its
// indexes by relation name, so this persisted kind belongs only to KV.
#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum KvIndexKind {
    UserGsi,
    HiddenTtl,
}

/// Metadata retains the identity after DeleteTable removes its public name
/// binding, so bounded reclamation can still identify the old incarnation.
#[derive(Deserialize, Serialize)]
struct StoredCatalogRecord {
    identity: KvTableIdentity,
    table_info: StoredTableInfo,
}

/// One KV catalog view bound to the caller's native transaction or snapshot.
/// Name lookup binds physical identity and revision before a logical table
/// image is loaded or reused from the metadata cache.
pub struct KvCatalogAttempt<'a, A> {
    pub(crate) attempt: &'a mut A,
    pub(crate) protection: ReadProtection,
    identities: Vec<KvTableIdentity>,
    dynamo_mutations: Vec<crate::KvMutation>,
    pub(crate) projection_tree: crate::gsi::TreeChanges,
    pub(crate) dynamo_history: Option<crate::stream_history::KvSystemWriteState>,
    pub(crate) metadata_cache: Option<Arc<TableMetadataCache>>,
    pub(crate) prepared_writes: StorageResult<crate::write_preparation::KvPreparedWrites>,
}

impl<'a, A: KvAttempt> KvCatalogAttempt<'a, A> {
    pub fn new(attempt: &'a mut A, protection: ReadProtection) -> Self {
        Self {
            attempt,
            protection,
            identities: Vec::new(),
            dynamo_mutations: Vec::new(),
            projection_tree: Default::default(),
            dynamo_history: None,
            metadata_cache: None,
            prepared_writes: Ok(Default::default()),
        }
    }

    pub(crate) fn with_metadata_cache(mut self, cache: Arc<TableMetadataCache>) -> Self {
        self.metadata_cache = Some(cache);
        self
    }

    pub(crate) fn append_dynamo_mutations(
        &mut self,
        changes: Vec<crate::KvMutation>,
    ) -> StorageResult<()> {
        if self.dynamo_mutations.try_reserve(changes.len()).is_err() {
            let error = storage_types::InfrastructureResultKind::NativeCapacityRejected.into();
            self.attempt
                .root_attempt()
                .settlement()
                .reject_error(&error);
            return Err(error);
        }
        self.dynamo_mutations.extend(changes);
        Ok(())
    }

    pub(crate) fn flush_dynamo_mutations(&mut self) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            let mut changes = std::mem::take(&mut catalog.dynamo_mutations);
            let projections = std::mem::take(&mut catalog.projection_tree).into_mutations()?;
            // Keep allocation history and commit ordinals for every write in
            // this attempt. FoundationDB's staged sequence stamp is unreadable
            // until commit.
            let history = catalog
                .dynamo_history
                .as_mut()
                .map(|history| history.drain_mutations())
                .transpose()?
                .unwrap_or_default();
            let count = projections
                .len()
                .checked_add(history.len())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes
                .try_reserve(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes.extend(projections);
            changes.extend(history);
            if changes.is_empty() {
                return Ok(());
            }
            catalog.attempt.stage(changes)
        })();
        operation.complete(result)
    }

    /// Available only after the table has been bound in this attempt.
    pub fn identity_for(&self, name: &TableName) -> StorageResult<&KvTableIdentity> {
        self.identities
            .iter()
            .find(|identity| &identity.table_name == name)
            .ok_or_else(|| StorageError::internal("KV table identity is not bound"))
    }

    fn bind_identity(&mut self, identity: KvTableIdentity) {
        if let Some(bound) = self
            .identities
            .iter_mut()
            .find(|bound| bound.table_name == identity.table_name)
        {
            *bound = identity;
        } else {
            self.identities.push(identity);
        }
    }

    fn bind_metadata(
        &mut self,
        name: &TableName,
        stamp: CatalogStamp,
        bytes: &[u8],
    ) -> StorageResult<Arc<BoundTableInfo>> {
        let result = (|| {
            let table_id = table_id_from_stamp(stamp)?;
            let record: StoredCatalogRecord = storage_serde::from_bytes(bytes)
                .map_err(|_| StorageError::internal("KV table metadata is corrupt"))?;
            if record.identity.deleted
                || record.identity.table_id != table_id
                || record.identity.revision != stamp.revision
                || record.identity != *self.identity_for(name)?
                || record.table_info.table_name != *name
            {
                return Err(StorageError::internal(
                    "KV name lookup points to stale metadata",
                ));
            }
            Ok(Arc::new(BoundTableInfo {
                history_id: stamp.history_id,
                info: Arc::new(record.table_info),
            }))
        })();
        result.inspect_err(|error| {
            storage_common::diagnostics::trace_native_failure(
                self.attempt.backend_engine(),
                storage_common::diagnostics::NativeOperation::CatalogBindMetadata,
                error,
            )
        })
    }

    /// Read the name, reservation, and table-ID allocator in one native read.
    pub async fn read_table_layout(
        &mut self,
        table: &StoredTableInfo,
    ) -> StorageResult<CreateTableLayout<(Option<Vec<u8>>, Option<Vec<u8>>)>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let name_state = catalog
                .attempt
                .get_batch(
                    vec![
                        name_lookup_key(&table.table_name),
                        name_lock_key(&table.table_name),
                        TABLE_ID_ALLOCATOR_KEY.to_vec(),
                        crate::stream_write::counter::user_stream_sequence_key(
                            &storage_types::StreamName::table_stream(&table.table_name),
                        )?,
                    ],
                    ReadProtection::Conflict,
                )
                .await?;
            if name_state.len() != 4 {
                return Err(StorageError::internal(
                    "KV name reservation read count is invalid",
                ));
            }
            let mut name_state = name_state.into_iter();
            let active = name_state.next().flatten().is_some();
            let locked_until = name_state
                .next()
                .flatten()
                .as_deref()
                .map(decode_name_lock)
                .transpose()?;
            let allocator = name_state.next().flatten();
            let stream_sequence = name_state.next().flatten();
            let reservation = if active {
                TableNameReservation::Active
            } else if let Some(until) = locked_until {
                TableNameReservation::LockedUntil(until)
            } else {
                TableNameReservation::Available
            };
            Ok(CreateTableLayout {
                reservation,
                seed: (allocator, stream_sequence),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub fn insert_table_layout(
        &mut self,
        table: &StoredTableInfo,
        (allocator, stream_sequence): (Option<Vec<u8>>, Option<Vec<u8>>),
    ) -> StorageResult<KvTableIdentity> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = (|| {
            let table_id = allocator
                .as_deref()
                .map(decode_table_id)
                .transpose()?
                .unwrap_or(1);
            let name_key = name_lookup_key(&table.table_name);
            let next_id = table_id
                .checked_add(1)
                .ok_or_else(|| StorageError::internal("KV table ID space is exhausted"))?;
            let identity = KvTableIdentity::for_new_table(table_id, table)?;
            let record = StoredCatalogRecord {
                identity: identity.clone(),
                table_info: table.clone(),
            };
            let mut changes = vec![
                KvMutation::Put {
                    key: TABLE_ID_ALLOCATOR_KEY.to_vec(),
                    value: next_id.to_be_bytes().to_vec(),
                },
                KvMutation::Put {
                    key: name_key,
                    value: encode_lookup(&identity)?,
                },
                KvMutation::Put {
                    key: metadata_key(table_id),
                    value: storage_serde::to_bytes(&record)?,
                },
            ];
            if stream_sequence.is_none() {
                changes.push(KvMutation::Put {
                    key: crate::stream_write::counter::user_stream_sequence_key(
                        &storage_types::StreamName::table_stream(&table.table_name),
                    )?,
                    value: 1_u64.to_be_bytes().to_vec(),
                });
            }
            if table
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled)
            {
                changes.push(KvMutation::Put {
                    key: table_stream_key(&table.table_name),
                    value: table.created_at.timestamp_millis().to_be_bytes().to_vec(),
                });
            }
            catalog.attempt.stage(changes)?;
            catalog.bind_identity(identity.clone());
            Ok(identity)
        })();
        settlement.complete(result)
    }

    /// Remove the public name binding atomically and retain a tombstone for
    /// bounded background reclamation of this incarnation's physical keys.
    pub(crate) async fn tombstone_table(
        &mut self,
        table: &BoundTableInfo,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut identity = catalog.identity_for(&table.info.table_name)?.clone();
            if TableHistoryId::from(identity.table_id) != table.history_id {
                return Err(StorageError::internal(
                    "KV table identity changed during deletion",
                ));
            }
            crate::gsi::delete_gsi_builds(catalog.attempt, &identity, None).await?;
            identity.deleted = true;
            let record = StoredCatalogRecord {
                identity: identity.clone(),
                table_info: table.info.as_ref().clone(),
            };
            let locked_until = name_locked_until(captured_at);
            let mut changes = Vec::with_capacity(4);
            changes.push(KvMutation::Delete {
                key: name_lookup_key(&table.info.table_name),
            });
            if table
                .info
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled)
            {
                changes.push(KvMutation::Delete {
                    key: table_stream_key(&table.info.table_name),
                });
            }
            changes.push(KvMutation::Put {
                key: name_lock_key(&table.info.table_name),
                value: encode_name_lock(identity.table_id, locked_until),
            });
            changes.push(KvMutation::Put {
                key: metadata_key(identity.table_id),
                value: storage_serde::to_bytes(&record)?,
            });
            changes.push(KvMutation::Put {
                key: crate::table_reclaim::reclaim_key(identity.table_id),
                value: storage_serde::to_bytes(&TableReclaimTarget {
                    history_id: table.history_id,
                    table_name: table.info.table_name.clone(),
                    name_locked_until: locked_until,
                    phase: TableReclaimPhase::MetadataCleanup,
                    gsi_targets: Vec::new(),
                })?,
            });
            catalog.attempt.stage(changes)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn replace_table_layout(
        &mut self,
        current: &BoundTableInfo,
        identity: KvTableIdentity,
        table: StoredTableInfo,
    ) -> StorageResult<BoundTableInfo> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let bound = catalog.identity_for(&current.info.table_name)?;
            if bound.table_id != identity.table_id
                || bound.revision != identity.revision
                || TableHistoryId::from(identity.table_id) != current.history_id
                || identity.table_name != current.info.table_name
                || table.table_name != current.info.table_name
                || identity.deleted
            {
                return Err(StorageError::internal(
                    "KV table update changed its incarnation",
                ));
            }
            let mut identity = identity;
            identity.revision = identity
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| StorageError::internal("KV catalog revision exhausted"))?;
            let record = StoredCatalogRecord {
                identity: identity.clone(),
                table_info: table.clone(),
            };
            let mut changes = vec![
                KvMutation::Put {
                    key: name_lookup_key(&identity.table_name),
                    value: encode_lookup(&identity)?,
                },
                KvMutation::Put {
                    key: metadata_key(identity.table_id),
                    value: storage_serde::to_bytes(&record)?,
                },
            ];
            let was_enabled = current
                .info
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled);
            let is_enabled = table
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled);
            match (was_enabled, is_enabled) {
                (false, true) => changes.push(KvMutation::Put {
                    key: table_stream_key(&table.table_name),
                    value: table.created_at.timestamp_millis().to_be_bytes().to_vec(),
                }),
                (true, false) => changes.push(KvMutation::Delete {
                    key: table_stream_key(&table.table_name),
                }),
                _ => {}
            }
            catalog.attempt.stage(changes)?;
            catalog.bind_identity(identity);
            Ok(BoundTableInfo {
                history_id: current.history_id,
                info: Arc::new(table),
            })
        }
        .await;
        settlement.complete(result)
    }
}

impl KvTableIdentity {
    fn for_new_table(table_id: u32, table: &StoredTableInfo) -> StorageResult<Self> {
        let requested = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default();
        let indexes = requested
            .iter()
            .enumerate()
            .map(|(position, index)| {
                let index_id = u16::try_from(position + 1)
                    .map_err(|_| StorageError::internal("KV GSI identity space exhausted"))?;
                Ok(KvIndexIdentity {
                    index_id,
                    index_name: index.index_name.clone(),
                    index_kind: KvIndexKind::UserGsi,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let index_id_high_water = indexes.last().map_or(0, |index| index.index_id);
        Ok(Self {
            table_id,
            revision: INITIAL_REVISION,
            table_name: table.table_name.clone(),
            indexes,
            index_id_high_water,
            deleted: false,
        })
    }

    /// Keep numeric index identities stable and never reuse a deleted ID.
    pub(crate) fn reconcile_user_indexes(&self, table: &StoredTableInfo) -> StorageResult<Self> {
        if self.table_name != table.table_name || self.deleted {
            return Err(StorageError::internal(
                "KV update has another table identity",
            ));
        }
        let mut next = self.clone();
        let requested = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default();
        next.indexes.retain(|index| {
            index.index_kind != KvIndexKind::UserGsi
                || requested
                    .iter()
                    .any(|candidate| candidate.index_name == index.index_name)
        });
        for index in requested {
            if next.indexes.iter().any(|candidate| {
                candidate.index_kind == KvIndexKind::UserGsi
                    && candidate.index_name == index.index_name
            }) {
                continue;
            }
            next.index_id_high_water = next
                .index_id_high_water
                .checked_add(1)
                .ok_or_else(|| StorageError::internal("KV index identity space exhausted"))?;
            next.indexes.push(KvIndexIdentity {
                index_id: next.index_id_high_water,
                index_name: index.index_name.clone(),
                index_kind: KvIndexKind::UserGsi,
            });
        }
        Ok(next)
    }
}

impl<A: KvAttempt> CatalogAttempt for KvCatalogAttempt<'_, A> {
    async fn advance_catalog_stamp(
        &mut self,
        name: &TableName,
        expected: CatalogStamp,
    ) -> StorageResult<CatalogStamp> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let table_id = table_id_from_stamp(expected)?;
            let key = metadata_key(table_id);
            let bytes = catalog
                .attempt
                .get(&key, ReadProtection::Conflict)
                .await?
                .ok_or_else(|| StorageError::table_not_found(name))?;
            let mut record: StoredCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if record.identity.revision != expected.revision
                || record.identity.table_name != *name
                || record.identity.deleted
                || record.identity.table_id != table_id
            {
                return Err(storage_types::StorageEnum::TransactionConflict {
                    message: "catalog barrier revision changed".into(),
                }
                .into());
            }
            let revision = expected
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| StorageError::internal("catalog revision exhausted"))?;
            record.identity.revision = revision;
            catalog.attempt.stage(vec![
                KvMutation::Put {
                    key: name_lookup_key(name),
                    value: encode_lookup(&record.identity)?,
                },
                KvMutation::Put {
                    key,
                    value: storage_serde::to_bytes(&record)?,
                },
            ])?;
            catalog.bind_identity(record.identity);
            Ok(CatalogStamp {
                revision,
                ..expected
            })
        }
        .await;
        settlement.complete(result)
    }

    async fn read_catalog_stamp(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<CatalogStamp>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let key = name_lookup_key(name);
            let Some(bytes) = catalog.attempt.get(&key, catalog.protection).await? else {
                return Ok(None);
            };
            let (stamp, identity) = decode_lookup(name, &bytes)?;
            catalog.bind_identity(identity);
            Ok(Some(stamp))
        }
        .await;
        settlement.complete(result)
    }

    async fn read_catalog_stamps(
        &mut self,
        names: &[TableName],
    ) -> StorageResult<Vec<Option<CatalogStamp>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let keys = names.iter().map(name_lookup_key).collect();
            let values = catalog.attempt.get_batch(keys, catalog.protection).await?;
            if values.len() != names.len() {
                return Err(StorageError::internal(
                    "KV catalog stamp batch count is invalid",
                ));
            }
            names
                .iter()
                .zip(values)
                .map(|(name, value)| {
                    let Some(bytes) = value else { return Ok(None) };
                    let (stamp, identity) = decode_lookup(name, &bytes)?;
                    catalog.bind_identity(identity);
                    Ok(Some(stamp))
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }

    async fn read_table_at_stamp(
        &mut self,
        name: &TableName,
        stamp: CatalogStamp,
    ) -> StorageResult<Arc<BoundTableInfo>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let table_id = table_id_from_stamp(stamp)?;
            let key = metadata_key(table_id);
            let bytes = catalog
                .attempt
                .get(&key, catalog.protection)
                .await?
                .ok_or_else(|| {
                    let error = StorageError::internal("KV name lookup points to missing metadata");
                    storage_common::diagnostics::trace_native_failure(
                        catalog.attempt.backend_engine(),
                        storage_common::diagnostics::NativeOperation::CatalogReadTable,
                        &error,
                    );
                    error
                })?;
            catalog.bind_metadata(name, stamp, &bytes)
        }
        .await;
        settlement.complete(result)
    }

    async fn read_tables_at_stamps(
        &mut self,
        requests: &[(TableName, CatalogStamp)],
    ) -> StorageResult<Vec<Arc<BoundTableInfo>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let keys = requests
                .iter()
                .map(|(_, stamp)| table_id_from_stamp(*stamp).map(metadata_key))
                .collect::<StorageResult<Vec<_>>>()?;
            let values = catalog.attempt.get_batch(keys, catalog.protection).await?;
            if values.len() != requests.len() {
                return Err(StorageError::internal(
                    "KV catalog metadata batch count is invalid",
                ));
            }
            requests
                .iter()
                .zip(values)
                .map(|((name, stamp), bytes)| {
                    let bytes = bytes.ok_or_else(|| {
                        let error =
                            StorageError::internal("KV name lookup points to missing metadata");
                        storage_common::diagnostics::trace_native_failure(
                            catalog.attempt.backend_engine(),
                            storage_common::diagnostics::NativeOperation::CatalogReadTable,
                            &error,
                        );
                        error
                    })?;
                    catalog.bind_metadata(name, *stamp, &bytes)
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> ListTablesAttempt for KvCatalogAttempt<'_, A> {
    async fn list_name_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogNamePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut begin = vec![LOOKUP_FAMILY];
            if let Some(after) = after {
                begin.extend_from_slice(after.as_ref().as_bytes());
                begin.push(0);
            }
            let requested = NonZeroU32::new(limit)
                .ok_or_else(|| StorageError::internal("catalog list limit is zero"))?;
            let page = catalog
                .attempt
                .scan(
                    KvRange {
                        entry_bounds: None,
                        begin,
                        end: vec![LOOKUP_FAMILY + 1],
                        reverse: false,
                        limit: requested,
                        max_bytes: CATALOG_READ_BYTES,
                    },
                    catalog.protection,
                )
                .await?;
            let names = page
                .entries
                .into_iter()
                .map(|entry| decode_lookup_name(&entry.key))
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(CatalogNamePage {
                names,
                more: page.more,
            })
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> ListStreamsAttempt for KvCatalogAttempt<'_, A> {
    async fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .attempt
                .get(&table_stream_key(name), catalog.protection)
                .await?
                .map(|bytes| {
                    let timestamp: [u8; 8] = bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| StorageError::internal("invalid stream catalog timestamp"))?;
                    Ok(TimestampMillis::from_timestamp(i64::from_be_bytes(
                        timestamp,
                    )))
                })
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn list_stream_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogStreamPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut begin = vec![TABLE_STREAM_FAMILY];
            if let Some(after) = after {
                begin.extend_from_slice(after.as_ref().as_bytes());
                begin.push(0);
            }
            let requested = NonZeroU32::new(limit)
                .ok_or_else(|| StorageError::internal("stream catalog limit is zero"))?;
            let page = catalog
                .attempt
                .scan(
                    KvRange {
                        entry_bounds: None,
                        begin,
                        end: vec![TABLE_STREAM_FAMILY + 1],
                        reverse: false,
                        limit: requested,
                        max_bytes: CATALOG_READ_BYTES,
                    },
                    catalog.protection,
                )
                .await?;
            let entries = page
                .entries
                .into_iter()
                .map(|entry| {
                    let name = entry
                        .key
                        .strip_prefix(&[TABLE_STREAM_FAMILY])
                        .ok_or_else(|| StorageError::internal("invalid stream catalog key"))?;
                    let name = std::str::from_utf8(name)
                        .map_err(|_| StorageError::internal("invalid stream catalog table name"))?;
                    let timestamp: [u8; 8] =
                        entry.value.as_slice().try_into().map_err(|_| {
                            StorageError::internal("invalid stream catalog timestamp")
                        })?;
                    Ok((
                        TableName::new(name),
                        TimestampMillis::from_timestamp(i64::from_be_bytes(timestamp)),
                    ))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(CatalogStreamPage {
                entries,
                more: page.more,
            })
        }
        .await;
        settlement.complete(result)
    }
}

fn decode_lookup_name(key: &[u8]) -> StorageResult<TableName> {
    let name = key
        .strip_prefix(&[LOOKUP_FAMILY])
        .ok_or_else(|| StorageError::internal("invalid KV catalog lookup key"))?;
    let name = std::str::from_utf8(name)
        .map_err(|_| StorageError::internal("invalid KV catalog table name"))?;
    Ok(TableName::new(name))
}

fn name_lookup_key(name: &TableName) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.as_ref().len());
    key.push(LOOKUP_FAMILY);
    key.extend_from_slice(name.as_ref().as_bytes());
    key
}

fn table_stream_key(name: &TableName) -> Vec<u8> {
    let mut key = name_lookup_key(name);
    key[0] = TABLE_STREAM_FAMILY;
    key
}

pub(crate) fn name_lock_key(name: &TableName) -> Vec<u8> {
    let mut key = name_lookup_key(name);
    key[0] = TABLE_NAME_LOCK_FAMILY;
    key
}

fn decode_name_lock(bytes: &[u8]) -> StorageResult<storage_types::TimestampMillis> {
    let raw: [u8; 8] = bytes
        .get(4..)
        .ok_or_else(|| StorageError::internal("invalid KV name reservation width"))?
        .try_into()
        .map_err(|_| StorageError::internal("invalid KV name reservation width"))?;
    let id: [u8; 4] = bytes[..4]
        .try_into()
        .map_err(|_| StorageError::internal("invalid KV name reservation ID"))?;
    if u32::from_be_bytes(id) == 0 {
        return Err(StorageError::internal("KV name reservation ID is zero"));
    }
    Ok(storage_types::TimestampMillis::from_timestamp(
        i64::from_be_bytes(raw),
    ))
}

pub(crate) fn encode_name_lock(table_id: u32, until: TimestampMillis) -> Vec<u8> {
    let mut value = Vec::with_capacity(12);
    value.extend_from_slice(&table_id.to_be_bytes());
    value.extend_from_slice(&until.timestamp_millis().to_be_bytes());
    value
}

pub(crate) fn metadata_key(table_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(5);
    key.push(METADATA_FAMILY);
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

fn decode_table_id(bytes: &[u8]) -> StorageResult<u32> {
    let raw: [u8; 4] = bytes
        .try_into()
        .map_err(|_| StorageError::internal("invalid KV table-name lookup width"))?;
    Ok(u32::from_be_bytes(raw))
}

fn encode_lookup(identity: &KvTableIdentity) -> StorageResult<Vec<u8>> {
    storage_serde::to_bytes(identity)
}

fn decode_lookup(name: &TableName, bytes: &[u8]) -> StorageResult<(CatalogStamp, KvTableIdentity)> {
    let identity: KvTableIdentity = storage_serde::from_bytes(bytes)?;
    if identity.table_id == 0 || identity.deleted || identity.table_name != *name {
        return Err(StorageError::internal(
            "invalid KV table-name lookup identity",
        ));
    }
    let stamp = CatalogStamp {
        history_id: TableHistoryId::from(identity.table_id),
        revision: identity.revision,
    };
    Ok((stamp, identity))
}

fn table_id_from_stamp(stamp: CatalogStamp) -> StorageResult<u32> {
    table_id_from_history(stamp.history_id)
}

pub(crate) fn table_id_from_history(history_id: TableHistoryId) -> StorageResult<u32> {
    let bytes = history_id.as_ref();
    if bytes[..12] != [0; 12] {
        return Err(StorageError::internal("invalid KV table identity stamp"));
    }
    Ok(u32::from_be_bytes(bytes[12..].try_into().map_err(
        |_| StorageError::internal("KV table identity stamp has invalid width"),
    )?))
}

pub(crate) fn verify_deleted_metadata(
    bytes: &[u8],
    target: &TableReclaimTarget,
) -> StorageResult<()> {
    let record: StoredCatalogRecord = storage_serde::from_bytes(bytes)
        .map_err(|_| StorageError::internal("KV deleted table metadata is corrupt"))?;
    if record.identity.table_id != table_id_from_history(target.history_id)?
        || !record.identity.deleted
        || record.identity.table_name != target.table_name
        || record.table_info.table_name != target.table_name
    {
        return Err(StorageError::internal(
            "KV table reclaim marker has live metadata",
        ));
    }
    Ok(())
}

/// Decode one protected physical catalog record without publishing/cache
/// rebinding.
pub(crate) fn decode_index_incarnation(
    bytes: &[u8],
    history: TableHistoryId,
    target: &storage_backfill::GsiBuildTarget,
) -> StorageResult<Option<storage_types::maintenance::TargetIncarnation>> {
    let record: StoredCatalogRecord = storage_serde::from_bytes(bytes)?;
    let identity = record.identity;
    if identity.deleted
        || identity.table_id != table_id_from_history(history)?
        || identity.table_name != target.table_name
    {
        return Err(StorageError::internal(
            "KV index binding differs from protected catalog",
        ));
    }
    let mut matches = identity.indexes.iter().filter(|index| {
        index.index_kind == KvIndexKind::UserGsi && index.index_name == target.index_name
    });
    let incarnation = matches
        .next()
        .map(|index| crate::gsi::index_incarnation(identity.table_id, index.index_id).into());
    if matches.next().is_some() {
        return Err(StorageError::internal(
            "KV index binding has duplicate identities",
        ));
    }
    Ok(incarnation)
}
