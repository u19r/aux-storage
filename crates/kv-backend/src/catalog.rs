//! Persisted KV table-name lookup and metadata layout, shared by both drivers.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, CatalogNamePage, CatalogStamp, CatalogStreamPage,
    CreateTableLayout, ListingAttempt, TableReclaimPhase, TableReclaimTarget,
};
use storage_types::{
    BoundTableInfo, GsiPhase, IndexName, MissExpectation, RegionalBinding, StorageError,
    StorageResult, StoredGsiBinding, StoredTableInfo, TableBinding, TableHistoryId, TableName,
    TimeToLiveStatus, TimestampMillis, storage_serde,
};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{INITIAL_REVISION, LOOKUP_FAMILY, TABLE_ID_ALLOCATOR_KEY, TABLE_STREAM_FAMILY},
};

/// Complete persisted identity for catalog mutation, backfill and reclamation.
/// Ordinary item addressing uses confirmed table and index bindings.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub struct KvTableIdentity {
    pub table_id: u32,
    pub revision: NonZeroU64,
    pub table_name: TableName,
    pub indexes: Vec<KvIndexIdentity>,
    pub index_id_high_water: u16,
    pub deleted: bool,
    pub external_cleanup_required: bool,
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

/// The `Metadata | table_id` value: the table image and its writer-visible
/// binding (W6F section 4.5). Each change of the binding goes through
/// `replace_table_layout` and changes the revision of the name lookup, so a
/// writer that confirms its stamp needs no GSI build or TTL record read.
/// Metadata retains the identity after DeleteTable removes its public name
/// binding, so bounded reclamation can still identify the old incarnation.
#[derive(Deserialize, Serialize)]
struct StoredCatalogRecord {
    identity: KvTableIdentity,
    table_info: StoredTableInfo,
    gsis: StoredGsiBinding,
    ttl: Option<StoredTtlBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    regional: Option<RegionalBinding>,
}

/// The writer-visible TTL policy. The full policy record and its sweep
/// checkpoint are job state under `ttl_policy::ttl_config_key`.
#[derive(Deserialize, Serialize)]
struct StoredTtlBinding {
    attribute: String,
    status: TimeToLiveStatus,
}

impl StoredCatalogRecord {
    fn new(identity: KvTableIdentity, table_info: StoredTableInfo, binding: &TableBinding) -> Self {
        Self {
            identity,
            table_info,
            gsis: StoredGsiBinding::from(binding),
            ttl: binding.ttl.as_ref().map(|ttl| StoredTtlBinding {
                attribute: ttl.attribute.clone(),
                status: ttl.status.clone(),
            }),
            regional: binding.regional.as_deref().cloned(),
        }
    }

    fn into_bound(self, history_id: TableHistoryId) -> BoundTableInfo {
        let binding = TableBinding {
            gsis: self.gsis.gsis.into(),
            gsi_high_water: self.gsis.high_water,
            ttl: self.ttl.map(|ttl| storage_types::TtlBinding {
                attribute: ttl.attribute,
                status: ttl.status,
            }),
            regional: self.regional.map(Arc::new),
            ..TableBinding::for_info(&self.table_info)
        };
        BoundTableInfo {
            history_id,
            info: Arc::new(self.table_info),
            binding,
        }
    }
}

/// One KV catalog view bound to the caller's native transaction or snapshot.
/// Protected name/metadata reads bind complete physical identities for catalog
/// changes. A checked warm table-cache hit needs no complete identity.
pub struct KvCatalogAttempt<'a, A> {
    pub(crate) attempt: &'a mut A,
    pub(crate) protection: ReadProtection,
    identities: Vec<KvTableIdentity>,
    /// Bindings that this attempt staged; a later catalog change in the
    /// same attempt starts from them, not from the caller's older copy.
    written_bindings: Vec<(TableName, TableBinding)>,
    dynamo_mutations: Vec<crate::KvMutation>,
    pub(crate) primary_layouts: Vec<(Vec<u8>, bool)>,
    pub(crate) projection_changes: crate::gsi::ProjectionChanges,
    pub(crate) prepared_writes: StorageResult<crate::write_preparation::KvPreparedWrites>,
    pub(crate) queue_wakes: queue_provider::QueueWakeSlots,
    /// The queue port's catalog hints and this attempt's start time (W7 P7);
    /// `None` outside `with_attempt`.
    pub(crate) queue_catalog_hints: Option<(
        std::sync::Arc<queue_provider::QueueCatalogHints>,
        storage_types::TimestampMillis,
    )>,
    /// A hinted catalog row that no read of this attempt has verified yet.
    pub(crate) queue_catalog_check: Option<queue_provider::QueueCatalogRecord>,
    /// An absence read of new message ids made in an earlier wave of this
    /// attempt under a hinted incarnation that the same wave confirmed
    /// (W8-E-e); taken by the next `insert_messages`.
    pub(crate) queue_absence: Option<crate::queue::CheckedAbsence>,
}

impl<'a, A: KvAttempt> KvCatalogAttempt<'a, A> {
    pub fn new(attempt: &'a mut A, protection: ReadProtection) -> Self {
        Self {
            attempt,
            protection,
            identities: Vec::new(),
            written_bindings: Vec::new(),
            dynamo_mutations: Vec::new(),
            primary_layouts: Vec::new(),
            projection_changes: Default::default(),
            prepared_writes: Ok(Default::default()),
            queue_wakes: Default::default(),
            queue_catalog_hints: None,
            queue_catalog_check: None,
            queue_absence: None,
        }
    }

    pub(crate) fn append_dynamo_mutations(
        &mut self,
        changes: Vec<crate::KvMutation>,
    ) -> StorageResult<()> {
        if self.dynamo_mutations.try_reserve(changes.len()).is_err() {
            let error = storage_types::AttemptFailure::CAPACITY.into();
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
            let projections = std::mem::take(&mut catalog.projection_changes).into_mutations()?;
            // The attempt retains stream ordinals across every flush.
            let count = projections.len();
            changes
                .try_reserve(count)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            changes.extend(projections);
            if changes.is_empty() {
                return Ok(());
            }
            catalog
                .attempt
                .stage_with_primary_layouts(changes, &mut catalog.primary_layouts)
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

    /// Lifecycle owners need the complete persisted identity even when table
    /// authority came from a checked cache entry without a name lookup.
    pub(crate) async fn load_table_identity(
        &mut self,
        table: &BoundTableInfo,
    ) -> StorageResult<KvTableIdentity> {
        if let Ok(identity) = self.identity_for(&table.table_name) {
            return Ok(identity.clone());
        }
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let table_id = table_id_from_history(table.history_id)?;
            let bytes = catalog
                .attempt
                .get(&metadata_key(table_id), ReadProtection::Conflict)
                .await?
                .ok_or_else(|| StorageError::internal("KV lifecycle metadata is absent"))?;
            let record: StoredCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if record.identity.deleted
                || record.identity.table_id != table_id
                || record.identity.table_name != table.table_name
                || record.table_info.table_name != table.table_name
            {
                return Err(StorageError::internal(
                    "KV lifecycle metadata has another identity",
                ));
            }
            catalog.bind_identity(record.identity.clone());
            Ok(record.identity)
        }
        .await;
        settlement.complete(result)
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

    /// The binding of `table` that this attempt stores: the one this
    /// attempt staged last, or the caller's binding confirmed by its stamp.
    pub(crate) fn current_binding(&self, table: &BoundTableInfo) -> TableBinding {
        self.written_bindings
            .iter()
            .find(|(name, _)| *name == table.info.table_name)
            .map_or_else(|| table.binding.clone(), |(_, binding)| binding.clone())
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
            Ok(Arc::new(record.into_bound(stamp.history_id)))
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
    ) -> StorageResult<CreateTableLayout<Option<Vec<u8>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let name_state = catalog
                .attempt
                .get_batch(
                    vec![
                        name_lookup_key(&table.table_name),
                        TABLE_ID_ALLOCATOR_KEY.to_vec(),
                    ],
                    ReadProtection::Conflict,
                )
                .await?;
            if name_state.len() != 2 {
                return Err(StorageError::internal(
                    "KV name reservation read count is invalid",
                ));
            }
            let mut name_state = name_state.into_iter();
            let active = name_state.next().flatten().is_some();
            let allocator = name_state.next().flatten();
            Ok(CreateTableLayout {
                name_in_use: active,
                seed: allocator,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub async fn insert_table_layout(
        &mut self,
        table: &StoredTableInfo,
        allocator: Option<Vec<u8>>,
        regional: Option<Arc<RegionalBinding>>,
    ) -> StorageResult<KvTableIdentity> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
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
            // An empty new table needs no backfill: every GSI is active
            // (`register_gsis` checks the registered builds).
            let mut binding = TableBinding::for_info(table);
            for index in table
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
            {
                binding = identity.with_added_gsi(
                    &binding,
                    index,
                    &table.attribute_definitions,
                    GsiPhase::Active,
                )?;
            }
            binding.regional = regional;
            let record = StoredCatalogRecord::new(identity.clone(), table.clone(), &binding);
            let mut changes = vec![
                KvMutation::PutUnchunked {
                    key: TABLE_ID_ALLOCATOR_KEY.to_vec(),
                    value: next_id.to_be_bytes().to_vec(),
                },
                KvMutation::PutUnchunked {
                    key: name_key,
                    value: encode_lookup(&identity)?,
                },
                KvMutation::Put {
                    key: metadata_key(table_id),
                    value: storage_serde::to_bytes(&record)?,
                },
                KvMutation::PutUnchunked {
                    key: crate::stream_write::counter::user_stream_sequence_key(
                        &storage_types::TableHistoryId::from(table_id).append_stream_name(),
                    )?,
                    value: 1u64.to_be_bytes().to_vec(),
                },
            ];
            if table.has_enabled_stream() {
                changes.push(KvMutation::PutUnchunked {
                    key: table_stream_key(&table.table_name),
                    value: encode_stream_identity(identity.table_id, table)?,
                });
            }
            // A new table keeps the catalog version: no node caches a binding
            // or a miss for an absent name, and a write that observed the
            // absence read the name key under conflict. Deletion changed the
            // version for any earlier incarnation of this name.
            catalog
                .attempt
                .stage_with_primary_layouts(changes, &mut catalog.primary_layouts)?;
            catalog.bind_identity(identity.clone());
            Ok(identity)
        }
        .await;
        settlement.complete(result)
    }

    /// Remove the public name binding atomically and retain a tombstone for
    /// bounded background reclamation of this incarnation's physical keys.
    pub(crate) async fn tombstone_table(
        &mut self,
        table: &BoundTableInfo,
        phase: TableReclaimPhase,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut identity = catalog.load_table_identity(table).await?;
            if TableHistoryId::from(identity.table_id) != table.history_id {
                return Err(StorageError::internal(
                    "KV table identity changed during deletion",
                ));
            }
            catalog
                .attempt
                .root_attempt()
                .native_record_catalog_change()
                .await?;
            crate::gsi::delete_gsi_builds(catalog.attempt, &identity, None).await?;
            identity.deleted = true;
            let record = StoredCatalogRecord::new(
                identity.clone(),
                table.info.as_ref().clone(),
                &catalog.current_binding(table),
            );
            let mut changes = Vec::with_capacity(4);
            changes.push(KvMutation::DeleteUnchunked {
                key: name_lookup_key(&table.info.table_name),
            });
            if table
                .info
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled)
            {
                changes.push(KvMutation::DeleteUnchunked {
                    key: table_stream_key(&table.info.table_name),
                });
            }
            changes.push(KvMutation::Put {
                key: metadata_key(identity.table_id),
                value: storage_serde::to_bytes(&record)?,
            });
            changes.push(KvMutation::Put {
                key: crate::table_reclaim::reclaim_key(identity.table_id),
                value: storage_serde::to_bytes(&TableReclaimTarget {
                    history_id: table.history_id,
                    table_name: table.info.table_name.clone(),
                    phase,
                    gsi_targets: Vec::new(),
                })?,
            });
            catalog
                .attempt
                .stage_with_primary_layouts(changes, &mut catalog.primary_layouts)
        }
        .await;
        settlement.complete(result)
    }

    /// The one KV catalog change path: store `table` with `binding` and
    /// change the revision of the name lookup, which each writer reads with
    /// a conflict.
    pub(crate) async fn replace_table_layout(
        &mut self,
        current: &BoundTableInfo,
        identity: KvTableIdentity,
        table: StoredTableInfo,
        binding: TableBinding,
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
            let binding = TableBinding {
                stream: storage_types::StreamBinding::for_info(&table),
                ..binding
            };
            let record = StoredCatalogRecord::new(identity.clone(), table.clone(), &binding);
            let mut changes = vec![
                KvMutation::PutUnchunked {
                    key: name_lookup_key(&identity.table_name),
                    value: encode_lookup(&identity)?,
                },
                KvMutation::Put {
                    key: metadata_key(identity.table_id),
                    value: storage_serde::to_bytes(&record)?,
                },
            ];
            let was_enabled = current.info.has_enabled_stream();
            let is_enabled = table.has_enabled_stream();
            match (was_enabled, is_enabled) {
                (false, true) => changes.push(KvMutation::PutUnchunked {
                    key: table_stream_key(&table.table_name),
                    value: encode_stream_identity(identity.table_id, &table)?,
                }),
                (true, false) => changes.push(KvMutation::DeleteUnchunked {
                    key: table_stream_key(&table.table_name),
                }),
                _ => {}
            }
            catalog
                .attempt
                .root_attempt()
                .native_record_catalog_change()
                .await?;
            catalog
                .attempt
                .stage_with_primary_layouts(changes, &mut catalog.primary_layouts)?;
            catalog.bind_identity(identity);
            match catalog
                .written_bindings
                .iter_mut()
                .find(|(name, _)| *name == table.table_name)
            {
                Some((_, written)) => *written = binding.clone(),
                None => catalog
                    .written_bindings
                    .push((table.table_name.clone(), binding.clone())),
            }
            Ok(BoundTableInfo {
                history_id: current.history_id,
                binding,
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
            external_cleanup_required: false,
        })
    }

    /// The binding with one more GSI, whose index ID is the physical ID of
    /// this identity. Both allocate from the same high water.
    pub(crate) fn with_added_gsi(
        &self,
        binding: &TableBinding,
        index: &storage_types::GlobalSecondaryIndex,
        attributes: &[storage_types::AttributeDefinition],
        phase: GsiPhase,
    ) -> StorageResult<TableBinding> {
        let index_id = self
            .indexes
            .iter()
            .find(|candidate| {
                candidate.index_kind == KvIndexKind::UserGsi
                    && candidate.index_name == index.index_name
            })
            .ok_or_else(|| StorageError::internal("KV bound GSI has no index identity"))?
            .index_id;
        let next = binding.with_added_gsi(
            index,
            attributes,
            crate::gsi::index_incarnation(self.table_id, index_id).into(),
            phase,
        )?;
        if next.gsi_high_water != index_id {
            return Err(StorageError::internal(
                "KV GSI index ID differs from its binding",
            ));
        }
        Ok(next)
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
    async fn read_writer_routes(
        &mut self,
    ) -> StorageResult<Option<std::sync::Arc<stream_provider::SystemStreamLineageCatalog>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let result = crate::stream_history::read_writer_routes(&*operation.as_mut().attempt)
            .await
            .map(Some);
        operation.complete(result)
    }

    fn install_writer_routes(
        &mut self,
        routes: std::sync::Arc<stream_provider::SystemStreamLineageCatalog>,
    ) -> StorageResult<()> {
        self.attempt.root_attempt().install_writer_routes(routes)
    }

    fn protect_catalog_stamps(&mut self, names: &[&TableName]) -> StorageResult<()> {
        let keys = names
            .iter()
            .map(|name| name_lookup_key(name))
            .collect::<Vec<_>>();
        self.attempt.native_protect_reads(&keys, &[])
    }

    fn protect_writer_routes(&mut self) -> StorageResult<()> {
        let (keys, ranges) = crate::stream_history::writer_route_keys()?;
        self.attempt.native_protect_reads(&keys, &ranges)
    }

    async fn read_catalog_version(
        &mut self,
    ) -> StorageResult<Option<storage_types::CatalogVersion>> {
        self.attempt.root_attempt().native_catalog_version().await
    }

    fn pipeline_catalog_version(
        &mut self,
        expected: storage_types::CatalogVersion,
    ) -> StorageResult<bool> {
        self.attempt
            .root_attempt()
            .native_pipeline_catalog_version(expected)
    }

    fn pipelined_catalog_version(&mut self) -> Option<Option<storage_types::CatalogVersion>> {
        self.attempt
            .root_attempt()
            .native_pipelined_catalog_version()
    }

    async fn store_external_cleanup_barrier(
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
            // An absent record misses the caller's observation, as a deleted
            // or newer one does; the caller answers after a fresh read.
            let bytes = catalog
                .attempt
                .get(&key, ReadProtection::Conflict)
                .await?
                .ok_or_else(|| {
                    StorageError::write_miss("catalog_barrier", MissExpectation::Revision)
                })?;
            let mut record: StoredCatalogRecord = storage_serde::from_bytes(&bytes)?;
            if record.identity.revision != expected.revision
                || record.identity.table_name != *name
                || record.identity.deleted
                || record.identity.table_id != table_id
            {
                return Err(StorageError::write_miss(
                    "catalog_barrier",
                    MissExpectation::Revision,
                ));
            }
            let revision = expected
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| StorageError::internal("catalog revision exhausted"))?;
            record.identity.revision = revision;
            record.identity.external_cleanup_required = true;
            catalog
                .attempt
                .root_attempt()
                .native_record_catalog_change()
                .await?;
            catalog.attempt.stage(vec![
                KvMutation::PutUnchunked {
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
    ) -> StorageResult<storage_types::Exact<Option<CatalogStamp>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let keys = names.iter().map(name_lookup_key).collect();
            let values = catalog
                .attempt
                .get_batch_exact(keys, catalog.protection)
                .await?;
            values.try_zip_map(names, |name, value| {
                let Some(bytes) = value else { return Ok(None) };
                let (stamp, identity) = decode_lookup(name, &bytes)?;
                catalog.bind_identity(identity);
                Ok(Some(stamp))
            })
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
    ) -> StorageResult<storage_types::Exact<Arc<BoundTableInfo>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let keys = requests
                .iter()
                .map(|(_, stamp)| table_id_from_stamp(*stamp).map(metadata_key))
                .collect::<StorageResult<Vec<_>>>()?;
            let values = catalog
                .attempt
                .get_batch_exact(keys, catalog.protection)
                .await?;
            values.try_zip_map(requests, |(name, stamp), bytes| {
                let bytes = bytes.ok_or_else(|| {
                    let error = StorageError::internal("KV name lookup points to missing metadata");
                    storage_common::diagnostics::trace_native_failure(
                        catalog.attempt.backend_engine(),
                        storage_common::diagnostics::NativeOperation::CatalogReadTable,
                        &error,
                    );
                    error
                })?;
                catalog.bind_metadata(name, *stamp, &bytes)
            })
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> ListingAttempt for KvCatalogAttempt<'_, A> {
    async fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<storage_types::TableStreamIdentity>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .attempt
                .get(&table_stream_key(name), catalog.protection)
                .await?
                .map(|bytes| decode_stream_identity(&bytes))
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
                    Ok((TableName::new(name), decode_stream_identity(&entry.value)?))
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

/// PERF-FDB-1: a pipelined read whose catalog version moved returns this
/// without decoding and without rejecting the attempt; shared code reads the
/// stamps next and reads again or answers the stamp conflict. It is a
/// conflict so that it retries if it ever escaped.
pub(crate) fn pipelined_version_moved() -> StorageError {
    StorageError::logical_conflict("pipelined catalog version moved before decode")
}

pub(crate) fn metadata_key(table_id: u32) -> Vec<u8> {
    crate::table_key_layout::table_prefix(
        table_id,
        crate::table_key_layout::TableSubFamily::Catalog,
    )
    .to_vec()
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

pub(crate) fn decode_table_history_state(
    bytes: &[u8],
    history: TableHistoryId,
) -> StorageResult<storage_types::TableHistoryState> {
    let record: StoredCatalogRecord = storage_serde::from_bytes(bytes)?;
    if record.identity.table_id != table_id_from_history(history)?
        || record.identity.table_name != record.table_info.table_name
    {
        return Err(StorageError::internal(
            "KV history catalog identity differs from its key",
        ));
    }
    Ok(if record.identity.deleted {
        storage_types::TableHistoryState::Deleting
    } else {
        storage_types::TableHistoryState::Active(std::sync::Arc::new(
            storage_types::TableHistorySchema::try_from((
                record.table_info.table_name,
                record.table_info.key_schema,
            ))?,
        ))
    })
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

fn encode_stream_identity(table_id: u32, table: &StoredTableInfo) -> StorageResult<Vec<u8>> {
    let enabled_at = table
        .stream_enabled_at
        .ok_or_else(|| StorageError::internal("enabled KV stream has no enable stamp"))?;
    let mut value = Vec::with_capacity(12);
    value.extend_from_slice(&table_id.to_be_bytes());
    value.extend_from_slice(&enabled_at.timestamp_millis().to_be_bytes());
    Ok(value)
}

/// Global catalog records of a table incarnation restored from its root range:
/// the metadata record and name lookup with a revision above both the copy and
/// the current lookup, the allocator above the table ID, and the stream
/// identity.
#[cfg(test)]
pub(crate) fn restored_catalog_mutations(
    metadata: &[u8],
    current_lookup: Option<&[u8]>,
    allocator: Option<&[u8]>,
) -> StorageResult<Vec<KvMutation>> {
    let mut record: StoredCatalogRecord = storage_serde::from_bytes(metadata)?;
    let current = current_lookup
        .map(storage_serde::from_bytes::<KvTableIdentity>)
        .transpose()?;
    let floor = current.map_or(record.identity.revision, |c| {
        c.revision.max(record.identity.revision)
    });
    record.identity.revision = floor
        .checked_add(1)
        .ok_or_else(|| StorageError::internal("KV catalog revision exhausted"))?;
    let table_id = record.identity.table_id;
    let next = allocator
        .map(decode_table_id)
        .transpose()?
        .unwrap_or(1)
        .max(table_id + 1);
    let name = &record.table_info.table_name;
    let mut changes = vec![
        KvMutation::PutUnchunked {
            key: name_lookup_key(name),
            value: encode_lookup(&record.identity)?,
        },
        KvMutation::PutUnchunked {
            key: TABLE_ID_ALLOCATOR_KEY.to_vec(),
            value: next.to_be_bytes().to_vec(),
        },
    ];
    if record.table_info.has_enabled_stream() {
        changes.push(KvMutation::PutUnchunked {
            key: table_stream_key(name),
            value: encode_stream_identity(table_id, &record.table_info)?,
        });
    }
    changes.push(KvMutation::Put {
        key: metadata_key(table_id),
        value: storage_serde::to_bytes(&record)?,
    });
    Ok(changes)
}

fn decode_stream_identity(value: &[u8]) -> StorageResult<storage_types::TableStreamIdentity> {
    let bytes: &[u8; 12] = value
        .try_into()
        .map_err(|_| StorageError::internal("invalid stream catalog identity"))?;
    let [a, b, c, d, timestamp @ ..] = *bytes;
    let table_id = u32::from_be_bytes([a, b, c, d]);
    let timestamp = i64::from_be_bytes(timestamp);
    Ok(storage_types::TableStreamIdentity {
        history_id: table_id.into(),
        enabled_at: TimestampMillis::from_timestamp(timestamp),
    })
}
