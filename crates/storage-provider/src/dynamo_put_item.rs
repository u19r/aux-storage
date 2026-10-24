//! Shared PutItem shape, table validation, and condition decision.
//!
//! Physical item, index, TTL, stream, and receipt effects are applied by the
//! SQL or KV family in the same native attempt after `accept_old` accepts.

use std::{future::Future, sync::Arc};

use storage_backfill::{GsiProjectionLifecycle, GsiProjectionWrite};
use storage_common::{TtlConfigRecord, TtlIndexWrite, TtlIndexWriteBatch};
use storage_types::{
    AllOld, AttributeMap, BoundTableInfo, IndexerDeclaration, ItemProvenance, ItemRevision,
    KeyAttributes, LogicalItemSize, PutItemRequest, StorageEnum, StorageError, StorageResult,
    StoredWireItem, StreamRetentionDuration, TableName, WireItem,
    return_values_on_condition_check_failure_all_old, validate_transact_put_item_key,
};

use crate::{
    DescribeTimeToLiveAttempt, DynamoStreamWriteAttempt, ItemPrimaryEffects, SemanticDecision,
    StreamTrimScope, StreamTrimState, TableMetadataCache, TtlPolicyLookup, UncheckedCondition,
    key_attributes_from_item,
};

/// The native write attempt returns one durable state, including a deleted
/// key's revision. It never invents a live image from a tombstone.
pub enum ItemWriteObservation {
    Absent,
    Live {
        stored: Box<StoredWireItem>,
        revision: ItemRevision,
        provenance: ItemProvenance,
        ttl_epoch: Option<i64>,
    },
    Deleted {
        revision: ItemRevision,
        provenance: ItemProvenance,
    },
}

/// Independent protected item reads in one native attempt. Condition checks
/// do not need mutable GSI lifecycle state.
#[derive(Clone, Copy)]
pub enum ItemWriteObservationRead<'a> {
    Write(&'a BoundTableInfo, &'a KeyAttributes),
    ConditionCheck(&'a BoundTableInfo, &'a KeyAttributes),
}

impl<'a> ItemWriteObservationRead<'a> {
    pub fn target(&self) -> (&'a BoundTableInfo, &'a KeyAttributes) {
        match self {
            Self::Write(table, key) | Self::ConditionCheck(table, key) => (table, key),
        }
    }

    fn needs_gsi_lifecycle(&self) -> bool {
        matches!(self, Self::Write(table, _) if !table.info.global_secondary_indexes.as_deref().unwrap_or_default().is_empty())
    }
}

/// One lifecycle read per written table with at least one current GSI.
pub fn lifecycle_tables_for_writes<'a>(
    reads: &[ItemWriteObservationRead<'a>],
) -> StorageResult<Vec<&'a BoundTableInfo>> {
    let mut tables = Vec::new();
    tables
        .try_reserve_exact(reads.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for read in reads {
        let table = read.target().0;
        if read.needs_gsi_lifecycle()
            && !tables
                .iter()
                .any(|known: &&BoundTableInfo| known.info.table_name == table.info.table_name)
        {
            tables.push(table);
        }
    }
    Ok(tables)
}

/// Attach decoded physical observations without another SQL/KV policy branch.
pub fn combine_write_observations(
    reads: &[ItemWriteObservationRead<'_>],
    items: Vec<ItemWriteObservation>,
    lifecycles: Vec<(TableName, GsiProjectionLifecycle)>,
) -> StorageResult<Vec<(ItemWriteObservation, GsiProjectionLifecycle)>> {
    if items.len() != reads.len() {
        return Err(StorageError::internal("write observation count differs"));
    }
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(items.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for (read, item) in reads.iter().zip(items) {
        let lifecycle = if read.needs_gsi_lifecycle() {
            lifecycles
                .iter()
                .find(|(name, _)| name == &read.target().0.info.table_name)
                .ok_or_else(|| StorageError::internal("GSI lifecycle was not observed"))?
                .1
                .clone()
        } else {
            GsiProjectionLifecycle::default()
        };
        observations.push((item, lifecycle));
    }
    Ok(observations)
}

/// The accepted primary revision fences an item projection; backfill uses its
/// independently observed projection revision.
pub enum GsiWriteSource<'a> {
    Item(&'a ItemPrimaryEffects),
    Backfill,
}

impl GsiWriteSource<'_> {
    pub fn validate(&self, write: &GsiProjectionWrite) -> StorageResult<()> {
        if let Self::Item(effects) = self
            && write.item_version != effects.revision()
        {
            return Err(StorageError::internal(
                "GSI write revision differs from primary",
            ));
        }
        Ok(())
    }
}

/// Key-complete old state used by every write decision after its protected
/// read.
pub(crate) struct ObservedWrite {
    pub item: Option<storage_types::VerifiedWireItem>,
    pub indexers: Vec<String>,
    pub revision: Option<ItemRevision>,
    pub provenance: Option<ItemProvenance>,
    pub ttl_epoch: Option<i64>,
}

impl ObservedWrite {
    pub fn try_new(observed: ItemWriteObservation, key: &KeyAttributes) -> StorageResult<Self> {
        let (item, indexers, revision, provenance, ttl_epoch) = match observed {
            ItemWriteObservation::Absent => (None, Vec::new(), None, None, None),
            ItemWriteObservation::Live {
                mut stored,
                revision,
                provenance,
                ttl_epoch,
            } => {
                let indexers = std::mem::take(&mut stored.indexers);
                let item = storage_types::VerifiedWireItem::try_from_stored(*stored, key)?;
                (
                    Some(item),
                    indexers,
                    Some(revision),
                    Some(provenance),
                    ttl_epoch,
                )
            }
            ItemWriteObservation::Deleted {
                revision,
                provenance,
            } => (None, Vec::new(), Some(revision), Some(provenance), None),
        };
        Ok(Self {
            item,
            indexers,
            revision,
            provenance,
            ttl_epoch,
        })
    }
}

/// The family lowerer protects the item and its revision authority inside
/// the write attempt already opened by its driver.
pub trait ItemWriteObservationAttempt: DescribeTimeToLiveAttempt {
    /// Observe independent TTL policy and retention state in one native read
    /// wave where the driver supports batched reads.
    fn prepare_item_write(
        &mut self,
        table: &BoundTableInfo,
        retention_scope: Option<&StreamTrimScope>,
    ) -> impl Future<Output = StorageResult<(Option<TtlConfigRecord>, Option<StreamTrimState>)>> + Send
    {
        async move {
            let ttl_lookup = self
                .item_write_cache()
                .map_or(TtlPolicyLookup::Miss(0), |cache| {
                    cache.lookup_ttl_policy(table)
                });
            let needs_ttl = matches!(ttl_lookup, TtlPolicyLookup::Miss(_));
            let (loaded_ttl, retention) = self
                .read_item_write_prerequisites(table, retention_scope, needs_ttl)
                .await?;
            let ttl = match ttl_lookup {
                TtlPolicyLookup::Hit(config) => config,
                TtlPolicyLookup::Miss(generation) => {
                    if let Some(cache) = self.item_write_cache() {
                        cache.publish_ttl_policy(table, loaded_ttl.clone(), generation);
                    }
                    loaded_ttl
                }
            };
            Ok((ttl, retention))
        }
    }

    fn item_write_cache(&self) -> Option<&TableMetadataCache>;

    fn read_item_write_prerequisites(
        &mut self,
        table: &BoundTableInfo,
        retention_scope: Option<&StreamTrimScope>,
        needs_ttl: bool,
    ) -> impl Future<Output = StorageResult<(Option<TtlConfigRecord>, Option<StreamTrimState>)>> + Send;

    /// Observe the item and mutable GSI state under the same native attempt.
    /// The family batches their physical reads into one driver request. The
    /// table cache holds schema only: GSI build state can change independently
    /// of its catalog revision and must be conflict protected for this write.
    fn observe_for_write(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> impl Future<Output = StorageResult<(ItemWriteObservation, GsiProjectionLifecycle)>> + Send
    {
        async move {
            let observations = self
                .observe_for_writes(&[ItemWriteObservationRead::Write(table, key)])
                .await?;
            let [observation] = observations.try_into().map_err(|_| {
                storage_types::StorageError::internal("item write observation count differs")
            })?;
            Ok(observation)
        }
    }

    /// Read independent item states and each table's mutable GSI lifecycle
    /// together. SQL may need one dependent tombstone round for absent rows.
    fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> impl Future<Output = StorageResult<Vec<(ItemWriteObservation, GsiProjectionLifecycle)>>> + Send;
}

/// Physical upsert effects shared by PutItem and UpdateItem.
pub trait UpsertAttempt: ItemWriteObservationAttempt + DynamoStreamWriteAttempt {
    /// Prepare immutable image dependencies and append primary/TTL effects
    /// inside this attempt. The caller also prepares projections, streams and
    /// receipts before the complete native batch can settle.
    fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &ItemPrimaryEffects,
        writes: TtlIndexWriteBatch,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// One shared TTL decision for the primary row and its physical index.
pub struct PutTtlEffects {
    epoch: Option<i64>,
    writes: TtlIndexWriteBatch,
}

impl PutTtlEffects {
    #[must_use]
    pub fn epoch(&self) -> Option<i64> {
        self.epoch
    }

    #[must_use]
    pub fn into_writes(self) -> TtlIndexWriteBatch {
        self.writes
    }
}

/// Shared revision decision and physical row metadata for an accepted put.
/// Request facts that can be checked before opening a native attempt.
#[derive(Clone)]
pub struct PutItemShape {
    pub table_name: TableName,
    item: WireItem,
    item_size: LogicalItemSize,
    indexers: Vec<String>,
    condition: Option<UncheckedCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
    item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl PutItemShape {
    pub fn try_from_request(request: PutItemRequest) -> StorageResult<Self> {
        let (request, item_size) = request.normalized_for_dynamodb()?;
        Self::from_normalized_request(request, item_size)
    }

    /// Parent batch/transaction validation covers member request fields.
    pub(crate) fn from_validated_request(request: PutItemRequest) -> StorageResult<Self> {
        let (item, item_size) = request.item.normalized_for_write()?;
        Self::from_normalized_request(PutItemRequest { item, ..request }, item_size)
    }

    fn from_normalized_request(
        request: PutItemRequest,
        item_size: LogicalItemSize,
    ) -> StorageResult<Self> {
        let condition = UncheckedCondition::parse(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
        Ok(Self {
            table_name: request.table_name,
            item: request.item,
            item_size,
            indexers: request.indexers.unwrap_or_default(),
            condition,
            return_old: matches!(request.return_values, Some(AllOld::AllOld)),
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
            item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
    }

    /// Schema-dependent validation follows the authoritative catalog read
    /// within this operation's native attempt.
    pub fn bind_table(
        self,
        table: Arc<BoundTableInfo>,
        ttl_config: Option<&TtlConfigRecord>,
    ) -> StorageResult<BoundPutItem> {
        let fields = table
            .info
            .key_schema
            .iter()
            .map(|key| key.attribute_name.as_str())
            .chain(self.indexers.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let attributes = self.item.project_attributes(&fields)?;
        validate_transact_put_item_key(&table.info, &attributes)?;
        let key = key_attributes_from_item(&attributes, &table.info.key_schema)?;
        let declaration = IndexerDeclaration::try_new(self.indexers, table.info.max_indexers)?;
        storage_types::IndexedWireItem::validate_logical_item(&attributes, &declaration)?;
        let prepared_gsi =
            storage_backfill::PreparedGsiPut::try_new(&table.info, &self.item, &declaration)?;
        let route = stream_provider::DynamoMutationRoute::try_new(
            &table.info.table_name,
            &table.info.key_schema,
            &key,
        )?;
        let stream_image =
            stream_provider::PreparedStreamImage::try_new(&route, self.item.clone())?;
        let ttl = storage_common::PreparedTtlPut::try_new(&table.info, ttl_config, &self.item)?;
        Ok(BoundPutItem {
            ttl,
            stream_image,
            route,
            prepared_gsi,
            table,
            key,
            new_item: self.item,
            item_size: self.item_size,
            declaration,
            condition: self.condition,
            return_old: self.return_old,
            return_old_on_condition_failure: self.return_old_on_condition_failure,
            item_stream_ttl_hours: self.item_stream_ttl_hours,
        })
    }
}

/// Validated logical PutItem. A family lowerer protects and reads its current
/// item before this value evaluates the condition.
pub struct BoundPutItem {
    ttl: storage_common::PreparedTtlPut,
    stream_image: stream_provider::PreparedStreamImage,
    route: stream_provider::DynamoMutationRoute,
    prepared_gsi: storage_backfill::PreparedGsiPut,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub new_item: WireItem,
    pub item_size: LogicalItemSize,
    pub declaration: IndexerDeclaration,
    condition: Option<UncheckedCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl BoundPutItem {
    pub fn gsi_puts(&self) -> impl Iterator<Item = &storage_backfill::GsiProjectedRow> {
        self.prepared_gsi.rows()
    }

    /// Condition failure has no physical effects. The returned pair is the
    /// only old/new item authority for the ensuing GSI, TTL, and stream work.
    pub fn accept_old(
        self,
        observed: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<(AcceptedUpsert, bool)>> {
        let old = ObservedWrite::try_new(observed, &self.key)?;
        if let SemanticDecision::Rejected(error) = check_write_condition(
            self.condition.as_ref(),
            old.item.as_deref(),
            self.return_old_on_condition_failure,
        )? {
            return Ok(SemanticDecision::Rejected(error));
        }
        Ok(SemanticDecision::Accepted((
            AcceptedUpsert {
                ttl: UpsertTtlPreparation::NewItem(self.ttl),
                stream_image: self.stream_image,
                route: self.route,
                table: self.table,
                key: self.key,
                new_item: self.new_item,
                item_size: self.item_size,
                old_item: old.item,
                declaration: self.declaration,
                old_indexers: old.indexers,
                old_revision: old.revision,
                old_provenance: old.provenance,
                old_ttl_epoch: old.ttl_epoch,
                item_stream_ttl_hours: self.item_stream_ttl_hours,
                gsi_lifecycle: lifecycle,
                prepared_gsi: self.prepared_gsi,
            },
            self.return_old,
        )))
    }
}

/// Shared accepted image pair. Side effects must be staged by the caller's
/// native attempt before its owner can return an acknowledged response.
pub(crate) enum UpsertTtlPreparation {
    NewItem(storage_common::PreparedTtlPut),
    ObservedItem,
}

pub struct AcceptedUpsert {
    pub(crate) ttl: UpsertTtlPreparation,
    pub(crate) stream_image: stream_provider::PreparedStreamImage,
    pub(crate) route: stream_provider::DynamoMutationRoute,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub new_item: WireItem,
    pub item_size: LogicalItemSize,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub declaration: IndexerDeclaration,
    pub old_indexers: Vec<String>,
    pub old_revision: Option<ItemRevision>,
    pub old_provenance: Option<ItemProvenance>,
    pub old_ttl_epoch: Option<i64>,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    pub(crate) gsi_lifecycle: GsiProjectionLifecycle,
    pub(crate) prepared_gsi: storage_backfill::PreparedGsiPut,
}

impl AcceptedUpsert {
    pub fn ttl_effects(&self, config: Option<&TtlConfigRecord>) -> StorageResult<PutTtlEffects> {
        let writes = match &self.ttl {
            UpsertTtlPreparation::NewItem(prepared) => {
                prepared.finish(config, self.old_item.as_deref())?
            }
            UpsertTtlPreparation::ObservedItem => TtlIndexWriteBatch::for_wire_items(
                &self.table.info,
                config,
                self.old_item.as_deref(),
                Some(&self.new_item),
            )?,
        };
        let epoch = writes
            .iter()
            .fold(self.old_ttl_epoch, |_, write| match write {
                TtlIndexWrite::Delete(_) => None,
                TtlIndexWrite::Upsert(key) => Some(key.ttl_seconds()),
            });
        Ok(PutTtlEffects { epoch, writes })
    }
}

pub(crate) fn old_image_for_response(
    old: Option<&WireItem>,
    requested: bool,
) -> StorageResult<Option<AttributeMap>> {
    if !requested {
        return Ok(None);
    }
    old.map(AttributeMap::try_from).transpose()
}

/// Evaluate a Dynamo condition after the protected old image is known.
/// Both put and delete use the same error and optional old-image rule.
pub(crate) fn check_write_condition(
    condition: Option<&UncheckedCondition>,
    old: Option<&WireItem>,
    return_old_on_failure: bool,
) -> StorageResult<SemanticDecision<()>> {
    let matches = match condition {
        Some(condition) => match SemanticDecision::try_from(condition.check_borrowed(old))? {
            SemanticDecision::Accepted(matches) => matches,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        },
        None => true,
    };
    if !matches {
        let error = match (return_old_on_failure, old) {
            (true, Some(item)) => StorageEnum::ConditionalCheckFailedWithItem {
                item: AttributeMap::try_from(item)?,
            },
            _ => StorageEnum::ConditionalCheckFailed,
        };
        return Ok(SemanticDecision::Rejected(error.into()));
    }
    Ok(SemanticDecision::Accepted(()))
}
