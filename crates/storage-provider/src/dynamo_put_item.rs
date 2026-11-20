//! Shared PutItem shape, table validation, and condition decision.
//!
//! Physical item, index, TTL, stream, and receipt effects are applied by the
//! SQL or KV family in the same native attempt after `accept_old` accepts.

use std::{future::Future, sync::Arc};

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlIndexKey, TtlIndexWriteBatch};
use storage_types::{
    AllOld, AttributeMap, BoundTableInfo, IndexerDeclaration, ItemProvenance, KeyAttributes,
    LogicalItemSize, PutItemRequest, RegionalMarker, StorageEnum, StorageError, StorageResult,
    StoredWireItem, TableName, WireItem, condition::CompiledCondition,
    return_values_on_condition_check_failure_all_old, validate_transact_put_item_key,
};

use crate::{
    AcceptedDeleteItem, DynamoStreamWriteAttempt, LiveItemMetadata, SemanticDecision,
    TtlPolicyAttempt,
};

/// The native write attempt returns protected physical presence.
pub enum ItemWriteObservation {
    Absent,
    Live {
        stored: Box<StoredWireItem>,
        provenance: ItemProvenance,
        ttl_epoch: Option<i64>,
    },
    /// No live item; the key has a regional delete marker (paired tables only).
    Deleted {
        marker: RegionalMarker,
    },
}

impl ItemWriteObservation {
    /// Attach the key's marker, read by the backend in the same native call as
    /// the item.
    pub fn with_marker(self, marker: Option<RegionalMarker>) -> StorageResult<Self> {
        match (self, marker) {
            (observation, None) => Ok(observation),
            (Self::Absent | Self::Deleted { .. }, Some(marker)) => Ok(Self::Deleted { marker }),
            (Self::Live { .. }, Some(_)) => Err(StorageError::internal(
                "a live item has a regional delete marker",
            )),
        }
    }
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

    /// A paired write target reads its key's regional delete marker in the
    /// same native call as the item (REG-0 "Paired write"). A condition check
    /// never does: it sees a marked key as absent.
    pub fn reads_marker(&self) -> bool {
        matches!(self, Self::Write(table, _) if table.binding.regional.is_some())
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
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
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
    items: storage_types::Exact<ItemWriteObservation>,
    lifecycles: Vec<(TableName, GsiProjectionLifecycle)>,
) -> StorageResult<storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>> {
    items.try_zip_map(reads, |read, item| {
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
        Ok((item, lifecycle))
    })
}

/// Key-complete old state used by every write decision after its protected
/// read.
pub(crate) struct ObservedWrite {
    pub item: Option<storage_types::VerifiedWireItem>,
    pub indexers: Vec<String>,
    pub provenance: Option<ItemProvenance>,
    pub marker: Option<RegionalMarker>,
}

impl ObservedWrite {
    pub fn try_new(observed: ItemWriteObservation, key: &KeyAttributes) -> StorageResult<Self> {
        let (item, indexers, provenance, marker) = match observed {
            ItemWriteObservation::Absent => (None, Vec::new(), None, None),
            ItemWriteObservation::Deleted { marker } => (None, Vec::new(), None, Some(marker)),
            ItemWriteObservation::Live {
                mut stored,
                provenance,
                ttl_epoch: _,
            } => {
                let indexers = std::mem::take(&mut stored.indexers);
                let item = storage_types::VerifiedWireItem::try_from_stored(*stored, key)?;
                (Some(item), indexers, Some(provenance), None)
            }
        };
        Ok(Self {
            item,
            indexers,
            provenance,
            marker,
        })
    }
}

/// The family lowerer protects the item, including absence, inside
/// the write attempt already opened by its driver. The TTL policy and GSI
/// phases come from the confirmed catalog binding, not from a read.
#[derive(Clone, Copy)]
pub struct ItemWritePrerequisites {
    pub needs_history: bool,
}

/// All requested names and valid item targets of one protected write
/// observation.
pub struct WriteObservationBatch<'a> {
    pub tables: &'a [(&'a TableName, Option<crate::TableReadCandidate<'a>>)],
    pub reads: &'a [ItemWriteObservationRead<'a>],
    pub receipt_key: Option<crate::ReceiptSlot<'a>>,
    pub prerequisites: ItemWritePrerequisites,
    pub cache: Option<&'a crate::TableMetadataCache>,
}

pub struct WriteObservationFailure {
    /// Positions in `WriteObservationBatch::tables` of the tables whose item
    /// statement failed; empty when the failed statement read no item table.
    pub candidates: Vec<usize>,
    pub error: storage_types::StorageError,
}

pub struct WriteObservationBatchResult {
    pub receipt: Option<Option<Vec<u8>>>,
    pub catalog_version: Option<storage_types::CatalogVersion>,
    pub stamps: Option<storage_types::Exact<Option<crate::CatalogStamp>>>,
    pub observations: Result<
        Option<storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>>,
        WriteObservationFailure,
    >,
    pub publications: Vec<crate::CachePublication>,
}

/// Sequential physical families share authority ordering and the whole-batch
/// guard.
pub async fn read_write_observations_default<A>(
    attempt: &mut A,
    batch: WriteObservationBatch<'_>,
) -> StorageResult<WriteObservationBatchResult>
where
    A: ItemWriteAttempt + crate::CatalogAttempt + crate::DynamoTransactWriteAttempt,
{
    let receipt = match batch.receipt_key {
        Some(slot) => Some(attempt.read_transact_receipt(slot).await?),
        None => None,
    };
    if crate::dynamo_pipelined_authority::begin_pipelined_authority(
        attempt,
        batch
            .tables
            .iter()
            .map(|(_, candidate)| candidate.and_then(|candidate| candidate.catalog_version)),
    )? {
        let values = attempt.observe_for_writes(batch.reads).await;
        let authority = crate::dynamo_pipelined_authority::finish_pipelined_authority(
            attempt,
            batch.tables.iter().copied(),
            values.is_ok(),
        )
        .await?;
        let (version, stamps, values) = match authority {
            crate::dynamo_pipelined_authority::PipelinedAuthority::Observed {
                version,
                stamps,
                confirmed: true,
            } => (version, stamps, Some(values?)),
            crate::dynamo_pipelined_authority::PipelinedAuthority::Observed {
                version,
                stamps,
                confirmed: false,
            } => (version, stamps, None),
            crate::dynamo_pipelined_authority::PipelinedAuthority::ReadAgain {
                version,
                stamps,
            } => (
                version,
                stamps,
                Some(attempt.observe_for_writes(batch.reads).await?),
            ),
            crate::dynamo_pipelined_authority::PipelinedAuthority::Failed => {
                return Err(match values {
                    Err(error) => error,
                    Ok(_) => StorageError::internal("failed write observation succeeded"),
                });
            }
        };
        let mut publications = Vec::new();
        let observations = match values {
            // A failure under a confirmed binding was returned above
            // (`values?`).
            Some(values) => {
                if batch.prerequisites.needs_history {
                    publications.extend(
                        crate::dynamo_catalog::prepare_writer_routes_for_version(
                            attempt,
                            batch.cache,
                            version,
                        )
                        .await?,
                    );
                    if let Some(read) = batch.reads.first() {
                        attempt
                            .read_item_write_prerequisites(read.target().0, batch.prerequisites)
                            .await?;
                    }
                }
                Some(values)
            }
            None => None,
        };
        return Ok(WriteObservationBatchResult {
            receipt,
            catalog_version: version,
            stamps: Some(stamps),
            observations: Ok(observations),
            publications,
        });
    }
    let version = attempt.read_catalog_version().await?;
    let stamps = crate::dynamo_catalog::observe_catalog_stamps(
        attempt,
        batch.tables.iter().copied(),
        version,
    )
    .await?;
    let matches = batch
        .tables
        .iter()
        .zip(&stamps)
        .all(|((_, candidate), actual)| candidate.map(|candidate| candidate.stamp) == *actual);
    let mut publications = Vec::new();
    let observations = if matches {
        if batch.prerequisites.needs_history {
            publications.extend(
                crate::dynamo_catalog::prepare_writer_routes_for_version(
                    attempt,
                    batch.cache,
                    version,
                )
                .await?,
            );
            if let Some(read) = batch.reads.first() {
                attempt
                    .read_item_write_prerequisites(read.target().0, batch.prerequisites)
                    .await?;
            }
        }
        let values = attempt.observe_for_writes(batch.reads).await?;
        Some(values)
    } else {
        None
    };
    Ok(WriteObservationBatchResult {
        receipt,
        catalog_version: version,
        stamps: Some(stamps),
        observations: Ok(observations),
        publications,
    })
}

/// Physical upsert effects shared by PutItem and UpdateItem.
///
/// A physical delete lowerer uses the same protected read as PutItem. It must
/// persist primary removal, TTL removal, GSI removal, and stream
/// effects before acknowledging the native attempt.
///
/// Native-family preparation. Ordinary callers can encode before admission;
/// callers that select a layout inside the attempt prepare through that view.
pub trait ItemWriteAttempt:
    TtlPolicyAttempt + DynamoStreamWriteAttempt + crate::RegionalMarkerAttempt
{
    type PreparedWrites: Default + Send + 'static;

    /// Observe stream routing in one native read wave.
    fn prepare_item_write(
        &mut self,
        table: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            self.read_item_write_prerequisites(
                table,
                ItemWritePrerequisites {
                    needs_history: table.info.requires_stream_entry(),
                },
            )
            .await
        }
    }

    fn read_item_write_prerequisites(
        &mut self,
        table: &BoundTableInfo,
        prerequisites: ItemWritePrerequisites,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Observe the item under the same native attempt and return the GSI
    /// lifecycle that this write maintains.
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
            let [observation] = observations.into_array::<1>()?;
            Ok(observation)
        }
    }

    /// Read independent item states and each table's mutable GSI lifecycle
    /// together in the same protected attempt.
    fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> impl Future<
        Output = StorageResult<
            storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>,
        >,
    > + Send;

    /// Prepare immutable image dependencies and append primary/TTL effects
    /// inside this attempt. The caller also prepares projections, streams and
    /// receipts before the complete native batch can settle. It never stages a
    /// regional marker: the caller lowers `item.regional`.
    fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &LiveItemMetadata<'_>,
        writes: TtlIndexWriteBatch,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Stage the removal of a live item (`item.old_item` is `Some`): its
    /// primary row and `writes`. It never stages a regional marker: the caller
    /// lowers `item.regional`.
    fn stage_primary_delete(
        &mut self,
        item: &AcceptedDeleteItem,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()>;

    /// Install preparation and retain any failure in native settlement, even
    /// when no upsert consumes it. Later installation cannot reopen the
    /// attempt.
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>);
    /// Encode and install against this attempt's physical layout. Failure
    /// discards previous preparation and rejects native settlement with its
    /// typed cause; no root-layout values are borrowed.
    fn prepare_put_items(&mut self, items: &[&crate::BoundPutItem]) -> StorageResult<()>;
}

/// One shared TTL decision for the primary row and its physical index.
pub struct PutTtlEffects<'a> {
    key: Option<std::borrow::Cow<'a, TtlIndexKey>>,
    writes: TtlIndexWriteBatch,
}

impl PutTtlEffects<'_> {
    pub fn metadata(
        &self,
        provenance: storage_types::ItemProvenance,
    ) -> crate::LiveItemMetadata<'_> {
        crate::LiveItemMetadata::new(provenance, self.key.as_deref())
    }

    pub fn take_writes(&mut self) -> TtlIndexWriteBatch {
        std::mem::replace(&mut self.writes, TtlIndexWriteBatch::empty())
    }
}

/// Request facts that can be checked before opening a native attempt.
pub struct PutItemShape {
    pub table_name: TableName,
    item: Arc<WireItem>,
    item_size: LogicalItemSize,
    indexers: Vec<String>,
    condition: Option<CompiledCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
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
        let condition = CompiledCondition::compile_write(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
        Ok(Self {
            table_name: request.table_name,
            item: Arc::new(request.item),
            item_size,
            indexers: request.indexers.unwrap_or_default(),
            condition,
            return_old: matches!(request.return_values, Some(AllOld::AllOld)),
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
        })
    }

    /// Schema-dependent validation follows the authoritative catalog read
    /// within this operation's native attempt.
    pub fn bind_table(self: Arc<Self>, table: Arc<BoundTableInfo>) -> StorageResult<BoundPutItem> {
        let fields = table
            .info
            .key_schema
            .iter()
            .map(|key| key.attribute_name.as_str())
            .chain(self.indexers.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let attributes = self.item.project_attributes(&fields)?;
        validate_transact_put_item_key(&table.info, &attributes)?;
        let key = KeyAttributes::from_schema(&table.info.key_schema, &attributes)
            .map_err(|_| StorageError::validation("One or more parameter values were invalid"))?;
        let declaration =
            IndexerDeclaration::try_new(self.indexers.clone(), table.info.max_indexers)?;
        storage_types::validate_logical_item(&attributes, &declaration)?;
        let prepared_gsi =
            storage_backfill::PreparedGsiPut::try_new(&table.info, &self.item, &declaration)?;
        let history_route = crate::dynamo_write_execution::prepare_history_route(&table, &key)?;
        let ttl = storage_common::PreparedTtlPut::try_new(
            &table.info,
            table.binding.ttl.as_ref(),
            &self.item,
        )?;
        Ok(BoundPutItem {
            history_route,
            ttl,
            prepared_gsi,
            table,
            new_item: storage_types::VerifiedWireItem::try_from_shared(
                Arc::clone(&self.item),
                &key,
            )?,
            key,
            item_size: self.item_size,
            declaration,
            shape: self,
        })
    }
}

/// Validated logical PutItem. A family lowerer protects and reads its current
/// item before this value evaluates the condition.
pub struct BoundPutItem {
    history_route: Option<stream_provider::DynamoMutationRoute>,
    ttl: storage_common::PreparedTtlPut,
    prepared_gsi: storage_backfill::PreparedGsiPut,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub new_item: storage_types::VerifiedWireItem<Arc<WireItem>>,
    pub item_size: LogicalItemSize,
    pub declaration: IndexerDeclaration,
    shape: Arc<PutItemShape>,
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
            self.shape.condition.as_ref(),
            old.item.as_deref(),
            self.shape.return_old_on_condition_failure,
        )? {
            return Ok(SemanticDecision::Rejected(error));
        }
        Ok(SemanticDecision::Accepted((
            AcceptedUpsert {
                history_route: self.history_route,
                ttl: UpsertTtlPreparation::NewItem(self.ttl),
                table: self.table,
                key: self.key,
                new_item: self.new_item,
                item_size: self.item_size,
                old_item: old.item,
                declaration: self.declaration,
                old_indexers: old.indexers,
                old_provenance: old.provenance,
                old_marker: old.marker,
                regional: None,
                gsi_lifecycle: lifecycle,
                prepared_gsi: self.prepared_gsi,
            },
            self.shape.return_old,
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
    pub(crate) history_route: Option<stream_provider::DynamoMutationRoute>,
    pub(crate) ttl: UpsertTtlPreparation,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub new_item: storage_types::VerifiedWireItem<Arc<WireItem>>,
    pub item_size: LogicalItemSize,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub declaration: IndexerDeclaration,
    pub old_indexers: Vec<String>,
    pub old_provenance: Option<ItemProvenance>,
    pub old_marker: Option<RegionalMarker>,
    /// Set by `prepare_accepted_upsert`; read by the backend lowering.
    pub regional: Option<crate::RegionalUpsert>,
    pub(crate) gsi_lifecycle: GsiProjectionLifecycle,
    pub(crate) prepared_gsi: storage_backfill::PreparedGsiPut,
}

impl AcceptedUpsert {
    pub fn ttl_effects(&self) -> StorageResult<PutTtlEffects<'_>> {
        let config = self.table.binding.ttl.as_ref();
        let (key, writes) = match &self.ttl {
            UpsertTtlPreparation::NewItem(prepared) => (
                prepared.new_key().map(std::borrow::Cow::Borrowed),
                prepared.finish(config, self.old_item.as_deref())?,
            ),
            UpsertTtlPreparation::ObservedItem => {
                let prepared = storage_common::PreparedTtlPut::try_new(
                    &self.table.info,
                    config,
                    &self.new_item,
                )?;
                let writes = prepared.finish(config, self.old_item.as_deref())?;
                let key = match prepared {
                    storage_common::PreparedTtlPut::Disabled => None,
                    storage_common::PreparedTtlPut::NewItem { key, .. } => {
                        key.map(std::borrow::Cow::Owned)
                    }
                };
                (key, writes)
            }
        };
        Ok(PutTtlEffects { key, writes })
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
    condition: Option<&CompiledCondition>,
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
