use std::sync::Arc;

use queue_provider::{
    MessageId, QueueMessage, ReceiveMessageRequest, ReceiveMessageResponse, SendMessageRequest,
};
use storage_common::{TtlConfigRecord, TtlIndexWriteBatch};
use storage_types::{
    BoundedVec, DeleteItemRequest, IndexerDeclaration, KeyAttributes, NonEmptyString, NonZeroVec,
    PutItemRequest, ReturnValuesOldNewUpdated, StorageError, StorageResult, StoredTableInfo,
    StreamItemId, StreamName, StreamRetentionDuration, TableKey, TableName, UpdateItemRequest,
    WireItem,
};
use stream_provider::StreamItem;

use super::conditions::ConditionFailReceipt;
use crate::{PhysicalWrite, TableStreamContext, TableUpdateContext, TtlWriteContext};

/// Stream-record intent derived without a database read: table identity and
/// key plus provenance from the write context. Stream item ids and
/// timestamps are allocated by the backend inside the transaction.
///
/// Owned and `Send + 'static` so blocking backends can capture the batch
/// directly.
#[derive(Clone)]
pub struct StreamWriteIntent {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub context: TableStreamContext,
}

/// TTL intent derived without a database read. The insert/update/delete
/// diff itself needs the old item and is computed in `finish_with_old` via
/// `TtlIndexWriteBatch::for_wire_items`.
///
/// The table snapshot is `Arc`-shared (usually a cache entry): cloning the
/// batch clones the `Arc`, not the table.
#[derive(Clone)]
pub struct TtlWriteIntent {
    pub config: Option<TtlConfigRecord>,
    pub context: TtlWriteContext,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    pub table: Arc<StoredTableInfo>,
}

/// Foreground put input. The request travels inside its manager-derived
/// `PhysicalWrite` (ordinal plus TTL sidecar); table metadata and stream
/// context arrive alongside, never inferred inside the provider.
#[derive(Clone)]
pub struct TablePutInput {
    pub write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
    pub table: Arc<StoredTableInfo>,
    pub stream: TableStreamContext,
    pub ttl_config: Option<TtlConfigRecord>,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

/// Foreground update input. Update semantics reuse the shared
/// `TableUpdateContext` bundle alongside the manager-derived write.
#[derive(Clone)]
pub struct TableUpdateInput {
    pub write: PhysicalWrite<UpdateItemRequest, TtlWriteContext>,
    pub table: Arc<StoredTableInfo>,
    pub context: TableUpdateContext,
}

impl TablePutInput {
    pub fn for_single_put(
        write: PhysicalWrite<PutItemRequest, TtlWriteContext>,
        table: Arc<StoredTableInfo>,
        immediate_gsi_consistency: bool,
        ttl_config: Option<TtlConfigRecord>,
    ) -> Self {
        let item_stream_ttl_hours = write.request().aux_item_stream_ttl_hours;
        Self {
            write,
            table,
            stream: TableStreamContext {
                item_revision: None,
                stream_item_id: None,
                target_item_stream_version: None,
                created_at: None,
                immediate_gsi_consistency,
                gsi_lifecycle: None,
            },
            ttl_config,
            item_stream_ttl_hours,
        }
    }
}

#[derive(Clone)]
pub struct SingleUpdateOptions {
    pub immediate_gsi_consistency: bool,
    pub ttl_config: Option<TtlConfigRecord>,
}

impl TableUpdateInput {
    pub fn for_single_update(
        write: PhysicalWrite<UpdateItemRequest, TtlWriteContext>,
        table: Arc<StoredTableInfo>,
        options: SingleUpdateOptions,
    ) -> Self {
        let item_stream_ttl_hours = write.request().aux_item_stream_ttl_hours;
        let ttl_context = write.context().clone();
        Self {
            write,
            table,
            context: TableUpdateContext {
                stream: TableStreamContext {
                    item_revision: None,
                    stream_item_id: None,
                    target_item_stream_version: None,
                    created_at: None,
                    immediate_gsi_consistency: options.immediate_gsi_consistency,
                    gsi_lifecycle: None,
                },
                index: 0,
                ttl_config: options.ttl_config,
                ttl_context,
                item_stream_ttl_hours,
            },
        }
    }
}

/// Foreground delete input.
#[derive(Clone)]
pub struct TableDeleteInput {
    pub write: PhysicalWrite<DeleteItemRequest, TtlWriteContext>,
    pub table: Arc<StoredTableInfo>,
    pub stream: TableStreamContext,
    pub ttl_config: Option<TtlConfigRecord>,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl TableDeleteInput {
    pub fn for_single_delete(
        write: PhysicalWrite<DeleteItemRequest, TtlWriteContext>,
        table: Arc<StoredTableInfo>,
        immediate_gsi_consistency: bool,
        ttl_config: Option<TtlConfigRecord>,
    ) -> Self {
        let item_stream_ttl_hours = write.request().aux_item_stream_ttl_hours;
        Self {
            write,
            table,
            stream: TableStreamContext {
                item_revision: None,
                stream_item_id: None,
                target_item_stream_version: None,
                created_at: None,
                immediate_gsi_consistency,
                gsi_lifecycle: None,
            },
            ttl_config,
            item_stream_ttl_hours,
        }
    }
}

/// Foreground condition-check input (transact-write member). The expression
/// is required by type; parsing happens in preparation.
#[derive(Clone)]
pub struct TableCheckInput {
    pub ordinal: usize,
    pub table_name: TableName,
    pub table: Arc<StoredTableInfo>,
    pub key: KeyAttributes,
    pub condition_expression: String,
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    pub return_values_on_condition_check_failure: Option<String>,
}

/// One batch of foreground writes sharing a single transaction at stage
/// time. Members keep their manager-assigned ordinals so receipts align
/// with requests. `try_new` proves the transaction-framed bound (non-empty,
/// at most 100 members) once, here, and never re-checks it downstream; the
/// shared write-preparation owner constructs the struct literal directly so
/// ordinary batches keep their own empty/batch-limit behaviour instead of
/// adopting the transaction rules.
#[derive(Clone)]
pub struct TableWriteBatchInput {
    pub puts: Vec<TablePutInput>,
    pub updates: Vec<TableUpdateInput>,
    pub deletes: Vec<TableDeleteInput>,
    pub checks: Vec<TableCheckInput>,
}

impl TableWriteBatchInput {
    pub fn try_new(
        puts: Vec<TablePutInput>,
        updates: Vec<TableUpdateInput>,
        deletes: Vec<TableDeleteInput>,
        checks: Vec<TableCheckInput>,
    ) -> StorageResult<Self> {
        let total = puts.len() + updates.len() + deletes.len() + checks.len();
        if total == 0 {
            return Err(StorageError::validation(
                "Transaction batch cannot be empty",
            ));
        }
        if total > 100 {
            return Err(StorageError::validation(
                "Transaction batch cannot contain more than 100 operations",
            ));
        }
        Ok(Self {
            puts,
            updates,
            deletes,
            checks,
        })
    }

    /// Consume the grouped batch into the shared member vocabulary while
    /// retaining each request's manager-assigned ordinal.  Native adapters
    /// use this before authoritative metadata preparation so they do not
    /// reconstruct a second semantic request shape from stale snapshots.
    pub fn into_members(self) -> Vec<super::write_preparation::TableWriteMember> {
        let mut members = Vec::with_capacity(
            self.puts.len() + self.updates.len() + self.deletes.len() + self.checks.len(),
        );
        members.extend(
            self.puts
                .into_iter()
                .map(|put| super::write_preparation::TableWriteMember::Put(put.write)),
        );
        members.extend(
            self.updates
                .into_iter()
                .map(|update| super::write_preparation::TableWriteMember::Update(update.write)),
        );
        members.extend(
            self.deletes
                .into_iter()
                .map(|delete| super::write_preparation::TableWriteMember::Delete(delete.write)),
        );
        members.extend(self.checks.into_iter().map(|check| {
            super::write_preparation::TableWriteMember::Check(
                super::write_preparation::TableCheckMember::new(
                    check.ordinal,
                    storage_types::TransactConditionCheckRequest {
                        table_name: check.table_name,
                        key: check.key,
                        condition_expression: check.condition_expression,
                        expression_attribute_names: check.expression_attribute_names,
                        expression_attribute_values: check.expression_attribute_values,
                        return_values_on_condition_check_failure: check
                            .return_values_on_condition_check_failure,
                    },
                ),
            )
        }));
        members
    }
}

/// Finished put: old-item diffs resolved, ready for backend lowering.
#[derive(Clone)]
pub struct FinishedTablePut {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub table_key: TableKey,
    pub new_item: WireItem,
    pub old_item: Option<WireItem>,
    pub new_indexers: IndexerDeclaration,
    pub ttl_writes: TtlIndexWriteBatch,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    pub ttl_context: TtlWriteContext,
    pub stream: TableStreamContext,
}

/// Finished update: applied image plus diffs, ready for backend lowering.
#[derive(Clone)]
pub struct FinishedTableUpdate {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub table_key: TableKey,
    pub new_item: WireItem,
    pub old_item: Option<WireItem>,
    /// None inherits the stored declaration from the old item at stage time.
    pub new_indexers: Option<IndexerDeclaration>,
    pub ttl_writes: TtlIndexWriteBatch,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    pub ttl_context: TtlWriteContext,
    pub stream: TableStreamContext,
    pub return_values: Option<ReturnValuesOldNewUpdated>,
}

/// Finished delete: old image captured for stream and receipts.
#[derive(Clone)]
pub struct FinishedTableDelete {
    pub ordinal: usize,
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub table_key: TableKey,
    pub old_item: Option<WireItem>,
    pub ttl_writes: TtlIndexWriteBatch,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    pub ttl_context: TtlWriteContext,
    pub stream: TableStreamContext,
}

/// Per-member receipt preserving the manager-assigned ordinal.
pub struct TableWriteReceipt {
    pub ordinal: usize,
    pub old_item: Option<WireItem>,
    pub new_item: Option<WireItem>,
    pub condition_failed: Option<ConditionFailReceipt>,
}

/// Ordered receipts for one staged batch.
pub struct WriteReceipts {
    pub receipts: Vec<TableWriteReceipt>,
}

/// Error for staged table writes. Condition failures carry the ordinal,
/// old image, and whether the caller asked for it back, so the delegation
/// layer can map `ReturnValuesOnConditionCheckFailure` without re-reading.
#[derive(Debug)]
pub enum TableWriteError {
    Failed(StorageError),
    ConditionFailed {
        ordinal: usize,
        old_item: Option<Box<WireItem>>,
        return_old_on_condition_failure: bool,
    },
}

impl TableWriteError {
    pub fn condition_failed(
        ordinal: usize,
        old_item: Option<Box<WireItem>>,
        return_old_on_condition_failure: bool,
    ) -> Self {
        Self::ConditionFailed {
            ordinal,
            old_item,
            return_old_on_condition_failure,
        }
    }
}

impl From<StorageError> for TableWriteError {
    fn from(error: StorageError) -> Self {
        Self::Failed(error)
    }
}

impl From<TableWriteError> for StorageError {
    fn from(error: TableWriteError) -> Self {
        match error {
            TableWriteError::Failed(error) => error,
            TableWriteError::ConditionFailed {
                old_item,
                return_old_on_condition_failure,
                ..
            } => match (return_old_on_condition_failure, old_item) {
                (true, Some(old_item)) => {
                    let item = old_item.into_attribute_map();
                    match item {
                        Ok(item) => {
                            storage_types::StorageEnum::ConditionalCheckFailedWithItem { item }
                                .into()
                        }
                        Err(_) => storage_types::StorageEnum::ConditionalCheckFailed.into(),
                    }
                }
                _ => storage_types::StorageEnum::ConditionalCheckFailed.into(),
            },
        }
    }
}

/// Snapshot for pure batch-level validation. Read-dependent validation
/// (conditions, update application) happens at finish time, not here.
pub struct ValidationSnapshot {}

/// Foreground queue write input: one or more sends sharing a transaction
/// at stage time. Single sends travel as a one-member batch. The bound is
/// proven once, here, and never re-checked downstream.
#[derive(Debug)]
pub struct QueueWriteInput {
    pub sends: BoundedVec<QueueSend, 1, 10>,
}

impl QueueWriteInput {
    pub fn try_new(sends: Vec<QueueSend>) -> StorageResult<Self> {
        Ok(Self {
            sends: BoundedVec::try_new(sends)?,
        })
    }

    /// Group sends by queue URL, preserving manager ordinals. The native
    /// batch entry requires a single queue per call, so multi-queue batches
    /// fan out per URL; groups target disjoint queues and commit
    /// independently. Shared by every backend; only the per-group native
    /// send verb differs.
    pub fn into_groups_by_queue(self) -> Vec<(String, Vec<(usize, SendMessageRequest)>)> {
        let mut groups: Vec<(String, Vec<(usize, SendMessageRequest)>)> = Vec::new();
        for send in self.sends.into_inner() {
            if let Some((_, requests)) = groups
                .iter_mut()
                .find(|(queue_url, _)| queue_url == &send.request.queue_url)
            {
                requests.push((send.ordinal, send.request));
            } else {
                groups.push((
                    send.request.queue_url.clone(),
                    vec![(send.ordinal, send.request)],
                ));
            }
        }
        groups
    }
}

/// One send with its manager-assigned ordinal. Wire shapes arrive validated
/// at the request boundary; this struct only pairs the ordinal. A single
/// struct serves input and staging: there is no derivation between phases
/// to give phase-specific shapes content.
#[derive(Debug)]
pub struct QueueSend {
    pub ordinal: usize,
    pub request: SendMessageRequest,
}

/// Foreground queue read input reusing the provider request shape.
pub struct QueueReadInput {
    pub request: ReceiveMessageRequest,
}

impl QueueReadInput {
    /// SQS receive defaults shared by every backend fetch. The manager's
    /// wait-budget loop stays above this trait; these are the single-receive
    /// effective values.
    pub fn max_messages(&self) -> u32 {
        self.request
            .max_number_of_messages
            .unwrap_or(queue_provider::DEFAULT_RECEIVE_MAX_MESSAGES)
    }

    pub fn visibility_timeout(&self) -> storage_types::DurationSeconds {
        storage_types::DurationSeconds::from(
            self.request
                .visibility_timeout
                .unwrap_or(queue_provider::DEFAULT_RECEIVE_VISIBILITY_TIMEOUT_SECS),
        )
    }

    pub fn wait_time_seconds(&self) -> storage_types::DurationSeconds {
        storage_types::DurationSeconds::from(
            self.request
                .wait_time_seconds
                .unwrap_or(queue_provider::DEFAULT_RECEIVE_WAIT_TIME_SECONDS)
                .min(queue_provider::MAX_WAIT_TIME_SECONDS),
        )
    }
}

/// Page of fetched queue messages reusing the provider response shape.
pub type QueuePage = ReceiveMessageResponse;

/// Intra-transaction fetch descriptor for queue state a stage needs
/// (existence, deduplication, visibility). Backends interpret the ids
/// against their native layout.
pub struct QueueNativeRead {
    pub queue_url: String,
    pub message_ids: Vec<String>,
}

/// Native queue page for stage-time decisions.
pub struct QueueNativePage {
    pub messages: Vec<QueueMessage>,
}

/// Per-send receipt preserving the manager-assigned ordinal.
pub struct QueueWriteReceipt {
    pub ordinal: usize,
    pub message_id: MessageId,
}

/// Receipts for one staged queue batch.
pub struct QueueReceipts {
    pub receipts: Vec<QueueWriteReceipt>,
}

impl QueueReceipts {
    /// Receipts in manager-ordinal order from unordered per-group send
    /// outcomes. Shared by every backend; groups commit independently.
    pub fn in_ordinal_order(mut pairs: Vec<(usize, MessageId)>) -> Self {
        pairs.sort_by_key(|(ordinal, _)| *ordinal);
        Self {
            receipts: pairs
                .into_iter()
                .map(|(ordinal, message_id)| QueueWriteReceipt {
                    ordinal,
                    message_id,
                })
                .collect(),
        }
    }
}

/// Foreground stream append input: one or more items sharing a transaction
/// at stage time. The non-empty bound is proven once, here, and never
/// re-checked downstream.
pub struct StreamWriteInput {
    pub stream: StreamName,
    pub appends: NonZeroVec<StreamAppend>,
}

impl StreamWriteInput {
    pub fn try_new(stream: StreamName, appends: Vec<StreamAppend>) -> StorageResult<Self> {
        Ok(Self {
            stream,
            appends: NonZeroVec::try_new(appends)?,
        })
    }
}

/// One item to append with its manager-assigned ordinal. Ids and timestamps
/// are backend-allocated inside the transaction at stage time. A single
/// struct serves input and staging: there is no derivation between phases
/// to give phase-specific shapes content.
pub struct StreamAppend {
    pub ordinal: usize,
    pub item_data: NonZeroVec<u8>,
    pub partition_key: Option<NonEmptyString>,
}

impl StreamAppend {
    pub fn try_new(
        ordinal: usize,
        item_data: Vec<u8>,
        partition_key: Option<String>,
    ) -> StorageResult<Self> {
        Ok(Self {
            ordinal,
            item_data: NonZeroVec::try_new(item_data)?,
            partition_key: partition_key.map(NonEmptyString::try_new).transpose()?,
        })
    }
}

/// Foreground stream read input.
pub struct StreamReadInput {
    pub stream: StreamName,
    pub exclusive_start_key: Option<StreamItemId>,
    pub limit: std::num::NonZeroU32,
    pub scan_forward: bool,
}

impl StreamReadInput {
    pub fn try_new(
        stream: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
        scan_forward: bool,
    ) -> StorageResult<Self> {
        let limit = std::num::NonZeroU32::new(limit)
            .ok_or_else(|| StorageError::validation("Stream read limit must be non-zero"))?;
        if limit.get() > storage_types::DYNAMODB_STREAM_RECORDS_LIMIT_MAX {
            return Err(StorageError::validation(
                storage_types::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
            ));
        }
        Ok(Self {
            stream,
            exclusive_start_key,
            limit,
            scan_forward,
        })
    }
}

/// Intra-transaction fetch descriptor for stream state a stage needs.
/// Backends interpret the ids against their native layout.
pub struct StreamNativeRead {
    pub stream: StreamName,
    pub item_ids: Vec<StreamItemId>,
}

/// Native stream state for stage-time decisions.
pub struct StreamNativeState {
    pub items: Vec<StreamItem>,
}

/// Per-append receipt preserving the manager-assigned ordinal.
pub struct StreamWriteReceipt {
    pub ordinal: usize,
    pub item_id: StreamItemId,
}

/// Receipts for one staged stream batch.
pub struct StreamReceipts {
    pub receipts: Vec<StreamWriteReceipt>,
}

/// Foreground TTL index write input. Table-driven TTL diffs ride along
/// table writes via `finish_with_old`; this entry covers direct index
/// maintenance (replication and test paths share it with
/// `TtlSweepRepository` semantics). TTL rows are table-scoped, so the batch
/// names its table; backends resolve the identity once and never re-check it
/// per write.
pub struct TtlWriteInput {
    pub table_name: TableName,
    pub writes: TtlIndexWriteBatch,
}

impl TtlWriteInput {
    pub fn try_new(table_name: TableName, writes: TtlIndexWriteBatch) -> StorageResult<Self> {
        if writes.is_empty() {
            return Err(StorageError::validation(
                "TTL batch must contain at least one write",
            ));
        }
        Ok(Self { table_name, writes })
    }
}

/// Receipts for one staged TTL batch.
pub struct TtlReceipts {
    pub applied_writes: usize,
}
