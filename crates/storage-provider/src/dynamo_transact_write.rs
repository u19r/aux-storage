//! Shared TransactWriteItems validation, replay, and one-attempt execution.

use std::{future::Future, sync::Arc};

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    BoundTableInfo, CancellationReason, DeleteItemRequest, DynamoRequestValidate as _,
    KeyAttributes, PutItemRequest, StorageEnum, StorageError, StorageResult, TableName,
    TimestampMillis, TransactConditionCheckRequest, TransactWriteItem, TransactWriteItemsRequest,
    TransactWriteItemsResponse, UpdateItemRequest, condition::CompiledCondition,
    context::WrappedError as _, return_values_on_condition_check_failure_all_old,
    transact_key_fingerprint, transaction_canceled_for_indexed_reasons,
    transaction_cancellation_reason_at, transaction_write_binding_error_with_len,
    validate_transact_key,
};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, BoundDeleteItem, BoundPutItem, BoundUpdateItem,
    DeleteItemShape, ItemWriteAttempt, ItemWriteObservation, ItemWriteObservationRead,
    PutItemShape, SemanticDecision, UpdateItemShape, prepare_accepted_delete,
    prepare_accepted_upsert,
    transact_idempotency::{TransactTokenRequest, encode_transact_receipt},
};

/// Where one transact receipt lives. A plain request token uses the
/// receipt table. A scheduled-job effect uses its execution's receipt
/// group in the scheduled-job data keyspace, so the job store removes it
/// with the execution.
#[derive(Clone, Copy, Debug)]
pub enum ReceiptSlot<'a> {
    Request(&'a str),
    JobEffect(&'a scheduled_bg_jobs::store::StoreKey),
}

/// The lowerer only protects and stores opaque bytes. Token identity, expiry,
/// replay, and response decisions are shared across all five drivers.
pub trait DynamoTransactWriteAttempt: ItemWriteAttempt + crate::CatalogAttempt {
    fn read_write_observations(
        &mut self,
        batch: crate::WriteObservationBatch<'_>,
    ) -> impl Future<Output = StorageResult<crate::WriteObservationBatchResult>> + Send {
        crate::read_write_observations_default(self, batch)
    }

    /// Loads an ordered scheduled-job row with conflict protection in this
    /// same native attempt. Unsupported adapters must fail closed.
    fn read_scheduled_jobs(
        &mut self,
        _keys: &[scheduled_bg_jobs::store::StoreKey],
    ) -> impl Future<Output = StorageResult<Vec<Option<Vec<u8>>>>> + Send {
        async {
            Err(StorageError::unsupported(
                "atomic scheduled-job effects unavailable",
            ))
        }
    }

    fn read_transact_receipt(
        &mut self,
        slot: ReceiptSlot<'_>,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send;

    fn stage_transact_receipt(
        &mut self,
        slot: ReceiptSlot<'_>,
        receipt: Vec<u8>,
    ) -> StorageResult<()>;
}

pub struct DynamoTransactWriteShape {
    members: Arc<[MemberShape]>,
    token: Option<TransactTokenRequest>,
    effect_authority: Option<crate::job_write_authority::JobWriteAuthority>,
}

enum MemberShape {
    Put(Arc<PutItemShape>),
    Update(Arc<UpdateItemShape>),
    Delete(Arc<DeleteItemShape>),
    Check(Arc<ConditionCheckShape>),
}

struct ConditionCheckShape {
    table_name: TableName,
    key: KeyAttributes,
    condition: CompiledCondition,
    return_old_on_failure: bool,
}

struct BoundConditionCheck {
    table: Arc<BoundTableInfo>,
    key: KeyAttributes,
    shape: Arc<ConditionCheckShape>,
}

enum BoundMember {
    Put(Box<BoundPutItem>),
    Update(BoundUpdateItem),
    Delete(Box<BoundDeleteItem>),
    Check(BoundConditionCheck),
}

pub enum AcceptedTransactWriteMember {
    Put(Box<AcceptedUpsert>),
    Update(Box<AcceptedUpsert>),
    Delete(Box<AcceptedDeleteItem>),
    ConditionCheck(Box<AcceptedTransactConditionCheck>),
}

/// The successful condition's protected observation survives acceptance.
/// Regional capture must not re-read the checked item or invent a mutation.
pub struct AcceptedTransactConditionCheck {
    pub old_item: Option<storage_types::VerifiedWireItem>,
}

impl MemberShape {
    fn table_name(&self) -> &TableName {
        match self {
            Self::Put(shape) => &shape.table_name,
            Self::Update(shape) => &shape.table_name,
            Self::Delete(shape) => &shape.table_name,
            Self::Check(shape) => &shape.table_name,
        }
    }

    fn bind(&self, table: Arc<BoundTableInfo>) -> StorageResult<BoundMember> {
        let key = match &self {
            Self::Put(_) => None,
            Self::Update(shape) => Some(shape.key()),
            Self::Delete(shape) => Some(shape.key()),
            Self::Check(shape) => Some(&shape.key),
        };
        if let Some(key) = key {
            validate_transact_key(&table.info, key)?;
        }
        match self {
            Self::Put(shape) => Arc::clone(shape)
                .bind_table(Arc::clone(&table))
                .map(Box::new)
                .map(BoundMember::Put),
            Self::Update(shape) => Arc::clone(shape).bind_table(table).map(BoundMember::Update),
            Self::Delete(shape) => Arc::clone(shape)
                .bind_table(table)
                .map(Box::new)
                .map(BoundMember::Delete),
            Self::Check(shape) => Ok(BoundMember::Check(BoundConditionCheck {
                table,
                key: shape.key.clone(),
                shape: Arc::clone(shape),
            })),
        }
    }
}

impl BoundMember {
    fn target(&self) -> (&BoundTableInfo, &KeyAttributes) {
        match self {
            Self::Put(bound) => (&bound.table, &bound.key),
            Self::Update(bound) => (&bound.table, &bound.key),
            Self::Delete(bound) => (&bound.table, &bound.key),
            Self::Check(bound) => (&bound.table, &bound.key),
        }
    }
}

impl DynamoTransactWriteShape {
    pub fn try_from_request(request: TransactWriteItemsRequest) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        let token = TransactTokenRequest::try_from_request(&request)?;
        let members = request
            .transact_items
            .into_iter()
            .map(MemberShape::try_from)
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(Self {
            members: members.into(),
            token,
            effect_authority: None,
        })
    }

    /// Requires a runtime-issued witness and binds receipt replay to its
    /// logical execution rather than its replaceable claim generation.
    pub(crate) fn authorize_effect(
        mut self,
        authority: crate::job_write_authority::JobWriteAuthority,
    ) -> StorageResult<Self> {
        let token = self.token.as_mut().ok_or_else(|| {
            StorageError::validation("scheduled-job effects require a replay token")
        })?;
        token.authorize_effect(&authority)?;
        self.effect_authority = Some(authority);
        Ok(self)
    }

    pub fn bind(
        self: Arc<Self>,
        lookup: StorageResult<impl Fn(&TableName) -> StorageResult<Arc<BoundTableInfo>>>,
        captured_at: TimestampMillis,
    ) -> BoundTransactWrite {
        let receipt = self
            .token
            .as_ref()
            .map(|request| {
                let bytes = encode_transact_receipt(request, captured_at)?;
                Ok(PreparedTransactReceipt {
                    shape: Arc::clone(&self),
                    bytes,
                })
            })
            .transpose();
        let members = match lookup {
            Ok(lookup) => bind_members(&self.members, lookup),
            Err(error) if matches!(error.as_ref(), StorageEnum::TableNotFound { .. }) => {
                Ok(SemanticDecision::Rejected(error))
            }
            Err(error) => Err(error),
        };
        BoundTransactWrite {
            members,
            receipt,
            effect_authority: self.effect_authority.clone(),
        }
    }

    pub(crate) fn token(&self) -> Option<&TransactTokenRequest> {
        self.token.as_ref()
    }

    pub fn table_names(&self) -> Vec<TableName> {
        let mut names = Vec::new();
        for member in self.members.iter() {
            if !names.contains(member.table_name()) {
                names.push(member.table_name().clone());
            }
        }
        names
    }
}

fn bind_members(
    members: &[MemberShape],
    lookup: impl Fn(&TableName) -> StorageResult<Arc<BoundTableInfo>>,
) -> StorageResult<SemanticDecision<Vec<BoundMember>>> {
    let total = members.len();
    let mut seen = Vec::with_capacity(total);
    let mut bound = Vec::with_capacity(total);
    for (ordinal, member) in members.iter().enumerate() {
        let table = match lookup(member.table_name()) {
            Ok(table) => table,
            Err(error) if matches!(error.as_ref(), StorageEnum::TableNotFound { .. }) => {
                return Ok(SemanticDecision::Rejected(error));
            }
            Err(error) => return Err(error),
        };
        let item = match member.bind(table) {
            Ok(item) => item,
            Err(error)
                if matches!(
                    error.as_ref(),
                    StorageEnum::Validation { .. }
                        | StorageEnum::RawValidation { .. }
                        | StorageEnum::KeyValidation(_)
                ) =>
            {
                return Ok(SemanticDecision::Rejected(
                    transaction_write_binding_error_with_len(ordinal, total, error),
                ));
            }
            Err(error) => return Err(error),
        };
        let (table, key) = item.target();
        let fingerprint = transact_key_fingerprint(&table.info, key)?;
        if seen.contains(&fingerprint) {
            return Ok(SemanticDecision::Rejected(StorageError::validation(
                "Transaction request cannot include multiple operations on one item",
            )));
        }
        seen.push(fingerprint);
        bound.push(item);
    }
    Ok(SemanticDecision::Accepted(bound))
}

pub struct BoundTransactWrite {
    receipt: StorageResult<Option<PreparedTransactReceipt>>,
    members: StorageResult<SemanticDecision<Vec<BoundMember>>>,
    effect_authority: Option<crate::job_write_authority::JobWriteAuthority>,
}

impl BoundTransactWrite {
    pub fn put_items(&self) -> Vec<&BoundPutItem> {
        let Ok(SemanticDecision::Accepted(members)) = &self.members else {
            return Vec::new();
        };
        members
            .iter()
            .filter_map(|member| match member {
                BoundMember::Put(item) => Some(item.as_ref()),
                _ => None,
            })
            .collect()
    }

    pub(crate) async fn accept_observations<A: DynamoTransactWriteAttempt>(
        self,
        attempt: &mut A,
        clock: &dyn storage_types::Clock,
        observations: storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>,
    ) -> StorageResult<SemanticDecision<AcceptedTransactWrite>> {
        let authority = self.effect_authority.as_ref();
        let members = match self.members? {
            SemanticDecision::Accepted(members) => members,
            SemanticDecision::Rejected(error) => {
                Self::check_effect_authority(authority, attempt, clock).await?;
                return Ok(SemanticDecision::Rejected(error));
            }
        };
        // The job rows are read with a row lock, which assigns the Postgres
        // transaction ID. Read them after the item locks so that the stream
        // positions of this attempt follow the item lock order.
        let accepted = accept_members(members, observations)?;
        Self::check_effect_authority(authority, attempt, clock).await?;
        let members = match accepted {
            SemanticDecision::Accepted(members) => members,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        let receipt = self.receipt?;
        Ok(SemanticDecision::Accepted(AcceptedTransactWrite {
            members,
            receipt,
        }))
    }

    async fn check_effect_authority<A: DynamoTransactWriteAttempt>(
        authority: Option<&crate::job_write_authority::JobWriteAuthority>,
        attempt: &mut A,
        clock: &dyn storage_types::Clock,
    ) -> StorageResult<()> {
        if let Some(authority) = authority {
            let values = attempt
                .read_scheduled_jobs(authority.observation_keys())
                .await?
                .into_iter()
                .map(|value| value.map(bytes::Bytes::from))
                .collect::<Vec<_>>();
            let valid = authority
                .matches_observations(
                    &values,
                    scheduled_bg_jobs::Timestamp::from_millis(clock.now().timestamp_millis()),
                )
                .map_err(|_| StorageError::internal("scheduled-job authority is corrupt"))?;
            if !valid {
                return Err(StorageEnum::ConditionalCheckFailed.into());
            }
        }
        Ok(())
    }

    pub(crate) fn observation_reads(&self) -> Vec<ItemWriteObservationRead<'_>> {
        let Ok(SemanticDecision::Accepted(members)) = &self.members else {
            return Vec::new();
        };
        members
            .iter()
            .map(|member| {
                let (table, key) = member.target();
                match member {
                    BoundMember::Check(_) => ItemWriteObservationRead::ConditionCheck(table, key),
                    _ => ItemWriteObservationRead::Write(table, key),
                }
            })
            .collect()
    }

    pub(crate) fn needs_history(&self) -> bool {
        matches!(&self.members, Ok(SemanticDecision::Accepted(members)) if members.iter().any(|member| match member {
            BoundMember::Put(item) => item.table.info.requires_stream_entry(),
            BoundMember::Update(item) => item.table.info.requires_stream_entry(),
            BoundMember::Delete(item) => item.table.info.requires_stream_entry(),
            BoundMember::Check(_) => false,
        }))
    }
}

pub struct AcceptedTransactWrite {
    members: Vec<AcceptedTransactWriteMember>,
    receipt: Option<PreparedTransactReceipt>,
}

struct PreparedTransactReceipt {
    shape: Arc<DynamoTransactWriteShape>,
    bytes: Vec<u8>,
}

impl AcceptedTransactWrite {
    pub async fn apply<A: DynamoTransactWriteAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for member in self.members {
                match member {
                    AcceptedTransactWriteMember::Put(item)
                    | AcceptedTransactWriteMember::Update(item) => {
                        prepare_accepted_upsert(attempt, *item, captured_at).await?;
                    }
                    AcceptedTransactWriteMember::Delete(mut item) => {
                        prepare_accepted_delete(attempt, &mut item, captured_at).await?;
                    }
                    AcceptedTransactWriteMember::ConditionCheck(_) => {}
                }
            }

            let response = TransactWriteItemsResponse {
                consumed_capacity: None,
                item_collection_metrics: None,
            };
            if let Some(receipt) = self.receipt {
                let request = receipt
                    .shape
                    .token
                    .as_ref()
                    .ok_or_else(|| StorageError::internal("prepared receipt lacks a token"))?;
                attempt.stage_transact_receipt(request.slot(), receipt.bytes)?;
            }
            Ok(response)
        }
        .await;
        settlement.complete(result)
    }
}

fn accept_members(
    bound: Vec<BoundMember>,
    observations: storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>,
) -> StorageResult<SemanticDecision<Vec<AcceptedTransactWriteMember>>> {
    let total = bound.len();
    let mut accepted = Vec::with_capacity(total);
    let mut reasons = vec![None; total];
    for (ordinal, (item, observation)) in bound.into_iter().zip(observations).enumerate() {
        match accept_member(item, observation)? {
            SemanticDecision::Accepted(member) => accepted.push(member),
            SemanticDecision::Rejected(error) => {
                let error = match error.into_transact_empty_key_validation() {
                    Ok(validation) => return Err(validation),
                    Err(error) => error,
                };
                reasons[ordinal] = Some(cancellation_reason(&error).ok_or_else(|| {
                    StorageError::internal("transaction rejection has no cancellation reason")
                })?);
            }
        }
    }
    if let Some(error) = transaction_canceled_for_indexed_reasons(reasons) {
        return Ok(SemanticDecision::Rejected(error));
    }
    if let Err(error) = require_resolved_write_capacity(&accepted) {
        return Ok(SemanticDecision::Rejected(error));
    }
    Ok(SemanticDecision::Accepted(accepted))
}

fn require_resolved_write_capacity(members: &[AcceptedTransactWriteMember]) -> StorageResult<()> {
    let mut remaining = storage_types::MAX_TRANSACTION_ITEM_BYTES;
    for (ordinal, member) in members.iter().enumerate() {
        let bytes = match member {
            AcceptedTransactWriteMember::Put(item) | AcceptedTransactWriteMember::Update(item) => {
                u64::from(item.item_size)
            }
            AcceptedTransactWriteMember::Delete(item) => item
                .old_item
                .as_deref()
                .map(storage_types::WireItem::logical_size)
                .transpose()?
                .map_or(0, u64::from),
            AcceptedTransactWriteMember::ConditionCheck(item) => item
                .old_item
                .as_deref()
                .map(storage_types::WireItem::logical_size)
                .transpose()?
                .map_or(0, u64::from),
        };
        remaining = remaining.checked_sub(bytes).ok_or_else(|| {
            transaction_write_binding_error_with_len(
                ordinal,
                members.len(),
                StorageError::validation("Aggregate size of transaction write items exceeds 4 MB"),
            )
        })?;
    }
    Ok(())
}

fn accept_member(
    member: BoundMember,
    (observed, lifecycle): (ItemWriteObservation, GsiProjectionLifecycle),
) -> StorageResult<SemanticDecision<AcceptedTransactWriteMember>> {
    Ok(match member {
        BoundMember::Put(bound) => bound
            .accept_old(observed, lifecycle)?
            .map(|(accepted, _)| AcceptedTransactWriteMember::Put(Box::new(accepted))),
        BoundMember::Update(bound) => bound
            .accept_old(observed, lifecycle)?
            .map(|(accepted, _)| AcceptedTransactWriteMember::Update(Box::new(accepted))),
        BoundMember::Delete(bound) => bound
            .accept_old(observed, lifecycle)?
            .map(|accepted| AcceptedTransactWriteMember::Delete(Box::new(accepted))),
        BoundMember::Check(bound) => check_condition(bound, observed)?
            .map(|accepted| AcceptedTransactWriteMember::ConditionCheck(Box::new(accepted))),
    })
}

fn check_condition(
    bound: BoundConditionCheck,
    observed: ItemWriteObservation,
) -> StorageResult<SemanticDecision<AcceptedTransactConditionCheck>> {
    let old = crate::dynamo_put_item::ObservedWrite::try_new(observed, &bound.key)?;
    Ok(crate::dynamo_put_item::check_write_condition(
        Some(&bound.shape.condition),
        old.item.as_deref(),
        bound.shape.return_old_on_failure,
    )?
    .map(|()| AcceptedTransactConditionCheck { old_item: old.item }))
}

fn cancellation_reason(error: &StorageError) -> Option<CancellationReason> {
    match error.to_enum() {
        StorageEnum::ConditionalCheckFailedWithItem { item } => Some(
            CancellationReason::conditional_check_failed(Some(item.clone())),
        ),
        _ => transaction_cancellation_reason_at(error, 0),
    }
}

impl TryFrom<TransactWriteItem> for MemberShape {
    type Error = StorageError;

    fn try_from(item: TransactWriteItem) -> StorageResult<Self> {
        match (item.put, item.update, item.delete, item.condition_check) {
            (Some(put), None, None, None) => {
                let mut request = PutItemRequest::new(put.table_name, put.item);
                request.indexers = put.indexers;
                request.condition_expression = put.condition_expression;
                request.expression_attribute_names = put.expression_attribute_names;
                request.expression_attribute_values = put.expression_attribute_values;
                request.return_values_on_condition_check_failure =
                    put.return_values_on_condition_check_failure;
                PutItemShape::from_validated_request(request)
                    .map(Arc::new)
                    .map(Self::Put)
            }
            (None, Some(update), None, None) => {
                UpdateItemShape::from_validated_request(UpdateItemRequest {
                    table_name: update.table_name,
                    key: update.key,
                    indexers: update.indexers,
                    update_expression: Some(update.update_expression),
                    attribute_updates: None,
                    condition_expression: update.condition_expression,
                    expression_attribute_names: update.expression_attribute_names,
                    expression_attribute_values: update.expression_attribute_values,
                    expected: None,
                    conditional_operator: None,
                    return_values: None,
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                    return_values_on_condition_check_failure: update
                        .return_values_on_condition_check_failure,
                })
                .map(Arc::new)
                .map(Self::Update)
            }
            (None, None, Some(delete), None) => {
                let mut request = DeleteItemRequest::new(delete.table_name, delete.key);
                request.condition_expression = delete.condition_expression;
                request.expression_attribute_names = delete.expression_attribute_names;
                request.expression_attribute_values = delete.expression_attribute_values;
                request.return_values_on_condition_check_failure =
                    delete.return_values_on_condition_check_failure;
                DeleteItemShape::from_validated_request(request)
                    .map(Arc::new)
                    .map(Self::Delete)
            }
            (None, None, None, Some(check)) => {
                condition_check_shape(check).map(Arc::new).map(Self::Check)
            }
            _ => Err(StorageError::validation(
                "Each TransactWriteItem must contain exactly one operation",
            )),
        }
    }
}

fn condition_check_shape(
    mut check: TransactConditionCheckRequest,
) -> StorageResult<ConditionCheckShape> {
    let condition = CompiledCondition::compile_write(
        Some(check.condition_expression),
        check.expression_attribute_names,
        check.expression_attribute_values,
    )?
    .ok_or_else(|| StorageError::validation("ConditionCheck requires a condition"))?;
    check.key.normalize_numbers_for_write();
    Ok(ConditionCheckShape {
        table_name: check.table_name,
        key: check.key,
        condition,
        return_old_on_failure: return_values_on_condition_check_failure_all_old(
            check.return_values_on_condition_check_failure.as_ref(),
        ),
    })
}
