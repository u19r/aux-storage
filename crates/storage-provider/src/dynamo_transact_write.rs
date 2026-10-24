//! Shared TransactWriteItems validation, replay, and one-attempt execution.

use std::{future::Future, sync::Arc};

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    BoundTableInfo, DeleteItemRequest, DynamoRequestValidate as _, ItemProvenance, KeyAttributes,
    PutItemRequest, StorageEnum, StorageError, StorageResult, TableName, TimestampMillis,
    TransactConditionCheckRequest, TransactWriteItem, TransactWriteItemsRequest,
    TransactWriteItemsResponse, UpdateItemRequest, context::WrappedError as _,
    return_values_on_condition_check_failure_all_old, transact_key_fingerprint,
    transaction_canceled_for_indexed_reasons, transaction_cancellation_reason_at,
    transaction_write_binding_error_with_len, validate_transact_key,
};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, BoundDeleteItem, BoundPutItem, BoundUpdateItem,
    DeleteItemAttempt, DeleteItemShape, ItemWriteObservation, ItemWriteObservationRead,
    PutItemShape, SemanticDecision, TableMetadataCache, UncheckedCondition, UpdateItemShape,
    UpsertAttempt, prepare_accepted_delete, prepare_accepted_upsert,
    transact_idempotency::{TransactTokenRequest, encode_transact_receipt, find_transact_response},
};

/// The lowerer only protects and stores opaque bytes. Token identity, expiry,
/// replay, and response decisions are shared across all five drivers.
pub trait DynamoTransactWriteAttempt: UpsertAttempt + DeleteItemAttempt {
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
        token: &str,
    ) -> impl Future<Output = StorageResult<Option<Vec<u8>>>> + Send;

    fn write_transact_receipt(
        &mut self,
        token: &str,
        receipt: Vec<u8>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

#[derive(Clone)]
pub struct DynamoTransactWriteShape {
    members: Vec<MemberShape>,
    token: Option<TransactTokenRequest>,
    effect_authority: Option<crate::job_write_authority::JobWriteAuthority>,
}

#[derive(Clone)]
enum MemberShape {
    Put(PutItemShape),
    Update(UpdateItemShape),
    Delete(DeleteItemShape),
    Check(ConditionCheckShape),
}

#[derive(Clone)]
struct ConditionCheckShape {
    table_name: TableName,
    key: KeyAttributes,
    condition: UncheckedCondition,
    return_old_on_failure: bool,
}

struct BoundConditionCheck {
    table: Arc<BoundTableInfo>,
    key: KeyAttributes,
    condition: UncheckedCondition,
    return_old_on_failure: bool,
}

enum BoundMember {
    Put(Box<BoundPutItem>),
    Update(BoundUpdateItem),
    Delete(BoundDeleteItem),
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
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub old_indexers: Vec<String>,
    pub old_provenance: Option<ItemProvenance>,
}

impl AcceptedTransactWriteMember {
    pub fn mutation(&self) -> Option<crate::AcceptedMutationRef<'_>> {
        match self {
            Self::Put(item) | Self::Update(item) => Some(crate::AcceptedMutationRef::Upsert(item)),
            Self::Delete(item) => Some(crate::AcceptedMutationRef::Delete(item)),
            Self::ConditionCheck(_) => None,
        }
    }
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

    fn bind(
        self,
        table: Arc<BoundTableInfo>,
        ttl: Option<&crate::TtlConfigRecord>,
    ) -> StorageResult<BoundMember> {
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
            Self::Put(shape) => shape
                .bind_table(Arc::clone(&table), ttl)
                .map(Box::new)
                .map(BoundMember::Put),
            Self::Update(shape) => shape.bind_table(table).map(BoundMember::Update),
            Self::Delete(shape) => shape.bind_table(table).map(BoundMember::Delete),
            Self::Check(mut shape) => {
                shape.key.normalize_numbers_for_write();
                Ok(BoundMember::Check(BoundConditionCheck {
                    table,
                    key: shape.key,
                    condition: shape.condition,
                    return_old_on_failure: shape.return_old_on_failure,
                }))
            }
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
            members,
            token,
            effect_authority: None,
        })
    }

    pub fn try_from_job_effect(
        request: TransactWriteItemsRequest,
        authority: scheduled_bg_jobs::JobEffectAuthority,
    ) -> StorageResult<Self> {
        Self::try_from_request(request)?.authorize_effect(
            crate::job_write_authority::JobWriteAuthority::Effect(authority),
        )
    }

    pub fn try_from_job_projection(
        request: crate::JobTerminalProjection,
        authority: scheduled_bg_jobs::JobTerminalAuthority,
    ) -> StorageResult<Self> {
        Self::try_from_request(request.into_request())?.authorize_effect(
            crate::job_write_authority::JobWriteAuthority::Terminal(authority),
        )
    }

    pub async fn read_replay<A: DynamoTransactWriteAttempt>(
        &self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<SemanticDecision<Option<TransactWriteItemsResponse>>> {
        find_transact_response(attempt, self.token.as_ref(), captured_at).await
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
        token.authorize_effect(&authority);
        self.effect_authority = Some(authority);
        Ok(self)
    }

    pub fn bind(
        self,
        lookup: StorageResult<impl Fn(&TableName) -> StorageResult<crate::WriteTableBinding>>,
        captured_at: TimestampMillis,
    ) -> BoundTransactWrite {
        let receipt = self
            .token
            .map(|request| {
                let bytes = encode_transact_receipt(&request, captured_at)?;
                Ok(PreparedTransactReceipt { request, bytes })
            })
            .transpose();
        let members = lookup.and_then(|lookup| bind_members(self.members, lookup));
        BoundTransactWrite {
            members,
            receipt,
            effect_authority: self.effect_authority,
        }
    }

    pub fn table_names(&self) -> Vec<TableName> {
        let mut names = Vec::new();
        for member in &self.members {
            if !names.contains(member.table_name()) {
                names.push(member.table_name().clone());
            }
        }
        names
    }
}

fn bind_members(
    members: Vec<MemberShape>,
    lookup: impl Fn(&TableName) -> StorageResult<crate::WriteTableBinding>,
) -> StorageResult<SemanticDecision<Vec<BoundMember>>> {
    let total = members.len();
    let mut seen = Vec::with_capacity(total);
    let mut bound = Vec::with_capacity(total);
    for (ordinal, member) in members.into_iter().enumerate() {
        let crate::WriteTableBinding { table, ttl } = lookup(member.table_name())?;
        let item = match member.bind(table, ttl.as_ref()) {
            Ok(item) => item,
            Err(error) if matches!(error.as_ref(), StorageEnum::Validation { .. }) => {
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

    pub async fn prepare<A: DynamoTransactWriteAttempt>(
        self,
        attempt: &mut A,
        clock: &dyn storage_types::Clock,
    ) -> StorageResult<SemanticDecision<AcceptedTransactWrite>> {
        Self::check_effect_authority(self.effect_authority.as_ref(), attempt, clock).await?;
        let members = match self.members? {
            SemanticDecision::Accepted(members) => members,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        let members = match accept_members(attempt, members).await? {
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

    pub(crate) async fn execute<A: DynamoTransactWriteAttempt>(
        self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        tables: &crate::dynamo_write_preparation::PreparedWriteTables,
        times: (TimestampMillis, std::sync::Arc<dyn storage_types::Clock>),
    ) -> StorageResult<TransactWriteItemsResponse> {
        let (captured_at, clock) = times;
        let Self {
            receipt,
            members,
            effect_authority,
        } = self;
        let receipt = receipt?;
        if let Some(response) = find_transact_response(
            attempt,
            receipt.as_ref().map(|receipt| &receipt.request),
            captured_at,
        )
        .await?
        .into_result()?
        {
            return Ok(response);
        }
        tables.confirm(attempt, cache).await?;
        Self {
            members,
            receipt: Ok(receipt),
            effect_authority,
        }
        .prepare(attempt, clock.as_ref())
        .await?
        .into_result()?
        .apply(attempt, captured_at, |_, _| Ok(ItemProvenance::default()))
        .await
    }
}

pub struct AcceptedTransactWrite {
    members: Vec<AcceptedTransactWriteMember>,
    receipt: Option<PreparedTransactReceipt>,
}

struct PreparedTransactReceipt {
    request: TransactTokenRequest,
    bytes: Vec<u8>,
}

impl AcceptedTransactWrite {
    pub fn members(&self) -> &[AcceptedTransactWriteMember] {
        &self.members
    }

    pub async fn apply<A: DynamoTransactWriteAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
        mut provenance: impl FnMut(usize, &AcceptedTransactWriteMember) -> StorageResult<ItemProvenance>
        + Send,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for (ordinal, member) in self.members.into_iter().enumerate() {
                let provenance = provenance(ordinal, &member)?;
                match member {
                    AcceptedTransactWriteMember::Put(item)
                    | AcceptedTransactWriteMember::Update(item) => {
                        prepare_accepted_upsert(attempt, *item, captured_at, provenance).await?;
                    }
                    AcceptedTransactWriteMember::Delete(item) => {
                        prepare_accepted_delete(attempt, &item, captured_at, provenance).await?;
                    }
                    AcceptedTransactWriteMember::ConditionCheck(_) => {}
                }
            }
            attempt.flush_dynamo_write().await?;

            let response = TransactWriteItemsResponse {
                consumed_capacity: None,
                item_collection_metrics: None,
            };
            if let Some(receipt) = self.receipt {
                attempt
                    .write_transact_receipt(receipt.request.token(), receipt.bytes)
                    .await?;
            }
            Ok(response)
        }
        .await;
        settlement.complete(result)
    }
}

async fn accept_members<A: DynamoTransactWriteAttempt>(
    attempt: &mut A,
    bound: Vec<BoundMember>,
) -> StorageResult<SemanticDecision<Vec<AcceptedTransactWriteMember>>> {
    let total = bound.len();
    let mut accepted = Vec::with_capacity(total);
    let mut reasons = vec![None; total];
    let reads = bound
        .iter()
        .map(|member| {
            let (table, key) = member.target();
            match member {
                BoundMember::Check(_) => ItemWriteObservationRead::ConditionCheck(table, key),
                _ => ItemWriteObservationRead::Write(table, key),
            }
        })
        .collect::<Vec<_>>();
    let observations = attempt.observe_for_writes(&reads).await?;
    if observations.len() != total {
        return Err(StorageError::internal(
            "transaction write observation count differs",
        ));
    }
    for (ordinal, (item, observation)) in bound.into_iter().zip(observations).enumerate() {
        match accept_member(item, observation)? {
            SemanticDecision::Accepted(member) => accepted.push(member),
            SemanticDecision::Rejected(error) => {
                reasons[ordinal] = Some(cancellation_reason(&error)?.ok_or_else(|| {
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
        Some(&bound.condition),
        old.item.as_deref(),
        bound.return_old_on_failure,
    )?
    .map(|()| AcceptedTransactConditionCheck {
        table: bound.table,
        key: bound.key,
        old_item: old.item,
        old_indexers: old.indexers,
        old_provenance: old.provenance,
    }))
}

fn cancellation_reason(error: &StorageError) -> StorageResult<Option<String>> {
    match error.to_enum() {
        StorageEnum::ConditionalCheckFailedWithItem { item } => Ok(Some(format!(
            "ConditionalCheckFailed\t{}\t{}",
            storage_types::DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE,
            serde_json::to_string(item)?,
        ))),
        _ => Ok(transaction_cancellation_reason_at(error, 0)),
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
                request.aux_item_stream_ttl_hours = put.aux_item_stream_ttl_hours;
                PutItemShape::from_validated_request(request).map(Self::Put)
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
                    aux_item_stream_ttl_hours: update.aux_item_stream_ttl_hours,
                })
                .map(Self::Update)
            }
            (None, None, Some(delete), None) => {
                let mut request = DeleteItemRequest::new(delete.table_name, delete.key);
                request.condition_expression = delete.condition_expression;
                request.expression_attribute_names = delete.expression_attribute_names;
                request.expression_attribute_values = delete.expression_attribute_values;
                request.return_values_on_condition_check_failure =
                    delete.return_values_on_condition_check_failure;
                request.aux_item_stream_ttl_hours = delete.aux_item_stream_ttl_hours;
                DeleteItemShape::from_validated_request(request).map(Self::Delete)
            }
            (None, None, None, Some(check)) => condition_check_shape(check).map(Self::Check),
            _ => Err(StorageError::validation(
                "Each TransactWriteItem must contain exactly one operation",
            )),
        }
    }
}

fn condition_check_shape(
    check: TransactConditionCheckRequest,
) -> StorageResult<ConditionCheckShape> {
    let condition = UncheckedCondition::parse(
        Some(check.condition_expression),
        check.expression_attribute_names,
        check.expression_attribute_values,
    )?
    .ok_or_else(|| StorageError::validation("ConditionCheck requires a condition"))?;
    Ok(ConditionCheckShape {
        table_name: check.table_name,
        key: check.key,
        condition,
        return_old_on_failure: return_values_on_condition_check_failure_all_old(
            check.return_values_on_condition_check_failure.as_ref(),
        ),
    })
}
