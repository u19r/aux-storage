//! One shared BatchWriteItem decision over PutItem and DeleteItem members.
//!
//! The request has no all-or-nothing requirement. This implementation commits
//! each accepted group separately and reports only the uncommitted suffix as
//! unprocessed after a native owner proves the current attempt aborted.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    BatchWriteItemRequest, BatchWriteItemResponse, BoundTableInfo, DeleteItemRequest,
    DynamoRequestValidate as _, PutItemRequest, StorageEnum, StorageError, StorageResult,
    TableName, TimestampMillis, WriteRequest, batch_write_key_validation_error,
    transact_key_fingerprint,
};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, BoundDeleteItem, BoundPutItem, DeleteItemShape,
    ItemWriteAttempt, ItemWriteObservation, ItemWriteObservationRead, PutItemShape,
    SemanticDecision, prepare_accepted_delete, prepare_accepted_upsert,
};

pub(crate) const DUPLICATE_BATCH_WRITE_KEY_MESSAGE: &str =
    "Provided list of item keys contains duplicates";

/// A member key of the wrong shape (missing, extra, or of the wrong type) is
/// a key-schema mismatch, unlike PutItem and DeleteItem, which name the
/// attribute or give the generic text.
fn batch_write_key_shape_error(error: StorageError) -> StorageError {
    if let StorageEnum::Validation { message, .. } = error.as_ref()
        && (message == "One or more parameter values were invalid"
            || message
                .starts_with("One or more parameter values were invalid: Type mismatch for key ")
            || message.starts_with("One or more parameter values were invalid: Missing the key "))
    {
        return StorageError::validation("The provided key element does not match the schema");
    }
    error
}

#[derive(Clone)]
pub struct DynamoBatchWriteShape {
    members: Arc<[BatchMember]>,
    range: std::ops::Range<usize>,
}

struct BatchMember {
    table_name: TableName,
    original: WriteRequest,
    shape: MemberShape,
}

enum MemberShape {
    Put(Arc<PutItemShape>),
    Delete(Arc<DeleteItemShape>),
}

enum BoundBatchWriteMember {
    Put(Box<BoundPutItem>),
    Delete(Box<BoundDeleteItem>),
}

pub enum AcceptedBatchWriteMember {
    Put(Box<AcceptedUpsert>),
    Delete(Box<AcceptedDeleteItem>),
}

impl BoundBatchWriteMember {
    fn target(&self) -> (&BoundTableInfo, &storage_types::KeyAttributes) {
        match self {
            Self::Put(bound) => (&bound.table, &bound.key),
            Self::Delete(bound) => (&bound.table, &bound.key),
        }
    }

    fn accept_old(
        self,
        observation: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<AcceptedBatchWriteMember>> {
        match self {
            Self::Put(item) => item.accept_old(observation, lifecycle).map(|decision| {
                decision.map(|(item, _)| AcceptedBatchWriteMember::Put(Box::new(item)))
            }),
            Self::Delete(item) => item.accept_old(observation, lifecycle).map(|decision| {
                decision.map(|item| AcceptedBatchWriteMember::Delete(Box::new(item)))
            }),
        }
    }
}

impl AcceptedBatchWriteMember {
    /// Stage one member; the caller flushes all members in the same attempt.
    async fn prepare_effects<A: ItemWriteAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        match self {
            Self::Put(item) => {
                prepare_accepted_upsert(attempt, *item, captured_at).await?;
                Ok(())
            }
            Self::Delete(mut item) => {
                prepare_accepted_delete(attempt, &mut item, captured_at).await
            }
        }
    }
}

impl DynamoBatchWriteShape {
    pub fn try_from_request(request: BatchWriteItemRequest) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        let count = request.request_items.values().map(Vec::len).sum::<usize>();
        let mut members = Vec::with_capacity(count);
        for (table_name, writes) in request.request_items {
            for original in writes {
                let shape = match (&original.put_request, &original.delete_request) {
                    (Some(put), None) => {
                        let mut request = PutItemRequest::new(table_name.clone(), put.item.clone());
                        request.indexers = put.indexers.clone();
                        MemberShape::Put(Arc::new(PutItemShape::from_validated_request(request)?))
                    }
                    (None, Some(delete)) => {
                        let request =
                            DeleteItemRequest::new(table_name.clone(), delete.key.clone());
                        MemberShape::Delete(Arc::new(DeleteItemShape::from_validated_request(
                            request,
                        )?))
                    }
                    _ => {
                        return Err(StorageError::validation(
                            "WriteRequest must contain exactly one of PutRequest or DeleteRequest",
                        ));
                    }
                };
                members.push(BatchMember {
                    table_name: table_name.clone(),
                    original,
                    shape,
                });
            }
        }
        Ok(Self {
            members: members.into(),
            range: 0..count,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.range.len()
    }

    pub(crate) fn select_members(&self, range: std::ops::Range<usize>) -> Self {
        Self {
            members: Arc::clone(&self.members),
            range,
        }
    }

    pub fn bind(
        &self,
        lookup: StorageResult<
            impl Fn(&TableName) -> StorageResult<Arc<storage_types::BoundTableInfo>>,
        >,
    ) -> StorageResult<SemanticDecision<BoundBatchWrite>> {
        let lookup = match lookup {
            Ok(lookup) => lookup,
            Err(error) if matches!(error.as_ref(), StorageEnum::TableNotFound { .. }) => {
                return Ok(SemanticDecision::Rejected(batch_write_table_lookup_error(
                    error,
                )));
            }
            Err(error) => return Err(error),
        };
        let result = (|| {
            let mut seen = Vec::with_capacity(self.members.len());
            let mut bound = Vec::with_capacity(self.members.len());
            for member in self.members[self.range.clone()].iter() {
                let table = lookup(&member.table_name)?;
                let item = match &member.shape {
                    MemberShape::Put(shape) => BoundBatchWriteMember::Put(Box::new(
                        Arc::clone(shape)
                            .bind_table(Arc::clone(&table))
                            .map_err(batch_write_key_shape_error)?,
                    )),
                    MemberShape::Delete(shape) => BoundBatchWriteMember::Delete(Box::new(
                        Arc::clone(shape).bind_table(table).map_err(|error| {
                            batch_write_key_validation_error(batch_write_key_shape_error(error))
                        })?,
                    )),
                };
                let (table, key) = item.target();
                let fingerprint = transact_key_fingerprint(&table.info, key)?;
                if seen.contains(&fingerprint) {
                    return Err(StorageError::validation(DUPLICATE_BATCH_WRITE_KEY_MESSAGE));
                }
                seen.push(fingerprint);
                bound.push(item);
            }
            Ok(BoundBatchWrite {
                members: bound,
                member_offset: self.range.start,
            })
        })();
        SemanticDecision::try_from(result)
    }

    fn all_unprocessed(&self) -> BatchWriteItemResponse {
        let mut groups = storage_types::VecMap::new();
        for member in self.members[self.range.clone()].iter() {
            groups
                .get_or_insert_with(member.table_name.clone(), Vec::new)
                .push(member.original.clone());
        }
        BatchWriteItemResponse {
            unprocessed_items: Some(groups),
            item_collection_metrics: None,
            consumed_capacity: None,
        }
    }

    /// Native owners classify these outcomes only after proving nonacceptance.
    /// An ambiguous commit cannot produce an all-unprocessed response.
    pub fn unprocessed_after_known_abort(
        &self,
        error: &StorageError,
    ) -> Option<BatchWriteItemResponse> {
        matches!(
            error.attempt_kind(),
            Some(
                storage_types::AttemptKind::Conflict
                    | storage_types::AttemptKind::Capacity
                    | storage_types::AttemptKind::TransactionBytes
                    | storage_types::AttemptKind::Unavailable
                    | storage_types::AttemptKind::Deadline
            )
        )
        .then(|| self.all_unprocessed())
    }

    pub fn table_names(&self) -> Vec<TableName> {
        let mut names = Vec::new();
        for member in self.members[self.range.clone()].iter() {
            if !names.contains(&member.table_name) {
                names.push(member.table_name.clone());
            }
        }
        names
    }
}

pub fn batch_write_table_lookup_error(error: StorageError) -> StorageError {
    error.into_data_plane_table_not_found()
}

pub struct BoundBatchWrite {
    member_offset: usize,
    members: Vec<BoundBatchWriteMember>,
}

impl BoundBatchWrite {
    pub(crate) fn needs_writer_routes(&self) -> bool {
        self.members
            .iter()
            .any(|member| member.target().0.info.requires_stream_entry())
    }

    /// Select original members after complete request validation. Consuming
    /// the initial inventory avoids binding/cloning the first group twice.
    pub(crate) fn select_prefix(mut self, count: usize) -> Self {
        self.members.truncate(count);
        self
    }

    /// Planning hint only; old images and exact native lowering belong to the
    /// native owner. Payload lengths borrow existing bytes without encoding.
    pub(crate) fn estimate_group_members(&self, limit: usize, maximum: usize) -> usize {
        let mut bytes = 0usize;
        for (offset, member) in self.members.iter().take(maximum).enumerate() {
            let estimate = match member {
                BoundBatchWriteMember::Put(item) => {
                    let image = item.new_item.payload_len();
                    let stream_image = if item.table.info.requires_stream_entry() {
                        image
                    } else {
                        0
                    };
                    let projections = item
                        .gsi_puts()
                        .map(|row| {
                            match row.projection {
                                storage_types::FrozenImageProjection::All => image,
                                // Small field projections are a planning hint;
                                // large selected fields still use native
                                // rejection.
                                storage_types::FrozenImageProjection::Fields(_) => 1024,
                            }
                        })
                        .sum::<usize>();
                    image
                        .saturating_add(stream_image)
                        .saturating_add(projections)
                        .saturating_add(4096)
                }
                BoundBatchWriteMember::Delete(_) => 4096,
            };
            if offset > 0 && bytes.saturating_add(estimate) > limit {
                return offset;
            }
            bytes = bytes.saturating_add(estimate);
        }
        maximum.min(self.members.len())
    }

    pub fn put_items(&self) -> Vec<&BoundPutItem> {
        self.members
            .iter()
            .filter_map(|member| match member {
                BoundBatchWriteMember::Put(item) => Some(item.as_ref()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn observation_reads(&self) -> Vec<ItemWriteObservationRead<'_>> {
        self.members
            .iter()
            .map(|member| {
                let (table, key) = member.target();
                ItemWriteObservationRead::Write(table, key)
            })
            .collect()
    }

    pub(crate) fn accept_observations(
        self,
        observations: storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>,
    ) -> StorageResult<SemanticDecision<AcceptedBatchWrite>> {
        let mut members = Vec::with_capacity(self.members.len());
        for (ordinal, (member, (observed, lifecycle))) in
            self.members.into_iter().zip(observations).enumerate()
        {
            match member.accept_old(observed, lifecycle)? {
                SemanticDecision::Accepted(member) => {
                    members.push((self.member_offset + ordinal, member))
                }
                SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
            }
        }
        Ok(SemanticDecision::Accepted(AcceptedBatchWrite {
            members,
            unprocessed: storage_types::VecMap::new(),
        }))
    }
}

/// Complete semantic acceptance, before capture or synchronous member effects.
pub struct AcceptedBatchWrite {
    members: Vec<(usize, AcceptedBatchWriteMember)>,
    unprocessed: storage_types::VecMap<TableName, Vec<WriteRequest>>,
}

impl AcceptedBatchWrite {
    /// Consume one accepted inventory inside its caller's native attempt.
    /// Only this complete effect path constructs a committed batch response.
    pub async fn apply<A: ItemWriteAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for (_ordinal, member) in self.members {
                member.prepare_effects(attempt, captured_at).await?;
            }
            Ok(BatchWriteItemResponse {
                unprocessed_items: Some(self.unprocessed),
                item_collection_metrics: None,
                consumed_capacity: None,
            })
        }
        .await;
        settlement.complete(result)
    }
}

/// One BatchWriteItem group as a write plan. `members` is the member count
/// of the last try, read by `batch_write_item` to advance or halve the group.
pub(crate) struct BatchGroupPlan {
    shape: DynamoBatchWriteShape,
    names: Vec<TableName>,
    maximum: usize,
    captured_at: TimestampMillis,
    members: AtomicUsize,
}

impl BatchGroupPlan {
    pub(crate) fn new(
        shape: DynamoBatchWriteShape,
        maximum: usize,
        captured_at: TimestampMillis,
    ) -> Self {
        let names = shape.table_names();
        Self {
            shape,
            names,
            maximum,
            captured_at,
            members: AtomicUsize::new(0),
        }
    }

    pub(crate) fn members(&self) -> usize {
        self.members.load(Ordering::Relaxed)
    }
}

impl crate::dynamo_write_session::WritePlan for BatchGroupPlan {
    type Bound = SemanticDecision<BoundBatchWrite>;
    type Output = ();

    fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.names.iter()
    }

    async fn prepare<P: crate::DynamoPort>(
        &self,
        port: &P,
        session: &crate::dynamo_table_bindings::CatalogSession,
        deadline: storage_types::AttemptDeadline,
    ) -> StorageResult<(
        crate::dynamo_write_preparation::PreparedWriteTables,
        Self::Bound,
    )> {
        // A try that fails before its store reports no members, not the
        // previous try's count.
        self.members.store(0, Ordering::Relaxed);
        let tables = crate::dynamo_write_preparation::PreparedWriteTables::load(
            port,
            session.cache(),
            self.names.clone(),
            session.bindings(),
            deadline,
        )
        .await?;
        let bound = self.shape.bind(
            tables
                .ensure_present()
                .map(|()| |name: &TableName| tables.table(name)),
        )?;
        let count = match (&bound, port.native_transaction_byte_limit()) {
            (
                SemanticDecision::Accepted(bound),
                infrastructure_provider::physical_store::NativeTransactionByteLimit::Fixed(limit),
            ) => bound.estimate_group_members(limit.get(), self.maximum),
            _ => self.maximum,
        };
        self.members.store(count, Ordering::Relaxed);
        let bound = bound.map(|bound| bound.select_prefix(count));
        Ok((tables, bound))
    }

    fn put_items(bound: &Self::Bound) -> Option<Vec<&BoundPutItem>> {
        match bound {
            SemanticDecision::Accepted(bound) => Some(bound.put_items()),
            SemanticDecision::Rejected(_) => None,
        }
    }

    async fn accept_and_apply<
        A: crate::CreateTableAttempt
            + crate::DeleteTableAttempt
            + crate::ItemReadAttempt
            + crate::ItemWriteAttempt
            + crate::DynamoTransactWriteAttempt
            + crate::ListingAttempt
            + crate::maintenance::StreamRetentionPort,
    >(
        &self,
        attempt: &mut A,
        write: &crate::dynamo_write_session::WriteTry,
        bound: Self::Bound,
        writes: StorageResult<<A as crate::ItemWriteAttempt>::PreparedWrites>,
    ) -> StorageResult<(SemanticDecision<()>, Vec<crate::CachePublication>)> {
        let reads = match &bound {
            SemanticDecision::Accepted(bound) => bound.observation_reads(),
            SemanticDecision::Rejected(_) => Vec::new(),
        };
        let needs_history =
            matches!(&bound, SemanticDecision::Accepted(bound) if bound.needs_writer_routes());
        let (_, accepted) = write.observe(attempt, &reads, None, needs_history).await?;
        let crate::dynamo_write_preparation::AcceptedObservations {
            decision,
            publications,
        } = accepted;
        let observations = match decision? {
            SemanticDecision::Accepted(observations) => observations,
            SemanticDecision::Rejected(error) => {
                return Ok((SemanticDecision::Rejected(error), Vec::new()));
            }
        };
        let decision = match bound {
            SemanticDecision::Rejected(error) => SemanticDecision::Rejected(error),
            SemanticDecision::Accepted(bound) => {
                attempt.install_prepared_writes(writes);
                match bound.accept_observations(observations.ok_or_else(|| {
                    StorageError::internal("confirmed write observations were skipped")
                })?)? {
                    SemanticDecision::Rejected(error) => SemanticDecision::Rejected(error),
                    SemanticDecision::Accepted(accepted) => {
                        accepted.apply(attempt, self.captured_at).await?;
                        SemanticDecision::Accepted(())
                    }
                }
            }
        };
        crate::DynamoStreamWriteAttempt::finish_dynamo_write(attempt).await?;
        Ok((decision, publications))
    }
}
