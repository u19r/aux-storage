//! One shared BatchWriteItem decision over PutItem and DeleteItem members.
//!
//! The request has no all-or-nothing requirement. This implementation commits
//! its accepted members together, and reports the whole request as unprocessed
//! only after a native owner proves the attempt aborted.

use std::sync::Arc;

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    BatchWriteItemRequest, BatchWriteItemResponse, BoundTableInfo, DeleteItemRequest,
    DynamoRequestValidate as _, ItemProvenance, PutItemRequest, StorageEnum, StorageError,
    StorageResult, TableName, TimestampMillis, WriteRequest, batch_write_key_validation_error,
    context::WrappedError as _, transact_key_fingerprint,
};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, BoundDeleteItem, BoundPutItem, DeleteItemAttempt,
    DeleteItemShape, ItemWriteObservation, ItemWriteObservationAttempt, ItemWriteObservationRead,
    PutItemShape, SemanticDecision, UpsertAttempt, prepare_accepted_delete,
    prepare_accepted_upsert,
};

pub(crate) const DUPLICATE_BATCH_WRITE_KEY_MESSAGE: &str =
    "Provided list of item keys contains duplicates";

#[derive(Clone)]
pub struct DynamoBatchWriteShape {
    members: Arc<[BatchMember]>,
}

struct BatchMember {
    table_name: TableName,
    original: WriteRequest,
    shape: MemberShape,
}

#[derive(Clone)]
enum MemberShape {
    Put(Box<PutItemShape>),
    Delete(Box<DeleteItemShape>),
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
    pub fn mutation(&self) -> crate::AcceptedMutationRef<'_> {
        match self {
            Self::Put(item) => crate::AcceptedMutationRef::Upsert(item),
            Self::Delete(item) => crate::AcceptedMutationRef::Delete(item),
        }
    }

    /// Stage one member; the caller flushes all members in the same attempt.
    async fn prepare_effects<A: UpsertAttempt + DeleteItemAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
        provenance: ItemProvenance,
    ) -> StorageResult<()> {
        match self {
            Self::Put(item) => {
                prepare_accepted_upsert(attempt, *item, captured_at, provenance).await?;
                Ok(())
            }
            Self::Delete(item) => {
                prepare_accepted_delete(attempt, &item, captured_at, provenance).await
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
                        request.aux_item_stream_ttl_hours = put.aux_item_stream_ttl_hours;
                        MemberShape::Put(Box::new(PutItemShape::from_validated_request(request)?))
                    }
                    (None, Some(delete)) => {
                        let mut request =
                            DeleteItemRequest::new(table_name.clone(), delete.key.clone());
                        request.aux_item_stream_ttl_hours = delete.aux_item_stream_ttl_hours;
                        MemberShape::Delete(Box::new(DeleteItemShape::from_validated_request(
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
        })
    }

    pub fn bind(
        &self,
        lookup: StorageResult<impl Fn(&TableName) -> StorageResult<crate::WriteTableBinding>>,
    ) -> StorageResult<BoundBatchWrite> {
        let lookup = lookup.map_err(batch_write_table_lookup_error)?;
        let mut seen = Vec::with_capacity(self.members.len());
        let mut bound = Vec::with_capacity(self.members.len());
        for member in self.members.iter() {
            let crate::WriteTableBinding { table, ttl } = lookup(&member.table_name)?;
            let item = match member.shape.clone() {
                MemberShape::Put(shape) => BoundBatchWriteMember::Put(Box::new(
                    shape.bind_table(Arc::clone(&table), ttl.as_ref())?,
                )),
                MemberShape::Delete(shape) => BoundBatchWriteMember::Delete(Box::new(
                    shape
                        .bind_table(table)
                        .map_err(batch_write_key_validation_error)?,
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
        Ok(BoundBatchWrite { members: bound })
    }

    fn all_unprocessed(&self) -> BatchWriteItemResponse {
        let mut groups = storage_types::VecMap::new();
        for member in self.members.iter() {
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
        error
            .native_attempt_rejection()
            .is_some()
            .then(|| self.all_unprocessed())
    }

    pub fn table_names(&self) -> Vec<TableName> {
        let mut names = Vec::new();
        for member in self.members.iter() {
            if !names.contains(&member.table_name) {
                names.push(member.table_name.clone());
            }
        }
        names
    }
}

pub fn batch_write_table_lookup_error(error: StorageError) -> StorageError {
    if let StorageEnum::TableNotFound { name, .. } = error.to_enum() {
        StorageError::from(StorageEnum::ResourceNotFound {
            resource_type: "table",
            resource_id: name.to_string(),
        })
    } else {
        error
    }
}

pub struct BoundBatchWrite {
    members: Vec<BoundBatchWriteMember>,
}

impl BoundBatchWrite {
    pub fn put_items(&self) -> Vec<&BoundPutItem> {
        self.members
            .iter()
            .filter_map(|member| match member {
                BoundBatchWriteMember::Put(item) => Some(item.as_ref()),
                _ => None,
            })
            .collect()
    }

    pub async fn prepare<
        A: ItemWriteObservationAttempt
            + infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        self,
        attempt: &mut A,
    ) -> StorageResult<AcceptedBatchWrite> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let reads = self
                .members
                .iter()
                .map(|member| {
                    let (table, key) = member.target();
                    ItemWriteObservationRead::Write(table, key)
                })
                .collect::<Vec<_>>();
            let observations = attempt.observe_for_writes(&reads).await?;
            if observations.len() != self.members.len() {
                return Err(StorageError::internal(
                    "batch write observation count differs",
                ));
            }
            let members = self
                .members
                .into_iter()
                .zip(observations)
                .enumerate()
                .map(|(ordinal, (member, (observed, lifecycle)))| {
                    Ok((
                        ordinal,
                        member.accept_old(observed, lifecycle)?.into_result()?,
                    ))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(AcceptedBatchWrite {
                members,
                unprocessed: storage_types::VecMap::new(),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn execute<A: UpsertAttempt + DeleteItemAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<BatchWriteItemResponse> {
        self.prepare(attempt)
            .await?
            .apply(attempt, captured_at, |_, _| Ok(ItemProvenance::default()))
            .await
    }
}

/// Complete semantic acceptance, before capture or synchronous member effects.
pub struct AcceptedBatchWrite {
    members: Vec<(usize, AcceptedBatchWriteMember)>,
    unprocessed: storage_types::VecMap<TableName, Vec<WriteRequest>>,
}

impl AcceptedBatchWrite {
    pub fn members(&self) -> impl Iterator<Item = (usize, &AcceptedBatchWriteMember)> {
        self.members
            .iter()
            .map(|(ordinal, member)| (*ordinal, member))
    }

    /// Select before staging effects and preserve the exact original rejected
    /// requests.
    pub fn select(
        &mut self,
        shape: &DynamoBatchWriteShape,
        selected: &[bool],
    ) -> StorageResult<()> {
        if selected.len() != shape.members.len() || self.members.len() != selected.len() {
            return Err(StorageError::internal(
                "batch selection differs from its original inventory",
            ));
        }
        self.members.retain(|(ordinal, _)| selected[*ordinal]);
        for (original, selected) in shape.members.iter().zip(selected) {
            if !selected {
                self.unprocessed
                    .get_or_insert_with(original.table_name.clone(), Vec::new)
                    .push(original.original.clone());
            }
        }
        Ok(())
    }

    /// Consume one accepted inventory inside its caller's native attempt.
    /// Only this complete effect path constructs a committed batch response.
    pub async fn apply<A: UpsertAttempt + DeleteItemAttempt>(
        self,
        attempt: &mut A,
        captured_at: TimestampMillis,
        mut provenance: impl FnMut(usize, &AcceptedBatchWriteMember) -> StorageResult<ItemProvenance>
        + Send,
    ) -> StorageResult<BatchWriteItemResponse> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for (ordinal, member) in self.members {
                let provenance = provenance(ordinal, &member)?;
                member
                    .prepare_effects(attempt, captured_at, provenance)
                    .await?;
            }
            attempt.flush_dynamo_write().await?;
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
