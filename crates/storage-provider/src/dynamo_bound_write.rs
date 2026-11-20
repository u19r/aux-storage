//! Bound item decisions, accepted effects, and response materialization.

use std::{future::Future, sync::Arc};

use storage_types::{
    BoundTableInfo, DeleteItemResponse, PutItemResponse, StorageResult, TableName, TimestampMillis,
    UpdateItemResponse,
};

use crate::{
    AcceptedDeleteItem, BoundDeleteItem, BoundPutItem, BoundUpdateItem, DeleteItemShape,
    ItemWriteAttempt, PutItemShape, SemanticDecision, UpdateItemShape,
    dynamo_put_item::old_image_for_response,
    dynamo_update_item::UpdateReturnShape,
    dynamo_write_execution::{
        UpsertResponseImages, prepare_accepted_delete, prepare_accepted_upsert,
    },
};

pub trait DynamoItemWriteShape: Send + Sync + 'static {
    type Bound: BoundItemWrite;
    fn table_name(&self) -> &TableName;
    fn bind(self: Arc<Self>, table: Arc<BoundTableInfo>) -> StorageResult<Self::Bound>;
}

/// Decide against protected old state before staging accepted effects.
pub enum AcceptedMutationRef<'accepted> {
    Upsert(&'accepted crate::AcceptedUpsert),
    Delete(&'accepted AcceptedDeleteItem),
}

impl AcceptedMutationRef<'_> {
    pub fn table(&self) -> &BoundTableInfo {
        match self {
            Self::Upsert(item) => &item.table,
            Self::Delete(item) => &item.table,
        }
    }
}

pub trait BoundItemWrite: Sized + Send + 'static {
    type Accepted: Send + 'static;
    type Response: Send + 'static;
    type Outcome: Send + 'static;
    fn accepted_mutation(accepted: &Self::Accepted) -> AcceptedMutationRef<'_>;
    fn put_items(&self) -> Vec<&BoundPutItem> {
        Vec::new()
    }
    fn observation_read(&self) -> crate::ItemWriteObservationRead<'_>;
    fn accept_observation(
        self,
        observed: crate::ItemWriteObservation,
        lifecycle: storage_backfill::GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<Self::Accepted>>;
    /// Apply effects in the borrowed native attempt without committing it.
    fn apply_accepted<A: ItemWriteAttempt>(
        attempt: &mut A,
        accepted: Self::Accepted,
        created_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Outcome>> + Send;
    fn finish(outcome: Self::Outcome) -> StorageResult<Self::Response>;
}

impl DynamoItemWriteShape for PutItemShape {
    type Bound = BoundPutItem;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(self: Arc<Self>, table: Arc<BoundTableInfo>) -> StorageResult<Self::Bound> {
        self.bind_table(table)
    }
}

impl DynamoItemWriteShape for UpdateItemShape {
    type Bound = BoundUpdateItem;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(self: Arc<Self>, table: Arc<BoundTableInfo>) -> StorageResult<Self::Bound> {
        self.bind_table(table)
    }
}

impl DynamoItemWriteShape for DeleteItemShape {
    type Bound = BoundDeleteItem;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(self: Arc<Self>, table: Arc<BoundTableInfo>) -> StorageResult<Self::Bound> {
        self.bind_table(table)
    }
}

impl BoundItemWrite for BoundPutItem {
    type Accepted = (crate::AcceptedUpsert, bool);
    type Response = PutItemResponse;
    type Outcome = (UpsertResponseImages, bool);
    fn accepted_mutation(accepted: &Self::Accepted) -> AcceptedMutationRef<'_> {
        AcceptedMutationRef::Upsert(&accepted.0)
    }
    fn put_items(&self) -> Vec<&BoundPutItem> {
        vec![self]
    }

    fn observation_read(&self) -> crate::ItemWriteObservationRead<'_> {
        crate::ItemWriteObservationRead::Write(&self.table, &self.key)
    }

    fn accept_observation(
        self,
        observed: crate::ItemWriteObservation,
        lifecycle: storage_backfill::GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: ItemWriteAttempt>(
        attempt: &mut A,
        (item, return_old): Self::Accepted,
        created_at: TimestampMillis,
    ) -> StorageResult<Self::Outcome> {
        let item = prepare_accepted_upsert(attempt, item, created_at).await?;
        Ok((item, return_old))
    }

    fn finish((item, return_old): Self::Outcome) -> StorageResult<Self::Response> {
        Ok(PutItemResponse {
            attributes: old_image_for_response(item.old_item.as_ref(), return_old)?,
            consumed_capacity: None,
        })
    }
}

impl BoundItemWrite for BoundUpdateItem {
    type Accepted = (crate::AcceptedUpsert, UpdateReturnShape);
    type Response = UpdateItemResponse;
    type Outcome = (UpsertResponseImages, UpdateReturnShape);
    fn accepted_mutation(accepted: &Self::Accepted) -> AcceptedMutationRef<'_> {
        AcceptedMutationRef::Upsert(&accepted.0)
    }

    fn observation_read(&self) -> crate::ItemWriteObservationRead<'_> {
        crate::ItemWriteObservationRead::Write(&self.table, &self.key)
    }

    fn accept_observation(
        self,
        observed: crate::ItemWriteObservation,
        lifecycle: storage_backfill::GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: ItemWriteAttempt>(
        attempt: &mut A,
        (item, response): Self::Accepted,
        created_at: TimestampMillis,
    ) -> StorageResult<Self::Outcome> {
        let item = prepare_accepted_upsert(attempt, item, created_at).await?;
        Ok((item, response))
    }

    fn finish((_, response): Self::Outcome) -> StorageResult<Self::Response> {
        Ok(response.into_response())
    }
}

impl BoundItemWrite for BoundDeleteItem {
    type Accepted = AcceptedDeleteItem;
    type Response = DeleteItemResponse;
    type Outcome = AcceptedDeleteItem;
    fn accepted_mutation(accepted: &Self::Accepted) -> AcceptedMutationRef<'_> {
        AcceptedMutationRef::Delete(accepted)
    }

    fn observation_read(&self) -> crate::ItemWriteObservationRead<'_> {
        crate::ItemWriteObservationRead::Write(&self.table, &self.key)
    }

    fn accept_observation(
        self,
        observed: crate::ItemWriteObservation,
        lifecycle: storage_backfill::GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: ItemWriteAttempt>(
        attempt: &mut A,
        mut item: Self::Accepted,
        created_at: TimestampMillis,
    ) -> StorageResult<Self::Outcome> {
        prepare_accepted_delete(attempt, &mut item, created_at).await?;
        Ok(item)
    }

    fn finish(item: Self::Outcome) -> StorageResult<Self::Response> {
        item.response()
    }
}

/// PutItem, UpdateItem or DeleteItem as one write plan.
pub(crate) struct ItemWritePlan<S> {
    shape: Arc<S>,
    created_at: TimestampMillis,
}

impl<S> ItemWritePlan<S> {
    pub(crate) fn new(shape: S, created_at: TimestampMillis) -> Self {
        Self {
            shape: Arc::new(shape),
            created_at,
        }
    }
}

/// The binding of one item write try and whether its table needs history.
pub(crate) struct ItemWriteBound<B> {
    decision: SemanticDecision<B>,
    needs_history: bool,
}

impl<S: DynamoItemWriteShape> crate::dynamo_write_session::WritePlan for ItemWritePlan<S> {
    type Bound = ItemWriteBound<S::Bound>;
    type Output = <S::Bound as BoundItemWrite>::Response;

    fn table_names(&self) -> impl Iterator<Item = &TableName> {
        std::iter::once(self.shape.table_name())
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
        let prepared = crate::dynamo_write_preparation::prepare_write_table(
            port,
            session.cache(),
            self.shape.table_name(),
            session.bindings(),
            deadline,
        )
        .await?;
        let needs_history = prepared.table.info.requires_stream_entry();
        let decision =
            SemanticDecision::try_from(Arc::clone(&self.shape).bind(Arc::clone(&prepared.table)))?;
        Ok((
            crate::dynamo_write_preparation::PreparedWriteTables::from_prepared(prepared),
            ItemWriteBound {
                decision,
                needs_history,
            },
        ))
    }

    fn put_items(bound: &Self::Bound) -> Option<Vec<&BoundPutItem>> {
        match &bound.decision {
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
    ) -> StorageResult<(SemanticDecision<Self::Output>, Vec<crate::CachePublication>)> {
        let reads = match &bound.decision {
            SemanticDecision::Accepted(bound) => vec![bound.observation_read()],
            SemanticDecision::Rejected(_) => Vec::new(),
        };
        let needs_history = !reads.is_empty() && bound.needs_history;
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
        let observations = observations.ok_or_else(|| {
            storage_types::StorageError::internal("confirmed write observations were skipped")
        })?;
        let bound = match bound.decision {
            SemanticDecision::Accepted(bound) => bound,
            SemanticDecision::Rejected(error) => {
                return Ok((SemanticDecision::Rejected(error), publications));
            }
        };
        let [(observed, lifecycle)] = observations.into_array::<1>()?;
        attempt.install_prepared_writes(writes);
        let decision = match bound.accept_observation(observed, lifecycle)? {
            SemanticDecision::Accepted(accepted) => {
                let outcome = S::Bound::apply_accepted(attempt, accepted, self.created_at).await?;
                SemanticDecision::Accepted(S::Bound::finish(outcome)?)
            }
            SemanticDecision::Rejected(error) => SemanticDecision::Rejected(error),
        };
        crate::DynamoStreamWriteAttempt::finish_dynamo_write(attempt).await?;
        Ok((decision, publications))
    }
}
