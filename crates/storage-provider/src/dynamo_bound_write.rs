//! Bound item decisions, accepted effects, and response materialization.

use std::{future::Future, sync::Arc};

use storage_types::{
    BoundTableInfo, DeleteItemResponse, ItemProvenance, PutItemResponse, StorageResult, TableName,
    TimestampMillis, UpdateItemResponse,
};

use crate::{
    AcceptedDeleteItem, BoundDeleteItem, BoundPutItem, BoundUpdateItem, DeleteItemAttempt,
    DeleteItemShape, ItemWriteObservationAttempt, PutItemShape, SemanticDecision, UpdateItemShape,
    UpsertAttempt,
    dynamo_put_item::old_image_for_response,
    dynamo_update_item::UpdateReturnShape,
    dynamo_write_execution::{
        UpsertResponseImages, prepare_accepted_delete, prepare_accepted_upsert,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemWriteCause {
    Put,
    Update,
    Delete,
}

/// A qualified physical table and TTL policy from the caller's preparation
/// boundary.
pub struct WriteTableBinding {
    pub table: Arc<BoundTableInfo>,
    pub ttl: Option<crate::TtlConfigRecord>,
}

pub trait DynamoItemWriteShape: Clone + Send + 'static {
    type Bound: BoundItemWrite;
    const CAUSE: ItemWriteCause;
    fn table_name(&self) -> &TableName;
    fn bind(
        self,
        table: Arc<BoundTableInfo>,
        ttl: Option<&crate::TtlConfigRecord>,
    ) -> StorageResult<Self::Bound>;
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
    fn execute<A: UpsertAttempt + DeleteItemAttempt>(
        self,
        attempt: &mut A,
        created_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Outcome>> + Send {
        async move {
            let accepted = self.prepare(attempt).await?.into_result()?;
            Self::apply_accepted(attempt, accepted, created_at, ItemProvenance::default()).await
        }
    }
    fn prepare<A: ItemWriteObservationAttempt>(
        self,
        attempt: &mut A,
    ) -> impl Future<Output = StorageResult<SemanticDecision<Self::Accepted>>> + Send;
    /// Apply effects in the borrowed native attempt without committing it.
    fn apply_accepted<A: UpsertAttempt + DeleteItemAttempt>(
        attempt: &mut A,
        accepted: Self::Accepted,
        created_at: TimestampMillis,
        provenance: ItemProvenance,
    ) -> impl Future<Output = StorageResult<Self::Outcome>> + Send;
    fn finish(outcome: Self::Outcome) -> StorageResult<Self::Response>;
}

impl DynamoItemWriteShape for PutItemShape {
    type Bound = BoundPutItem;
    const CAUSE: ItemWriteCause = ItemWriteCause::Put;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(
        self,
        table: Arc<BoundTableInfo>,
        ttl: Option<&crate::TtlConfigRecord>,
    ) -> StorageResult<Self::Bound> {
        self.bind_table(table, ttl)
    }
}

impl DynamoItemWriteShape for UpdateItemShape {
    type Bound = BoundUpdateItem;
    const CAUSE: ItemWriteCause = ItemWriteCause::Update;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(
        self,
        table: Arc<BoundTableInfo>,
        _ttl: Option<&crate::TtlConfigRecord>,
    ) -> StorageResult<Self::Bound> {
        self.bind_table(table)
    }
}

impl DynamoItemWriteShape for DeleteItemShape {
    type Bound = BoundDeleteItem;
    const CAUSE: ItemWriteCause = ItemWriteCause::Delete;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn bind(
        self,
        table: Arc<BoundTableInfo>,
        _ttl: Option<&crate::TtlConfigRecord>,
    ) -> StorageResult<Self::Bound> {
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

    async fn prepare<A: ItemWriteObservationAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        let (observed, lifecycle) = attempt.observe_for_write(&self.table, &self.key).await?;
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: UpsertAttempt + DeleteItemAttempt>(
        attempt: &mut A,
        (item, return_old): Self::Accepted,
        created_at: TimestampMillis,
        provenance: ItemProvenance,
    ) -> StorageResult<Self::Outcome> {
        let item = prepare_accepted_upsert(attempt, item, created_at, provenance).await?;
        attempt.flush_dynamo_write().await?;
        Ok((item, return_old))
    }

    fn finish((item, return_old): Self::Outcome) -> StorageResult<Self::Response> {
        Ok(PutItemResponse {
            attributes: old_image_for_response(item.old_item.as_ref(), return_old)?,
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

    async fn prepare<A: ItemWriteObservationAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        let (observed, lifecycle) = attempt.observe_for_write(&self.table, &self.key).await?;
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: UpsertAttempt + DeleteItemAttempt>(
        attempt: &mut A,
        (item, response): Self::Accepted,
        created_at: TimestampMillis,
        provenance: ItemProvenance,
    ) -> StorageResult<Self::Outcome> {
        let item = prepare_accepted_upsert(attempt, item, created_at, provenance).await?;
        attempt.flush_dynamo_write().await?;
        Ok((item, response))
    }

    fn finish((item, response): Self::Outcome) -> StorageResult<Self::Response> {
        response.finish(item)
    }
}

impl BoundItemWrite for BoundDeleteItem {
    type Accepted = AcceptedDeleteItem;
    type Response = DeleteItemResponse;
    type Outcome = AcceptedDeleteItem;
    fn accepted_mutation(accepted: &Self::Accepted) -> AcceptedMutationRef<'_> {
        AcceptedMutationRef::Delete(accepted)
    }

    async fn prepare<A: ItemWriteObservationAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<SemanticDecision<Self::Accepted>> {
        let (observed, lifecycle) = attempt.observe_for_write(&self.table, &self.key).await?;
        self.accept_old(observed, lifecycle)
    }

    async fn apply_accepted<A: UpsertAttempt + DeleteItemAttempt>(
        attempt: &mut A,
        item: Self::Accepted,
        created_at: TimestampMillis,
        provenance: ItemProvenance,
    ) -> StorageResult<Self::Outcome> {
        prepare_accepted_delete(attempt, &item, created_at, provenance).await?;
        attempt.flush_dynamo_write().await?;
        Ok(item)
    }

    fn finish(item: Self::Outcome) -> StorageResult<Self::Response> {
        item.response()
    }
}
