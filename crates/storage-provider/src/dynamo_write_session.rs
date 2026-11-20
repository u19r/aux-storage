//! One Dynamo write or read operation: its retry owner, its native attempts
//! and its catalog settlement. A plan supplies only the steps that differ.

use std::{future::Future, sync::Arc};

use storage_types::{
    AttemptDeadline, AttemptPhase, AttemptPlan, AttemptRequest, ReadVersionPolicy, StorageResult,
    TableName,
};

use crate::{
    DynamoPort,
    dynamo_read_execution::DynamoReadShape,
    dynamo_table_bindings::CatalogSession,
    dynamo_write_preparation::{AcceptedObservations, PreparedWriteTables},
};

/// The steps of one Dynamo item write that differ between PutItem/UpdateItem/
/// DeleteItem, a BatchWriteItem group and TransactWriteItems. `run_write`
/// owns retry, deadline, read version, admission, the attempt and the
/// catalog settlement.
pub(crate) trait WritePlan: Send + Sync + 'static {
    /// The per-try binding, moved into the attempt.
    type Bound: Send + 'static;
    /// The answer of a committed, accepted attempt.
    type Output: Send + 'static;

    /// The tables this write resolves; a missing relation invalidates them.
    fn table_names(&self) -> impl Iterator<Item = &TableName>;

    /// The read-version policy of the first try; every later try is `Fresh`.
    fn first_read_version<P: DynamoPort>(&self, port: &P) -> ReadVersionPolicy {
        port.ordinary_write_read_version_policy()
    }

    /// Prepare the tables (from the cache, or one ReadOnly attempt on a miss)
    /// and bind the request, before admission.
    fn prepare<P: DynamoPort>(
        &self,
        port: &P,
        session: &CatalogSession,
        deadline: AttemptDeadline,
    ) -> impl Future<Output = StorageResult<(PreparedWriteTables, Self::Bound)>> + Send;

    /// The put items to encode before admission; `None` when the binding was
    /// rejected (nothing is encoded).
    fn put_items(bound: &Self::Bound) -> Option<Vec<&crate::BoundPutItem>>;

    /// Inside the attempt: observe, decide, apply, and finish the write. The
    /// publications are those of the confirmed observations; a rejection
    /// before confirmation returns none.
    fn accept_and_apply<
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
        write: &WriteTry,
        bound: Self::Bound,
        writes: StorageResult<<A as crate::ItemWriteAttempt>::PreparedWrites>,
    ) -> impl Future<
        Output = StorageResult<(
            crate::SemanticDecision<Self::Output>,
            Vec<crate::CachePublication>,
        )>,
    > + Send;
}

/// The catalog view of one write try, moved into its attempt.
pub(crate) struct WriteTry {
    tables: PreparedWriteTables,
    cache: Arc<crate::TableMetadataCache>,
    bindings: crate::RequestTableBindings,
}

impl WriteTry {
    /// Read the protected observations and confirm the prepared tables'
    /// stamps in this attempt. The receipt (TransactWriteItems with a token)
    /// is returned apart: it answers before the confirmation.
    pub(crate) async fn observe<
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
        reads: &[crate::ItemWriteObservationRead<'_>],
        receipt_key: Option<crate::ReceiptSlot<'_>>,
        needs_history: bool,
    ) -> StorageResult<(Option<Option<Vec<u8>>>, AcceptedObservations)> {
        let inventory = self.tables.inventory();
        let generation = self.cache.generation();
        let mut observed = crate::DynamoTransactWriteAttempt::read_write_observations(
            attempt,
            crate::WriteObservationBatch {
                tables: &inventory,
                reads,
                receipt_key,
                prerequisites: crate::ItemWritePrerequisites { needs_history },
                cache: Some(&self.cache),
            },
        )
        .await?;
        let receipt = observed.receipt.take();
        let accepted =
            self.tables
                .accept_observations(&self.cache, &self.bindings, observed, generation)?;
        Ok((receipt, accepted))
    }
}

/// Run one write under the started operation deadline of `request`: known
/// aborts get a fresh try; an unknown outcome returns unchanged.
pub(crate) async fn run_write<P, W>(
    port: &P,
    request: AttemptRequest,
    session: &CatalogSession,
    plan: Arc<W>,
) -> StorageResult<W::Output>
where
    P: DynamoPort,
    W: WritePlan,
{
    let first_read_version = plan.first_read_version(port);
    storage_common::retry::retry_started_tries(port, request, |request, attempt_try| {
        let plan = Arc::clone(&plan);
        async move {
            let deadline = request.deadline;
            let (tables, bound) = plan.prepare(port, session, deadline).await?;
            let writes = match W::put_items(&bound) {
                Some(items) => port.prepare_put_items(&items),
                None => Ok(P::PreparedWrites::default()),
            };
            let read_version = if attempt_try.is_first() {
                first_read_version
            } else {
                ReadVersionPolicy::Fresh
            };
            let write = WriteTry {
                tables,
                cache: Arc::clone(session.cache()),
                bindings: session.bindings().for_write_attempt(read_version),
            };
            let promotion = write.bindings.clone();
            let mut attempt_request = request;
            attempt_request.read_version = read_version;
            deadline.require_remaining(AttemptPhase::Acquire)?;
            let attempt_plan = Arc::clone(&plan);
            let result = port
                .with_attempt(attempt_request, move |attempt| {
                    Box::pin(async move {
                        attempt_plan
                            .accept_and_apply(attempt, &write, bound, writes)
                            .await
                    })
                })
                .await;
            let decision =
                session.settle(result.inspect(|_| promotion.promote()), plan.table_names())?;
            decision.into_result()
        }
    })
    .await
}

/// Run one read in a ReadOnly attempt per try; publish after `Ok`.
pub(crate) async fn run_read<P, S>(
    port: &P,
    plan: AttemptPlan,
    shape: S,
) -> StorageResult<S::Response>
where
    P: DynamoPort,
    S: DynamoReadShape,
{
    let session = CatalogSession::new(port.metadata_cache());
    let shape = Arc::new(shape);
    storage_common::retry::retry_tries(port, port.attempt_deadlines(), plan, |request, _| {
        let attempt_shape = Arc::clone(&shape);
        let attempt_session = session.clone();
        let shape = &shape;
        let session = &session;
        async move {
            let result = port
                .with_attempt(request, move |attempt| {
                    Box::pin(async move {
                        attempt_shape
                            .execute(attempt, attempt_session.cache(), attempt_session.bindings())
                            .await
                    })
                })
                .await;
            session.settle(result, shape.read_table_names())
        }
    })
    .await
}
