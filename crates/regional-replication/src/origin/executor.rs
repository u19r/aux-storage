use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::content::MutationCause;
use serde::Serialize;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    BoundItemWrite, DYNAMO_WRITE_RETRY, DynamoItemWriteShape, DynamoPort, ItemWriteCause,
    resolve_table_in_attempt,
};
use storage_types::{AttemptMode, ItemProvenance, StorageError};

use super::receipt::{OriginOperationReceipt, RecoveryOperation};
use crate::{
    journal::{
        JournalStore,
        origin::{OriginCaptureInput, OriginMutation, capture_origin_mutations},
    },
    native_record::corrupt_record,
};

pub(crate) struct OriginExecution {
    pub journal: Arc<JournalStore>,
    pub codec: Arc<CanonicalJsonCodec>,
}

pub(crate) struct OriginFailure {
    pub recovery: RecoveryOperation,
    pub error: StorageError,
}

/// Consume a fresh customer execution. Only native conflicts retry; the
/// returned recovery capability can observe an uncertain commit but cannot run
/// it.
pub(crate) async fn execute_item_write<P: DynamoPort, S: DynamoItemWriteShape>(
    port: &P,
    execution: OriginExecution,
    shape: S,
) -> Result<<S::Bound as BoundItemWrite>::Response, OriginFailure>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    <S::Bound as BoundItemWrite>::Response: Serialize + Sync,
{
    execute_operation(port, execution, ItemOperation(shape)).await
}

pub(super) struct OriginAttemptContext {
    pub journal: Arc<JournalStore>,
    pub codec: Arc<CanonicalJsonCodec>,
    pub cache: Arc<storage_provider::TableMetadataCache>,
    pub clock: Arc<dyn storage_types::Clock>,
    pub operation: replication_types::identity::resource::OperationIdentity,
    pub created_at: storage_types::TimestampMillis,
    pub wall_ms: u64,
}

pub(super) struct OriginAttemptOutput<R> {
    pub response: R,
    pub publications: Vec<storage_provider::CachePublication>,
    pub events: Vec<replication_types::event::identity::CanonicalEventId>,
}

pub(super) trait OriginOperation: Clone + Send + 'static {
    type Response: Serialize + Send + Sync + 'static;
    const KIND: super::receipt::OriginOperationKind;
    fn execute<A: storage_provider::DynamoAttempt + PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        context: &OriginAttemptContext,
    ) -> impl std::future::Future<
        Output = storage_types::StorageResult<OriginAttemptOutput<Self::Response>>,
    > + Send;
}

pub(super) async fn execute_operation<P: DynamoPort, O: OriginOperation>(
    port: &P,
    execution: OriginExecution,
    operation: O,
) -> Result<O::Response, OriginFailure>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let recovery = RecoveryOperation {
        identity: replication_types::identity::resource::OperationIdentity::fresh(),
        kind: O::KIND,
    };
    let cache = port.metadata_cache();
    let publication_cache = Arc::clone(&cache);
    let created_at = port.now();
    let wall_ms = u64::try_from(created_at.timestamp_millis()).map_err(|_| OriginFailure {
        recovery,
        error: corrupt_record(),
    })?;
    let context = Arc::new(OriginAttemptContext {
        journal: execution.journal,
        codec: execution.codec,
        cache,
        clock: port.clock(),
        operation: recovery.identity(),
        created_at,
        wall_ms,
    });
    let result = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let operation = operation.clone();
        let context = Arc::clone(&context);
        async move {
            port.with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    let output = operation.execute(attempt, &context).await?;
                    if !output.events.is_empty() {
                        context
                            .journal
                            .store_operation_receipt(
                                attempt,
                                &OriginOperationReceipt {
                                    operation: context.operation,
                                    cause: O::KIND,
                                    response: &output.response,
                                    events: output.events,
                                },
                            )
                            .await?;
                        context.journal.require_acceptance_capacity(attempt).await?;
                    }
                    Ok((output.response, output.publications))
                })
            })
            .await
        }
    })
    .await;
    match result {
        Ok((response, publications)) => {
            publication_cache.publish(publications);
            Ok(response)
        }
        Err(error) => Err(OriginFailure { recovery, error }),
    }
}

#[derive(Clone)]
struct ItemOperation<S>(S);

impl<S: DynamoItemWriteShape> OriginOperation for ItemOperation<S>
where <S::Bound as BoundItemWrite>::Response: Serialize + Sync
{
    type Response = <S::Bound as BoundItemWrite>::Response;
    const KIND: super::receipt::OriginOperationKind =
        super::receipt::OriginOperationKind::Item(S::CAUSE);

    async fn execute<A: storage_provider::DynamoAttempt + PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        context: &OriginAttemptContext,
    ) -> storage_types::StorageResult<OriginAttemptOutput<Self::Response>> {
        let shape = self.0;
        let (table, publication) =
            resolve_table_in_attempt(attempt, &context.cache, shape.table_name()).await?;
        let configured = context.journal.load_capture_state(attempt, &table).await?;
        context
            .journal
            .require_origin_admission(configured.as_ref())?;
        let ttl = if S::CAUSE == ItemWriteCause::Put {
            attempt.read_ttl_config(&table).await?
        } else {
            None
        };
        let bound = shape.bind(table, ttl.as_ref())?;
        attempt.prepare_put_items(&bound.put_items())?;
        let accepted = bound.prepare(attempt).await?.into_result()?;
        let mut captured = match configured
            .as_ref()
            .filter(|table| table.state().captures_origin())
        {
            Some(table) => {
                capture_origin_mutations(
                    &context.journal,
                    attempt,
                    OriginCaptureInput {
                        mutations: vec![OriginMutation {
                            table,
                            mutation: S::Bound::accepted_mutation(&accepted),
                            cause: match S::CAUSE {
                                ItemWriteCause::Put => MutationCause::Put,
                                ItemWriteCause::Update => MutationCause::Update,
                                ItemWriteCause::Delete => MutationCause::Delete,
                            },
                            ordinal: 0.into(),
                        }],
                        wall_ms: context.wall_ms,
                        transaction: None,
                        observations: Vec::new(),
                    },
                    &context.codec,
                )
                .await?
            }
            None => Vec::new(),
        };
        let events = captured
            .iter()
            .map(|mutation| mutation.event.clone())
            .collect();
        let provenance = captured
            .pop()
            .map_or_else(ItemProvenance::default, |mutation| mutation.provenance);
        let outcome =
            S::Bound::apply_accepted(attempt, accepted, context.created_at, provenance).await?;
        Ok(OriginAttemptOutput {
            response: S::Bound::finish(outcome)?,
            publications: publication.into_iter().collect(),
            events,
        })
    }
}
