//! Shared validation, retry, and native attempt ownership for destination
//! writes.

use storage_types::{
    StorageEnum, StorageResult, TableName, TimestampMillis, TransactWriteItemsRequest,
    TransactWriteItemsResponse, context::WrappedError as _,
};

use crate::{DynamoPort, DynamoTransactWriteShape};

pub(crate) async fn write_transaction<P: DynamoPort>(
    port: &P,
    request: TransactWriteItemsRequest,
    authority: Option<crate::job_write_authority::JobWriteAuthority>,
) -> StorageResult<TransactWriteItemsResponse> {
    let fresh_only = authority.is_some();
    let shape = DynamoTransactWriteShape::try_from_request(request)?;
    let shape = match authority {
        Some(authority) => shape.authorize_effect(authority)?,
        None => shape,
    };
    let plan = std::sync::Arc::new(TransactWritePlan::new(
        shape,
        port.now(),
        port.clock(),
        fresh_only,
    ));
    let session = crate::dynamo_table_bindings::CatalogSession::new(port.metadata_cache());
    crate::dynamo_write_session::run_write(
        port,
        crate::constants::TRANSACTION_WRITE.start(port.attempt_deadlines()),
        &session,
        plan,
    )
    .await
    .map_err(|error| {
        if is_transaction_bytes_rejected(&error) {
            transaction_size_validation()
        } else {
            error
        }
    })
    .inspect_err(|error| {
        if matches!(error.to_enum(), StorageEnum::IdempotentParameterMismatch) {
            tracing::info!(
                operation = "TransactWriteItems",
                error_code = "IdempotentParameterMismatchException",
                reason = "token_reused_with_different_request",
                "transaction request rejected"
            );
        }
    })
}

pub(crate) fn is_transaction_bytes_rejected(error: &storage_types::StorageError) -> bool {
    error.attempt_kind() == Some(storage_types::AttemptKind::TransactionBytes)
}

/// The public answer when one native transaction cannot hold the request.
pub(crate) fn transaction_size_validation() -> storage_types::StorageError {
    storage_types::StorageError::validation("Transaction size exceeds the provider limit")
}

/// TransactWriteItems (or a job effect) as one write plan.
struct TransactWritePlan {
    shape: std::sync::Arc<DynamoTransactWriteShape>,
    names: Vec<TableName>,
    captured_at: TimestampMillis,
    clock: std::sync::Arc<dyn storage_types::Clock>,
    /// A job effect reads at a fresh version on every try.
    fresh_only: bool,
}

impl TransactWritePlan {
    fn new(
        shape: DynamoTransactWriteShape,
        captured_at: TimestampMillis,
        clock: std::sync::Arc<dyn storage_types::Clock>,
        fresh_only: bool,
    ) -> Self {
        let names = shape.table_names();
        Self {
            shape: std::sync::Arc::new(shape),
            names,
            captured_at,
            clock,
            fresh_only,
        }
    }
}

impl crate::dynamo_write_session::WritePlan for TransactWritePlan {
    type Bound = crate::dynamo_transact_write::BoundTransactWrite;
    type Output = TransactWriteItemsResponse;

    fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.names.iter()
    }

    fn first_read_version<P: DynamoPort>(&self, port: &P) -> storage_types::ReadVersionPolicy {
        if self.fresh_only {
            storage_types::ReadVersionPolicy::Fresh
        } else {
            port.ordinary_write_read_version_policy()
        }
    }

    async fn prepare<P: DynamoPort>(
        &self,
        port: &P,
        session: &crate::dynamo_table_bindings::CatalogSession,
        deadline: storage_types::AttemptDeadline,
    ) -> StorageResult<(
        crate::dynamo_write_preparation::PreparedWriteTables,
        Self::Bound,
    )> {
        let tables = crate::dynamo_write_preparation::PreparedWriteTables::load(
            port,
            session.cache(),
            self.names.clone(),
            session.bindings(),
            deadline,
        )
        .await?;
        let bound = std::sync::Arc::clone(&self.shape).bind(
            tables
                .ensure_present()
                .map(|()| |name: &TableName| tables.table(name)),
            self.captured_at,
        );
        Ok((tables, bound))
    }

    fn put_items(bound: &Self::Bound) -> Option<Vec<&crate::BoundPutItem>> {
        Some(bound.put_items())
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
    ) -> StorageResult<(
        crate::SemanticDecision<TransactWriteItemsResponse>,
        Vec<crate::CachePublication>,
    )> {
        attempt.install_prepared_writes(writes);
        let reads = bound.observation_reads();
        let (receipt, accepted) = write
            .observe(
                attempt,
                &reads,
                self.shape.token().map(|token| token.slot()),
                bound.needs_history(),
            )
            .await?;
        let crate::dynamo_write_preparation::AcceptedObservations {
            decision: confirmation,
            publications,
        } = accepted;
        let receipt = match receipt {
            Some(bytes) => Some(crate::transact_idempotency::accept_transact_response(
                self.shape.token(),
                bytes,
                self.captured_at,
            )?),
            None => None,
        };
        if self.shape.token().is_some() && receipt.is_none() {
            return Err(storage_types::StorageError::internal(
                "successful transaction observation has no receipt",
            ));
        }
        let (decision, publications) = match receipt {
            Some(crate::SemanticDecision::Accepted(Some(response))) => {
                (crate::SemanticDecision::Accepted(response), Vec::new())
            }
            Some(crate::SemanticDecision::Rejected(error)) => {
                (crate::SemanticDecision::Rejected(error), Vec::new())
            }
            _ => match confirmation? {
                crate::SemanticDecision::Rejected(error) => {
                    (crate::SemanticDecision::Rejected(error), Vec::new())
                }
                crate::SemanticDecision::Accepted(observations) => {
                    let decision = match bound
                        .accept_observations(
                            attempt,
                            self.clock.as_ref(),
                            observations.ok_or_else(|| {
                                storage_types::StorageError::internal(
                                    "confirmed write observations were skipped",
                                )
                            })?,
                        )
                        .await?
                    {
                        crate::SemanticDecision::Rejected(error) => {
                            crate::SemanticDecision::Rejected(error)
                        }
                        crate::SemanticDecision::Accepted(accepted) => {
                            crate::SemanticDecision::Accepted(
                                accepted.apply(attempt, self.captured_at).await?,
                            )
                        }
                    };
                    (decision, publications)
                }
            },
        };
        crate::DynamoStreamWriteAttempt::finish_dynamo_write(attempt).await?;
        Ok((decision, publications))
    }
}
