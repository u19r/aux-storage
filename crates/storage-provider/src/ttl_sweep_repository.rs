use async_trait::async_trait;
use storage_common::{TtlConfigRecord, TtlSweepPageCommand, TtlSweepPageResult, TtlSweepRequest};
use storage_types::{StorageResult, TableName};

use crate::{
    DYNAMO_WRITE_RETRY, DynamoPort,
    maintenance::{
        DirectExpirationTransaction, ExpirationAttempt, ExpirationPort,
        read_direct_expiration_policy_in_attempt,
    },
};

/// Executes the deterministic TTL page command used by native replication and
/// the internal test helper.
#[async_trait]
pub trait TtlSweepRepository: Send + Sync {
    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>>;

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult>;

    async fn execute_ttl_sweep_request(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult> {
        let policy = self
            .read_ttl_sweep_policy(&request.table_name)
            .await?
            .ok_or_else(|| {
                storage_types::StorageError::validation("ttl sweep requires an enabled ttl policy")
            })?;
        let command = TtlSweepPageCommand::try_new(
            request.table_name,
            request.input,
            policy.policy_generation,
            storage_types::TimestampMillis::now(),
        )?;
        self.execute_ttl_sweep_page(command).await
    }
}

#[async_trait]
impl<P: DynamoPort> TtlSweepRepository for P
where for<'native> P::Attempt<'native>: ExpirationPort
{
    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        let table_name = table_name.clone();
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                read_direct_expiration_policy_in_attempt(attempt, &table_name)
                    .await
                    .map(|policy| policy.map(|(config, _)| config))
            })
        })
        .await
    }

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult> {
        storage_common::retry::retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let command = command.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    ExpirationAttempt::new(attempt)
                        .execute_direct_expiration(&command)
                        .await
                })
            })
        })
        .await
    }

    async fn execute_ttl_sweep_request(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult> {
        storage_common::retry::retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    ExpirationAttempt::new(attempt)
                        .execute_direct_request(&request)
                        .await
                })
            })
        })
        .await
    }
}
