use async_trait::async_trait;
use storage_common::{TtlSweepPageCommand, TtlSweepPageResult, TtlSweepPolicy, TtlSweepRequest};
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
    ) -> StorageResult<Option<TtlSweepPolicy>>;

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult>;

    async fn execute_ttl_sweep_request(
        &self,
        request: TtlSweepRequest,
    ) -> StorageResult<TtlSweepPageResult>;
}

#[async_trait]
impl<P: DynamoPort> TtlSweepRepository for P
where for<'native> P::Attempt<'native>: ExpirationPort
{
    async fn read_ttl_sweep_policy(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlSweepPolicy>> {
        let table_name = table_name.clone();
        self.with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                read_direct_expiration_policy_in_attempt(attempt, &table_name, &Default::default())
                    .await
                    .map(|policy| {
                        policy.map(|(config, table)| TtlSweepPolicy {
                            target: table.as_ref().into(),
                            config,
                        })
                    })
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
                    ExpirationAttempt::new(attempt, Default::default())
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
        let bindings = crate::RequestTableBindings::default();
        storage_common::retry::retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            let bindings = bindings.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    ExpirationAttempt::new(attempt, bindings)
                        .execute_direct_request(&request)
                        .await
                })
            })
        })
        .await
    }
}
