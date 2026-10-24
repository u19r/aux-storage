use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::{
    TtlConfigRecord, TtlSweepPageCommand, TtlSweepPageResult, TtlSweepRequest,
    retry::retry_native_conflicts,
};
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, TtlSweepRepository,
    maintenance::{DirectExpirationTransaction, ExpirationAttempt, ExpirationPort},
};
use storage_types::{AttemptMode, StorageResult, TableName};

use super::{expiration_admission::RegionalExpirationAdmission, runtime::RegionalDynamo};

#[async_trait]
impl<P: DynamoPort> TtlSweepRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: ExpirationPort + PhysicalRecordAttempt
{
    async fn read_ttl_sweep_policy(
        &self,
        name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        self.journal.native_admission.require()?;
        let name = name.clone();
        let admission = RegionalExpirationAdmission {
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
        };
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let name = name.clone();
            let admission = admission.clone();
            self.port
                .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        ExpirationAttempt::with_admission(attempt, admission)
                            .read_direct_policy(&name)
                            .await
                            .map(|policy| policy.map(|(config, _)| config))
                    })
                })
        })
        .await
    }

    async fn execute_ttl_sweep_page(
        &self,
        command: TtlSweepPageCommand,
    ) -> StorageResult<TtlSweepPageResult> {
        self.journal.native_admission.require()?;
        let admission = RegionalExpirationAdmission {
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
        };
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let command = command.clone();
            let admission = admission.clone();
            self.port
                .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        ExpirationAttempt::with_admission(attempt, admission)
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
        self.journal.native_admission.require()?;
        let admission = RegionalExpirationAdmission {
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
        };
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            let admission = admission.clone();
            self.port
                .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        ExpirationAttempt::with_admission(attempt, admission)
                            .execute_direct_request(&request)
                            .await
                    })
                })
        })
        .await
    }
}
