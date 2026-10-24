use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort,
    maintenance::{
        ExpirationAttempt, ExpirationPort, ExpirationRepository, ExpirationRequest,
        ExpirationTransaction,
    },
};
use storage_types::{AttemptMode, StorageResult, maintenance::StepResult};

use super::{expiration_admission::RegionalExpirationAdmission, runtime::RegionalDynamo};

#[async_trait]
impl<P: DynamoPort> ExpirationRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: ExpirationPort + PhysicalRecordAttempt
{
    async fn advance_expiration(
        &self,
        request: ExpirationRequest,
    ) -> StorageResult<StepResult<()>> {
        let admission = RegionalExpirationAdmission {
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
        };
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            let admission = admission.clone();
            Arc::clone(&self.journal).with_admitted_attempt(
                (
                    self.port.as_ref(),
                    storage_provider::native_admission::NativeWork::Background,
                ),
                crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                move |attempt, _| {
                    Box::pin(async move {
                        ExpirationAttempt::with_admission(attempt, admission)
                            .advance_expiration(&request)
                            .await
                    })
                },
            )
        })
        .await
    }
}
