use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort,
    maintenance::{GsiAttempt, GsiBuildRepository, GsiBuildRequest, GsiPort},
};
use storage_types::{AttemptMode, StorageResult, maintenance::StepResult};

use super::{gsi_admission::RegionalGsiAdmission, runtime::RegionalDynamo};

#[async_trait]
impl<P: DynamoPort> GsiBuildRepository for RegionalDynamo<P>
where for<'native> P::Attempt<'native>: GsiPort + PhysicalRecordAttempt
{
    async fn advance_gsi_build(&self, request: GsiBuildRequest) -> StorageResult<StepResult<()>> {
        let admission = RegionalGsiAdmission {
            journal: Arc::clone(&self.journal),
        };
        let advance = retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
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
                        GsiAttempt::advance_with_admission(attempt, &request, admission).await
                    })
                },
            )
        })
        .await?;
        if let Some(policy) = advance.ttl_activation {
            self.port.metadata_cache().invalidate(&policy.table_name);
        }
        Ok(advance.result)
    }
}
