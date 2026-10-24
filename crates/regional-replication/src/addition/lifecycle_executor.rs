use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{AddRegionPlan, AdditionChange, AdditionRecord};
use crate::journal::JournalStore;

/// Accept the immutable plan, source slot, and discoverable pending work in
/// one native attempt. No separate queue publication can be lost after commit.
pub async fn accept_addition_plan<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<AddRegionPlan>,
    configuration: &replication_provider::runtime_configuration::deployment::QualifiedConfiguration,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    journal.require_deployment(configuration.as_ref())?;
    plan.require_deployment(configuration.as_ref())?;
    execute_addition(port, journal, plan, Arc::new(AdditionChange::Observe))
        .await
        .map(|_| ())
}

pub(crate) async fn execute_addition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<AddRegionPlan>,
    change: Arc<AdditionChange>,
) -> StorageResult<AdditionRecord>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let plan = Arc::clone(&plan);
        let change = Arc::clone(&change);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(
                            async move { journal.execute_addition(attempt, &plan, &change).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}

/// Cancellation is durable before any peer cleanup; the seeded controller
/// retains the operation until target cancellation and source release finish.
pub async fn cancel_addition_plan<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<AddRegionPlan>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_addition(port, journal, plan, Arc::new(AdditionChange::Cancel))
        .await
        .map(|_| ())
}
