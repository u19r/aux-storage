use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{
    plan::EnablePlan,
    record::{EnablePhase, EnableRecord},
};
use crate::journal::JournalStore;

pub(crate) enum EnableChange {
    Observe,
    Reserve(Box<replication_types::configuration::installed::InstalledTableState>),
    Advance(EnablePhase),
    Cancel,
}

/// Accept the immutable plan, source slot, and discoverable pending work in
/// one native attempt. No separate queue publication can be lost after commit.
pub async fn accept_enable_plan<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<EnablePlan>,
    configuration: &replication_provider::runtime_configuration::deployment::QualifiedConfiguration,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    journal.require_deployment(configuration.as_ref())?;
    plan.require_deployment(configuration.as_ref())?;
    execute_enable(port, journal, plan, Arc::new(EnableChange::Observe))
        .await
        .map(|_| ())
}

/// Persist cancellation before the seeded controller cleans up peers and holds.
pub async fn cancel_enable_plan<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<EnablePlan>,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_enable(port, journal, plan, Arc::new(EnableChange::Cancel))
        .await
        .map(|_| ())
}

pub(crate) async fn execute_enable<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    plan: Arc<EnablePlan>,
    change: Arc<EnableChange>,
) -> StorageResult<EnableRecord>
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
                            async move { journal.execute_enable(attempt, &plan, &change).await },
                        )
                    },
                )
                .await
        }
    })
    .await
}
