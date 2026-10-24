use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageResult};

use super::HistoryCollector;

impl<P: DynamoPort> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn load_complete(
        &self,
        proposal: Arc<ControlProposal>,
    ) -> StorageResult<Option<super::HistoryAdoptionDefinition>> {
        let journal = Arc::clone(&self.journal);
        Arc::clone(&journal)
            .with_admitted_attempt(
                (
                    self.port.as_ref(),
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(async move {
                        journal
                            .load_completed_history_definition(
                                attempt,
                                &proposal,
                                storage_types::ReadProtection::Snapshot,
                            )
                            .await
                    })
                },
            )
            .await
    }
}
