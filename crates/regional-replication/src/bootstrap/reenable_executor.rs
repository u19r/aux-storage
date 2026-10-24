//! Native target declaration attempts shared by the configured bootstrap RPC.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{
    HiddenTargetRequest, ReenableHistoryDefinition, ReenableHistoryPage,
    TargetRetainedHistoryObservation,
};
use crate::journal::JournalStore;

#[derive(Clone)]
pub(crate) enum RetainedDeclaration {
    Begin,
    Observe,
    Page(Arc<ReenableHistoryPage>),
}

pub(crate) async fn execute_retained_declaration<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    binding: (Arc<HiddenTargetRequest>, Arc<ReenableHistoryDefinition>),
    action: RetainedDeclaration,
) -> StorageResult<Option<TargetRetainedHistoryObservation>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let binding = binding.clone();
        let action = action.clone();
        async move {
            let mode = if matches!(action, RetainedDeclaration::Observe) {
                AttemptMode::ReadOnly
            } else {
                AttemptMode::ReadWrite
            };
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(mode),
                    move |attempt, _| {
                        Box::pin(async move {
                            match action {
                                RetainedDeclaration::Begin => journal
                                    .begin_target_retained_history(attempt, &binding.0, &binding.1)
                                    .await
                                    .map(Some),
                                RetainedDeclaration::Observe => {
                                    journal
                                        .observe_target_retained_history(
                                            attempt, &binding.0, &binding.1,
                                        )
                                        .await
                                }
                                RetainedDeclaration::Page(page) => journal
                                    .accept_target_retained_page(
                                        attempt,
                                        &binding.0,
                                        (&binding.1, &page),
                                    )
                                    .await
                                    .map(Some),
                            }
                        })
                    },
                )
                .await
        }
    })
    .await
}
