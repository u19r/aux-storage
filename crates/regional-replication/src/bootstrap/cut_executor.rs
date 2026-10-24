use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{HiddenTargetRequest, SourceCaptureCut, SourceCaptureDefinition};
use crate::journal::JournalStore;

pub(crate) async fn declare_bootstrap_cut<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (
        Arc<HiddenTargetRequest>,
        Arc<SourceCaptureDefinition>,
        Arc<SourceCaptureCut>,
    ),
) -> StorageResult<SourceCaptureCut>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let input = (
            Arc::clone(&input.0),
            Arc::clone(&input.1),
            Arc::clone(&input.2),
        );
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .declare_bootstrap_cut(attempt, &input.0, (&input.1, &input.2))
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}
