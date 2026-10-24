use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{
    DYNAMO_WRITE_RETRY, DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt,
};
use storage_types::{AttemptMode, StorageResult};

use super::{HiddenTargetRequest, HiddenVerificationStatus, TargetBootstrapDefinition};
use crate::journal::JournalStore;

pub(crate) async fn advance_hidden_verification<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (
        Arc<HiddenTargetRequest>,
        Arc<TargetBootstrapDefinition>,
        Arc<CanonicalJsonCodec>,
        Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
    ),
) -> StorageResult<HiddenVerificationStatus>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + PrimaryStateScanAttempt
        + ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let input = (
            Arc::clone(&input.0),
            Arc::clone(&input.1),
            Arc::clone(&input.2),
            Arc::clone(&input.3),
        );
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .advance_hidden_verification(
                                    attempt,
                                    &input.0,
                                    (&input.1, &input.2, &input.3),
                                )
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}
