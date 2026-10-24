use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{BaselineCopyScope, CommittedBaselinePage, copy_scope::BaselineCopyBudget};
use crate::journal::{JournalStore, PlannedBaselineCopy};

pub(super) async fn plan_baseline_copy<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BaselineCopyScope>,
    input: (Arc<CanonicalJsonCodec>, BaselineCopyBudget),
) -> StorageResult<Option<PlannedBaselineCopy>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let scope = Arc::clone(&scope);
        let input = (Arc::clone(&input.0), input.1);
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
                                .plan_baseline_copy(attempt, scope, (&input.0, input.1))
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn acknowledge_baseline_copy<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (Arc<PlannedBaselineCopy>, Arc<CommittedBaselinePage>),
) -> StorageResult<bool>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let input = (Arc::clone(&input.0), Arc::clone(&input.1));
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
                                .acknowledge_baseline_copy(attempt, &input.0, &input.1)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}

pub(super) async fn load_source_bootstrap_definition<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    proposal: Arc<replication_types::configuration::proposal::ControlProposal>,
) -> StorageResult<super::TargetBootstrapDefinition>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let proposal = Arc::clone(&proposal);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .load_source_bootstrap_definition(
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
    })
    .await
}
