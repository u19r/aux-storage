//! Admitted bounded cursor turns shared with the native transfer receipts.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::BaselineCopyScope;
use crate::journal::JournalStore;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RetainedTransferStage {
    Baselines(u64),
    Originals(u64),
    Complete,
}

pub(super) async fn load_retained_transfer_stage<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BaselineCopyScope>,
) -> StorageResult<RetainedTransferStage>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
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
                        .load_retained_transfer_stage(
                            attempt,
                            &scope,
                            storage_types::ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
}

pub(super) async fn advance_retained_transfer_stage<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: Arc<BaselineCopyScope>,
    stage: RetainedTransferStage,
) -> StorageResult<()>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let scope = Arc::clone(&scope);
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
                                .advance_retained_transfer_stage(attempt, &scope, stage)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await
}
