//! Admitted source and target attempts for immutable baseline dependencies.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{
    BaselineCopyScope, HiddenTargetRequest, ReenableBaselinePage, ReenableBaselineReceipt,
    copy_scope::BaselineCopyBudget,
};
use crate::journal::JournalStore;

pub(super) struct RetainedBaselineExecution {
    pub scope: Arc<BaselineCopyScope>,
    pub ordinal: u64,
    pub page: Arc<ReenableBaselinePage>,
    pub receipt: Arc<ReenableBaselineReceipt>,
    pub codec: Arc<CanonicalJsonCodec>,
}

pub(super) async fn plan_reenable_baseline<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    selection: (Arc<BaselineCopyScope>, u64),
    encoding: (Arc<CanonicalJsonCodec>, BaselineCopyBudget),
) -> StorageResult<Option<ReenableBaselinePage>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let selection = selection.clone();
        let encoding = encoding.clone();
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
                                .plan_reenable_baseline(
                                    attempt,
                                    (&selection.0, selection.1),
                                    (&encoding.0, encoding.1),
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

pub(super) async fn record_reenable_baseline<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<RetainedBaselineExecution>,
) -> StorageResult<bool>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let execution = Arc::clone(&execution);
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
                                .record_reenable_baseline(
                                    attempt,
                                    (&execution.scope, execution.ordinal),
                                    (&execution.page, &execution.receipt),
                                    &execution.codec,
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

pub(crate) async fn accept_retained_baselines<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    selection: (Arc<HiddenTargetRequest>, u64, Arc<ReenableBaselinePage>),
    encoding: (Arc<CanonicalJsonCodec>, Arc<WorkLimits>),
) -> StorageResult<ReenableBaselineReceipt>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let selection = selection.clone();
        let encoding = encoding.clone();
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
                            let budget = BaselineCopyBudget {
                                records: encoding.1.import_events,
                                bytes: encoding.1.import_bytes,
                            };
                            journal
                                .accept_target_retained_baselines(
                                    attempt,
                                    &selection.0,
                                    (selection.1, &selection.2),
                                    (&encoding.0, budget),
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
