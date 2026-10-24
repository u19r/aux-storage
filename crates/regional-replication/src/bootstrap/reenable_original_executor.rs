//! Source attempts for exact retained originals and durable delivery receipts.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::regional_import::RegionalImport;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::BaselineCopyScope;
use crate::{journal::JournalStore, transport::RemoteImportReceipt};

pub(super) struct RetainedOriginalExecution {
    pub scope: Arc<BaselineCopyScope>,
    pub ordinal: u64,
    pub import: Arc<RegionalImport>,
    pub receipt: Arc<RemoteImportReceipt>,
    pub codec: Arc<CanonicalJsonCodec>,
    pub limits: Arc<WorkLimits>,
}

pub(super) async fn plan_reenable_original<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    selection: (Arc<BaselineCopyScope>, u64),
    encoding: (Arc<CanonicalJsonCodec>, Arc<WorkLimits>),
) -> StorageResult<Option<RegionalImport>>
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .plan_reenable_original(
                                    attempt,
                                    (&selection.0, selection.1),
                                    (&encoding.0, &encoding.1),
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

pub(super) async fn record_reenable_original<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<RetainedOriginalExecution>,
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
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .record_reenable_original(
                                    attempt,
                                    (&execution.scope, execution.ordinal),
                                    (&execution.import, &execution.receipt),
                                    (&execution.codec, &execution.limits),
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
