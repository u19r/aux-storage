use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::coverage::DeliveryCoverage,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::regional_import::RegionalImport;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::DeliveryScope;
use crate::{journal::JournalStore, transport::RemoteImportReceipt};

pub struct DeliveryExecution {
    pub scope: DeliveryScope,
    pub import: Arc<RegionalImport>,
    pub receipt: Arc<RemoteImportReceipt>,
    pub codec: Arc<CanonicalJsonCodec>,
    pub limits: Arc<WorkLimits>,
}

/// Persist only an already validated configured-peer response. Known native
/// conflicts retry fresh observations; an unknown result is returned unchanged.
pub async fn record_delivery<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: Arc<DeliveryExecution>,
) -> StorageResult<DeliveryCoverage>
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
                                .record_delivery(
                                    attempt,
                                    (&execution.scope, &execution.import, &execution.receipt),
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

pub async fn load_delivery_progress<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    scope: DeliveryScope,
) -> StorageResult<DeliveryCoverage>
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
                Box::pin(async move { journal.load_delivery_progress(attempt, &scope).await })
            },
        )
        .await
}
