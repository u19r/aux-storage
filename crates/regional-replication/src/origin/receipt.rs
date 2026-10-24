use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{event::identity::CanonicalEventId, identity::resource::OperationIdentity};
use serde::{Deserialize, Serialize};
use storage_provider::{DynamoPort, ItemWriteCause};
use storage_types::{AttemptMode, StorageResult};

use crate::journal::JournalStore;

/// Recovery carries no executable request or authority to allocate another
/// event.
#[derive(Clone, Copy)]
pub(crate) struct RecoveryOperation {
    pub(super) identity: OperationIdentity,
    pub(super) kind: OriginOperationKind,
}

impl RecoveryOperation {
    pub fn identity(self) -> OperationIdentity {
        self.identity
    }
    pub(crate) fn require_receipt(&self, receipt: &OriginOperationReceipt) -> StorageResult<()> {
        if receipt.operation != self.identity || receipt.cause != self.kind {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginOperationReceipt<R = Box<serde_json::value::RawValue>> {
    pub operation: OperationIdentity,
    pub cause: OriginOperationKind,
    pub response: R,
    pub events: Vec<CanonicalEventId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OriginOperationKind {
    Item(ItemWriteCause),
    Batch,
    Transaction,
}

pub(crate) enum OriginReceiptObservation {
    Durable(OriginOperationReceipt),
    Unresolved,
}

/// A fresh read can prove a durable response, but absence never proves abort.
pub(crate) async fn reconcile_item_write<P: DynamoPort>(
    port: &P,
    journal: std::sync::Arc<JournalStore>,
    recovery: RecoveryOperation,
) -> StorageResult<OriginReceiptObservation>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    std::sync::Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move {
                    Ok(
                        match journal.load_operation_receipt(attempt, recovery).await? {
                            Some(receipt) => OriginReceiptObservation::Durable(receipt),
                            None => OriginReceiptObservation::Unresolved,
                        },
                    )
                })
            },
        )
        .await
}
