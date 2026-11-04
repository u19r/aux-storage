//! Bounded reclamation of physically qualified table records.
use infrastructure_provider::{
    page::RecordPageRequest,
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange},
};
use serde::{Deserialize, Serialize};
use storage_provider::TableReclaimStep;
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::JournalStore;
use crate::constants;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(in crate::journal) enum TableOwnedFamily {
    SourceHold,
    ImportFragment,
    DeliveryActivity,
    ImportCoverage,
    Acceptance,
    Event,
    Baseline,
    ImportPage,
    RetainedPosition,
    DeliveryReceipt,
    DeliveryProgress,
    ArchivedEvent,
    ArchivedBaseline,
    ExpectedWinner,
    ProjectionOwner,
    SourceCopy,
    SourceCopyPage,
    SourceTransfer,
    AdoptedTransfer,
    RetainedTransfer,
    BaselineCopy,
    CausalStack,
    CausalVisited,
}

impl TableOwnedFamily {
    pub(in crate::journal) fn next(self) -> Option<Self> {
        match self {
            Self::SourceHold => Some(Self::ImportFragment),
            Self::ImportFragment => Some(Self::DeliveryActivity),
            Self::DeliveryActivity => Some(Self::ImportCoverage),
            Self::ImportCoverage => Some(Self::Acceptance),
            Self::Acceptance => Some(Self::Event),
            Self::Event => Some(Self::Baseline),
            Self::Baseline => Some(Self::ImportPage),
            Self::ImportPage => Some(Self::RetainedPosition),
            Self::RetainedPosition => Some(Self::DeliveryReceipt),
            Self::DeliveryReceipt => Some(Self::DeliveryProgress),
            Self::DeliveryProgress => Some(Self::ArchivedEvent),
            Self::ArchivedEvent => Some(Self::ArchivedBaseline),
            Self::ArchivedBaseline => Some(Self::ExpectedWinner),
            Self::ExpectedWinner => Some(Self::ProjectionOwner),
            Self::ProjectionOwner => Some(Self::SourceCopy),
            Self::SourceCopy => Some(Self::SourceCopyPage),
            Self::SourceCopyPage => Some(Self::SourceTransfer),
            Self::SourceTransfer => Some(Self::AdoptedTransfer),
            Self::AdoptedTransfer => Some(Self::RetainedTransfer),
            Self::RetainedTransfer => Some(Self::BaselineCopy),
            Self::BaselineCopy => Some(Self::CausalStack),
            Self::CausalStack => Some(Self::CausalVisited),
            Self::CausalVisited => None,
        }
    }

    fn prefix(self) -> Vec<u8> {
        match self {
            Self::SourceHold => constants::SOURCE_HOLD_SUFFIX.to_vec(),
            Self::ImportFragment => constants::IMPORT_FRAGMENT_PENDING_SUFFIX.to_vec(),
            Self::DeliveryActivity => constants::DELIVERY_ACTIVITY_SUFFIX.to_vec(),
            Self::ImportCoverage => constants::IMPORT_COVERAGE_SUFFIX.to_vec(),
            Self::Acceptance => constants::ACCEPTANCE_SUFFIX.to_vec(),
            Self::Event => constants::EVENT_SUFFIX.to_vec(),
            Self::Baseline => constants::BASELINE_SUFFIX.to_vec(),
            Self::ImportPage => constants::IMPORT_PAGE_SUFFIX.to_vec(),
            Self::RetainedPosition => constants::RETAINED_POSITION_SUFFIX.to_vec(),
            Self::DeliveryReceipt => constants::DELIVERY_RECEIPT_SUFFIX.to_vec(),
            Self::DeliveryProgress => constants::DELIVERY_PROGRESS_SUFFIX.to_vec(),
            Self::ArchivedEvent => {
                [constants::ARCHIVE_POINTER_SUFFIX, constants::EVENT_SUFFIX].concat()
            }
            Self::ExpectedWinner => constants::EXPECTED_WINNER_SUFFIX.to_vec(),
            Self::ProjectionOwner => constants::PROJECTION_OWNER_SUFFIX.to_vec(),
            Self::SourceCopy => constants::SOURCE_COPY_SUFFIX.to_vec(),
            Self::SourceCopyPage => constants::SOURCE_COPY_PAGE_SUFFIX.to_vec(),
            Self::SourceTransfer => constants::SOURCE_TRANSFER_SUFFIX.to_vec(),
            Self::AdoptedTransfer => constants::ADOPTED_TRANSFER_SUFFIX.to_vec(),
            Self::RetainedTransfer => constants::RETAINED_TRANSFER_PROGRESS_SUFFIX.to_vec(),
            Self::BaselineCopy => constants::BASELINE_COPY_SUFFIX.to_vec(),
            Self::CausalStack => constants::CAUSAL_STACK_SUFFIX.to_vec(),
            Self::CausalVisited => constants::CAUSAL_VISITED_SUFFIX.to_vec(),
            Self::ArchivedBaseline => [
                constants::ARCHIVE_POINTER_SUFFIX,
                constants::BASELINE_SUFFIX,
            ]
            .concat(),
        }
    }
}

impl JournalStore {
    pub(in crate::journal) async fn reclaim_table_owned_family<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        family: TableOwnedFamily,
    ) -> StorageResult<TableReclaimStep> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let attempt = operation.as_mut();
            self.cancel_table_archives(attempt, history).await?;
            let page = self.load_table_owned_page(attempt, history, family).await?;
            let mut changes = match family {
                TableOwnedFamily::Acceptance => {
                    self.prepare_table_receiver_removals(attempt, history, &page.records)
                        .await?
                }
                TableOwnedFamily::DeliveryActivity => {
                    self.prepare_table_delivery_activity_removals(attempt, history, &page.records)
                        .await?
                }
                TableOwnedFamily::ImportFragment => {
                    self.prepare_table_fragment_hold_removals(attempt, history, &page.records)
                        .await?
                }
                TableOwnedFamily::SourceHold => {
                    self.prepare_table_source_hold_index_removals(attempt, &page.records)
                        .await?
                }
                _ => Vec::new(),
            };
            changes.extend(page.records.into_iter().map(|row| RecordChange::Delete {
                key: row.key,
                expected: row.revision,
            }));
            self.records.store(attempt, &changes).await?;
            Ok(if page.continuation.is_some() {
                TableReclaimStep::Advanced
            } else {
                TableReclaimStep::Complete
            })
        }
        .await;
        operation.complete(result)
    }

    async fn load_table_owned_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        family: TableOwnedFamily,
    ) -> StorageResult<infrastructure_provider::page::RecordPage> {
        let first = self.table_family_prefix(history, family)?;
        self.records
            .scan(
                attempt,
                RecordPageRequest {
                    range: self.record_prefix_range(&first)?,
                    after: None,
                    max_bytes: self.records.limits.max_bytes(),
                    protection: ReadProtection::Conflict,
                },
            )
            .await
    }

    fn table_family_prefix(
        &self,
        history: TableHistoryId,
        family: TableOwnedFamily,
    ) -> StorageResult<DurableRecordKey> {
        let mut suffix = family.prefix();
        suffix.extend_from_slice(history.as_ref());
        self.record_key(suffix)
    }
}
