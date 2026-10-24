//! Bounded reader obligations observed from native receipts, without trim
//! authority.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{
    configuration::membership::MembershipRevision,
    identity::{
        origin::{JournalGeneration, OriginId, OriginPosition},
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::{
    checkpoint::CanonicalCheckpointId,
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    retention::CheckpointRetention,
    table_cut::CanonicalTableCut,
    view::DeliveryPosition,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderHoldDiscoveryRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub retention_holds: ReaderHoldDiscoveryAction,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReaderHoldDiscoveryAction {
    Start,
    Continue(Box<ReaderHoldCursor>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderHoldCursor {
    pub(crate) stamp: RetentionInventoryStamp,
    pub(crate) after: DurableRecordKey,
}

/// One native inventory snapshot scope, repeated on terminal and empty pages
/// too.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionInventoryStamp {
    pub(crate) trim: super::trim_authority::TrimRevision,
    pub(crate) region: RegionId,
    pub(crate) group: DatabaseGroupId,
    pub(crate) generation: JournalGeneration,
    pub(crate) tenant: replication_types::identity::resource::TenantId,
    pub(crate) table: replication_types::identity::resource::TableIdentity,
    pub(crate) physical: storage_types::TableHistoryId,
    pub(crate) lineage: TableLineage,
    pub(crate) configuration: replication_types::configuration::table::ConfigRevision,
    pub(crate) membership: MembershipRevision,
    pub(crate) state_revision: storage_types::Revision,
    pub(crate) retention: super::retention::RetentionRevision,
}

/// Receiver views verify the entire partition prefix, including excluded
/// lineages.
#[derive(Serialize)]
pub struct ReceiverHoldRange {
    pub trim: super::trim_authority::TrimRevision,
    pub lineage: TableLineage,
    pub receiver: OriginId,
    pub through: DeliveryPosition,
}

#[derive(Serialize)]
pub struct CaptureHoldRange {
    pub lineage: TableLineage,
    pub origin: OriginId,
    pub after: OriginPosition,
    pub through: Option<OriginPosition>,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
pub enum ReaderHoldObservation {
    Receiver {
        operation: CanonicalCheckpointId,
        retention: CheckpointRetention,
        range: ReceiverHoldRange,
    },
    Source {
        operation: CanonicalCheckpointId,
        retention: CheckpointRetention,
        cut: CanonicalTableCut,
    },
    Capture {
        range: CaptureHoldRange,
    },
    AdoptionInventory {
        operation: replication_types::identity::resource::ControlOperationId,
        table: replication_types::identity::resource::TableIdentity,
        bootstrap: replication_types::protocol::CanonicalDigest,
    },
    RetainedInventory {
        operation: replication_types::identity::resource::ControlOperationId,
        table: replication_types::identity::resource::TableIdentity,
        captured_cuts: u64,
        status: RetainedInventoryStatus,
    },
    ImportFragmentPending {
        binding: replication_types::configuration::membership::RegionalTableBinding,
        page: replication_types::event::interval::TablePageId,
    },
    ArchivePending {
        generation: JournalGeneration,
        metadata: replication_types::protocol::CanonicalDigest,
        content: replication_types::protocol::CanonicalDigest,
    },
}

#[derive(Serialize)]
pub enum RetainedInventoryStatus {
    Collecting,
    Ready,
}

#[derive(Serialize)]
pub struct ReaderHoldPage {
    pub stamp: RetentionInventoryStamp,
    pub hold: Option<ReaderHoldObservation>,
    pub continuation: Option<ReaderHoldCursor>,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn discover_reader_holds(
        &self,
        request: ReaderHoldDiscoveryRequest,
    ) -> StorageResult<ReaderHoldPage> {
        if !self.matches((&request.serving_region, &request.serving_group)) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        let source = self
            .transport
            .reserve_history_source()
            .await
            .map_err(|_| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let port = std::sync::Arc::clone(&self.port);
        let journal = std::sync::Arc::clone(&self.journal);
        let limits = self.transport.source_limits();
        crate::transport::settle_source_with(source, move |source| async move {
            std::sync::Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port.as_ref(),
                        storage_provider::native_admission::NativeWork::RangeRead,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .load_reader_hold_page(attempt, &request, (&limits, source))
                                .await
                        })
                    },
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native reader hold discovery failed"))
        })
    }
}
