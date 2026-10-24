//! Native holds for portable complete original-source cuts.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::membership::MembershipRevision,
    identity::{
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::{
    checkpoint::{CanonicalCheckpointId, CanonicalCheckpointState},
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    table_cut::CanonicalTableCut,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PortableCheckpointAction {
    CreateCapture,
    CreateImport(CanonicalTableCut),
    RolloverCapture { previous: CanonicalCheckpointId },
    Observe,
    Release,
}

impl PortableCheckpointAction {
    pub(crate) fn creation(&self) -> Option<super::checkpoint::CheckpointCreation> {
        use super::checkpoint::CheckpointCreation;
        match self {
            Self::CreateCapture | Self::CreateImport(_) => Some(CheckpointCreation::Direct),
            Self::RolloverCapture { previous } => Some(CheckpointCreation::Replaces(*previous)),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableCheckpointRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub portable_checkpoint: CanonicalCheckpointId,
    pub action: PortableCheckpointAction,
    pub retention: super::retention::CheckpointRetention,
}

#[derive(Serialize)]
pub struct PortableCheckpointStatus {
    pub operation: CanonicalCheckpointId,
    pub state: CanonicalCheckpointState,
    pub cut: CanonicalTableCut,
    pub retention: super::retention::CheckpointRetention,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

#[derive(Serialize)]
pub(crate) struct PortableCheckpointObservation {
    pub(crate) operation: CanonicalCheckpointId,
    pub(crate) state: CanonicalCheckpointState,
    pub(crate) cut: CanonicalTableCut,
    pub(crate) retention: super::retention::CheckpointRetention,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn change_portable_checkpoint(
        &self,
        request: PortableCheckpointRequest,
    ) -> StorageResult<Option<PortableCheckpointStatus>> {
        if !self.matches((&request.serving_region, &request.serving_group)) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        if let Some(creation) = request.action.creation() {
            creation.require_distinct(request.portable_checkpoint)?;
        }
        let source = self
            .transport
            .reserve_history_source()
            .await
            .map_err(|_| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let port = Arc::clone(&self.port);
        let journal = Arc::clone(&self.journal);
        let codec = Arc::clone(&self.codec);
        let limits = self.transport.source_limits();
        let request = Arc::new(request);
        crate::transport::settle_source_with(source, move |source| async move {
            let result = retry_native_conflicts(port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let request = Arc::clone(&request);
                let journal = Arc::clone(&journal);
                let codec = Arc::clone(&codec);
                let port = Arc::clone(&port);
                let limits = Arc::clone(&limits);
                async move {
                    std::sync::Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                port.as_ref(),
                                storage_provider::native_admission::NativeWork::Write,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, observed_at| {
                                Box::pin(async move {
                                    super::retention::settle_expiry(
                                        journal
                                            .change_portable_checkpoint(
                                                attempt,
                                                (&request, observed_at),
                                                (&limits, &codec),
                                            )
                                            .await,
                                    )
                                })
                            },
                        )
                        .await
                }
            })
            .await??;
            Ok(result.map(|observed| PortableCheckpointStatus {
                operation: observed.operation,
                state: observed.state,
                cut: observed.cut,
                retention: observed.retention,
                _source: source,
            }))
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native portable checkpoint failed"))
        })
    }
}
