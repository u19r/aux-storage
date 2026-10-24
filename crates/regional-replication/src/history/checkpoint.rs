//! Durable receiver-local checkpoint creation, reconciliation and release.

use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageError, StorageResult};

use super::{
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    selection::CanonicalViewCursor,
    view::CanonicalViewRequest,
};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "uuid::Uuid", into = "uuid::Uuid")]
pub struct CanonicalCheckpointId(uuid::Uuid);

impl TryFrom<uuid::Uuid> for CanonicalCheckpointId {
    type Error = StorageError;
    fn try_from(value: uuid::Uuid) -> StorageResult<Self> {
        if value.is_nil() {
            return Err(StorageError::validation(
                "Canonical checkpoint operation must be nonzero",
            ));
        }
        Ok(Self(value))
    }
}

impl From<CanonicalCheckpointId> for uuid::Uuid {
    fn from(value: CanonicalCheckpointId) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CanonicalCheckpointAction {
    Create,
    Rollover { previous: CanonicalCheckpointId },
    Observe,
    Release,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum CheckpointCreation {
    Direct,
    Replaces(CanonicalCheckpointId),
}

impl CheckpointCreation {
    pub(crate) fn require_distinct(self, operation: CanonicalCheckpointId) -> StorageResult<()> {
        if self == Self::Replaces(operation) {
            return Err(StorageError::validation(
                "Rollover checkpoint IDs must differ",
            ));
        }
        Ok(())
    }
}

impl CanonicalCheckpointAction {
    pub(crate) fn creation(self) -> Option<CheckpointCreation> {
        match self {
            Self::Create => Some(CheckpointCreation::Direct),
            Self::Rollover { previous } => Some(CheckpointCreation::Replaces(previous)),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCheckpointRequest {
    pub view: CanonicalViewRequest,
    pub operation: CanonicalCheckpointId,
    pub action: CanonicalCheckpointAction,
    pub retention: super::retention::CheckpointRetention,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalCheckpointState {
    Held,
    Released,
    Expired,
}

#[derive(Serialize)]
pub struct CanonicalCheckpointStatus {
    pub operation: CanonicalCheckpointId,
    pub state: CanonicalCheckpointState,
    pub through: CanonicalViewCursor,
    pub retention: super::retention::CheckpointRetention,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

#[derive(Serialize)]
pub(crate) struct CanonicalCheckpointObservation {
    pub(crate) operation: CanonicalCheckpointId,
    pub(crate) state: CanonicalCheckpointState,
    pub(crate) through: CanonicalViewCursor,
    pub(crate) retention: super::retention::CheckpointRetention,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn change_checkpoint(
        &self,
        request: CanonicalCheckpointRequest,
    ) -> StorageResult<Option<CanonicalCheckpointStatus>> {
        if !self.matches((&request.view.serving_region, &request.view.serving_group)) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        if let Some(creation) = request.action.creation() {
            creation.require_distinct(request.operation)?;
        }
        if request.view.after.is_some() || request.view.checkpoint.is_some() {
            return Err(StorageError::validation(
                "Checkpoint operations require an initial live view selection",
            ));
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
            let observed = retry_native_conflicts(port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let request = Arc::clone(&request);
                let journal = Arc::clone(&journal);
                let codec = Arc::clone(&codec);
                let limits = Arc::clone(&limits);
                let port = Arc::clone(&port);
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
                                            .change_canonical_checkpoint(
                                                attempt,
                                                (&request, (&limits, &codec), observed_at),
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
            Ok(observed.map(|observed| CanonicalCheckpointStatus {
                operation: observed.operation,
                state: observed.state,
                through: observed.through,
                retention: observed.retention,
                _source: source,
            }))
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native canonical checkpoint failed"))
        })
    }
}
