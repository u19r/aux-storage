use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{
    CancelledControl, CommittedControl, CompletedControl, ControlOutcome, ParticipantEvidence,
};
use crate::journal::JournalStore;

pub enum ControlRequest {
    Prepare(Arc<ControlProposal>),
    Commit {
        operation: Arc<ControlProposal>,
        grants: Vec<ParticipantEvidence>,
    },
    Cancel(Arc<ControlProposal>),
    Install(CommittedControl),
    Finish {
        certificate: CommittedControl,
        installations: Vec<ParticipantEvidence>,
    },
    Release(CompletedControl),
    ReleaseCancelled(CancelledControl),
}

impl ControlRequest {
    pub(crate) fn proposal(&self) -> &ControlProposal {
        match self {
            Self::Prepare(operation) | Self::Cancel(operation) => operation,
            Self::Commit { operation, .. } => operation,
            Self::Install(certificate) | Self::Finish { certificate, .. } => &certificate.proposal,
            Self::Release(certificate) => &certificate.proposal,
            Self::ReleaseCancelled(certificate) => &certificate.proposal,
        }
    }
}

/// Known-aborted attempts retry with fresh native observations. An unknown
/// commit propagates without issuing evidence or inferring cancellation.
pub async fn execute_control<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
    request: Arc<ControlRequest>,
) -> StorageResult<ControlOutcome>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let region = journal.region().clone();
    let readiness = Arc::clone(&journal);
    let record = retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        let codec = Arc::clone(&codec);
        async move {
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Schema,
                    move |attempt, observed_at| {
                        Box::pin(async move {
                            journal
                                .execute_control(attempt, (&request, &codec), observed_at)
                                .await
                        })
                    },
                )
                .await
        }
    })
    .await?;
    if record.decision == super::ControlDecision::Installed
        && matches!(&record.proposal.operation().change, replication_types::configuration::control::ConfigurationChange::TableMetadata { definition, .. } if definition.created_indexes().next().is_some())
    {
        let record = Arc::new(record.clone());
        Arc::clone(&readiness)
            .with_admitted_attempt(
                (
                    port,
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(
                        async move { readiness.require_control_gsi_ready(attempt, &record).await },
                    )
                },
            )
            .await?;
    }
    Ok(ControlOutcome::from_durable(record, region))
}

/// Absence remains unresolved; it is never manufactured into an abort receipt.
pub async fn reconcile_control<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
    operation: Arc<ControlProposal>,
) -> StorageResult<Option<ControlOutcome>>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let region = journal.region().clone();
    let record = Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move {
                    let record = journal.observe_control(attempt, &operation).await?;
                    if let Some(record) = &record {
                        journal
                            .require_control_history(
                                attempt,
                                record,
                                (&codec, storage_types::ReadProtection::Snapshot),
                            )
                            .await?;
                        journal.require_control_gsi_ready(attempt, record).await?;
                    }
                    Ok(record)
                })
            },
        )
        .await?;
    Ok(record.map(|record| ControlOutcome::from_durable(record, region)))
}
