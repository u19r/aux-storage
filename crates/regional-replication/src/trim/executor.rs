//! Settled native reserve/observe/cancel operations preserve unknown outcomes.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageResult};

use super::{proposal::PublicTrimProposal, record::TrimOutcome};
use crate::journal::JournalStore;

pub enum TrimRequest {
    AuthorizeGrant(super::grant_turn::PrimaryTrimGrantTurn),
    Complete {
        proposal: Arc<PublicTrimProposal>,
        receipts: Vec<crate::transport::RemoteTrimReceipt>,
    },
    Finish(super::completion::PrimaryTrimCompletion),
    ReleaseCompletion {
        proposal: Arc<PublicTrimProposal>,
        receipts: Vec<crate::transport::RemoteTrimReceipt>,
    },
    InstallPrimary(Arc<PublicTrimProposal>),
    InstallParticipant(super::installation::PrimaryTrimInstallation),
    Commit {
        proposal: Arc<PublicTrimProposal>,
        receipts: Vec<crate::transport::RemoteTrimReceipt>,
    },
    Prepare(Arc<PublicTrimProposal>),
    Advance(Arc<PublicTrimProposal>),
    Observe(Arc<PublicTrimProposal>),
    Cancel(Arc<PublicTrimProposal>),
    ReleaseParticipantCancellation(super::record::PrimaryTrimCancellation),
    RecordPreparation {
        proposal: Arc<PublicTrimProposal>,
        receipts: Vec<crate::transport::RemoteTrimReceipt>,
    },
    ReleaseCancellation {
        proposal: Arc<PublicTrimProposal>,
        receipts: Vec<crate::transport::RemoteTrimReceipt>,
    },
}

impl TrimRequest {
    fn can_reconcile(&self, outcome: &TrimOutcome) -> bool {
        let Some(record) = &outcome.observation else {
            return false;
        };
        match self {
            Self::AuthorizeGrant(authorization) => {
                record.decision == super::record::TrimDecision::Cancelled
                    || outcome.grant_turn.as_ref() == Some(&authorization.turn)
                    || outcome.certificate.as_ref().is_some_and(|certificate| {
                        authorization.turn.target().is_ok_and(|region| {
                            certificate
                                .require_grant_turn(&authorization.turn, region)
                                .is_ok()
                        })
                    })
            }
            Self::Complete { .. } => {
                matches!(record.decision, super::record::TrimDecision::Completed(_))
            }
            Self::Finish(finish) => {
                matches!(record.decision, super::record::TrimDecision::Completed(_))
                    && outcome.completion.as_ref() == Some(&finish.completion)
            }
            Self::ReleaseCompletion { .. } => {
                matches!(record.decision, super::record::TrimDecision::Completed(_))
                    && outcome.completion_released
            }
            Self::InstallPrimary(_) => {
                matches!(
                    record.decision,
                    super::record::TrimDecision::Installed(_)
                        | super::record::TrimDecision::Completed(_)
                ) && outcome.installation.is_some()
            }
            Self::InstallParticipant(installation) => {
                matches!(
                    record.decision,
                    super::record::TrimDecision::Installed(_)
                        | super::record::TrimDecision::Completed(_)
                ) && outcome.installation.is_some()
                    && outcome.certificate.as_ref() == Some(&installation.certificate)
            }
            Self::Commit { .. } => {
                matches!(
                    record.decision,
                    super::record::TrimDecision::Committed(_)
                        | super::record::TrimDecision::Installed(_)
                        | super::record::TrimDecision::Completed(_)
                ) && outcome.certificate.is_some()
            }
            Self::RecordPreparation { .. } => {
                outcome.participants_prepared
                    || record.decision == super::record::TrimDecision::Cancelled
                    || (matches!(record.decision, super::record::TrimDecision::Completed(_))
                        && outcome.completion.is_some())
            }
            Self::Cancel(_) | Self::ReleaseParticipantCancellation(_) => {
                record.decision == super::record::TrimDecision::Cancelled
            }
            Self::ReleaseCancellation { .. } => {
                record.decision == super::record::TrimDecision::Cancelled
                    && outcome.cancellation_released
            }
            Self::Prepare(_) | Self::Advance(_) | Self::Observe(_) => true,
        }
    }

    pub(crate) fn require_region(
        &self,
        region: &replication_types::identity::replica::RegionId,
    ) -> StorageResult<()> {
        let proposal = self.proposal();
        proposal.participant(region)?;
        match self {
            Self::AuthorizeGrant(authorization) if authorization.turn.target()? != region => {
                Err(crate::native_record::corrupt_record())
            }
            Self::Cancel(_)
            | Self::ReleaseCancellation { .. }
            | Self::RecordPreparation { .. }
            | Self::Commit { .. }
            | Self::InstallPrimary(_)
            | Self::Complete { .. }
            | Self::ReleaseCompletion { .. }
                if region != &proposal.primary =>
            {
                Err(storage_types::StorageError::validation(
                    "Trim coordination requires its primary",
                ))
            }
            Self::ReleaseParticipantCancellation(_)
            | Self::InstallParticipant(_)
            | Self::Finish(_)
                if region == &proposal.primary =>
            {
                Err(crate::native_record::corrupt_record())
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn proposal(&self) -> &PublicTrimProposal {
        match self {
            Self::AuthorizeGrant(authorization) => &authorization.turn.proposal,
            Self::Finish(finish) => &finish.completion.certificate.proposal,
            Self::Complete { proposal, .. } | Self::ReleaseCompletion { proposal, .. } => proposal,
            Self::InstallParticipant(installation) => &installation.certificate.proposal,
            Self::InstallPrimary(proposal)
            | Self::Prepare(proposal)
            | Self::Advance(proposal)
            | Self::Observe(proposal)
            | Self::Cancel(proposal)
            | Self::ReleaseCancellation { proposal, .. }
            | Self::RecordPreparation { proposal, .. }
            | Self::Commit { proposal, .. } => proposal,
            Self::ReleaseParticipantCancellation(certificate) => &certificate.proposal,
        }
    }
}

pub async fn execute_trim<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    request: Arc<TrimRequest>,
    encoding: (Arc<WorkLimits>, Arc<CanonicalJsonCodec>),
) -> StorageResult<TrimOutcome>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    settle_trim_execution(port, journal, (request, None), encoding).await
}

pub(crate) async fn execute_qualified_trim<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (
        Arc<PublicTrimProposal>,
        super::time::age::QualifiedPrefixAge,
    ),
    encoding: (Arc<WorkLimits>, Arc<CanonicalJsonCodec>),
) -> StorageResult<TrimOutcome>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let request = Arc::new(TrimRequest::Advance(input.0));
    settle_trim_execution(port, journal, (request, Some(Arc::new(input.1))), encoding).await
}

async fn settle_trim_execution<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (
        Arc<TrimRequest>,
        Option<Arc<super::time::age::QualifiedPrefixAge>>,
    ),
    encoding: (Arc<WorkLimits>, Arc<CanonicalJsonCodec>),
) -> StorageResult<TrimOutcome>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let (request, age) = input;
    let qualified = age.is_some();
    request.proposal().validate()?;
    match execute_trim_attempt(
        port,
        Arc::clone(&journal),
        (Arc::clone(&request), age),
        encoding.clone(),
    )
    .await
    {
        Err(error)
            if error.is_commit_unknown()
                && !matches!(request.as_ref(), TrimRequest::Observe(_)) =>
        {
            let observe = Arc::new(TrimRequest::Observe(Arc::new(request.proposal().clone())));
            match execute_trim_attempt(port, journal, (observe, None), encoding).await {
                Ok(outcome)
                    if request.can_reconcile(&outcome)
                        && (!qualified
                            || outcome.observation.as_ref().is_some_and(|record| {
                                matches!(
                                    record.decision,
                                    super::record::TrimDecision::Granted(_)
                                        | super::record::TrimDecision::Committed(_)
                                        | super::record::TrimDecision::Installed(_)
                                        | super::record::TrimDecision::Completed(_)
                                        | super::record::TrimDecision::Cancelled
                                )
                            })) =>
                {
                    Ok(outcome)
                }
                _ => Err(error),
            }
        }
        result => result,
    }
}

async fn execute_trim_attempt<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (
        Arc<TrimRequest>,
        Option<Arc<super::time::age::QualifiedPrefixAge>>,
    ),
    encoding: (Arc<WorkLimits>, Arc<CanonicalJsonCodec>),
) -> StorageResult<TrimOutcome>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let (request, age) = input;
    retry_native_conflicts(port, DYNAMO_WRITE_RETRY, || {
        let journal = Arc::clone(&journal);
        let request = Arc::clone(&request);
        let age = age.as_ref().map(Arc::clone);
        let encoding = (Arc::clone(&encoding.0), Arc::clone(&encoding.1));
        async move {
            let mode = if matches!(request.as_ref(), TrimRequest::Observe(_)) {
                AttemptMode::ReadOnly
            } else {
                AttemptMode::ReadWrite
            };
            Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port,
                        storage_provider::native_admission::NativeWork::Recovery,
                    ),
                    crate::journal::NativeAttempt::Items(mode),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .execute_trim_operation(
                                    attempt,
                                    (&request, age.as_deref()),
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
