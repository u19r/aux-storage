//! A primary decision binds the complete native grant set without releasing
//! custody.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::protocol::CanonicalDigest;
use storage_types::{ReadProtection, Revision, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    constants::PUBLIC_TRIM_COMMIT_SUFFIX,
    native_record::corrupt_record,
    transport::RemoteTrimReceipt,
    trim::{
        commit::TrimCommitCertificate,
        record::{TrimDecision, TrimObservation},
    },
};

/// Exact native commit custody, never constructed from a transport description.
pub(in crate::journal) struct NativeTrimCommit {
    certificate: TrimCommitCertificate,
    local_grant: CanonicalDigest,
}

impl NativeTrimCommit {
    pub(in crate::journal) fn require_grant_turn(
        &self,
        turn: &crate::trim::grant_turn::TrimGrantTurn,
        region: &replication_types::identity::replica::RegionId,
    ) -> StorageResult<()> {
        self.certificate.require_grant_turn(turn, region)
    }

    pub(in crate::journal) fn into_certificate(self) -> TrimCommitCertificate {
        self.certificate
    }

    pub(in crate::journal) fn require_reservation(
        &self,
        proposal: &crate::trim::proposal::PublicTrimProposal,
        grant: CanonicalDigest,
    ) -> StorageResult<()> {
        self.certificate.require_proposal(proposal)?;
        if grant != self.local_grant {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(super) async fn commit_public_trim<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut TrimObservation,
        receipts: &[RemoteTrimReceipt],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local_grant = match record.decision {
                TrimDecision::Committed(_)
                | TrimDecision::Installed(_)
                | TrimDecision::Completed(_) => return Ok(()),
                TrimDecision::Granted(grant) => grant,
                _ => {
                    return Err(StorageError::unsupported(
                        "Trim commit requires complete native grants",
                    ));
                }
            };
            let child = TrimCommitCertificate::try_from_grants(
                &record.proposal,
                &self.region,
                local_grant,
                receipts,
            )?;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            record.decision = TrimDecision::Committed(self.records.content_digest(&child)?);
            let changes = [
                self.records.prepare(
                    self.identity_key(PUBLIC_TRIM_COMMIT_SUFFIX, &record.proposal.operation)?,
                    None,
                    &child,
                )?,
                self.prepare_public_trim_record(record, Some(Revision::INITIAL.checked_next()?))?,
            ];
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_public_trim_commit<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &TrimObservation,
        protection: ReadProtection,
    ) -> StorageResult<Option<NativeTrimCommit>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(PUBLIC_TRIM_COMMIT_SUFFIX, &record.proposal.operation)?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten();
            if matches!(record.decision, TrimDecision::Completed(_)) {
                let completion = self
                    .load_public_trim_completion(attempt, record, protection)
                    .await?;
                let commit = self
                    .decode_trim_commit_child(record, row.as_ref().ok_or_else(corrupt_record)?)?;
                if commit.certificate != completion.certificate {
                    return Err(corrupt_record());
                }
                return Ok(Some(commit));
            }
            self.decode_public_trim_commit(record, row.as_ref())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_public_trim_commit(
        &self,
        record: &TrimObservation,
        row: Option<&infrastructure_provider::record::DurableRecord>,
    ) -> StorageResult<Option<NativeTrimCommit>> {
        let digest = match record.decision {
            TrimDecision::Committed(digest) | TrimDecision::Installed(digest) => digest,
            TrimDecision::Completed(_) => return Err(corrupt_record()),
            _ => {
                return if row.is_none() {
                    Ok(None)
                } else {
                    Err(corrupt_record())
                };
            }
        };
        let commit = self.decode_trim_commit_child(record, row.ok_or_else(corrupt_record)?)?;
        if self.records.content_digest(&commit.certificate)? != digest {
            return Err(corrupt_record());
        }
        Ok(Some(commit))
    }

    fn decode_trim_commit_child(
        &self,
        record: &TrimObservation,
        row: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<NativeTrimCommit> {
        let proposal = &record.proposal;
        let child: TrimCommitCertificate = self.records.decode(row)?;
        if row.key != self.identity_key(PUBLIC_TRIM_COMMIT_SUFFIX, &proposal.operation)?
            || row.revision != Revision::INITIAL
        {
            return Err(corrupt_record());
        }
        child.require_proposal(proposal)?;
        let local_grant = child.grant_for(&self.region)?;
        Ok(NativeTrimCommit {
            certificate: child,
            local_grant,
        })
    }
}
