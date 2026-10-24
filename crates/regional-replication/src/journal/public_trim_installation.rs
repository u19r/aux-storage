//! Certified native installation advances visibility while retaining counted
//! custody.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::PUBLIC_TRIM_COMMIT_SUFFIX,
    native_record::corrupt_record,
    trim::{
        commit::TrimCommitCertificate,
        executor::TrimRequest,
        record::{TrimDecision, TrimObservation},
    },
};

impl JournalStore {
    pub(super) async fn install_public_trim<
        A: storage_provider::CatalogAttempt + PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        input: (&mut TrimObservation, Revision),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, revision) = input;
            let certificate = self
                .qualify_trim_installation(attempt, request, record)
                .await?;
            if matches!(
                record.decision,
                TrimDecision::Installed(_) | TrimDecision::Completed(_)
            ) {
                return Ok(());
            }
            let local = &certificate.proposal.participant(&self.region)?.state;
            let observed = self
                .observe_table_state(attempt, local.configuration.table, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            if observed.state() != local {
                return Err(storage_types::StorageError::validation(
                    "Trim table state changed",
                ));
            }
            self.require_public_trim_configuration(attempt, &certificate.proposal)
                .await?;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            let mut changes = self
                .prepare_public_trim_installation(attempt, &certificate)
                .await?;
            if self.region != record.proposal.primary {
                changes.push(self.records.prepare(
                    self.identity_key(PUBLIC_TRIM_COMMIT_SUFFIX, &record.proposal.operation)?,
                    None,
                    &certificate,
                )?);
            }
            record.decision = TrimDecision::Installed(self.records.content_digest(&certificate)?);
            changes.push(self.prepare_public_trim_record(record, Some(revision))?);
            self.records.store(attempt, &changes).await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn qualify_trim_installation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &TrimRequest,
        record: &TrimObservation,
    ) -> StorageResult<TrimCommitCertificate> {
        let certificate = match request {
            TrimRequest::InstallPrimary(_) => self
                .load_public_trim_commit(attempt, record, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?
                .into_certificate(),
            TrimRequest::InstallParticipant(installation) => installation.certificate.clone(),
            _ => return Err(corrupt_record()),
        };
        certificate.require_proposal(&record.proposal)?;
        let digest = self.records.content_digest(&certificate)?;
        match record.decision {
            TrimDecision::Completed(_) => {
                let completion = self
                    .load_public_trim_completion(attempt, record, ReadProtection::Conflict)
                    .await?;
                if completion.certificate != certificate {
                    return Err(corrupt_record());
                }
                let installed = self
                    .load_trim_installation_acknowledgement(
                        attempt,
                        &certificate,
                        ReadProtection::Conflict,
                    )
                    .await?;
                completion.require_local(&installed)?;
            }
            TrimDecision::Installed(actual) if actual == digest => {
                self.require_installed_trim_operation(
                    attempt,
                    &record.proposal,
                    actual,
                    ReadProtection::Conflict,
                )
                .await?;
            }
            TrimDecision::Committed(actual)
                if self.region == record.proposal.primary && actual == digest => {}
            TrimDecision::Granted(grant)
                if self.region != record.proposal.primary
                    && certificate.grant_for(&self.region)? == grant => {}
            _ => return Err(corrupt_record()),
        }
        Ok(certificate)
    }
}
