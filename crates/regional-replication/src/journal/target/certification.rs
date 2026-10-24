use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::RegionalTablePhase;
use storage_provider::DynamoAttempt;
use storage_types::{BoundTableInfo, ReadProtection, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{BootstrapVerification, VerificationProgress, VerificationStage},
    constants::CONTROL_SLOT_SUFFIX,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn require_reservation_receipt<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result =
            async {
                if root.cancelled {
                    return Err(corrupt_record());
                }
                if root.verification.as_ref().is_some_and(|progress| {
                    matches!(progress.stage, VerificationStage::Verified(_))
                }) {
                    let key = self.identity_key(
                        crate::constants::CONTROL_OPERATION_SUFFIX,
                        &root.request.operation.id,
                    )?;
                    let rows = self.records.load(attempt, &[key], protection).await?;
                    if let Some(row) = &rows[0] {
                        let control = self.decode_control_record(row)?;
                        let definition = self.qualify_target_bootstrap(root, &control.proposal)?;
                        if self
                            .reconcile_verified_activation(
                                attempt,
                                root,
                                (definition.proposal(), definition.content()),
                                protection,
                            )
                            .await?
                        {
                            return Ok(());
                        }
                    }
                }
                self.require_hidden_table(attempt, root, protection).await?;
                Ok(())
            }
            .await;
        settlement.complete(result)
    }

    pub(super) async fn reconcile_verified_activation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        definition: (
            &replication_types::configuration::proposal::ControlProposal,
            replication_types::protocol::CanonicalDigest,
        ),
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(progress) = &root.verification else {
                return Ok(false);
            };
            if !matches!(progress.stage, VerificationStage::Verified(_)) {
                return Ok(false);
            }
            self.require_verification_evidence(root, progress)?;
            let Some(row) = self.load_control(attempt, definition.0, protection).await? else {
                return Ok(false);
            };
            let control = self.decode_control(&row, definition.0)?;
            if !matches!(
                control.decision,
                crate::control::ControlDecision::Installed
                    | crate::control::ControlDecision::Completed
            ) {
                return Ok(false);
            }
            if control.bootstrap != Some(definition.1) {
                return Err(corrupt_record());
            }
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn complete_hidden_verification<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        target: (&TargetRecord, &BoundTableInfo),
        progress: &mut VerificationProgress,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(progress.stage, VerificationStage::MaintenanceAdmission)
                || target.0.state.phase != RegionalTablePhase::Joining
            {
                return Err(corrupt_record());
            }
            let key =
                self.identity_key(CONTROL_SLOT_SUFFIX, &target.0.state.configuration.table)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let operation = self.decode_control_slot(
                rows[0].as_ref().ok_or_else(corrupt_record)?,
                target.0.state.configuration.table,
            )?;
            if operation != ControlSlotOwner::Configuration(target.0.request.operation.id)
                || self.admits_expiration(attempt, target.1).await?
                || self.admits_gsi_build(attempt, target.1).await?
            {
                return Err(corrupt_record());
            }
            self.load_verification_ttl(attempt, target.0, target.1)
                .await?;
            self.require_table_gsi_ready(attempt, target.1).await?;
            if Some(progress.vector) != target.0.source_vector {
                return Err(corrupt_record());
            }
            if let Some(retained) = &target.0.reenable {
                self.require_target_retained_history(target.0, &retained.definition)?;
                if progress.cut_count != retained.definition.cuts() {
                    return Err(corrupt_record());
                }
            }
            let binding = (
                target.0.content,
                target.0.definition.ok_or_else(corrupt_record)?,
                target.0.state.physical,
            );
            progress.stage =
                VerificationStage::Verified(BootstrapVerification::complete(binding, progress)?);
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn require_verification_evidence(
        &self,
        root: &TargetRecord,
        progress: &VerificationProgress,
    ) -> StorageResult<()> {
        let VerificationStage::Verified(evidence) = &progress.stage else {
            return Err(corrupt_record());
        };
        if Some(progress.vector) != root.source_vector {
            return Err(corrupt_record());
        }
        if let Some(adoption) = &root.adoption {
            self.require_target_adoption(root, &adoption.definition)?;
            if progress.cut_count != adoption.declared_cuts {
                return Err(corrupt_record());
            }
        }
        if let Some(retained) = &root.reenable {
            self.require_target_retained_history(root, &retained.definition)?;
            if progress.cut_count != retained.definition.cuts() {
                return Err(corrupt_record());
            }
        }
        evidence.require_binding(
            (
                root.content,
                root.definition.ok_or_else(corrupt_record)?,
                root.state.physical,
            ),
            progress,
        )
    }
}

impl JournalStore {
    pub(in crate::journal) async fn require_verified_control_target<
        A: PhysicalRecordAttempt
            + storage_provider::DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
    ) -> StorageResult<(
        replication_types::configuration::installed::InstalledTableState,
        replication_types::protocol::CanonicalDigest,
    )> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(
                crate::constants::TARGET_BOOTSTRAP_SUFFIX,
                &proposal.operation().id,
            )?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Shared)
                .await?;
            let root: TargetRecord = self
                .records
                .decode(rows[0].as_ref().ok_or_else(corrupt_record)?)?;
            let definition = self.qualify_target_bootstrap(&root, proposal)?;
            let mut expected = proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .clone();
            expected.phase = RegionalTablePhase::Joining;
            if root.state != expected || !root.baselines_complete {
                return Err(corrupt_record());
            }
            let table = self
                .require_hidden_table(attempt, &root, ReadProtection::Shared)
                .await?;
            self.load_verification_ttl(attempt, &root, &table).await?;
            self.require_table_gsi_ready(attempt, &table).await?;
            self.require_verification_evidence(
                &root,
                root.verification.as_ref().ok_or_else(corrupt_record)?,
            )?;
            Ok((expected, definition.content()))
        }
        .await;
        settlement.complete(result)
    }
}
