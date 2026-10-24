use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::{DynamoAttempt, ItemTombstoneScanAttempt, PrimaryStateScanAttempt};
use storage_types::{ReadProtection, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{
        HiddenTargetRequest, HiddenVerificationStatus, TargetBootstrapDefinition,
        VerificationProgress, VerificationStage,
    },
    constants::TARGET_BOOTSTRAP_SUFFIX,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn advance_hidden_verification<
        A: PhysicalRecordAttempt
            + DynamoAttempt
            + PrimaryStateScanAttempt
            + ItemTombstoneScanAttempt
            + storage_provider::GsiProjectionScanAttempt
            + storage_provider::TtlProjectionScanAttempt
            + storage_provider::GsiProjectionMetadataAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        input: (
            &TargetBootstrapDefinition,
            &CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> StorageResult<HiddenVerificationStatus> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, mut root) = self
                .load_hidden_target(attempt, request, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            self.require_target_bootstrap(&root, input.0)?;
            if self
                .reconcile_verified_activation(
                    attempt,
                    &root,
                    (input.0.proposal(), input.0.content()),
                    ReadProtection::Shared,
                )
                .await?
            {
                return Ok(HiddenVerificationStatus::Verified);
            }
            let table = self
                .require_hidden_table(attempt, &root, ReadProtection::Shared)
                .await?;
            if !root.baselines_complete {
                return Err(corrupt_record());
            }
            let mut progress = root
                .verification
                .take()
                .map_or_else(|| input.0.verification_progress(), Ok)?;
            match &progress.stage {
                VerificationStage::AdoptedCuts(after) => {
                    self.verify_adopted_target_cuts(attempt, &root, (after.clone(), &mut progress))
                        .await?
                }
                VerificationStage::RetainedBaselines { .. } => {
                    self.verify_target_retained_baselines(
                        attempt,
                        &root,
                        &mut progress,
                        (input.1, input.2),
                    )
                    .await?
                }
                VerificationStage::RetainedCuts(after) => {
                    self.verify_target_retained_history(
                        attempt,
                        &root,
                        (*after, &mut progress),
                        (input.1, input.2),
                    )
                    .await?
                }
                VerificationStage::Cuts(after) => {
                    self.verify_target_vector(attempt, &root, (*after, &mut progress))
                        .await?
                }
                VerificationStage::Baselines(_) | VerificationStage::Events(_) => {
                    self.advance_target_reconstruction(attempt, &root, (&mut progress, input.1))
                        .await?
                }
                VerificationStage::Expected(_)
                | VerificationStage::Live(_)
                | VerificationStage::Deleted(_) => {
                    self.advance_primary_verification(
                        attempt,
                        (&root, table),
                        (&mut progress, input.1),
                    )
                    .await?
                }
                VerificationStage::ProjectionExpected(_)
                | VerificationStage::ProjectionRows { .. } => {
                    self.advance_gsi_verification(attempt, (&root, table), (&mut progress, input.1))
                        .await?
                }
                VerificationStage::TtlRows { .. } => {
                    self.advance_ttl_verification(attempt, (&root, table), (&mut progress, input.1))
                        .await?
                }
                VerificationStage::ProjectionMetadataBaselines(_)
                | VerificationStage::ProjectionMetadataEvents(_) => {
                    self.advance_projection_history(
                        attempt,
                        (&root, table),
                        (&mut progress, input.1),
                    )
                    .await?
                }
                VerificationStage::ProjectionTombstones { .. } => {
                    self.advance_projection_metadata(
                        attempt,
                        (&root, table),
                        (&mut progress, input.1),
                    )
                    .await?
                }
                VerificationStage::MaintenanceAdmission => {
                    self.complete_hidden_verification(attempt, (&root, &table), &mut progress)
                        .await?
                }
                VerificationStage::Verified(_) => {
                    self.require_verification_evidence(&root, &progress)?;
                    return Ok(HiddenVerificationStatus::Verified);
                }
            }
            let status = progress.stage.status();
            root.verification = Some(progress);
            let key = self.identity_key(TARGET_BOOTSTRAP_SUFFIX, &request.operation.id)?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(key, Some(row.revision), &root)?],
                )
                .await?;
            Ok(status)
        }
        .await;
        settlement.complete(result)
    }

    async fn verify_target_vector<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (u32, &mut VerificationProgress),
    ) -> StorageResult<()> {
        let (cuts, next) = self
            .load_verified_target_cuts(attempt, root, input.0)
            .await?;
        for cut in cuts {
            input.1.vector = cut.fold_vector(input.1.vector)?;
        }
        if next.is_none() && Some(input.1.vector) != root.source_vector {
            return Err(corrupt_record());
        }
        input.1.stage = next.map_or(VerificationStage::Baselines(None), VerificationStage::Cuts);
        Ok(())
    }
}

impl JournalStore {
    pub(super) async fn load_verification_ttl<A: storage_provider::DescribeTimeToLiveAttempt>(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        table: &storage_types::BoundTableInfo,
    ) -> StorageResult<Option<storage_common::TtlConfigRecord>> {
        let policy = attempt.read_ttl_config(table).await?;
        let frozen = root.request.schema.ttl.as_ref();
        if policy.as_ref().is_some_and(|policy| {
            policy.policy_generation != storage_common::TtlPolicyGeneration::one()
        }) || frozen.map(|policy| (&policy.attribute, &policy.status))
            != policy
                .as_ref()
                .map(|policy| (&policy.attribute_name, &policy.status))
        {
            return Err(corrupt_record());
        }
        Ok(policy)
    }
}
