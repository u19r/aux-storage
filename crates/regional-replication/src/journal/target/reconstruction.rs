use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_types::StorageResult;

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage, copy_scope::BaselineCopyBudget},
    journal::{JournalStore, reconstruction::WinnerAuthority},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn advance_target_reconstruction<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &TargetRecord,
        input: (&mut VerificationProgress, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (progress, codec) = input;
            match &progress.stage {
                VerificationStage::Baselines(after) => {
                    let budget = BaselineCopyBudget {
                        records: std::num::NonZeroUsize::new(
                            self.records
                                .limits
                                .max_records()
                                .get()
                                .saturating_sub(1)
                                .max(1),
                        )
                        .ok_or_else(corrupt_record)?,
                        bytes: self.records.limits.max_bytes(),
                    };
                    let page = self
                        .load_baseline_page(
                            attempt,
                            (root.state.configuration.lineage, after.clone()),
                            (codec, budget),
                        )
                        .await?;
                    for baseline in page.authorities {
                        root.require_baseline_scope(&baseline)?;
                        progress.history = replication_types::protocol::CanonicalDigest::of_json(
                            &(progress.history, baseline.provenance()),
                        )
                        .map_err(|_| corrupt_record())?;
                        let authority = WinnerAuthority::Baseline(Box::new(baseline));
                        self.fold_expected_winner(
                            attempt,
                            root.request.operation.id,
                            (authority.key()?, authority.provenance()?),
                        )
                        .await?;
                    }
                    progress.stage = page
                        .continuation
                        .map_or(VerificationStage::Events(None), |after| {
                            VerificationStage::Baselines(Some(after))
                        });
                }
                VerificationStage::Events(after) => {
                    let page = self
                        .load_retained_links(
                            attempt,
                            root.state.configuration.lineage,
                            (after.clone(), self.records.limits.max_records()),
                        )
                        .await?;
                    for retained in page.links {
                        self.require_reconstruction_link(attempt, root, &retained)
                            .await?;
                        let accepted = self
                            .load_acceptance(attempt, &retained.link.event, codec)
                            .await?
                            .ok_or_else(corrupt_record)?;
                        if accepted.receipt.digest != retained.link.digest {
                            return Err(corrupt_record());
                        }
                        retained
                            .link
                            .require_event(&accepted.event, &retained.origin)
                            .map_err(|_| corrupt_record())?;
                        progress.history = replication_types::protocol::CanonicalDigest::of_json(
                            &(progress.history, &retained),
                        )
                        .map_err(|_| corrupt_record())?;
                        if let Some(authority) = WinnerAuthority::from_event(accepted.event) {
                            self.fold_expected_winner(
                                attempt,
                                root.request.operation.id,
                                (authority.key()?, authority.provenance()?),
                            )
                            .await?;
                        }
                    }
                    progress.stage = page
                        .continuation
                        .map_or(VerificationStage::Expected(None), |after| {
                            VerificationStage::Events(Some(after))
                        });
                }
                _ => return Err(corrupt_record()),
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
