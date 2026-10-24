use std::{num::NonZeroUsize, sync::Arc};

use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::GsiProjectionMetadataAttempt;
use storage_types::{BoundTableInfo, StorageResult};

use super::record::TargetRecord;
use crate::{
    bootstrap::{VerificationProgress, VerificationStage, copy_scope::BaselineCopyBudget},
    journal::{JournalStore, reconstruction::WinnerAuthority},
    native_record::corrupt_record,
};

impl JournalStore {
    fn projection_history_page_limit(&self, table: &BoundTableInfo) -> StorageResult<NonZeroUsize> {
        let indexes = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|index| !index.index_name.is_ttl_index())
            .count();
        let per_event = indexes
            .checked_mul(2)
            .and_then(|keys| keys.checked_add(1))
            .ok_or_else(corrupt_record)?;
        let available = self.records.limits.max_records().get().saturating_sub(1);
        // Keep one whole original event admissible even when its bounded index
        // fanout exceeds the page target; never split its ownership checkpoint.
        NonZeroUsize::new((available / per_event).max(1)).ok_or_else(corrupt_record)
    }

    pub(super) async fn advance_projection_history<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + GsiProjectionMetadataAttempt,
    >(
        &self,
        attempt: &mut A,
        target: (&TargetRecord, Arc<BoundTableInfo>),
        input: (&mut VerificationProgress, &CanonicalJsonCodec),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (root, table) = target;
            match input.0.stage.clone() {
                VerificationStage::ProjectionMetadataBaselines(after) => {
                    let budget = BaselineCopyBudget {
                        records: NonZeroUsize::MIN,
                        bytes: self.records.limits.max_bytes(),
                    };
                    let page = self
                        .load_baseline_page(
                            attempt,
                            (root.state.configuration.lineage, after),
                            (input.1, budget),
                        )
                        .await?;
                    for baseline in page.authorities {
                        root.require_baseline_scope(&baseline)?;
                        self.register_projection_authority(
                            attempt,
                            (root.request.operation.id, &table),
                            (&WinnerAuthority::Baseline(Box::new(baseline)), input.1),
                        )
                        .await?;
                    }
                    input.0.stage = page
                        .continuation
                        .map_or(VerificationStage::ProjectionMetadataEvents(None), |after| {
                            VerificationStage::ProjectionMetadataBaselines(Some(after))
                        });
                }
                VerificationStage::ProjectionMetadataEvents(after) => {
                    let page = self
                        .load_retained_links(
                            attempt,
                            root.state.configuration.lineage,
                            (after, self.projection_history_page_limit(&table)?),
                        )
                        .await?;
                    for retained in page.links {
                        self.require_reconstruction_link(attempt, root, &retained)
                            .await?;
                        let accepted = self
                            .load_acceptance(attempt, &retained.link.event, input.1)
                            .await?
                            .ok_or_else(corrupt_record)?;
                        if accepted.receipt.digest != retained.link.digest {
                            return Err(corrupt_record());
                        }
                        retained
                            .link
                            .require_event(&accepted.event, &retained.origin)
                            .map_err(|_| corrupt_record())?;
                        if let Some(authority) = WinnerAuthority::from_event(accepted.event) {
                            self.register_projection_authority(
                                attempt,
                                (root.request.operation.id, &table),
                                (&authority, input.1),
                            )
                            .await?;
                        }
                    }
                    input.0.stage = match page.continuation {
                        Some(after) => VerificationStage::ProjectionMetadataEvents(Some(after)),
                        None if table
                            .global_secondary_indexes
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .all(|index| index.index_name.is_ttl_index()) =>
                        {
                            VerificationStage::MaintenanceAdmission
                        }
                        None => VerificationStage::ProjectionTombstones {
                            index: 0,
                            after: None,
                        },
                    };
                }
                _ => return Err(corrupt_record()),
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
