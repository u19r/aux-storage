use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::{
    baseline::BaselineItem,
    content::SourceImagesRef,
    identity::{CanonicalItemKey, VersionIdentity},
    order::WinnerProvenance,
};
use storage_types::{ItemProvenance, StorageResult};

use super::{super::TableStateObservation, mutation::MutationView};
use crate::{
    journal::{
        JournalStore,
        reconstruction::{RetainedAuthorityRequest, WinnerAuthority},
    },
    native_record::corrupt_record,
};

/// The verified authority stays alive until the next original has borrowed its
/// exact image bytes. Materialized provider images cannot replace this owner.
pub(super) struct OriginPredecessor {
    pub(super) authority: WinnerAuthority,
    pub(super) provenance: WinnerProvenance,
}

impl OriginPredecessor {
    fn try_new(authority: WinnerAuthority, images: SourceImagesRef<'_>) -> StorageResult<Self> {
        authority.require_materialized_images((images.old, images.old_indexers))?;
        let provenance = authority.provenance()?;
        Ok(Self {
            authority,
            provenance,
        })
    }

    fn try_from_baseline(
        authority: WinnerAuthority,
        table: &TableStateObservation,
        images: SourceImagesRef<'_>,
    ) -> StorageResult<Self> {
        let WinnerAuthority::Baseline(baseline) = authority else {
            return Err(corrupt_record());
        };
        if baseline.item().source_table != table.state().configuration.table {
            return Err(corrupt_record());
        }
        Self::try_new(WinnerAuthority::Baseline(baseline), images)
    }
}

enum OriginPredecessorPlan {
    Known(Box<WinnerProvenance>),
    ExistingBaseline(VersionIdentity),
    Missing,
    Capture,
}

impl JournalStore {
    pub(super) async fn observe_origin_predecessor<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&TableStateObservation, &MutationView<'_>, &CanonicalItemKey),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<OriginPredecessor>> {
        self.observe_origin_predecessors(attempt, &[input], codec)
            .await?
            .pop()
            .ok_or_else(corrupt_record)
    }

    pub(super) async fn observe_origin_predecessors<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        inputs: &[(&TableStateObservation, &MutationView<'_>, &CanonicalItemKey)],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Vec<Option<OriginPredecessor>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let plans = inputs
                .iter()
                .map(|input| self.plan_origin_predecessor(*input, codec))
                .collect::<StorageResult<Vec<_>>>()?;
            let requests = plans
                .iter()
                .zip(inputs)
                .filter_map(|(plan, input)| match plan {
                    OriginPredecessorPlan::Known(winner) => {
                        Some(RetainedAuthorityRequest::Winner(input.1.key, winner))
                    }
                    OriginPredecessorPlan::ExistingBaseline(identity) => {
                        Some(RetainedAuthorityRequest::Baseline(identity))
                    }
                    OriginPredecessorPlan::Missing | OriginPredecessorPlan::Capture => None,
                })
                .collect::<Vec<_>>();
            let mut authorities = self
                .load_retained_authorities(settlement.as_mut(), &requests, codec)
                .await?
                .into_iter();
            let mut observed = Vec::with_capacity(inputs.len());
            for (plan, input) in plans.into_iter().zip(inputs) {
                let (table, view, key) = *input;
                let predecessor = match plan {
                    OriginPredecessorPlan::Known(_) => Some(OriginPredecessor::try_new(
                        authorities.next().flatten().ok_or_else(corrupt_record)?,
                        view.images,
                    )?),
                    OriginPredecessorPlan::ExistingBaseline(_) => {
                        Some(OriginPredecessor::try_from_baseline(
                            authorities.next().flatten().ok_or_else(corrupt_record)?,
                            table,
                            view.images,
                        )?)
                    }
                    OriginPredecessorPlan::Missing => None,
                    OriginPredecessorPlan::Capture => Some(
                        self.capture_origin_predecessor(
                            settlement.as_mut(),
                            (table, view, key),
                            codec,
                        )
                        .await?,
                    ),
                };
                observed.push(predecessor);
            }
            if authorities.next().is_some() {
                return Err(corrupt_record());
            }
            Ok(observed)
        }
        .await;
        settlement.complete(result)
    }

    fn plan_origin_predecessor(
        &self,
        input: (&TableStateObservation, &MutationView<'_>, &CanonicalItemKey),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<OriginPredecessorPlan> {
        let (table, view, key) = input;
        let configuration = &table.state().configuration;
        if let Some(stored) = view
            .provenance
            .filter(|stored| stored.as_bytes() != ItemProvenance::LOCAL_BYTES)
        {
            let winner = codec
                .decode_winner_provenance(stored.as_bytes(), key)
                .map_err(|_| corrupt_record())?;
            if winner.lineage == configuration.lineage {
                return Ok(OriginPredecessorPlan::Known(Box::new(winner)));
            }
        }
        if view.provenance.is_none() && view.images.old.is_none() {
            return Ok(OriginPredecessorPlan::Missing);
        }
        if table.state().phase
            != replication_types::configuration::installed::RegionalTablePhase::Capturing
        {
            return Ok(OriginPredecessorPlan::ExistingBaseline(
                VersionIdentity::Baseline {
                    lineage: configuration.lineage,
                    key: key.clone(),
                },
            ));
        }
        Ok(OriginPredecessorPlan::Capture)
    }

    async fn capture_origin_predecessor<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&TableStateObservation, &MutationView<'_>, &CanonicalItemKey),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<OriginPredecessor> {
        let (table, view, key) = input;
        let configuration = &table.state().configuration;
        let exact = self
            .load_baseline_image(
                attempt,
                (view.physical.history_id, view.images.old, view.source),
                codec,
            )
            .await?;
        let baseline = BaselineItem {
            source_table: configuration.table,
            lineage: configuration.lineage,
            schema: configuration.schema,
            key: view.key.clone(),
            canonical_key: key.clone(),
            images: SourceImagesRef {
                old: None,
                new: exact.as_ref(),
                old_indexers: None,
                new_indexers: view.images.old_indexers,
            },
        };
        let authority = WinnerAuthority::Baseline(Box::new(
            self.store_baseline(attempt, (&baseline, view.physical.history_id), codec)
                .await?,
        ));
        OriginPredecessor::try_new(authority, view.images)
    }
}
