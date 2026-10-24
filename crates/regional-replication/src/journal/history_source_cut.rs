//! Native source authority and complete prefix evidence for cut-bound reads.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::frame::SourceFrame,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    event::interval::ClosedSourceInterval, identity::origin::OriginPosition,
    protocol::CanonicalDigest,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::JournalStore;
use crate::{
    history::{
        read::{CanonicalHistoryCut, CanonicalHistoryRequest},
        source_cut::CanonicalSourceCut,
        source_selection::{AvailableSourcePrefix, CanonicalSourceSelection},
        table_cut::{CanonicalTableCutAction, CanonicalTableCutRequest},
    },
    source_frame::{SourceFrameRequest, load_source_frame},
};

impl JournalStore {
    pub(crate) async fn observe_source_cut<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            CanonicalSourceSelection,
            crate::transport::SourceReservation,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<crate::history::source_cut::CanonicalSourceCutObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (selection, source) = input;
            let available = self.qualify_history_selection(attempt, &selection).await?;
            let through = available.through();
            let terminal_manifest = self
                .load_history_terminal_manifest(attempt, (&selection, through), encoding)
                .await?;
            let cut = CanonicalSourceCut {
                trim: available.bounds().trim,
                after: available.bounds().after,
                lineage: selection.lineage,
                membership: selection.membership,
                source: selection.source,
                origin: selection.origin,
                through,
                terminal_manifest,
            };
            cut.require_source()?;
            let observation = crate::history::source_cut::CanonicalSourceCutObservation {
                cut,
                _source: source,
            };
            crate::history::response_budget::encoded_length(
                &observation,
                encoding.0.frame_bytes.get(),
            )?;
            Ok(observation)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn qualify_history_selection<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        selection: &CanonicalSourceSelection,
    ) -> StorageResult<AvailableSourcePrefix> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let stamp = attempt
                .read_catalog_stamp(&selection.table)
                .await?
                .ok_or_else(|| StorageError::table_not_found(&selection.table))?;
            let physical = attempt.read_table_at_stamp(&selection.table, stamp).await?;
            let observed = self
                .observe_history_table(attempt, &physical, ReadProtection::Snapshot)
                .await?;
            let state = observed.state();
            if state.configuration.lineage != selection.lineage {
                let cut = self
                    .load_installed_retained_scope(
                        attempt,
                        state,
                        selection,
                        ReadProtection::Snapshot,
                    )
                    .await?;
                let authority = self
                    .load_public_trim_head(
                        attempt,
                        (state, selection.lineage),
                        ReadProtection::Snapshot,
                    )
                    .await?;
                return Ok(AvailableSourcePrefix::Retired(
                    authority.retired_source_bounds(selection, cut.through)?,
                ));
            }
            if state.membership != selection.membership {
                return Err(StorageError::validation(
                    "History table revision changed; restart discovery",
                ));
            }
            self.load_authorized_source_prefix(
                attempt,
                (state, &observed.authority),
                selection,
                ReadProtection::Snapshot,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_history_terminal_manifest<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (&CanonicalSourceSelection, OriginPosition),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<Option<CanonicalDigest>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (selection, through) = selection;
            let through_value = u64::from(through);
            if through_value == 0 {
                return Ok(None);
            }
            let range = ClosedSourceInterval::try_from(((through_value - 1).into(), through))
                .map_err(|_| crate::native_record::corrupt_record())?;
            let frame = self
                .load_history_frame(attempt, (selection, range), encoding)
                .await?;
            Ok(Some(frame.manifest().digest))
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_bounded_canonical_history<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &CanonicalHistoryRequest,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<crate::history::read::CanonicalHistoryObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let range = self.bound_source_range(request.range, std::num::NonZeroUsize::MIN)?;
            let cut = self
                .qualify_history_cut(attempt, request, encoding, observed_at)
                .await?;
            let available = match (&request.cut, &cut) {
                (Some(CanonicalHistoryCut::Checkpoint(_)), Some(cut)) => {
                    AvailableSourcePrefix::Held(
                        crate::history::source_selection::SourceHistoryBounds::try_new(
                            cut.trim,
                            cut.after,
                            cut.through,
                        )?,
                    )
                }
                _ => {
                    self.qualify_history_selection(attempt, &request.selection)
                        .await?
                }
            };
            available.require_range(request.range, range.through())?;
            if let Some(cut) = &cut {
                cut.require_selection(&request.selection)?;
                available.bounds().require_cut(cut)?;
                available.require_through(cut.through)?;
                if request.range.through() > cut.through {
                    return Err(StorageError::validation(
                        "History interval exceeds the supplied source cut",
                    ));
                }
                if range.through() != cut.through {
                    let digest = self
                        .load_history_terminal_manifest(
                            attempt,
                            (&request.selection, cut.through),
                            encoding,
                        )
                        .await?;
                    require_cut_manifest(digest, cut.terminal_manifest)?;
                }
            }
            let frame = self
                .load_history_frame(attempt, (&request.selection, range), encoding)
                .await?;
            if let Some(cut) = &cut
                && range.through() == cut.through
            {
                require_cut_manifest(Some(frame.manifest().digest), cut.terminal_manifest)?;
            }
            let (manifest, events) = frame.into_parts();
            let originals = events
                .into_iter()
                .map(|event| {
                    String::from_utf8(event.as_ref().bytes.as_ref().to_vec())
                        .map_err(|_| crate::native_record::corrupt_record())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let observation = crate::history::read::CanonicalHistoryObservation {
                manifest,
                originals,
            };
            crate::history::response_budget::encoded_length(
                &observation,
                encoding.0.frame_bytes.get(),
            )?;
            Ok(observation)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn qualify_history_cut<
        'cut,
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &'cut CanonicalHistoryRequest,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
        observed_at: storage_types::TimestampMillis,
    ) -> StorageResult<Option<std::borrow::Cow<'cut, CanonicalSourceCut>>> {
        match &request.cut {
            None => Ok(None),
            Some(CanonicalHistoryCut::Source(cut)) => Ok(Some(std::borrow::Cow::Borrowed(cut))),
            Some(CanonicalHistoryCut::Table(cut)) => {
                let cut = self
                    .qualify_vector_component(attempt, (&request.selection, cut), encoding)
                    .await?;
                Ok(Some(std::borrow::Cow::Owned(cut)))
            }
            Some(CanonicalHistoryCut::Checkpoint(operation)) => {
                let vector = self
                    .load_portable_checkpoint_cut(
                        attempt,
                        super::portable_checkpoint::PortableReadScope {
                            table: &request.selection.table,
                            lineage: request.selection.lineage,
                            membership: request.selection.membership,
                        },
                        *operation,
                        observed_at,
                    )
                    .await?;
                let vector = self
                    .verify_held_source_vector(
                        attempt,
                        (&request.selection.table, &vector),
                        encoding,
                    )
                    .await?;
                let cut = select_vector_component(vector, &request.selection)?;
                Ok(Some(std::borrow::Cow::Owned(cut)))
            }
        }
    }

    async fn qualify_vector_component<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalSourceSelection,
            &crate::history::table_cut::CanonicalTableCut,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<CanonicalSourceCut> {
        let (selection, cut) = input;
        let request = CanonicalTableCutRequest {
            serving_region: selection.serving_region.clone(),
            serving_group: selection.serving_group,
            table: selection.table.clone(),
            table_cut: CanonicalTableCutAction::Verify(cut.clone()),
        };
        let vector = self
            .load_table_source_vector(attempt, &request, encoding, ReadProtection::Snapshot)
            .await?;
        select_vector_component(vector, selection)
    }

    pub(in crate::journal) async fn load_history_frame<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        selection: (&CanonicalSourceSelection, ClosedSourceInterval),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<SourceFrame> {
        let (selection, range) = selection;
        let request = SourceFrameRequest {
            origin: selection.origin.clone(),
            range,
            membership: selection.membership,
            tables: vec![(selection.source.table, selection.lineage)],
        };
        load_source_frame(self, attempt, (&request, encoding.0), encoding.1)
            .await
            .map_err(StorageError::from)
    }
}

fn require_cut_manifest(
    observed: Option<CanonicalDigest>,
    expected: Option<CanonicalDigest>,
) -> StorageResult<()> {
    if observed != expected {
        return Err(StorageError::validation(
            "Canonical source cut terminal manifest does not match native history",
        ));
    }
    Ok(())
}

fn select_vector_component(
    vector: crate::history::table_cut::CanonicalTableCut,
    selection: &CanonicalSourceSelection,
) -> StorageResult<CanonicalSourceCut> {
    vector
        .sources
        .into_iter()
        .find(|cut| cut.origin == selection.origin && cut.source == selection.source)
        .ok_or_else(|| {
            StorageError::validation("History source is absent from the complete source vector")
        })
}
