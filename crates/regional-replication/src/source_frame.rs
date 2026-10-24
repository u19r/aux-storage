use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    delivery::{error::DeliveryError, frame::SourceFrame},
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::membership::MembershipRevision,
    error::ReplicationProtocolError,
    event::interval::{
        ClosedSourceInterval, ExclusionReason, IntervalCodec, PositionCoverage,
        SourceIntervalManifest,
    },
    identity::{
        origin::OriginId,
        resource::{TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};

use crate::journal::JournalStore;

/// Installed delivery selection. The runtime binds it to the peer's committed
/// membership before issuing a read; it contains no transport address.
pub struct SourceFrameRequest {
    pub origin: OriginId,
    pub range: ClosedSourceInterval,
    pub membership: MembershipRevision,
    pub tables: Vec<(TableIdentity, TableLineage)>,
}

pub async fn load_source_frame<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (&SourceFrameRequest, &WorkLimits),
    codec: &CanonicalJsonCodec,
) -> Result<SourceFrame, DeliveryError> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let result = async {
        select_source_frame(journal, settlement.as_mut(), input, codec)
            .await?
            .ok_or_else(frame_bound)
    }
    .await;
    settlement.complete(result)
}

/// An oversized candidate is a successful selection decision before hydration.
/// Native or identity failures still reject this complete attempt.
pub(crate) async fn select_source_frame<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (&SourceFrameRequest, &WorkLimits),
    codec: &CanonicalJsonCodec,
) -> Result<Option<SourceFrame>, DeliveryError> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (request, limits) = input;
        if !is_selection_bounded(request, limits)? {
            return Ok(None);
        }
        let manifest = if journal.is_local_origin(&request.origin) {
            journal
                .select_local_source_manifest(attempt, request, (codec, limits))
                .await?
        } else {
            journal
                .select_retained_source_manifest(attempt, request, (codec, limits))
                .await?
        };
        let Some(mut manifest) = manifest else {
            return Ok(None);
        };
        let encoded = match codec.encode_interval(&manifest) {
            Ok(encoded) => encoded,
            Err(ReplicationProtocolError::PayloadTooLarge { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        manifest.digest = CanonicalDigest::of(encoded.as_ref());
        let Some(remaining) = limits.frame_bytes.get().checked_sub(encoded.as_ref().len()) else {
            return Ok(None);
        };
        let Some(accepted) = journal
            .select_frame_acceptances(attempt, &manifest.links, (codec, remaining))
            .await?
        else {
            return Ok(None);
        };
        let events = accepted
            .into_iter()
            .map(|accepted| accepted.event)
            .collect();
        SourceFrame::try_new((manifest, events), codec, limits)
            .map(Some)
            .map_err(DeliveryError::from)
    }
    .await;
    settlement.complete(result)
}

/// One candidate retains only qualified selected links and complete-position
/// coverage.
pub(crate) struct SourceManifestSelection {
    manifest: SourceIntervalManifest,
    maximum_events: usize,
    maximum_bytes: usize,
}

impl SourceManifestSelection {
    pub(crate) fn new(request: &SourceFrameRequest, limits: &WorkLimits) -> Self {
        Self {
            manifest: SourceIntervalManifest {
                origin: request.origin.clone(),
                membership: request.membership,
                range: request.range,
                links: Vec::new(),
                coverage: Vec::new(),
                digest: [0; 32].into(),
            },
            maximum_events: limits.frame_events.get(),
            maximum_bytes: limits.frame_bytes.get(),
        }
    }

    pub(crate) fn append_position(
        &mut self,
        input: (
            Vec<replication_types::event::interval::CanonicalEventLink>,
            PositionCoverage,
        ),
        codec: &CanonicalJsonCodec,
    ) -> Result<bool, DeliveryError> {
        let (links, coverage) = input;
        if self
            .manifest
            .links
            .len()
            .checked_add(links.len())
            .is_none_or(|count| count > self.maximum_events)
            || self.manifest.coverage.len() >= self.maximum_events
        {
            return Ok(false);
        }
        self.manifest.links.extend(links);
        self.manifest.coverage.push(coverage);
        match codec.interval_content_length(&self.manifest, self.maximum_bytes) {
            Ok(_) => Ok(true),
            Err(ReplicationProtocolError::PayloadTooLarge { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn append_local_position(
        &mut self,
        position: crate::journal::SealedSourcePosition,
        input: (&[(TableIdentity, TableLineage)], &CanonicalJsonCodec),
    ) -> Result<bool, DeliveryError> {
        let (tables, codec) = input;
        let empty = position.source.events.is_empty();
        let links = position
            .source
            .events
            .into_iter()
            .filter(|link| tables.binary_search(&(link.table, link.lineage)).is_ok())
            .collect::<Vec<_>>();
        let coverage = if links.is_empty() {
            PositionCoverage::Excluded {
                position: position.source.position,
                sealed_receipt: position.receipt,
                reason: if empty {
                    ExclusionReason::NoEvent
                } else {
                    ExclusionReason::UnselectedTables
                },
            }
        } else {
            PositionCoverage::Selected {
                position: position.source.position,
                ordinals: links.iter().map(|link| link.ordinal).collect(),
            }
        };
        self.append_position((links, coverage), codec)
    }

    pub(crate) fn into_manifest(self) -> SourceIntervalManifest {
        self.manifest
    }
}

fn is_selection_bounded(
    request: &SourceFrameRequest,
    limits: &WorkLimits,
) -> Result<bool, DeliveryError> {
    if request.tables.is_empty() || request.tables.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(ReplicationProtocolError::InvalidScope {
            context: "source frame table selection",
        }
        .into());
    }
    let width = u64::from(request.range.through()) - u64::from(request.range.after());
    Ok(usize::try_from(width).is_ok_and(|width| width <= limits.frame_events.get()))
}

fn frame_bound() -> DeliveryError {
    ReplicationProtocolError::InvalidRange {
        context: "source frame configured bound",
    }
    .into()
}
