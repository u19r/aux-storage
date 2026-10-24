use replication_types::{
    configuration::membership::MembershipRevision,
    error::ReplicationProtocolError,
    event::interval::{CanonicalEventLink, PositionCoverage, SourceIntervalManifest},
    identity::{
        origin::{OriginId, OriginPosition},
        resource::{TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};

use crate::error::ReplicationResult;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSelection {
    pub table: TableIdentity,
    pub lineage: TableLineage,
}
/// Origin journal receipts loaded under protected authority; the full source
/// selection is immutable.
#[derive(Clone, Debug)]
pub struct SealedPositionEvidence {
    pub position: OriginPosition,
    pub receipt: CanonicalDigest,
    pub all_events: Vec<CanonicalEventLink>,
}
#[derive(Clone, Debug)]
pub struct CertifiedSourceExclusions {
    pub origin: OriginId,
    pub membership: MembershipRevision,
    pub manifest: CanonicalDigest,
    pub selected: Vec<TableSelection>,
    pub sealed_positions: Vec<SealedPositionEvidence>,
}
#[derive(Clone, Debug)]
pub struct IntervalCoverageDecision {
    manifest: SourceIntervalManifest,
}
impl IntervalCoverageDecision {
    pub fn validate_interval(
        manifest: SourceIntervalManifest,
        certified: &CertifiedSourceExclusions,
    ) -> ReplicationResult<Self> {
        manifest.validate_shape()?;
        if certified
            .selected
            .windows(2)
            .any(|w| (w[0].table, w[0].lineage) >= (w[1].table, w[1].lineage))
        {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "manifest.validate_interval",
            }
            .into());
        }
        if certified.origin != manifest.origin
            || certified.membership != manifest.membership
            || certified.manifest != manifest.digest
            || certified.sealed_positions.len() != manifest.coverage.len()
        {
            return Err(ReplicationProtocolError::SourceGap {
                context: "manifest.validate_interval",
            }
            .into());
        }
        let mut required = Vec::new();
        for (coverage, seal) in manifest.coverage.iter().zip(&certified.sealed_positions) {
            Self::validate_position(coverage, seal, certified, &mut required)?;
        }
        if required != manifest.links {
            return Err(ReplicationProtocolError::SourceGap {
                context: "manifest.validate_interval",
            }
            .into());
        }
        Ok(Self { manifest })
    }
    fn validate_position(
        coverage: &PositionCoverage,
        seal: &SealedPositionEvidence,
        certified: &CertifiedSourceExclusions,
        required: &mut Vec<CanonicalEventLink>,
    ) -> ReplicationResult<()> {
        Self::validate_source_selection(seal)?;
        let selected = Self::selected_events(seal, certified);
        Self::validate_selection_coverage(coverage, seal, &selected)?;
        required.extend(selected.into_iter().cloned());
        Ok(())
    }
    fn validate_source_selection(seal: &SealedPositionEvidence) -> ReplicationResult<()> {
        if seal
            .all_events
            .iter()
            .any(|link| link.position != seal.position)
            || seal
                .all_events
                .windows(2)
                .any(|w| w[0].ordinal >= w[1].ordinal)
        {
            return Err(ReplicationProtocolError::SourceGap {
                context: "manifest.validate_source_selection",
            }
            .into());
        }
        Ok(())
    }
    fn selected_events<'a>(
        seal: &'a SealedPositionEvidence,
        certified: &CertifiedSourceExclusions,
    ) -> Vec<&'a CanonicalEventLink> {
        seal.all_events
            .iter()
            .filter(|link| {
                certified
                    .selected
                    .binary_search_by_key(&(link.table, link.lineage), |s| (s.table, s.lineage))
                    .is_ok()
            })
            .collect()
    }
    fn validate_selection_coverage(
        coverage: &PositionCoverage,
        seal: &SealedPositionEvidence,
        selected: &[&CanonicalEventLink],
    ) -> ReplicationResult<()> {
        match coverage {
            PositionCoverage::Selected { position, ordinals } => {
                if *position != seal.position
                    || ordinals.len() != selected.len()
                    || !ordinals
                        .iter()
                        .zip(selected)
                        .all(|(ordinal, link)| *ordinal == link.ordinal)
                {
                    return Err(ReplicationProtocolError::SourceGap {
                        context: "manifest.validate_selection_coverage",
                    }
                    .into());
                }
            }
            PositionCoverage::Excluded {
                position,
                sealed_receipt,
                reason,
            } => {
                if *position != seal.position
                    || *sealed_receipt != seal.receipt
                    || !selected.is_empty()
                    || matches!(
                        reason,
                        replication_types::event::interval::ExclusionReason::NoEvent
                    ) && !seal.all_events.is_empty()
                {
                    return Err(ReplicationProtocolError::SourceGap {
                        context: "manifest.validate_selection_coverage",
                    }
                    .into());
                }
            }
        }
        Ok(())
    }
}

impl From<IntervalCoverageDecision> for SourceIntervalManifest {
    fn from(value: IntervalCoverageDecision) -> Self {
        value.manifest
    }
}
