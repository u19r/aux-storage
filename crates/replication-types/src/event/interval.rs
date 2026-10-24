use serde::{Deserialize, Serialize};

use super::identity::CanonicalEventId;
use crate::{
    configuration::membership::MembershipRevision,
    error::ReplicationProtocolError,
    identity::{
        origin::{MutationOrdinal, OriginId, OriginPosition},
        resource::{TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};

/// A stable encoding excludes the manifest digest itself.
pub trait IntervalCodec {
    fn encode_interval(
        &self,
        manifest: &SourceIntervalManifest,
    ) -> Result<crate::protocol::CanonicalBytes, ReplicationProtocolError>;
}

/// Coverage is (after, through]; each position declares every required ordinal
/// or exclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "(OriginPosition, OriginPosition)",
    into = "(OriginPosition, OriginPosition)"
)]
pub struct ClosedSourceInterval {
    after: OriginPosition,
    through: OriginPosition,
}

impl TryFrom<(OriginPosition, OriginPosition)> for ClosedSourceInterval {
    type Error = ReplicationProtocolError;
    fn try_from((after, through): (OriginPosition, OriginPosition)) -> Result<Self, Self::Error> {
        validate_source_interval(after, through)?;
        Ok(Self { after, through })
    }
}

impl From<ClosedSourceInterval> for (OriginPosition, OriginPosition) {
    fn from(value: ClosedSourceInterval) -> Self {
        (value.after, value.through)
    }
}

impl ClosedSourceInterval {
    pub fn contains(&self, position: OriginPosition) -> bool {
        self.after < position && position <= self.through
    }
    pub fn after(&self) -> OriginPosition {
        self.after
    }
    pub fn through(&self) -> OriginPosition {
        self.through
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEventLink {
    pub position: OriginPosition,
    pub ordinal: MutationOrdinal,
    /// Fresh native publisher clock, distinct from an expiration victim's
    /// order. This is publication evidence, not a wall-clock accuracy
    /// certificate.
    pub publication_clock: super::order::HybridLogicalClock,
    /// The table incarnation publishing this link. Expiration bytes retain
    /// their immutable victim source table even when another region publishes.
    pub table: TableIdentity,
    pub lineage: TableLineage,
    pub event: CanonicalEventId,
    pub digest: CanonicalDigest,
}

impl CanonicalEventLink {
    /// Original identities bind their publisher tuple; relayed expiration
    /// retains its victim.
    pub fn require_source(&self, origin: &OriginId) -> Result<(), ReplicationProtocolError> {
        self.event.validate()?;
        if u64::from(self.position) == 0
            || !self.event.is_bound_to(self.lineage)
            || matches!(&self.event, CanonicalEventId::Origin(id)
                if id.origin != *origin || id.position != self.position || id.ordinal != self.ordinal)
        {
            return Err(ReplicationProtocolError::InvalidOrigin {
                context: "publication event link source",
            });
        }
        Ok(())
    }

    /// Content binding is distinct from publisher admission. The native import
    /// owner authenticates the publishing table and verifies expiration bytes
    /// against their immutable victim before accepting them.
    pub fn require_event(
        &self,
        event: &super::content::CanonicalEvent,
        origin: &OriginId,
    ) -> Result<(), ReplicationProtocolError> {
        super::publication::CanonicalPublication::from(event).require_link(self, origin)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionCoverage {
    Selected {
        position: OriginPosition,
        ordinals: Vec<MutationOrdinal>,
    },
    Excluded {
        position: OriginPosition,
        sealed_receipt: CanonicalDigest,
        reason: ExclusionReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExclusionReason {
    NoEvent,
    UnselectedTables,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIntervalManifest {
    pub origin: OriginId,
    pub membership: MembershipRevision,
    pub range: ClosedSourceInterval,
    pub links: Vec<CanonicalEventLink>,
    pub coverage: Vec<PositionCoverage>,
    pub digest: CanonicalDigest,
}

/// Stable offsets into the ordered links selected by a table binding. The
/// full original manifest is retained, even when this native attempt is
/// partial.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "(u32, u32)", into = "(u32, u32)")]
pub struct ImportFragment {
    after: u32,
    through: u32,
}

impl TryFrom<(u32, u32)> for ImportFragment {
    type Error = ReplicationProtocolError;
    fn try_from((after, through): (u32, u32)) -> Result<Self, Self::Error> {
        if after >= through {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "import fragment span",
            });
        }
        Ok(Self { after, through })
    }
}

impl From<ImportFragment> for (u32, u32) {
    fn from(value: ImportFragment) -> Self {
        (value.after, value.through)
    }
}

impl ImportFragment {
    pub fn after(self) -> usize {
        self.after as usize
    }
    pub fn through(self) -> usize {
        self.through as usize
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePageId {
    pub origin: OriginId,
    pub range: ClosedSourceInterval,
    pub table: TableIdentity,
    pub lineage: TableLineage,
    pub manifest: CanonicalDigest,
    pub fragment: Option<ImportFragment>,
    pub selection: Option<CanonicalDigest>,
}

/// Original table selection for a globally offset import fragment.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "Vec<(TableIdentity, TableLineage)>")]
pub struct ImportTableSelection(Vec<(TableIdentity, TableLineage)>);

impl Serialize for ImportTableSelection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl TryFrom<Vec<(TableIdentity, TableLineage)>> for ImportTableSelection {
    type Error = ReplicationProtocolError;
    fn try_from(tables: Vec<(TableIdentity, TableLineage)>) -> Result<Self, Self::Error> {
        if tables.is_empty() || tables.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "fragment table selection",
            });
        }
        Ok(Self(tables))
    }
}

impl From<ImportTableSelection> for Vec<(TableIdentity, TableLineage)> {
    fn from(selection: ImportTableSelection) -> Self {
        selection.0
    }
}

impl ImportTableSelection {
    pub fn digest(&self) -> Result<CanonicalDigest, ReplicationProtocolError> {
        CanonicalDigest::of_json(&self.0).map_err(|_| ReplicationProtocolError::InvalidScope {
            context: "fragment selection encoding",
        })
    }

    pub fn require_table(
        &self,
        table: TableIdentity,
        lineage: TableLineage,
    ) -> Result<(), ReplicationProtocolError> {
        if !self.0.contains(&(table, lineage)) {
            return Err(ReplicationProtocolError::InvalidScope {
                context: "fragment page absent from selection",
            });
        }
        Ok(())
    }

    pub fn select_links<'manifest>(
        &self,
        manifest: &'manifest SourceIntervalManifest,
    ) -> Result<Vec<&'manifest CanonicalEventLink>, ReplicationProtocolError> {
        let mut links = Vec::new();
        for link in &manifest.links {
            let Ok(index) = self.0.binary_search_by_key(&link.table, |entry| entry.0) else {
                continue;
            };
            if self.0[index].1 != link.lineage {
                return Err(ReplicationProtocolError::LineageMismatch {
                    context: "fragment selection lineage",
                });
            }
            links.push(link);
        }
        Ok(links)
    }
}

impl SourceIntervalManifest {
    pub fn validate_shape(&self) -> Result<(), ReplicationProtocolError> {
        if self.links.windows(2).any(|pair| {
            if pair[0].position == pair[1].position {
                pair[0].publication_clock != pair[1].publication_clock
            } else {
                pair[0].publication_clock >= pair[1].publication_clock
            }
        }) {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "source publication clocks",
            });
        }
        let mut next = u64::from(self.range.after());
        let width = u64::from(self.range.through()) - next;
        if u64::try_from(self.coverage.len()).ok() != Some(width) {
            return Err(ReplicationProtocolError::SourceGap {
                context: "interval.validate_shape",
            });
        }
        let mut links = self.links.iter();
        for coverage in &self.coverage {
            next = next
                .checked_add(1)
                .ok_or(ReplicationProtocolError::CounterOverflow {
                    context: "interval.validate_shape",
                })?;
            validate_source_position(self, coverage, OriginPosition::from(next), &mut links)?;
        }
        if links.next().is_some() {
            return Err(ReplicationProtocolError::SourceGap {
                context: "interval.validate_shape",
            });
        }
        Ok(())
    }
}

fn validate_source_interval(
    after: OriginPosition,
    through: OriginPosition,
) -> Result<(), ReplicationProtocolError> {
    if after >= through {
        return Err(ReplicationProtocolError::InvalidRange {
            context: "interval.validate_source_interval",
        });
    }
    Ok(())
}

fn validate_source_position(
    manifest: &SourceIntervalManifest,
    coverage: &PositionCoverage,
    expected: OriginPosition,
    links: &mut std::slice::Iter<'_, CanonicalEventLink>,
) -> Result<(), ReplicationProtocolError> {
    match coverage {
        PositionCoverage::Excluded { position, .. } if *position == expected => {}
        PositionCoverage::Selected { position, ordinals } if *position == expected => {
            if ordinals.is_empty() || ordinals.windows(2).any(|w| w[0] >= w[1]) {
                return Err(ReplicationProtocolError::SourceGap {
                    context: "interval.validate_shape",
                });
            }
            for ordinal in ordinals {
                let link = links.next().ok_or(ReplicationProtocolError::SourceGap {
                    context: "interval.validate_shape",
                })?;
                if link.position != *position || link.ordinal != *ordinal {
                    return Err(ReplicationProtocolError::SourceGap {
                        context: "interval.validate_shape",
                    });
                }
                link.require_source(&manifest.origin)?;
            }
        }
        _ => {
            return Err(ReplicationProtocolError::SourceGap {
                context: "interval.validate_shape",
            });
        }
    }
    Ok(())
}
