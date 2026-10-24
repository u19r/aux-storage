//! Public history revisions are independent of table configuration revisions.

use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(transparent)]
pub struct TrimRevision(u64);

impl TrimRevision {
    pub(crate) const GENESIS: Self = Self(0);

    pub fn checked_next(self) -> StorageResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(crate::native_record::corrupt_record)
    }
}

/// Portable visibility description; only native survivor or activated custody
/// issues authority.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalTrimSnapshot {
    visibility: HistoricalTrimVisibility,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
enum HistoricalTrimVisibility {
    Genesis {
        lineage: replication_types::identity::resource::TableLineage,
    },
    Installed {
        certificate: Box<crate::trim::commit::TrimCommitCertificate>,
        floors: super::source_floor::SourcePublicationFloors,
    },
}

impl HistoricalTrimSnapshot {
    pub(crate) fn from_genesis(
        lineage: replication_types::identity::resource::TableLineage,
    ) -> Self {
        Self {
            visibility: HistoricalTrimVisibility::Genesis { lineage },
        }
    }

    pub(crate) fn from_installed(
        certificate: crate::trim::commit::TrimCommitCertificate,
        floors: super::source_floor::SourcePublicationFloors,
    ) -> Self {
        Self {
            visibility: HistoricalTrimVisibility::Installed {
                certificate: Box::new(certificate),
                floors,
            },
        }
    }

    pub(crate) fn revision(&self) -> TrimRevision {
        match &self.visibility {
            HistoricalTrimVisibility::Genesis { .. } => TrimRevision::GENESIS,
            HistoricalTrimVisibility::Installed { certificate, .. } => certificate.proposal.next,
        }
    }

    fn qualify_floors(
        &self,
        lineage: replication_types::identity::resource::TableLineage,
    ) -> StorageResult<Option<&super::source_floor::SourcePublicationFloors>> {
        match &self.visibility {
            HistoricalTrimVisibility::Genesis { lineage: original } if *original == lineage => {
                Ok(None)
            }
            HistoricalTrimVisibility::Installed {
                certificate,
                floors,
            } if certificate.proposal.candidate.lineage == lineage => {
                certificate.require_installed_floors(floors)?;
                Ok(Some(floors))
            }
            _ => Err(crate::native_record::corrupt_record()),
        }
    }

    pub(crate) fn into_qualified_installation(
        self,
        lineage: replication_types::identity::resource::TableLineage,
    ) -> StorageResult<
        Option<(
            crate::trim::commit::TrimCommitCertificate,
            super::source_floor::SourcePublicationFloors,
        )>,
    > {
        self.qualify_floors(lineage)?;
        Ok(match self.visibility {
            HistoricalTrimVisibility::Genesis { .. } => None,
            HistoricalTrimVisibility::Installed {
                certificate,
                floors,
            } => Some((*certificate, floors)),
        })
    }

    pub(crate) fn require_cut(
        &self,
        cut: &super::source_cut::CanonicalSourceCut,
    ) -> StorageResult<()> {
        cut.require_source()?;
        let floors = self.qualify_floors(cut.lineage)?;
        let after = match floors {
            None => 0.into(),
            Some(floors) => floors.floor_for(cut.into())?.min(cut.through),
        };
        if self.revision() != cut.trim || after != cut.after {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}
