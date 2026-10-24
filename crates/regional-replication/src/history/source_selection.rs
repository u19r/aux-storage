//! Exact installed original-source selection shared by cut and bounded reads.

use replication_types::{
    configuration::{installed::RegionalSourceBinding, membership::MembershipRevision},
    identity::{
        origin::{OriginId, OriginPosition},
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TableName};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalSourceSelection {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub source: RegionalSourceBinding,
    pub origin: OriginId,
}

pub(crate) struct SourceHistoryBounds {
    pub(crate) trim: super::trim_authority::TrimRevision,
    pub(crate) after: OriginPosition,
    pub(crate) through: OriginPosition,
}

impl SourceHistoryBounds {
    pub(crate) fn try_new(
        trim: super::trim_authority::TrimRevision,
        after: OriginPosition,
        through: OriginPosition,
    ) -> StorageResult<Self> {
        if after > through {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self {
            trim,
            after,
            through,
        })
    }

    pub(crate) fn require_cut(
        &self,
        cut: &super::source_cut::CanonicalSourceCut,
    ) -> StorageResult<()> {
        if cut.trim != self.trim || cut.after != self.after {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        Ok(())
    }
}

pub(crate) enum AvailableSourcePrefix {
    Current(SourceHistoryBounds),
    Retired(SourceHistoryBounds),
    Held(SourceHistoryBounds),
}

impl AvailableSourcePrefix {
    pub(crate) fn bounds(&self) -> &SourceHistoryBounds {
        match self {
            Self::Current(bounds) | Self::Retired(bounds) | Self::Held(bounds) => bounds,
        }
    }

    pub(crate) fn through(&self) -> OriginPosition {
        self.bounds().through
    }

    pub(crate) fn require_through(&self, requested: OriginPosition) -> StorageResult<()> {
        if requested <= self.through() {
            return Ok(());
        }
        match self {
            Self::Current(_) => Err(storage_types::StorageEnum::HistoryUnavailable.into()),
            Self::Retired(_) => Err(StorageError::validation(
                "History interval exceeds the final retired source cut",
            )),
            Self::Held(_) => Err(StorageError::validation(
                "History interval exceeds the held checkpoint cut",
            )),
        }
    }

    pub(crate) fn require_range(
        &self,
        requested: replication_types::event::interval::ClosedSourceInterval,
        bounded: OriginPosition,
    ) -> StorageResult<()> {
        if requested.after() < self.bounds().after {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        let through = match self {
            Self::Current(_) => bounded,
            Self::Retired(_) | Self::Held(_) => requested.through(),
        };
        self.require_through(through)
    }
}
