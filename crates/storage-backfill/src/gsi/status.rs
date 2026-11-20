//! Phase-only observations for foreground projection and query readiness.

use serde::{Deserialize, de::IgnoredAny};

use super::{GsiBuild, GsiBuildTarget, GsiLifecycleError, GsiRecordError, GsiState};

/// Foreground operations never need a backfill cursor or stream checkpoint.
#[derive(Deserialize)]
pub struct GsiBuildStatus {
    pub target: GsiBuildTarget,
    #[serde(rename = "state")]
    pub phase: GsiBuildPhase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(from = "StoredPhase")]
pub enum GsiBuildPhase {
    Filling,
    Building,
    Active,
}

/// Read the existing KV enum tag without materializing its state payload.
#[derive(Deserialize)]
enum StoredPhase {
    Filling(IgnoredAny),
    Building(IgnoredAny),
    Active(IgnoredAny),
}

impl From<StoredPhase> for GsiBuildPhase {
    fn from(phase: StoredPhase) -> Self {
        match phase {
            StoredPhase::Filling(_) => Self::Filling,
            StoredPhase::Building(_) => Self::Building,
            StoredPhase::Active(_) => Self::Active,
        }
    }
}

impl TryFrom<&str> for GsiBuildPhase {
    type Error = GsiRecordError;

    fn try_from(phase: &str) -> Result<Self, Self::Error> {
        match phase {
            "filling" => Ok(Self::Filling),
            "building" => Ok(Self::Building),
            "active" => Ok(Self::Active),
            _ => Err(GsiRecordError::Phase),
        }
    }
}

impl<Cursor> From<&GsiState<Cursor>> for GsiBuildPhase {
    fn from(state: &GsiState<Cursor>) -> Self {
        match state {
            GsiState::Filling(_) => Self::Filling,
            GsiState::Building(_) => Self::Building,
            GsiState::Active(_) => Self::Active,
        }
    }
}

/// All build phases maintain projections; only Active serves queries.
impl From<GsiBuildPhase> for storage_types::GsiPhase {
    fn from(phase: GsiBuildPhase) -> Self {
        match phase {
            GsiBuildPhase::Filling => Self::Filling,
            GsiBuildPhase::Building => Self::Building,
            GsiBuildPhase::Active => Self::Active,
        }
    }
}

impl<Cursor> From<GsiBuild<Cursor>> for GsiBuildStatus {
    fn from(build: GsiBuild<Cursor>) -> Self {
        Self {
            phase: (&build.state).into(),
            target: build.target,
        }
    }
}

impl GsiBuildPhase {
    pub const fn permits_projection(self) -> bool {
        true
    }

    pub const fn permits_queries(self) -> bool {
        matches!(self, Self::Active)
    }
}

impl GsiBuildStatus {
    pub fn require_distinct_targets(builds: &[Self]) -> Result<(), GsiLifecycleError> {
        for (index, build) in builds.iter().enumerate() {
            if builds[..index].iter().any(|previous| {
                previous.target.table_name == build.target.table_name
                    && previous.target.index_name == build.target.index_name
            }) {
                return Err(GsiLifecycleError::DuplicateIndex);
            }
            if builds[..index]
                .iter()
                .any(|previous| previous.target.incarnation == build.target.incarnation)
            {
                return Err(GsiLifecycleError::DuplicateIncarnation);
            }
        }
        Ok(())
    }
}

impl TryFrom<(storage_types::TableHistoryId, String, String, Vec<u8>)> for GsiBuildTarget {
    type Error = GsiRecordError;

    fn try_from(
        (history_id, table_name, index_name, incarnation): (
            storage_types::TableHistoryId,
            String,
            String,
            Vec<u8>,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            history_id,
            table_name: storage_types::TableName::try_from(table_name)
                .map_err(|_| GsiRecordError::TableName)?,
            index_name: storage_types::IndexName::new(&index_name),
            incarnation: storage_types::maintenance::TargetIncarnation::try_from(incarnation)
                .map_err(|_| GsiRecordError::Incarnation)?,
        })
    }
}

impl AsRef<GsiBuildTarget> for GsiBuildStatus {
    fn as_ref(&self) -> &GsiBuildTarget {
        &self.target
    }
}

impl<Cursor> AsRef<GsiBuildTarget> for GsiBuild<Cursor> {
    fn as_ref(&self) -> &GsiBuildTarget {
        &self.target
    }
}
