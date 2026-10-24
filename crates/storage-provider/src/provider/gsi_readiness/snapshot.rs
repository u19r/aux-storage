use storage_backfill::{GsiBuild, GsiBuildPhase, GsiBuildStatus, GsiLifecycleError};
use storage_types::{IndexName, IndexStatus, StorageError, StorageResult, TableDescription};

use super::{GsiIndexReadiness, GsiReadinessRecord, GsiReadinessSnapshot};

impl GsiIndexReadiness {
    #[must_use]
    const fn active() -> Self {
        Self {
            index_status: IndexStatus::Active,
            backfilling: false,
        }
    }

    #[must_use]
    const fn creating() -> Self {
        Self {
            index_status: IndexStatus::Creating,
            backfilling: true,
        }
    }

    #[must_use]
    const fn permits_queries(&self) -> bool {
        matches!(self.index_status, IndexStatus::Active) && !self.backfilling
    }
}

impl From<GsiBuildPhase> for GsiIndexReadiness {
    fn from(phase: GsiBuildPhase) -> Self {
        if phase.permits_queries() {
            Self::active()
        } else {
            Self::creating()
        }
    }
}

impl From<GsiBuildStatus> for GsiReadinessRecord {
    fn from(build: GsiBuildStatus) -> Self {
        Self {
            table_name: build.target.table_name,
            index_name: build.target.index_name,
            readiness: GsiIndexReadiness::from(build.phase),
        }
    }
}

impl From<Vec<GsiReadinessRecord>> for GsiReadinessSnapshot {
    fn from(records: Vec<GsiReadinessRecord>) -> Self {
        Self(records)
    }
}

impl<Build: Into<GsiBuildStatus>> TryFrom<Vec<Build>> for GsiReadinessSnapshot {
    type Error = GsiLifecycleError;

    fn try_from(builds: Vec<Build>) -> Result<Self, Self::Error> {
        let builds: Vec<GsiBuildStatus> = builds.into_iter().map(Into::into).collect();
        GsiBuildStatus::require_distinct_targets(&builds)?;
        Ok(Self(builds.into_iter().map(Into::into).collect()))
    }
}

impl<Cursor> From<GsiBuild<Cursor>> for GsiReadinessSnapshot {
    fn from(build: GsiBuild<Cursor>) -> Self {
        Self(vec![GsiBuildStatus::from(build).into()])
    }
}

impl GsiReadinessSnapshot {
    pub fn apply_to_description(self, description: &mut TableDescription) -> StorageResult<()> {
        let Some(indexes) = description.global_secondary_indexes.as_mut() else {
            return Ok(());
        };
        let names = indexes
            .iter()
            .map(|index| index.index_name.clone())
            .collect::<Vec<_>>();
        for (index, (_, state)) in indexes.iter_mut().zip(self.select(&names)?) {
            index.index_status = Some(state.index_status);
            index.backfilling = Some(state.backfilling);
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn into_records(self) -> Vec<GsiReadinessRecord> {
        self.0
    }

    pub fn require_queryable(mut self, index_name: &IndexName) -> StorageResult<()> {
        if self
            .take(index_name)
            .is_ok_and(|record| record.readiness.permits_queries())
        {
            return Ok(());
        }
        Err(StorageError::validation(format!(
            "Global secondary index {index_name} is not ready for queries"
        )))
    }

    pub fn select(
        mut self,
        index_names: &[IndexName],
    ) -> StorageResult<Vec<(IndexName, GsiIndexReadiness)>> {
        index_names
            .iter()
            .map(|index_name| {
                let record = self.take(index_name)?;
                Ok((index_name.clone(), record.readiness))
            })
            .collect()
    }

    fn take(&mut self, index_name: &IndexName) -> StorageResult<GsiReadinessRecord> {
        let position = self
            .0
            .iter()
            .position(|record| &record.index_name == index_name)
            .ok_or_else(|| StorageError::internal("GSI durable backfill state is missing"))?;
        Ok(self.0.swap_remove(position))
    }
}
