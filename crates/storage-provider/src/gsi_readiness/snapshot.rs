use storage_backfill::{
    GsiBuild, GsiBuildPhase, GsiBuildStatus, GsiBuildTarget, GsiLifecycleError,
};
use storage_types::{
    BoundTableInfo, GsiPhase, IndexName, IndexStatus, StorageError, StorageResult,
    TableDescription, TableStatus,
};

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
        let mut building = false;
        for (index, (_, state)) in indexes.iter_mut().zip(self.select(&names)?) {
            building |= state.index_status != IndexStatus::Active;
            // AWS shows `Backfilling` only while an index is not ACTIVE.
            index.backfilling =
                (state.index_status != IndexStatus::Active).then_some(state.backfilling);
            index.index_status = Some(state.index_status);
        }
        // AWS reports an ACTIVE table as UPDATING while one of its GSIs builds.
        if building && description.table_status == TableStatus::Active {
            description.table_status = TableStatus::Updating;
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
        // AWS answers a read of a CREATING GSI as if the index were absent
        // (P13).
        Err(StorageError::validation(format!(
            "The table does not have the specified index: {index_name}"
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

/// Readiness from the catalog observation already protected by this attempt.
/// Job transitions publish the binding phase in the same native transaction.
pub fn gsi_readiness_from_catalog(
    table: &BoundTableInfo,
    requested: Option<&IndexName>,
) -> StorageResult<GsiReadinessSnapshot> {
    let indexes = table
        .info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default();
    if requested.is_some_and(|name| !indexes.iter().any(|index| index.index_name == *name)) {
        return Err(StorageError::internal(
            "GSI identity is absent from catalog",
        ));
    }
    let records = indexes
        .iter()
        .filter(|index| requested.is_none_or(|name| index.index_name == *name))
        .map(|index| {
            let bound = table
                .binding
                .gsi(&index.index_name)
                .ok_or_else(|| StorageError::internal("GSI is absent from the catalog binding"))?;
            Ok(GsiReadinessRecord {
                table_name: table.table_name.clone(),
                index_name: index.index_name.clone(),
                readiness: match bound.phase {
                    GsiPhase::Active => GsiIndexReadiness::active(),
                    GsiPhase::Filling | GsiPhase::Building => GsiIndexReadiness::creating(),
                },
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(GsiReadinessSnapshot(records))
}

/// Current user-index build identities for table/index retirement.
pub fn gsi_build_targets_from_catalog(
    table: &BoundTableInfo,
) -> StorageResult<Vec<GsiBuildTarget>> {
    table
        .info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|index| {
            let bound = table
                .binding
                .gsi(&index.index_name)
                .ok_or_else(|| StorageError::internal("GSI is absent from the catalog binding"))?;
            Ok(GsiBuildTarget {
                history_id: table.history_id,
                table_name: table.table_name.clone(),
                index_name: index.index_name.clone(),
                incarnation: bound.build.clone(),
            })
        })
        .collect()
}
