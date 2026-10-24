//! Committed candidate upper bounds normalize by original publication identity.
use replication_types::identity::{
    origin::{OriginId, OriginPosition},
    resource::{TableIdentity, TableLineage},
};
use serde::Deserialize;
use storage_types::StorageResult;

use crate::{
    history::{publication_scope::SourcePublicationScope, table_cut::CanonicalTableCut},
    native_record::corrupt_record,
};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SourcePublicationFloor {
    pub(super) lineage: TableLineage,
    pub(super) origin: OriginId,
    pub(super) table: TableIdentity,
    pub(crate) through: OriginPosition,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "Vec<super::source_floor_encoding::SourceFloorGroup>")]
pub(crate) struct SourcePublicationFloors(
    pub(super) Vec<super::source_floor_encoding::SourceFloorGroup>,
);

impl SourcePublicationFloors {
    pub(crate) fn advance_candidate(
        candidate: &CanonicalTableCut,
        predecessor: Option<Self>,
    ) -> StorageResult<Self> {
        let mut next = predecessor.unwrap_or_else(|| Self(Vec::new()));
        if next
            .0
            .iter()
            .any(|group| group.lineage != candidate.lineage)
        {
            return Err(corrupt_record());
        }
        for cut in &candidate.sources {
            next.advance_floor(cut.into(), cut.through)?;
        }
        next.require_installation(candidate)?;
        Ok(next)
    }

    fn advance_floor(
        &mut self,
        scope: SourcePublicationScope<'_>,
        through: OriginPosition,
    ) -> StorageResult<()> {
        let index = match self.0.binary_search_by(|group| group.compare_scope(scope)) {
            Ok(index) => index,
            Err(index) => {
                self.0
                    .try_reserve(1)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                self.0.insert(
                    index,
                    super::source_floor_encoding::SourceFloorGroup::from_scope(scope),
                );
                index
            }
        };
        self.0[index].advance_floor(scope.origin.journal_partition, through)
    }

    pub(crate) fn require_installation(&self, candidate: &CanonicalTableCut) -> StorageResult<()> {
        self.require_groups()?;
        if self
            .0
            .iter()
            .any(|group| group.lineage != candidate.lineage)
        {
            return Err(corrupt_record());
        }
        for cut in &candidate.sources {
            if self.floor_for(cut.into())? < cut.through {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn introduce_sources(
        &mut self,
        before: &replication_types::configuration::installed::InstalledTableState,
        next: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<bool> {
        if before.configuration.lineage != next.configuration.lineage {
            return Err(corrupt_record());
        }
        let mut introduced = Self(Vec::new());
        for source in &next.sources {
            let mut origin = OriginId {
                region: source.region.clone(),
                database_group: source.database_group,
                journal_generation: source.generation,
                journal_partition: 0.into(),
            };
            for partition in 0..u32::from(source.partitions) {
                origin.journal_partition = u16::try_from(partition)
                    .map_err(|_| corrupt_record())?
                    .into();
                let scope =
                    SourcePublicationScope::new(next.configuration.lineage, &origin, source.table);
                if self.floor_for(scope).is_ok() {
                    continue;
                }
                if before.sources.iter().any(|old| {
                    old.region == source.region
                        && old.database_group == source.database_group
                        && old.generation == source.generation
                        && old.table == source.table
                        && partition < u32::from(old.partitions)
                }) {
                    return Err(corrupt_record());
                }
                if introduced.floor_for(scope).is_ok() {
                    return Err(corrupt_record());
                }
                introduced.advance_floor(scope, 0.into())?;
            }
        }
        let has_introduction = !introduced.0.is_empty();
        for floor in introduced.scopes() {
            self.advance_floor(floor.scope(), floor.through)?;
        }
        Ok(has_introduction)
    }

    pub(crate) fn require_sources(
        &self,
        state: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<()> {
        for source in &state.sources {
            let mut origin = OriginId {
                region: source.region.clone(),
                database_group: source.database_group,
                journal_generation: source.generation,
                journal_partition: 0.into(),
            };
            for partition in 0..u32::from(source.partitions) {
                origin.journal_partition = u16::try_from(partition)
                    .map_err(|_| corrupt_record())?
                    .into();
                self.floor_for(SourcePublicationScope::new(
                    state.configuration.lineage,
                    &origin,
                    source.table,
                ))?;
            }
        }
        Ok(())
    }

    pub(crate) fn require_predecessor(&self, predecessor: &Self) -> StorageResult<()> {
        for group in &predecessor.0 {
            let mut origin = group.origin.clone();
            for (partition, through) in &group.floors {
                origin.journal_partition = *partition;
                if self.floor_for(SourcePublicationScope::new(
                    group.lineage,
                    &origin,
                    group.table,
                ))? < *through
                {
                    return Err(corrupt_record());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn scopes(&self) -> impl Iterator<Item = SourcePublicationFloor> + '_ {
        self.scope_cursor()
    }

    pub(crate) fn floor_for(
        &self,
        scope: SourcePublicationScope<'_>,
    ) -> StorageResult<OriginPosition> {
        let index = self
            .0
            .binary_search_by(|group| group.compare_scope(scope))
            .map_err(|_| corrupt_record())?;
        self.0[index].floor_for(scope.origin.journal_partition)
    }
}

impl SourcePublicationFloor {
    pub(crate) fn scope(&self) -> SourcePublicationScope<'_> {
        SourcePublicationScope::new(self.lineage, &self.origin, self.table)
    }
}
