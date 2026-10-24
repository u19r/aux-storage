//! Canonical explicit partition pairs share one source header in memory and on
//! disk.
use replication_types::identity::{
    journal_partition::JournalPartition,
    origin::{OriginId, OriginPosition},
    resource::{TableIdentity, TableLineage},
};
use serde::{Deserialize, Serialize, Serializer};
use storage_types::{StorageError, StorageResult};

use super::{publication_scope::SourcePublicationScope, source_floor::SourcePublicationFloors};
use crate::native_record::corrupt_record;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceFloorGroup {
    pub(super) lineage: TableLineage,
    pub(super) origin: OriginId,
    pub(super) table: TableIdentity,
    pub(super) floors: Vec<(JournalPartition, OriginPosition)>,
}

impl SourceFloorGroup {
    pub(super) fn from_scope(scope: SourcePublicationScope<'_>) -> Self {
        let mut origin = scope.origin.clone();
        origin.journal_partition = 0.into();
        Self {
            lineage: scope.lineage,
            origin,
            table: scope.table,
            floors: Vec::new(),
        }
    }

    pub(super) fn scope(&self) -> SourcePublicationScope<'_> {
        SourcePublicationScope::new(self.lineage, &self.origin, self.table)
    }

    pub(super) fn compare_scope(&self, scope: SourcePublicationScope<'_>) -> std::cmp::Ordering {
        (
            self.lineage,
            &self.origin.region,
            self.origin.database_group,
            self.origin.journal_generation,
            self.table,
        )
            .cmp(&(
                scope.lineage,
                &scope.origin.region,
                scope.origin.database_group,
                scope.origin.journal_generation,
                scope.table,
            ))
    }

    pub(super) fn advance_floor(
        &mut self,
        partition: JournalPartition,
        through: OriginPosition,
    ) -> StorageResult<()> {
        match self
            .floors
            .binary_search_by_key(&partition, |floor| floor.0)
        {
            Ok(index) => self.floors[index].1 = self.floors[index].1.max(through),
            Err(index) => {
                self.floors
                    .try_reserve(1)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                self.floors.insert(index, (partition, through));
            }
        }
        Ok(())
    }

    pub(super) fn floor_for(&self, partition: JournalPartition) -> StorageResult<OriginPosition> {
        let index = self
            .floors
            .binary_search_by_key(&partition, |floor| floor.0)
            .map_err(|_| corrupt_record())?;
        Ok(self.floors[index].1)
    }

    fn require_pairs(&self) -> StorageResult<()> {
        if u16::from(self.origin.journal_partition) != 0
            || self.floors.is_empty()
            || self.floors.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl TryFrom<Vec<SourceFloorGroup>> for SourcePublicationFloors {
    type Error = StorageError;

    fn try_from(groups: Vec<SourceFloorGroup>) -> StorageResult<Self> {
        let floors = Self(groups);
        floors.require_groups()?;
        Ok(floors)
    }
}

impl SourcePublicationFloors {
    pub(super) fn require_groups(&self) -> StorageResult<()> {
        if self.0.is_empty()
            || self
                .0
                .windows(2)
                .any(|pair| pair[0].scope() >= pair[1].scope())
        {
            return Err(corrupt_record());
        }
        self.0.iter().try_fold(0usize, |count, group| {
            group.require_pairs()?;
            count
                .checked_add(group.floors.len())
                .ok_or_else(corrupt_record)
        })?;
        Ok(())
    }
}

impl Serialize for SourcePublicationFloors {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.require_groups()
            .map_err(|_| serde::ser::Error::custom("invalid source floor inventory"))?;
        self.0.serialize(serializer)
    }
}
