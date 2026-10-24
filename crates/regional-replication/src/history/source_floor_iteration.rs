//! Group cursors preserve full publication ordering without an expanded floor
//! inventory.
use super::{
    source_floor::{SourcePublicationFloor, SourcePublicationFloors},
    source_floor_encoding::SourceFloorGroup,
};

pub(super) struct SourceFloorScopes<'floor> {
    groups: &'floor [SourceFloorGroup],
    positions: Vec<usize>,
}

impl SourcePublicationFloors {
    pub(super) fn scope_cursor(&self) -> SourceFloorScopes<'_> {
        SourceFloorScopes {
            groups: &self.0,
            positions: vec![0; self.0.len()],
        }
    }
}

impl Iterator for SourceFloorScopes<'_> {
    type Item = SourcePublicationFloor;

    fn next(&mut self) -> Option<Self::Item> {
        let selected = self
            .groups
            .iter()
            .enumerate()
            .filter_map(|(index, group)| {
                let (partition, _) = group.floors.get(self.positions[index])?;
                Some((
                    index,
                    (
                        group.lineage,
                        &group.origin.region,
                        group.origin.database_group,
                        *partition,
                        group.origin.journal_generation,
                        group.table,
                    ),
                ))
            })
            .min_by(|left, right| left.1.cmp(&right.1))
            .map(|entry| entry.0)?;
        let group = &self.groups[selected];
        let (partition, through) = group.floors[self.positions[selected]];
        self.positions[selected] += 1;
        let mut origin = group.origin.clone();
        origin.journal_partition = partition;
        Some(SourcePublicationFloor {
            lineage: group.lineage,
            origin,
            table: group.table,
            through,
        })
    }
}
