use storage_types::{StorageError, StorageResult};

use super::{
    PartitionFamilyKind, PartitionParentChildren, PartitionRoutingStrategy, PartitionState,
    ResolvedPartitionFamily,
};

impl ResolvedPartitionFamily {
    /// Validate the single durable family record before publishing topology.
    pub fn validate(&self) -> StorageResult<()> {
        self.validate_topology()?;
        if self.config.partition_count != readable_partition_count(self)? {
            return Err(corruption("partition family member count is inconsistent"));
        }
        for membership in &self.memberships {
            if self
                .partition_by_id(membership.parent_partition_id)?
                .sealed_after_id
                != membership.boundary
                || self
                    .partition_by_id(membership.left_child_partition_id)?
                    .opened_after_id
                    != membership.boundary
                || self
                    .partition_by_id(membership.right_child_partition_id)?
                    .opened_after_id
                    != membership.boundary
            {
                return Err(corruption("partition split boundaries are inconsistent"));
            }
        }
        Ok(())
    }

    fn validate_topology(&self) -> StorageResult<()> {
        let expected_routing = match self.config.family_kind {
            PartitionFamilyKind::OrderedLog => PartitionRoutingStrategy::HashKeyOrdered,
            PartitionFamilyKind::StandardQueue => PartitionRoutingStrategy::StandardQueue,
        };
        if self.config.routing_strategy != expected_routing {
            return Err(corruption(
                "partition family routing strategy is inconsistent",
            ));
        }
        if self.partitions.is_empty() {
            return Err(corruption("partition family has no member rows"));
        }
        let mut partition_ids = self
            .partitions
            .iter()
            .map(|partition| partition.partition_id)
            .collect::<Vec<_>>();
        partition_ids.sort_unstable();
        if partition_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corruption("partition family contains duplicate member ids"));
        }
        let mut placement_slots = self
            .partitions
            .iter()
            .map(|partition| partition.placement_slot)
            .collect::<Vec<_>>();
        placement_slots.sort_unstable();
        if placement_slots.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corruption(
                "partition family contains duplicate placement slots",
            ));
        }
        let open_count = self
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        if open_count == 0
            || open_count < usize::from(self.config.min_open_partitions)
            || open_count > usize::from(self.config.max_open_partitions)
        {
            return Err(corruption("partition family open member count is invalid"));
        }
        match self.config.family_kind {
            PartitionFamilyKind::OrderedLog => {
                self.validate_ordered_log_memberships(&self.memberships)?;
                self.validate_ordered_log_ranges()?;
            }
            PartitionFamilyKind::StandardQueue => {
                if !self.memberships.is_empty() {
                    return Err(corruption(
                        "queue partition family contains ordered-log membership rows",
                    ));
                }
                if self
                    .partitions
                    .iter()
                    .any(|partition| partition.state == PartitionState::WriteClosed)
                {
                    return Err(corruption(
                        "queue partition family contains a write-closed member",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_ordered_log_memberships(
        &self,
        memberships: &[PartitionParentChildren],
    ) -> StorageResult<()> {
        if self.partitions.iter().any(|partition| {
            matches!(
                partition.state,
                PartitionState::Draining | PartitionState::Retired
            )
        }) {
            return Err(corruption(
                "ordered-log partition family contains a queue-only member state",
            ));
        }
        let mut parent_ids = memberships
            .iter()
            .map(|membership| membership.parent_partition_id)
            .collect::<Vec<_>>();
        parent_ids.sort_unstable();
        if parent_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corruption(
                "ordered-log partition family contains duplicate parent memberships",
            ));
        }
        let mut children = Vec::with_capacity(memberships.len().saturating_mul(2));
        for membership in memberships {
            if membership.parent_partition_id >= membership.left_child_partition_id
                || membership.parent_partition_id >= membership.right_child_partition_id
                || membership.left_child_partition_id == membership.right_child_partition_id
            {
                return Err(corruption(
                    "ordered-log partition membership has invalid identities",
                ));
            }
            let parent = self.partition_by_id(membership.parent_partition_id)?;
            let left = self.partition_by_id(membership.left_child_partition_id)?;
            let right = self.partition_by_id(membership.right_child_partition_id)?;
            if parent.state != PartitionState::WriteClosed
                || !left.is_readable()
                || !right.is_readable()
                || left.hash_start_inclusive != parent.hash_start_inclusive
                || left.hash_end_exclusive != Some(right.hash_start_inclusive)
                || right.hash_end_exclusive != parent.hash_end_exclusive
            {
                return Err(corruption(
                    "ordered-log parent and child membership is inconsistent",
                ));
            }
            children.push(membership.left_child_partition_id);
            children.push(membership.right_child_partition_id);
        }
        children.sort_unstable();
        if children.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corruption(
                "ordered-log child belongs to more than one parent",
            ));
        }
        let closed_count = self
            .partitions
            .iter()
            .filter(|partition| partition.state == PartitionState::WriteClosed)
            .count();
        if closed_count != memberships.len() {
            return Err(corruption(
                "ordered-log closed members do not match split memberships",
            ));
        }
        Ok(())
    }

    fn validate_ordered_log_ranges(&self) -> StorageResult<()> {
        let mut writable = self
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .collect::<Vec<_>>();
        writable.sort_unstable_by_key(|partition| partition.hash_start_inclusive);
        let mut writable = writable.into_iter();
        let first = writable
            .next()
            .ok_or_else(|| corruption("ordered-log partition family has no writable members"))?;
        if first.hash_start_inclusive != 0
            || first
                .hash_end_exclusive
                .is_some_and(|end| end <= first.hash_start_inclusive)
        {
            return Err(corruption(
                "ordered-log writable ranges do not start at the hash origin",
            ));
        }
        let mut previous_end = first.hash_end_exclusive;
        for partition in writable {
            if previous_end != Some(partition.hash_start_inclusive)
                || partition
                    .hash_end_exclusive
                    .is_some_and(|end| end <= partition.hash_start_inclusive)
            {
                return Err(corruption(
                    "ordered-log writable ranges contain a gap or overlap",
                ));
            }
            previous_end = partition.hash_end_exclusive;
        }
        if previous_end.is_some() {
            return Err(corruption(
                "ordered-log writable ranges do not cover the hash tail",
            ));
        }
        Ok(())
    }

    fn partition_by_id(&self, partition_id: u16) -> StorageResult<&super::PartitionInfo> {
        self.partitions
            .iter()
            .find(|partition| partition.partition_id == partition_id)
            .ok_or_else(|| corruption("partition membership references a missing member"))
    }
}

fn readable_partition_count(family: &ResolvedPartitionFamily) -> StorageResult<u16> {
    u16::try_from(
        family
            .partitions
            .iter()
            .filter(|partition| partition.is_readable())
            .count(),
    )
    .map_err(|_| corruption("partition family has too many readable members"))
}

fn corruption(message: &'static str) -> StorageError {
    StorageError::internal(message)
}
