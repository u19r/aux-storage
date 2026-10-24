use std::collections::BTreeMap;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    event::order::HybridLogicalClock, identity::journal_partition::JournalPartition,
};
use storage_types::StorageResult;

use crate::journal::{JournalStore, PartitionAllocation};

pub(super) async fn allocate_partitions<A: PhysicalRecordAttempt>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (
        impl Iterator<Item = (JournalPartition, Option<HybridLogicalClock>)> + Send,
        u64,
        Option<HybridLogicalClock>,
    ),
) -> StorageResult<BTreeMap<JournalPartition, PartitionAllocation>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (mutations, wall_ms, mut observed) = input;
        let mut partitions = BTreeMap::new();
        for (partition, clock) in mutations {
            observed = observed.max(clock);
            partitions.entry(partition).or_insert(());
        }
        let partitions = partitions.into_keys().collect::<Vec<_>>();
        let observations = journal
            .observe_partitions(
                attempt,
                &partitions,
                storage_types::ReadProtection::Conflict,
            )
            .await?;
        let floor = observations
            .iter()
            .map(|observation| observation.floor())
            .max()
            .unwrap_or(HybridLogicalClock {
                physical_ms: 0,
                logical: 0,
            });
        let clock = journal.issue_regional_clock((floor, wall_ms), observed)?;
        let mut allocations = BTreeMap::new();
        let mut changes = Vec::with_capacity(observations.len());
        for observation in observations {
            let (allocation, change) = journal.prepare_partition_allocation(observation, clock)?;
            allocations.insert(allocation.origin().journal_partition, allocation);
            changes.push(change);
        }
        journal.records.store(attempt, &changes).await?;
        Ok(allocations)
    }
    .await;
    settlement.complete(result)
}
