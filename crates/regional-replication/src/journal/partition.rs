use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    event::order::HybridLogicalClock,
    identity::{
        journal_partition::JournalPartition,
        origin::{OriginId, OriginPosition},
    },
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{initialization::LayoutState, store::JournalStore};
use crate::{constants::ORIGIN_LAYOUT_SUFFIX, native_record::corrupt_record};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionState {
    position: OriginPosition,
    delivery: crate::history::view::DeliveryPosition,
    floor: HybridLogicalClock,
}

pub struct PartitionObservation {
    origin: OriginId,
    revision: Revision,
    state: PartitionState,
}

pub struct PartitionAllocation {
    pub(super) origin: OriginId,
    pub(super) position: OriginPosition,
    pub(super) clock: HybridLogicalClock,
}

impl JournalStore {
    pub(super) fn prepare_initial_partition(
        &self,
        partition: u32,
    ) -> StorageResult<infrastructure_provider::record::RecordChange> {
        let partition = u16::try_from(partition)
            .map_err(|_| corrupt_record())?
            .into();
        self.records.prepare(
            self.partition_key(partition)?,
            None,
            &PartitionState {
                position: 0.into(),
                delivery: 0.into(),
                floor: HybridLogicalClock {
                    physical_ms: 0,
                    logical: 0,
                },
            },
        )
    }

    pub async fn load_partition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partition: JournalPartition,
    ) -> StorageResult<PartitionObservation> {
        self.observe_partition(attempt, partition, ReadProtection::Conflict)
            .await
    }

    pub async fn load_source_head<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partition: JournalPartition,
    ) -> StorageResult<OriginPosition> {
        Ok(self
            .observe_partition(attempt, partition, ReadProtection::Snapshot)
            .await?
            .state
            .position)
    }

    pub async fn load_source_heads<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partitions: &[JournalPartition],
        protection: ReadProtection,
    ) -> StorageResult<Vec<OriginPosition>> {
        Ok(self
            .observe_partitions(attempt, partitions, protection)
            .await?
            .into_iter()
            .map(|observation| observation.state.position)
            .collect())
    }

    async fn require_layout<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let layout_key = self.record_key(ORIGIN_LAYOUT_SUFFIX.to_vec())?;
        let layouts = self
            .records
            .load(attempt, std::slice::from_ref(&layout_key), protection)
            .await?;
        let row = layouts[0].as_ref().ok_or_else(corrupt_record)?;
        let layout: LayoutState = self.records.decode(row)?;
        if layout.version != crate::constants::REGIONAL_LAYOUT_VERSION
            || layout.tenant != self.tenant
            || layout.partitions != self.partitions
            || layout.initialized != u32::from(self.partitions)
        {
            return Err(storage_types::InfrastructureResultKind::Unavailable {
                context: storage_types::InfrastructureContext::RegionalSource,
            }
            .into());
        }
        Ok(())
    }

    async fn observe_partition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partition: JournalPartition,
        protection: ReadProtection,
    ) -> StorageResult<PartitionObservation> {
        self.observe_partitions(attempt, &[partition], protection)
            .await?
            .pop()
            .ok_or_else(corrupt_record)
    }

    pub(super) async fn observe_partitions<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partitions: &[JournalPartition],
        protection: ReadProtection,
    ) -> StorageResult<Vec<PartitionObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let origins = partitions
                .iter()
                .map(|partition| self.origin(*partition))
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = partitions
                .iter()
                .map(|partition| self.partition_key(*partition))
                .collect::<StorageResult<Vec<_>>>()?;
            self.require_layout(attempt, protection).await?;
            let rows = self.records.load(attempt, &keys, protection).await?;
            if rows.len() != origins.len() {
                return Err(corrupt_record());
            }
            origins
                .into_iter()
                .zip(rows)
                .map(|(origin, row)| {
                    let record =
                        row.ok_or(storage_types::InfrastructureResultKind::Unavailable {
                            context: storage_types::InfrastructureContext::RegionalSource,
                        })?;
                    Ok(PartitionObservation {
                        origin,
                        revision: record.revision,
                        state: self.records.decode(&record)?,
                    })
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }

    pub async fn allocate_partition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        observation: PartitionObservation,
        clock: HybridLogicalClock,
    ) -> StorageResult<PartitionAllocation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (allocation, change) = self.prepare_partition_allocation(observation, clock)?;
            self.records.store(attempt, &[change]).await?;
            Ok(allocation)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) fn prepare_partition_allocation(
        &self,
        observation: PartitionObservation,
        clock: HybridLogicalClock,
    ) -> StorageResult<(
        PartitionAllocation,
        infrastructure_provider::record::RecordChange,
    )> {
        if observation.origin != self.origin(observation.origin.journal_partition)?
            || clock <= observation.state.floor
        {
            return Err(corrupt_record());
        }
        let position = observation
            .state
            .position
            .checked_next()
            .map_err(|_| corrupt_record())?;
        let change = self.records.prepare(
            self.partition_key(observation.origin.journal_partition)?,
            Some(observation.revision),
            &PartitionState {
                position,
                floor: clock,
                ..observation.state
            },
        )?;
        Ok((
            PartitionAllocation {
                origin: observation.origin,
                position,
                clock,
            },
            change,
        ))
    }

    pub(in crate::journal) async fn allocate_acceptance_positions<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: &[(JournalPartition, Option<HybridLogicalClock>)],
    ) -> StorageResult<Vec<crate::history::view::DeliveryPosition>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if input.is_empty() {
                return Ok(Vec::new());
            }
            let partitions = input
                .iter()
                .map(|(partition, _)| *partition)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let rows = self
                .observe_partitions(attempt, &partitions, ReadProtection::Conflict)
                .await?;
            let mut observed = partitions
                .into_iter()
                .zip(rows)
                .collect::<std::collections::BTreeMap<_, _>>();
            let mut positions = Vec::with_capacity(input.len());
            for (partition, received) in input {
                let observation = observed.get_mut(partition).ok_or_else(corrupt_record)?;
                observation.state.delivery = observation.state.delivery.checked_next()?;
                if let Some(received) = received {
                    observation.state.floor = observation.state.floor.max(*received);
                }
                positions.push(observation.state.delivery);
            }
            let changes = observed
                .into_iter()
                .map(|(partition, observation)| {
                    self.records.prepare(
                        self.partition_key(partition)?,
                        Some(observation.revision),
                        &observation.state,
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            self.records.store(attempt, &changes).await?;
            Ok(positions)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn observe_delivery_position<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        partition: JournalPartition,
        protection: ReadProtection,
    ) -> StorageResult<crate::history::view::DeliveryPosition> {
        Ok(self
            .observe_partition(attempt, partition, protection)
            .await?
            .state
            .delivery)
    }
}

impl PartitionObservation {
    pub fn floor(&self) -> HybridLogicalClock {
        self.state.floor
    }
}

impl PartitionAllocation {
    pub fn origin(&self) -> &OriginId {
        &self.origin
    }
    pub fn position(&self) -> OriginPosition {
        self.position
    }
    pub fn clock(&self) -> HybridLogicalClock {
        self.clock
    }
}
