use std::num::NonZeroU16;

use sha2::{Digest, Sha256};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use super::{
    PartitionFamilyConfig, PartitionFamilyKind, PartitionInfo, PartitionLoadSample,
    PartitionLoadSampleRecord, PartitionRoutingStrategy, PartitionState, PartitionTransitionError,
    PiControllerState, ResolvedPartitionFamily,
};
use crate::constants::{
    DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND, DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW,
    DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS, DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND,
    PARTITION_AUTOSCALE_COOLDOWN_MS, PARTITION_LOAD_SAMPLE_RATE, PARTITION_PUBLISHER_MAX_BYTES,
    PARTITION_SAMPLE_WINDOW_MS,
};

impl PartitionState {
    #[must_use]
    pub const fn accepts_writes(self) -> bool {
        matches!(self, Self::Open)
    }

    #[must_use]
    pub const fn is_readable(self) -> bool {
        !matches!(self, Self::Retired)
    }

    #[must_use]
    pub const fn is_write_closed(self) -> bool {
        matches!(self, Self::WriteClosed | Self::Draining)
    }

    #[must_use]
    pub const fn is_draining(self) -> bool {
        matches!(self, Self::Draining)
    }

    pub fn transition_to(self, next: Self) -> Result<Self, PartitionTransitionError> {
        match (self, next) {
            (Self::Open, Self::WriteClosed | Self::Draining) | (Self::Draining, Self::Retired) => {
                Ok(next)
            }
            (current, requested) if current == requested => Ok(requested),
            (current, requested) => Err(PartitionTransitionError::new(current, requested)),
        }
    }
}

impl PartitionFamilyConfig {
    fn new(family_kind: PartitionFamilyKind, partition_count: NonZeroU16) -> Self {
        let partition_count = partition_count.get();
        Self {
            family_kind,
            routing_strategy: match family_kind {
                PartitionFamilyKind::OrderedLog => PartitionRoutingStrategy::HashKeyOrdered,
                PartitionFamilyKind::StandardQueue => PartitionRoutingStrategy::StandardQueue,
            },
            partition_count,
            autoscale_enabled: true,
            min_open_partitions: partition_count,
            max_open_partitions: 256.max(partition_count),
            family_epoch: 0,
            freeze: false,
            target_writes_per_second: DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND,
            target_bytes_per_second: DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND,
            target_conflicts_per_window: DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW,
            target_oldest_visible_age_ms: match family_kind {
                PartitionFamilyKind::OrderedLog => 0,
                PartitionFamilyKind::StandardQueue => {
                    DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS
                }
            },
            cooldown_until_ms: None,
            controller: PiControllerState::default(),
        }
    }

    pub fn note_topology_change(&mut self, now_ms: i64) {
        self.family_epoch = self.family_epoch.saturating_add(1);
        self.cooldown_until_ms = Some(now_ms.saturating_add(PARTITION_AUTOSCALE_COOLDOWN_MS));
        self.controller.high_streak = 0;
        self.controller.low_streak = 0;
    }
}

impl PartitionInfo {
    #[must_use]
    pub const fn new_open(
        partition_id: u16,
        placement_slot: u16,
        hash_start_inclusive: u64,
        hash_end_exclusive: Option<u64>,
    ) -> Self {
        Self {
            partition_id,
            placement_slot,
            state: PartitionState::Open,
            opened_after_id: None,
            sealed_after_id: None,
            hash_start_inclusive,
            hash_end_exclusive,
        }
    }

    #[must_use]
    pub const fn is_writable(&self) -> bool {
        self.state.accepts_writes()
    }

    #[must_use]
    pub const fn is_readable(&self) -> bool {
        self.state.is_readable()
    }

    #[must_use]
    pub const fn is_write_closed(&self) -> bool {
        self.state.is_write_closed()
    }

    #[must_use]
    pub const fn is_draining(&self) -> bool {
        self.state.is_draining()
    }

    pub fn mark_write_closed(&mut self) -> Result<(), PartitionTransitionError> {
        self.state = self.state.transition_to(PartitionState::WriteClosed)?;
        Ok(())
    }

    pub fn begin_draining(&mut self) -> Result<(), PartitionTransitionError> {
        self.state = self.state.transition_to(PartitionState::Draining)?;
        Ok(())
    }

    pub fn retire(&mut self) -> Result<(), PartitionTransitionError> {
        self.state = self.state.transition_to(PartitionState::Retired)?;
        Ok(())
    }
}

impl ResolvedPartitionFamily {
    pub fn route_ordered_log(&self, routing_key: &[u8]) -> StorageResult<&PartitionInfo> {
        if self.config.family_kind != PartitionFamilyKind::OrderedLog
            || self.config.routing_strategy != PartitionRoutingStrategy::HashKeyOrdered
        {
            return Err(StorageError::internal(
                "stream partition family has another routing mode",
            ));
        }
        // SHA-256 defines persisted partition placement. Changing the hash
        // would reroute existing keys and requires an explicit format cutover.
        let digest = Sha256::digest(routing_key);
        let hash = u64::from_be_bytes(
            digest[..8]
                .try_into()
                .map_err(|_| StorageError::internal("stream routing digest has invalid width"))?,
        );
        self.partitions
            .iter()
            .find(|partition| {
                partition.is_writable()
                    && hash >= partition.hash_start_inclusive
                    && partition.hash_end_exclusive.is_none_or(|end| hash < end)
            })
            .ok_or_else(|| StorageError::internal("ordered stream has no writable partition"))
    }

    pub fn new(family_kind: PartitionFamilyKind, partition_count: NonZeroU16) -> Self {
        let mut partitions = Vec::with_capacity(usize::from(partition_count.get()));
        for partition_id in 0..partition_count.get() {
            let (hash_start_inclusive, hash_end_exclusive) =
                initial_hash_range(partition_id, partition_count);
            partitions.push(PartitionInfo::new_open(
                partition_id,
                partition_id,
                hash_start_inclusive,
                hash_end_exclusive,
            ));
        }
        Self {
            config: PartitionFamilyConfig::new(family_kind, partition_count),
            partitions,
            memberships: Vec::new(),
        }
    }

    pub fn sort_by_hash_range(&mut self) {
        self.partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then(left.partition_id.cmp(&right.partition_id))
        });
    }

    pub fn sort_by_partition_id(&mut self) {
        self.partitions
            .sort_unstable_by_key(|partition| partition.partition_id);
    }

    pub fn refresh_partition_count(&mut self) {
        self.config.partition_count = self.managed_partition_count();
    }

    pub fn partition_mut(&mut self, partition_id: u16) -> Option<&mut PartitionInfo> {
        self.partitions
            .iter_mut()
            .find(|partition| partition.partition_id == partition_id)
    }

    #[must_use]
    pub fn managed_partition_count(&self) -> u16 {
        u16::try_from(
            self.partitions
                .iter()
                .filter(|partition| partition.is_readable())
                .count(),
        )
        .unwrap_or(u16::MAX)
    }
}

fn initial_hash_range(partition_id: u16, partition_count: NonZeroU16) -> (u64, Option<u64>) {
    const HASH_SPACE_SIZE: u128 = (u64::MAX as u128) + 1;
    let start = (u128::from(partition_id) * HASH_SPACE_SIZE) / u128::from(partition_count.get());
    let end = (u128::from(partition_id.saturating_add(1)) * HASH_SPACE_SIZE)
        / u128::from(partition_count.get());
    let end = if end >= HASH_SPACE_SIZE {
        None
    } else {
        Some(u64::try_from(end).unwrap_or(u64::MAX))
    };
    (u64::try_from(start).unwrap_or(0), end)
}

impl PartitionInfo {
    /// One unbiased append sample carries inverse-probability pressure and an
    /// operation identity captured before retry. Its write joins the item
    /// commit.
    pub fn sample_append(
        &self,
        key: &[u8],
        bytes: usize,
        entropy: u128,
        at: TimestampMillis,
    ) -> Option<PartitionLoadSampleRecord> {
        if !entropy.is_multiple_of(u128::from(PARTITION_LOAD_SAMPLE_RATE)) {
            // Sampling estimates load only; omitting this telemetry cannot
            // change the item transaction or partition authority. The selected
            // 1/64 cohort carries inverse-probability weights, and retries
            // reuse the entropy.
            return None;
        }
        let bucket = Sha256::digest(key)[0] & 63;
        Some(PartitionLoadSampleRecord {
            partition_id: self.partition_id,
            window_start_ms: at
                .timestamp_millis()
                .saturating_sub(at.timestamp_millis().rem_euclid(PARTITION_SAMPLE_WINDOW_MS)),
            publisher_id: format!("{entropy:032x}"),
            sample: PartitionLoadSample {
                writes: PARTITION_LOAD_SAMPLE_RATE,
                bytes: u64::try_from(bytes)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(PARTITION_LOAD_SAMPLE_RATE),
                routing_key_bucket_bitmap: 1u64 << bucket,
                ..Default::default()
            },
        })
    }
}

impl PartitionLoadSampleRecord {
    pub fn validate_for(&self, family: &ResolvedPartitionFamily) -> StorageResult<()> {
        if self.publisher_id.is_empty()
            || self.publisher_id.len() > PARTITION_PUBLISHER_MAX_BYTES
            || !family
                .partitions
                .iter()
                .any(|partition| partition.partition_id == self.partition_id)
        {
            return Err(StorageError::validation(
                "invalid partition load publisher or member",
            ));
        }
        Ok(())
    }
}
