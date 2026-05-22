#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeValue, StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis,
};
use stream_provider::StreamPartitioningMode;
use uuid::Uuid;

pub use crate::constants::{
    DEFAULT_ORDERED_LOG_PARTITION_COUNT, DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND,
    DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW, DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS,
    DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND, DEFAULT_STANDARD_QUEUE_PARTITION_COUNT,
};
use crate::{constants::PARTITION_AUTOSCALE_COOLDOWN_MS, newtypes::MessageVisibilityKey};

const ORDERED_LOG_DATA_PREFIX: &str = "plog";
const STANDARD_QUEUE_DATA_PREFIX: &str = "pqueue";
const PARTITION_CONTROL_PREFIX: &str = "sys/partition-control";
const STREAM_TABLE_SUFFIX: &[u8] = b"/stream-table";
const STREAM_ITEM_SEGMENT: &[u8] = b"/stream-item/";
const SYSTEM_STREAM_NAME: &[u8] = b"system-streams/tables";
const HASH_SPACE_SIZE: u128 = (u64::MAX as u128) + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionFamilyKind {
    OrderedLog,
    StandardQueue,
}

impl PartitionFamilyKind {
    fn key_component(self) -> &'static str {
        match self {
            Self::OrderedLog => "ordered-log",
            Self::StandardQueue => "standard-queue",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionRoutingStrategy {
    HashKeyOrdered,
    StandardQueue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionState {
    Open,
    WriteClosed,
    Draining,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionTransitionError {
    from: PartitionState,
    to: PartitionState,
}

impl PartitionTransitionError {
    #[must_use]
    pub const fn new(from: PartitionState, to: PartitionState) -> Self {
        Self { from, to }
    }

    #[must_use]
    pub const fn from(self) -> PartitionState {
        self.from
    }

    #[must_use]
    pub const fn to(self) -> PartitionState {
        self.to
    }
}

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PiControllerState {
    pub ewma_pressure: f64,
    pub integral: f64,
    pub high_streak: u32,
    pub low_streak: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFamilyConfig {
    pub family_kind: PartitionFamilyKind,
    pub routing_strategy: PartitionRoutingStrategy,
    pub partition_count: u16,
    pub autoscale_enabled: bool,
    pub min_open_partitions: u16,
    pub max_open_partitions: u16,
    pub family_epoch: u64,
    pub freeze: bool,
    #[serde(default = "default_partition_target_writes_per_second")]
    pub target_writes_per_second: u64,
    #[serde(default = "default_partition_target_bytes_per_second")]
    pub target_bytes_per_second: u64,
    #[serde(default = "default_partition_target_conflicts_per_window")]
    pub target_conflicts_per_window: u64,
    #[serde(default = "default_partition_target_oldest_visible_age_ms")]
    pub target_oldest_visible_age_ms: u64,
    #[serde(default)]
    pub cooldown_until_ms: Option<i64>,
    pub controller: PiControllerState,
}

impl PartitionFamilyConfig {
    pub fn note_topology_change(&mut self, now_ms: i64) {
        self.family_epoch = self.family_epoch.saturating_add(1);
        self.cooldown_until_ms = Some(now_ms.saturating_add(PARTITION_AUTOSCALE_COOLDOWN_MS));
        self.controller.high_streak = 0;
        self.controller.low_streak = 0;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_id: u16,
    pub placement_slot: u16,
    pub state: PartitionState,
    pub opened_after_id: Option<StreamItemId>,
    pub sealed_after_id: Option<StreamItemId>,
    pub hash_start_inclusive: u64,
    pub hash_end_exclusive: Option<u64>,
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

#[derive(Debug, Clone)]
pub struct ResolvedPartitionFamily {
    pub config: PartitionFamilyConfig,
    pub partitions: Vec<PartitionInfo>,
}

impl ResolvedPartitionFamily {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionLoadSample {
    pub writes: u64,
    pub bytes: u64,
    pub conflicts: u64,
    #[serde(default)]
    pub routing_key_bucket_bitmap: u64,
    pub queue_scan_work: u64,
    pub queue_claim_conflicts: u64,
    pub oldest_visible_age_ms: u64,
    pub visible_count: u64,
    pub invisible_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLoadSampleRecord {
    #[serde(default)]
    pub partition_id: u16,
    pub window_start_ms: i64,
    pub publisher_id: String,
    pub sample: PartitionLoadSample,
}

#[derive(Debug, Clone)]
pub struct RuntimePartitionLoadSample {
    pub family_kind: PartitionFamilyKind,
    pub family_component: String,
    pub partition_id: u16,
    pub sample: PartitionLoadSample,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPartitionMarker {
    pub partitioning_mode: StreamPartitioningMode,
    pub partition_count: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePartitionMarker {
    pub partition_count: u16,
}

const PARTITION_LOAD_SAMPLE_SEGMENT: &str = "samples";
const ORDERED_LOG_SPLIT_SEGMENT: &str = "splits";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedLogSplitMarker {
    pub parent_partition_id: u16,
    pub left_child_partition_id: u16,
    pub right_child_partition_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedLogSplitBoundary {
    pub parent_partition_id: u16,
    pub left_child_partition_id: u16,
    pub right_child_partition_id: u16,
    pub boundary: StreamItemId,
}

const fn default_partition_target_writes_per_second() -> u64 {
    DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND
}

const fn default_partition_target_bytes_per_second() -> u64 {
    DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND
}

const fn default_partition_target_conflicts_per_window() -> u64 {
    DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW
}

const fn default_partition_target_oldest_visible_age_ms() -> u64 {
    DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueReceiptHandleData {
    pub partition_id: u16,
    pub message_id_hex: String,
    pub visibility_timestamp_ms: i64,
    pub delivery_attempt: u32,
    pub claim_nonce: String,
}

impl QueueReceiptHandleData {
    pub fn encode(&self) -> StorageResult<String> {
        Ok(format!(
            "{:04x}.{}.{}.{}.{}",
            self.partition_id,
            self.message_id_hex,
            self.visibility_timestamp_ms,
            self.delivery_attempt,
            self.claim_nonce
        ))
    }

    pub fn decode(value: &str) -> StorageResult<Self> {
        let mut parts = value.split('.');
        let partition_id = parts
            .next()
            .ok_or_else(|| StorageError::internal("queue receipt handle missing partition id"))
            .and_then(|value| {
                u16::from_str_radix(value, 16).map_err(|error| {
                    StorageError::internal(&format!("parse queue partition id: {error}"))
                })
            })?;
        let message_id_hex = parts
            .next()
            .ok_or_else(|| StorageError::internal("queue receipt handle missing message id"))?
            .to_string();
        let visibility_timestamp_ms = parts
            .next()
            .ok_or_else(|| {
                StorageError::internal("queue receipt handle missing visibility timestamp")
            })
            .and_then(|value| {
                value.parse::<i64>().map_err(|error| {
                    StorageError::internal(&format!("parse queue visibility timestamp: {error}"))
                })
            })?;
        let delivery_attempt = parts
            .next()
            .ok_or_else(|| StorageError::internal("queue receipt handle missing delivery attempt"))
            .and_then(|value| {
                value.parse::<u32>().map_err(|error| {
                    StorageError::internal(&format!("parse queue delivery attempt: {error}"))
                })
            })?;
        let claim_nonce = parts
            .next()
            .ok_or_else(|| StorageError::internal("queue receipt handle missing claim nonce"))?
            .to_string();
        if parts.next().is_some() {
            return Err(StorageError::internal(
                "queue receipt handle contains unexpected trailing data",
            ));
        }
        Ok(Self {
            partition_id,
            message_id_hex,
            visibility_timestamp_ms,
            delivery_attempt,
            claim_nonce,
        })
    }
}

#[must_use]
pub fn default_partition_family_config(
    family_kind: PartitionFamilyKind,
    partition_count: u16,
) -> PartitionFamilyConfig {
    let routing_strategy = match family_kind {
        PartitionFamilyKind::OrderedLog => PartitionRoutingStrategy::HashKeyOrdered,
        PartitionFamilyKind::StandardQueue => PartitionRoutingStrategy::StandardQueue,
    };
    PartitionFamilyConfig {
        family_kind,
        routing_strategy,
        partition_count,
        autoscale_enabled: true,
        min_open_partitions: partition_count,
        max_open_partitions: 256,
        family_epoch: 0,
        freeze: false,
        target_writes_per_second: DEFAULT_PARTITION_TARGET_WRITES_PER_SECOND,
        target_bytes_per_second: DEFAULT_PARTITION_TARGET_BYTES_PER_SECOND,
        target_conflicts_per_window: DEFAULT_PARTITION_TARGET_CONFLICTS_PER_WINDOW,
        target_oldest_visible_age_ms: match family_kind {
            PartitionFamilyKind::OrderedLog => 0,
            PartitionFamilyKind::StandardQueue => DEFAULT_PARTITION_TARGET_OLDEST_VISIBLE_AGE_MS,
        },
        cooldown_until_ms: None,
        controller: PiControllerState::default(),
    }
}

#[must_use]
pub fn initial_partition_infos(partition_count: u16) -> Vec<PartitionInfo> {
    queue_partition_ids(partition_count)
        .map(|partition_id| {
            let (start, end) = hash_range_for_partition(partition_id, partition_count);
            PartitionInfo::new_open(partition_id, partition_id, start, end)
        })
        .collect()
}

pub fn writable_partitions(partitions: &[PartitionInfo]) -> impl Iterator<Item = &PartitionInfo> {
    partitions
        .iter()
        .filter(|partition| partition.is_writable())
}

pub fn readable_partitions(partitions: &[PartitionInfo]) -> impl Iterator<Item = &PartitionInfo> {
    partitions
        .iter()
        .filter(|partition| partition.is_readable())
}

#[must_use]
pub fn partition_family_config_key(
    family_kind: PartitionFamilyKind,
    family_component: &str,
) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/{family_component}/config",
        family_kind.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn partition_family_epoch_key(
    family_kind: PartitionFamilyKind,
    family_component: &str,
) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/{family_component}/epoch",
        family_kind.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn partition_info_prefix(family_kind: PartitionFamilyKind, family_component: &str) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/{family_component}/partition/",
        family_kind.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn partition_info_key(
    family_kind: PartitionFamilyKind,
    family_component: &str,
    partition_id: u16,
) -> Vec<u8> {
    let mut key = partition_info_prefix(family_kind, family_component);
    key.extend_from_slice(format!("{partition_id:04x}").as_bytes());
    key
}

#[must_use]
pub fn partition_family_kind_prefix(family_kind: PartitionFamilyKind) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/",
        family_kind.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn ordered_log_family_config_key(stream_name: &StreamName) -> Vec<u8> {
    partition_family_config_key(
        PartitionFamilyKind::OrderedLog,
        &ordered_log_family_component(stream_name),
    )
}

#[must_use]
pub fn ordered_log_partition_info_prefix(stream_name: &StreamName) -> Vec<u8> {
    partition_info_prefix(
        PartitionFamilyKind::OrderedLog,
        &ordered_log_family_component(stream_name),
    )
}

#[must_use]
pub fn ordered_log_partition_info_key(stream_name: &StreamName, partition_id: u16) -> Vec<u8> {
    partition_info_key(
        PartitionFamilyKind::OrderedLog,
        &ordered_log_family_component(stream_name),
        partition_id,
    )
}

#[must_use]
pub fn queue_family_config_key(queue_url: &str) -> Vec<u8> {
    partition_family_config_key(
        PartitionFamilyKind::StandardQueue,
        &queue_family_component(queue_url),
    )
}

#[must_use]
pub fn queue_partition_info_prefix(queue_url: &str) -> Vec<u8> {
    partition_info_prefix(
        PartitionFamilyKind::StandardQueue,
        &queue_family_component(queue_url),
    )
}

#[must_use]
pub fn queue_partition_info_key(queue_url: &str, partition_id: u16) -> Vec<u8> {
    partition_info_key(
        PartitionFamilyKind::StandardQueue,
        &queue_family_component(queue_url),
        partition_id,
    )
}

#[must_use]
pub fn partition_load_sample_prefix(
    family_kind: PartitionFamilyKind,
    family_component: &str,
) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/{family_component}/{PARTITION_LOAD_SAMPLE_SEGMENT}/",
        family_kind.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn partition_load_sample_partition_prefix(
    family_kind: PartitionFamilyKind,
    family_component: &str,
    partition_id: u16,
) -> Vec<u8> {
    let mut key = partition_load_sample_prefix(family_kind, family_component);
    key.extend_from_slice(format!("{partition_id:04x}/").as_bytes());
    key
}

#[must_use]
pub fn partition_load_sample_key(
    family_kind: PartitionFamilyKind,
    family_component: &str,
    partition_id: u16,
    window_start_ms: i64,
    publisher_id: &str,
) -> Vec<u8> {
    let mut key =
        partition_load_sample_partition_prefix(family_kind, family_component, partition_id);
    key.extend_from_slice(format!("{window_start_ms:013}/{publisher_id}").as_bytes());
    key
}

#[must_use]
pub fn ordered_log_split_marker_family_prefix(family_component: &str) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/{}/{family_component}/{ORDERED_LOG_SPLIT_SEGMENT}/",
        PartitionFamilyKind::OrderedLog.key_component()
    )
    .into_bytes()
}

#[must_use]
pub fn ordered_log_split_marker_prefix(
    family_component: &str,
    parent_partition_id: u16,
) -> Vec<u8> {
    let mut key = ordered_log_split_marker_family_prefix(family_component);
    key.extend_from_slice(format!("{parent_partition_id:04x}/").as_bytes());
    key
}

pub fn partition_family_config_bytes(config: &PartitionFamilyConfig) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(config)
}

#[must_use]
pub fn partition_family_epoch_bytes(config: &PartitionFamilyConfig) -> Vec<u8> {
    config.family_epoch.to_be_bytes().to_vec()
}

pub fn partition_info_bytes(info: &PartitionInfo) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(info)
}

pub fn parse_partition_family_config(bytes: &[u8]) -> StorageResult<PartitionFamilyConfig> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn parse_partition_info(bytes: &[u8]) -> StorageResult<PartitionInfo> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn partition_load_sample_bytes(sample: &PartitionLoadSampleRecord) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(sample)
}

pub fn ordered_log_split_marker_bytes(marker: &OrderedLogSplitMarker) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(marker)
}

pub fn parse_partition_load_sample(bytes: &[u8]) -> StorageResult<PartitionLoadSampleRecord> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn parse_ordered_log_split_marker(bytes: &[u8]) -> StorageResult<OrderedLogSplitMarker> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn merge_partition_load(target: &mut PartitionLoadSample, delta: &PartitionLoadSample) {
    target.writes = target.writes.saturating_add(delta.writes);
    target.bytes = target.bytes.saturating_add(delta.bytes);
    target.conflicts = target.conflicts.saturating_add(delta.conflicts);
    target.routing_key_bucket_bitmap |= delta.routing_key_bucket_bitmap;
    target.queue_scan_work = target.queue_scan_work.saturating_add(delta.queue_scan_work);
    target.queue_claim_conflicts = target
        .queue_claim_conflicts
        .saturating_add(delta.queue_claim_conflicts);
    target.oldest_visible_age_ms = target
        .oldest_visible_age_ms
        .max(delta.oldest_visible_age_ms);
    target.visible_count = target.visible_count.saturating_add(delta.visible_count);
    target.invisible_count = target.invisible_count.saturating_add(delta.invisible_count);
}

#[must_use]
pub fn routing_key_bucket_bit(routing_key_hash: u64) -> u64 {
    1_u64 << (routing_key_hash % u64::BITS as u64)
}

#[must_use]
pub const fn routing_key_bucket_count(sample: &PartitionLoadSample) -> u32 {
    sample.routing_key_bucket_bitmap.count_ones()
}

#[must_use]
pub fn partition_sample_window_start_ms(timestamp_ms: i64, window_seconds: i64) -> i64 {
    let window_ms = window_seconds.saturating_mul(1_000).max(1);
    timestamp_ms - timestamp_ms.rem_euclid(window_ms)
}

#[must_use]
pub fn partition_sample_retention_cutoff_ms(
    now_ms: i64,
    window_seconds: i64,
    retention_windows: i64,
) -> i64 {
    now_ms.saturating_sub(
        window_seconds
            .saturating_mul(retention_windows)
            .saturating_mul(1_000),
    )
}

#[must_use]
pub fn parse_partition_family_component_from_config_key(
    family_kind: PartitionFamilyKind,
    key: &[u8],
) -> Option<String> {
    let prefix = partition_family_kind_prefix(family_kind);
    let suffix = b"/config";
    let component = key.strip_prefix(prefix.as_slice())?.strip_suffix(suffix)?;
    String::from_utf8(component.to_vec()).ok()
}

#[must_use]
pub fn parse_ordered_log_split_boundary_from_key(
    family_component: &str,
    key: &[u8],
) -> Option<(u16, StreamItemId)> {
    let prefix = ordered_log_split_marker_family_prefix(family_component);
    let suffix = key.strip_prefix(prefix.as_slice())?;
    if suffix.len() != 17 || suffix.get(4).copied()? != b'/' {
        return None;
    }

    let parent_partition_id =
        u16::from_str_radix(std::str::from_utf8(&suffix[..4]).ok()?, 16).ok()?;
    let boundary = StreamItemId::try_from(&suffix[5..]).ok()?;
    Some((parent_partition_id, boundary))
}

#[must_use]
pub fn hash_range_for_partition(partition_id: u16, partition_count: u16) -> (u64, Option<u64>) {
    if partition_count <= 1 {
        return (0, None);
    }

    let start = (u128::from(partition_id) * HASH_SPACE_SIZE) / u128::from(partition_count);
    let end = (u128::from(partition_id + 1) * HASH_SPACE_SIZE) / u128::from(partition_count);
    let end = if end >= HASH_SPACE_SIZE {
        None
    } else {
        Some(u64::try_from(end).unwrap_or(u64::MAX))
    };

    (u64::try_from(start).unwrap_or(0), end)
}

#[must_use]
pub fn ordered_log_hash(routing_key: &[u8]) -> u64 {
    let digest = Uuid::new_v5(&Uuid::NAMESPACE_OID, routing_key).into_bytes();
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

#[must_use]
pub fn partition_contains_hash(partition: &PartitionInfo, hash: u64) -> bool {
    if hash < partition.hash_start_inclusive {
        return false;
    }
    match partition.hash_end_exclusive {
        Some(end) => hash < end,
        None => true,
    }
}

#[must_use]
pub fn find_partition_for_hash(partitions: &[PartitionInfo], hash: u64) -> Option<&PartitionInfo> {
    writable_partitions(partitions).find(|partition| partition_contains_hash(partition, hash))
}

#[must_use]
pub fn find_partition_by_id(
    partitions: &[PartitionInfo],
    partition_id: u16,
) -> Option<&PartitionInfo> {
    partitions
        .iter()
        .find(|partition| partition.partition_id == partition_id)
}

#[must_use]
pub fn open_partition_count(partitions: &[PartitionInfo]) -> u16 {
    u16::try_from(writable_partitions(partitions).count()).unwrap_or(u16::MAX)
}

#[must_use]
pub fn next_partition_id(partitions: &[PartitionInfo]) -> u16 {
    partitions
        .iter()
        .map(|partition| partition.partition_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

#[must_use]
pub fn next_placement_slot(partitions: &[PartitionInfo]) -> u16 {
    partitions
        .iter()
        .map(|partition| partition.placement_slot)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

#[must_use]
pub fn split_partition_children(
    parent: &PartitionInfo,
    left_partition_id: u16,
    right_partition_id: u16,
    left_slot: u16,
    right_slot: u16,
) -> Option<(PartitionInfo, PartitionInfo)> {
    let start = u128::from(parent.hash_start_inclusive);
    let end = parent
        .hash_end_exclusive
        .map_or(HASH_SPACE_SIZE, u128::from);
    if end <= start.saturating_add(1) {
        return None;
    }

    let midpoint = start + ((end - start) / 2);
    if midpoint <= start || midpoint >= end {
        return None;
    }

    let midpoint = u64::try_from(midpoint).ok()?;
    let mut left = PartitionInfo::new_open(
        left_partition_id,
        left_slot,
        parent.hash_start_inclusive,
        Some(midpoint),
    );
    left.opened_after_id = parent.sealed_after_id;
    let mut right = PartitionInfo::new_open(
        right_partition_id,
        right_slot,
        midpoint,
        parent.hash_end_exclusive,
    );
    right.opened_after_id = parent.sealed_after_id;
    Some((left, right))
}

pub fn apply_ordered_log_split_boundaries(
    partitions: &mut [PartitionInfo],
    boundaries: &[OrderedLogSplitBoundary],
) {
    for boundary in boundaries {
        for partition in partitions.iter_mut() {
            if partition.partition_id == boundary.parent_partition_id {
                partition.sealed_after_id = Some(boundary.boundary);
                continue;
            }
            if partition.partition_id == boundary.left_child_partition_id
                || partition.partition_id == boundary.right_child_partition_id
            {
                partition.opened_after_id = Some(boundary.boundary);
            }
        }
    }
}

#[must_use]
pub fn supports_pointer_stream_partitioning(stream_name: &StreamName) -> bool {
    let bytes = stream_name.as_ref();
    bytes == SYSTEM_STREAM_NAME || bytes.ends_with(STREAM_TABLE_SUFFIX)
}

#[must_use]
pub fn is_table_item_stream(stream_name: &StreamName) -> bool {
    stream_name
        .as_ref()
        .windows(STREAM_ITEM_SEGMENT.len())
        .any(|window| window == STREAM_ITEM_SEGMENT)
}

#[must_use]
pub fn ordered_log_partition_for_key(routing_key: &[u8], partition_count: u16) -> u16 {
    if partition_count <= 1 {
        return 0;
    }

    let hash = ordered_log_hash(routing_key);
    u16::try_from((u128::from(hash) * u128::from(partition_count)) / HASH_SPACE_SIZE).unwrap_or(0)
}

#[must_use]
pub fn ordered_log_family_component(stream_name: &StreamName) -> String {
    hex_component(stream_name.as_ref())
}

#[must_use]
pub fn ordered_log_partition_prefix(stream_name: &StreamName, partition_id: u16) -> Vec<u8> {
    ordered_log_partition_prefix_with_slot(stream_name, partition_id, partition_id)
}

#[must_use]
pub fn ordered_log_partition_prefix_with_slot(
    stream_name: &StreamName,
    placement_slot: u16,
    partition_id: u16,
) -> Vec<u8> {
    format!(
        "{ORDERED_LOG_DATA_PREFIX}/{placement_slot:04x}/{}/{partition_id:04x}/",
        ordered_log_family_component(stream_name)
    )
    .into_bytes()
}

#[must_use]
pub fn ordered_log_partition_prefixes(
    stream_name: &StreamName,
    partition_count: u16,
) -> Vec<Vec<u8>> {
    (0..partition_count)
        .map(|partition_id| ordered_log_partition_prefix(stream_name, partition_id))
        .collect()
}

#[must_use]
pub fn ordered_log_partition_prefixes_for_infos(
    stream_name: &StreamName,
    partitions: &[PartitionInfo],
) -> Vec<Vec<u8>> {
    readable_partitions(partitions)
        .map(|partition| {
            ordered_log_partition_prefix_with_slot(
                stream_name,
                partition.placement_slot,
                partition.partition_id,
            )
        })
        .collect()
}

pub fn parse_partitioned_stream_item_id(key: &[u8]) -> Option<StreamItemId> {
    if key.len() < 12 {
        return None;
    }
    let mut bytes = [0u8; 12];
    bytes.copy_from_slice(&key[key.len() - 12..]);
    Some(StreamItemId::from(bytes))
}

#[must_use]
pub fn stream_partition_marker_key(stream_name: &StreamName) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/streams/{}/marker",
        ordered_log_family_component(stream_name)
    )
    .into_bytes()
}

#[must_use]
pub fn queue_partition_marker_key(queue_url: &str) -> Vec<u8> {
    format!(
        "{PARTITION_CONTROL_PREFIX}/queues/{}/marker",
        queue_family_component(queue_url)
    )
    .into_bytes()
}

#[must_use]
pub fn queue_wake_key(queue_url: &str) -> Vec<u8> {
    format!(
        "{STANDARD_QUEUE_DATA_PREFIX}/{}/wake",
        queue_family_component(queue_url)
    )
    .into_bytes()
}

#[must_use]
pub fn queue_ready_hint_prefix(queue_url: &str) -> Vec<u8> {
    format!(
        "{STANDARD_QUEUE_DATA_PREFIX}/{}/ready_hint/",
        queue_family_component(queue_url)
    )
    .into_bytes()
}

#[must_use]
pub fn queue_ready_hint_key(queue_url: &str, placement_slot: u16, partition_id: u16) -> Vec<u8> {
    let mut key = queue_ready_hint_prefix(queue_url);
    key.extend_from_slice(format!("{placement_slot:04x}:{partition_id:04x}").as_bytes());
    key
}

pub fn queue_ready_hint_bytes(partition_id: u16, next_visible_at: TimestampMillis) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(10);
    bytes.extend_from_slice(&partition_id.to_be_bytes());
    bytes.extend_from_slice(&next_visible_at.timestamp_millis().to_be_bytes());
    bytes
}

#[must_use]
pub fn queue_family_component(queue_url: &str) -> String {
    hex_component(queue_url.as_bytes())
}

#[must_use]
pub fn queue_partition_prefix(queue_url: &str, partition_id: u16) -> Vec<u8> {
    queue_partition_prefix_with_slot(queue_url, partition_id, partition_id)
}

#[must_use]
pub fn queue_partition_prefix_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
) -> Vec<u8> {
    format!(
        "{STANDARD_QUEUE_DATA_PREFIX}/{placement_slot:04x}/{}/{partition_id:04x}/",
        queue_family_component(queue_url)
    )
    .into_bytes()
}

#[must_use]
pub fn queue_body_key(queue_url: &str, partition_id: u16, message_id_hex: &str) -> Vec<u8> {
    queue_body_key_with_slot(queue_url, partition_id, partition_id, message_id_hex)
}

#[must_use]
pub fn queue_body_prefix_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
) -> Vec<u8> {
    let mut key = queue_partition_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(b"body/");
    key
}

#[must_use]
pub fn queue_body_key_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
    message_id_hex: &str,
) -> Vec<u8> {
    let mut key = queue_body_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(message_id_hex.as_bytes());
    key
}

#[must_use]
pub fn queue_payload_key_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
    message_id_hex: &str,
) -> Vec<u8> {
    queue_body_key_with_slot(queue_url, placement_slot, partition_id, message_id_hex)
}

#[must_use]
pub fn queue_state_key(queue_url: &str, partition_id: u16, message_id_hex: &str) -> Vec<u8> {
    queue_state_key_with_slot(queue_url, partition_id, partition_id, message_id_hex)
}

#[must_use]
pub fn queue_state_key_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
    message_id_hex: &str,
) -> Vec<u8> {
    let mut key = queue_partition_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(b"state/");
    key.extend_from_slice(message_id_hex.as_bytes());
    key
}

#[must_use]
pub fn queue_ready_prefix(queue_url: &str, partition_id: u16) -> Vec<u8> {
    queue_ready_prefix_with_slot(queue_url, partition_id, partition_id)
}

#[must_use]
pub fn queue_ready_prefix_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
) -> Vec<u8> {
    let mut key = queue_partition_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(b"ready/");
    key
}

#[must_use]
pub fn queue_ready_key(
    queue_url: &str,
    partition_id: u16,
    visibility_key: &MessageVisibilityKey,
) -> Vec<u8> {
    queue_ready_key_with_slot(queue_url, partition_id, partition_id, visibility_key)
}

#[must_use]
pub fn queue_ready_key_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
    visibility_key: &MessageVisibilityKey,
) -> Vec<u8> {
    let mut key = queue_ready_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(visibility_key.as_bytes());
    key
}

#[must_use]
pub fn queue_checkpoint_key(queue_url: &str, partition_id: u16, message_id_hex: &str) -> Vec<u8> {
    queue_checkpoint_key_with_slot(queue_url, partition_id, partition_id, message_id_hex)
}

#[must_use]
pub fn queue_checkpoint_key_with_slot(
    queue_url: &str,
    placement_slot: u16,
    partition_id: u16,
    message_id_hex: &str,
) -> Vec<u8> {
    let mut key = queue_partition_prefix_with_slot(queue_url, placement_slot, partition_id);
    key.extend_from_slice(b"checkpoint/");
    key.extend_from_slice(message_id_hex.as_bytes());
    key
}

pub fn queue_partition_ids(partition_count: u16) -> impl Iterator<Item = u16> {
    0..partition_count
}

pub fn stream_partition_marker_bytes(partition_count: u16) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&StreamPartitionMarker {
        partitioning_mode: StreamPartitioningMode::KeyOrdered,
        partition_count,
    })
}

pub fn queue_partition_marker_bytes(partition_count: u16) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&QueuePartitionMarker { partition_count })
}

pub fn parse_stream_partition_marker(bytes: &[u8]) -> StorageResult<StreamPartitionMarker> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn parse_queue_partition_marker(bytes: &[u8]) -> StorageResult<QueuePartitionMarker> {
    storage_types::storage_serde::from_bytes(bytes)
}

pub fn wake_value_bytes() -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(&AttributeValue::S(Uuid::now_v7().to_string()))
}

pub fn decode_hex_component(value: &str) -> StorageResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(StorageError::internal("hex component has odd length"));
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk)
            .map_err(|error| StorageError::internal(&format!("decode hex component: {error}")))?;
        let byte = u8::from_str_radix(pair, 16)
            .map_err(|error| StorageError::internal(&format!("decode hex component: {error}")))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn hex_component(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
