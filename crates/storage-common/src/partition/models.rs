use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use storage_types::StreamItemId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionFamilyKind {
    OrderedLog,
    StandardQueue,
}

impl Display for PartitionFamilyKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::OrderedLog => "ordered-log",
            Self::StandardQueue => "standard-queue",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionRoutingStrategy {
    HashKeyOrdered,
    StandardQueue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionState {
    Open,
    WriteClosed,
    Draining,
    Retired,
}

impl Display for PartitionState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Open => "open",
            Self::WriteClosed => "write-closed",
            Self::Draining => "draining",
            Self::Retired => "retired",
        })
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PiControllerState {
    pub ewma_pressure: f64,
    pub integral: f64,
    pub high_streak: u32,
    pub low_streak: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionFamilyConfig {
    pub family_kind: PartitionFamilyKind,
    pub routing_strategy: PartitionRoutingStrategy,
    pub partition_count: u16,
    pub autoscale_enabled: bool,
    pub min_open_partitions: u16,
    pub max_open_partitions: u16,
    pub family_epoch: u64,
    pub freeze: bool,
    pub target_writes_per_second: u64,
    pub target_bytes_per_second: u64,
    pub target_conflicts_per_window: u64,
    pub target_oldest_visible_age_ms: u64,
    pub cooldown_until_ms: Option<i64>,
    pub controller: PiControllerState,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_id: u16,
    pub placement_slot: u16,
    pub state: PartitionState,
    pub opened_after_id: Option<StreamItemId>,
    pub sealed_after_id: Option<StreamItemId>,
    pub hash_start_inclusive: u64,
    pub hash_end_exclusive: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ResolvedPartitionFamily {
    pub config: PartitionFamilyConfig,
    pub partitions: Vec<PartitionInfo>,
    pub memberships: Vec<PartitionParentChildren>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PartitionLoadSample {
    pub writes: u64,
    pub bytes: u64,
    pub conflicts: u64,
    pub routing_key_bucket_bitmap: u64,
    pub queue_scan_work: u64,
    pub queue_claim_conflicts: u64,
    pub oldest_visible_age_ms: u64,
    pub visible_count: u64,
    pub invisible_count: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionLoadSampleRecord {
    pub partition_id: u16,
    pub window_start_ms: i64,
    pub publisher_id: String,
    pub sample: PartitionLoadSample,
}

pub struct PartitionLoadObservation {
    pub partition_id: u16,
    pub sample: PartitionLoadSample,
    pub queue_empty: bool,
}

pub struct PartitionFamilyReconciliation {
    pub expected_epoch: u64,
    pub write_required: bool,
    pub family: ResolvedPartitionFamily,
    pub topology_change: Option<PartitionTopologyChange>,
}

pub enum PartitionTopologyChange {
    SplitOrderedLog {
        parent_partition_id: u16,
        left_child_partition_id: u16,
        right_child_partition_id: u16,
    },
    AddQueuePartition {
        partition_id: u16,
    },
    BeginQueueDrain {
        partition_id: u16,
    },
    RetireQueuePartition {
        partition_id: u16,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionParentChildren {
    pub parent_partition_id: u16,
    pub left_child_partition_id: u16,
    pub right_child_partition_id: u16,
    pub boundary: Option<StreamItemId>,
}
