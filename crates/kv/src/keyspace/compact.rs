use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

const U48_MAX: u64 = 0x0000_FFFF_FFFF_FFFF;
const STREAM_ITEM_ID_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactKeyError {
    EmptyKey,
    UnknownFamily(u8),
    Truncated {
        family: KeyFamily,
        expected_at_least: usize,
        actual: usize,
    },
    InvalidKind {
        family: KeyFamily,
        kind: u8,
    },
    U48OutOfRange(u64),
}

impl fmt::Display for CompactKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => formatter.write_str("compact key is empty"),
            Self::UnknownFamily(byte) => {
                write!(formatter, "unknown compact key family 0x{byte:02x}")
            }
            Self::Truncated {
                family,
                expected_at_least,
                actual,
            } => write!(
                formatter,
                "truncated compact key family {}: expected at least {expected_at_least} bytes, \
                 got {actual}",
                family.code() as char
            ),
            Self::InvalidKind { family, kind } => write!(
                formatter,
                "invalid compact key kind 0x{kind:02x} for family {}",
                family.code() as char
            ),
            Self::U48OutOfRange(value) => {
                write!(formatter, "value {value} exceeds u48 maximum {U48_MAX}")
            }
        }
    }
}

impl std::error::Error for CompactKeyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFamily {
    TableMetadata,
    TableNameLookup,
    PrimaryItem,
    GsiItem,
    GsiTombstone,
    GsiBackfill,
    TtlConfig,
    TtlDueIndex,
    SystemStreamRow,
    TableStreamRow,
    ItemStreamRow,
    StreamTrimState,
    StreamTrimDue,
    StreamPointerTableIndex,
    StreamPointerItemIndex,
    PartitionControl,
    OrderedLogData,
    PartitionedQueueData,
    QueueMetadata,
    PubsubRecord,
    SyncRecord,
}

impl KeyFamily {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::TableMetadata => b'm',
            Self::TableNameLookup => b'M',
            Self::PrimaryItem => b'p',
            Self::GsiItem => b'g',
            Self::GsiTombstone => b'x',
            Self::GsiBackfill => b'b',
            Self::TtlConfig => b'c',
            Self::TtlDueIndex => b'e',
            Self::SystemStreamRow => b's',
            Self::TableStreamRow => b't',
            Self::ItemStreamRow => b'i',
            Self::StreamTrimState => b'r',
            Self::StreamTrimDue => b'd',
            Self::StreamPointerTableIndex => b'u',
            Self::StreamPointerItemIndex => b'v',
            Self::PartitionControl => b'n',
            Self::OrderedLogData => b'o',
            Self::PartitionedQueueData => b'q',
            Self::QueueMetadata => b'Q',
            Self::PubsubRecord => b'j',
            Self::SyncRecord => b'a',
        }
    }

    pub fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b'm' => Ok(Self::TableMetadata),
            b'M' => Ok(Self::TableNameLookup),
            b'p' => Ok(Self::PrimaryItem),
            b'g' => Ok(Self::GsiItem),
            b'x' => Ok(Self::GsiTombstone),
            b'b' => Ok(Self::GsiBackfill),
            b'c' => Ok(Self::TtlConfig),
            b'e' => Ok(Self::TtlDueIndex),
            b's' => Ok(Self::SystemStreamRow),
            b't' => Ok(Self::TableStreamRow),
            b'i' => Ok(Self::ItemStreamRow),
            b'r' => Ok(Self::StreamTrimState),
            b'd' => Ok(Self::StreamTrimDue),
            b'u' => Ok(Self::StreamPointerTableIndex),
            b'v' => Ok(Self::StreamPointerItemIndex),
            b'n' => Ok(Self::PartitionControl),
            b'o' => Ok(Self::OrderedLogData),
            b'q' => Ok(Self::PartitionedQueueData),
            b'Q' => Ok(Self::QueueMetadata),
            b'j' => Ok(Self::PubsubRecord),
            b'a' => Ok(Self::SyncRecord),
            other => Err(CompactKeyError::UnknownFamily(other)),
        }
    }

    #[must_use]
    pub const fn registry() -> &'static [(KeyFamily, &'static str)] {
        &[
            (Self::TableMetadata, "table metadata"),
            (Self::TableNameLookup, "table-name lookup"),
            (Self::PrimaryItem, "primary item"),
            (Self::GsiItem, "gsi item"),
            (Self::GsiTombstone, "gsi tombstone"),
            (Self::GsiBackfill, "gsi backfill"),
            (Self::TtlConfig, "ttl config"),
            (Self::TtlDueIndex, "ttl due index"),
            (Self::SystemStreamRow, "system stream row"),
            (Self::TableStreamRow, "table stream row"),
            (Self::ItemStreamRow, "item stream row"),
            (Self::StreamTrimState, "stream trim state"),
            (Self::StreamTrimDue, "stream trim due"),
            (Self::StreamPointerTableIndex, "stream pointer table index"),
            (Self::StreamPointerItemIndex, "stream pointer item index"),
            (Self::PartitionControl, "partition control"),
            (Self::OrderedLogData, "ordered log data"),
            (Self::PartitionedQueueData, "partitioned queue data"),
            (Self::QueueMetadata, "queue metadata and lookup"),
            (Self::PubsubRecord, "pubsub record"),
            (Self::SyncRecord, "sync/idempotency record"),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableStorageId(u32);

impl TableStorageId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IndexStorageId(u16);

impl IndexStorageId {
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct U48(u64);

impl U48 {
    pub fn new(value: u64) -> Result<Self, CompactKeyError> {
        if value <= U48_MAX {
            Ok(Self(value))
        } else {
            Err(CompactKeyError::U48OutOfRange(value))
        }
    }

    #[must_use]
    pub const fn masked(value: u64) -> Self {
        Self(value & U48_MAX)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

macro_rules! u48_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(U48);

        impl $name {
            pub fn new(value: u64) -> Result<Self, CompactKeyError> {
                U48::new(value).map(Self)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl From<U48> for $name {
            fn from(value: U48) -> Self {
                Self(value)
            }
        }
    };
}

u48_id!(QueueStorageId);
u48_id!(TopicStorageId);
u48_id!(SubscriptionStorageId);
u48_id!(DeliveryStorageId);
u48_id!(StreamStorageId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMetadataKind {
    Metadata,
    UrlLookup,
    NameLookup,
}

impl QueueMetadataKind {
    const fn code(self) -> u8 {
        match self {
            Self::Metadata => b'm',
            Self::UrlLookup => b'u',
            Self::NameLookup => b'n',
        }
    }

    fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b'm' => Ok(Self::Metadata),
            b'u' => Ok(Self::UrlLookup),
            b'n' => Ok(Self::NameLookup),
            other => Err(CompactKeyError::InvalidKind {
                family: KeyFamily::QueueMetadata,
                kind: other,
            }),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::UrlLookup => "url_lookup",
            Self::NameLookup => "name_lookup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueRecordKind {
    Ready,
    Body,
    State,
    Checkpoint,
    ReadyHint,
    DeleteLedger,
    Wake,
}

impl QueueRecordKind {
    const fn code(self) -> u8 {
        match self {
            Self::Ready => b'r',
            Self::Body => b'b',
            Self::State => b's',
            Self::Checkpoint => b'c',
            Self::ReadyHint => b'h',
            Self::DeleteLedger => b'd',
            Self::Wake => b'w',
        }
    }

    fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b'r' => Ok(Self::Ready),
            b'b' => Ok(Self::Body),
            b's' => Ok(Self::State),
            b'c' => Ok(Self::Checkpoint),
            b'h' => Ok(Self::ReadyHint),
            b'd' => Ok(Self::DeleteLedger),
            b'w' => Ok(Self::Wake),
            other => Err(CompactKeyError::InvalidKind {
                family: KeyFamily::PartitionedQueueData,
                kind: other,
            }),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Body => "body",
            Self::State => "state",
            Self::Checkpoint => "checkpoint",
            Self::ReadyHint => "ready_hint",
            Self::DeleteLedger => "delete_ledger",
            Self::Wake => "wake",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemRecordKind {
    StreamHighWater,
    IdempotencyToken,
    SyncApplyMutation,
    SyncLastApplied,
    SyncLogEntry,
    ItemRevision,
    QueueIdAllocator,
    TopicIdAllocator,
    SubscriptionIdAllocator,
    DeliveryIdAllocator,
}

impl SystemRecordKind {
    const fn code(self) -> u8 {
        match self {
            Self::StreamHighWater => b'h',
            Self::IdempotencyToken => b'i',
            Self::SyncApplyMutation => b'm',
            Self::SyncLastApplied => b'l',
            Self::SyncLogEntry => b'g',
            Self::ItemRevision => b'r',
            Self::QueueIdAllocator => b'q',
            Self::TopicIdAllocator => b't',
            Self::SubscriptionIdAllocator => b's',
            Self::DeliveryIdAllocator => b'd',
        }
    }

    fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b'h' => Ok(Self::StreamHighWater),
            b'i' => Ok(Self::IdempotencyToken),
            b'm' => Ok(Self::SyncApplyMutation),
            b'l' => Ok(Self::SyncLastApplied),
            b'g' => Ok(Self::SyncLogEntry),
            b'r' => Ok(Self::ItemRevision),
            b'q' => Ok(Self::QueueIdAllocator),
            b't' => Ok(Self::TopicIdAllocator),
            b's' => Ok(Self::SubscriptionIdAllocator),
            b'd' => Ok(Self::DeliveryIdAllocator),
            other => Err(CompactKeyError::InvalidKind {
                family: KeyFamily::SyncRecord,
                kind: other,
            }),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::StreamHighWater => "stream_high_water",
            Self::IdempotencyToken => "idempotency_token",
            Self::SyncApplyMutation => "sync_apply_mutation",
            Self::SyncLastApplied => "sync_last_applied",
            Self::SyncLogEntry => "sync_log_entry",
            Self::ItemRevision => "item_revision",
            Self::QueueIdAllocator => "queue_id_allocator",
            Self::TopicIdAllocator => "topic_id_allocator",
            Self::SubscriptionIdAllocator => "subscription_id_allocator",
            Self::DeliveryIdAllocator => "delivery_id_allocator",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionControlKind {
    Config,
    Epoch,
    PartitionInfo,
    LoadSample,
    SplitMarker,
    StreamMarker,
    QueueMarker,
}

impl PartitionControlKind {
    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::Config => b'c',
            Self::Epoch => b'e',
            Self::PartitionInfo => b'p',
            Self::LoadSample => b'l',
            Self::SplitMarker => b's',
            Self::StreamMarker => b'm',
            Self::QueueMarker => b'q',
        }
    }

    fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b'c' => Ok(Self::Config),
            b'e' => Ok(Self::Epoch),
            b'p' => Ok(Self::PartitionInfo),
            b'l' => Ok(Self::LoadSample),
            b's' => Ok(Self::SplitMarker),
            b'm' => Ok(Self::StreamMarker),
            b'q' => Ok(Self::QueueMarker),
            other => Err(CompactKeyError::InvalidKind {
                family: KeyFamily::PartitionControl,
                kind: other,
            }),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Epoch => "epoch",
            Self::PartitionInfo => "partition_info",
            Self::LoadSample => "load_sample",
            Self::SplitMarker => "split_marker",
            Self::StreamMarker => "stream_marker",
            Self::QueueMarker => "queue_marker",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubRecordKind {
    Topic,
    TopicName,
    Subscription,
    SubscriptionTopic,
    SubscriptionDedupe,
    Delivery,
    DeliverySubscription,
    DeliveryClaim,
}

impl PubsubRecordKind {
    const fn code(self) -> u8 {
        match self {
            Self::Topic => b't',
            Self::TopicName => b'n',
            Self::Subscription => b's',
            Self::SubscriptionTopic => b'i',
            Self::SubscriptionDedupe => b'd',
            Self::Delivery => b'v',
            Self::DeliverySubscription => b'u',
            Self::DeliveryClaim => b'c',
        }
    }

    fn from_code(code: u8) -> Result<Self, CompactKeyError> {
        match code {
            b't' => Ok(Self::Topic),
            b'n' => Ok(Self::TopicName),
            b's' => Ok(Self::Subscription),
            b'i' => Ok(Self::SubscriptionTopic),
            b'd' => Ok(Self::SubscriptionDedupe),
            b'v' => Ok(Self::Delivery),
            b'u' => Ok(Self::DeliverySubscription),
            b'c' => Ok(Self::DeliveryClaim),
            other => Err(CompactKeyError::InvalidKind {
                family: KeyFamily::PubsubRecord,
                kind: other,
            }),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::TopicName => "topic_name",
            Self::Subscription => "subscription",
            Self::SubscriptionTopic => "subscription_topic",
            Self::SubscriptionDedupe => "subscription_dedupe",
            Self::Delivery => "delivery",
            Self::DeliverySubscription => "delivery_subscription",
            Self::DeliveryClaim => "delivery_claim",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCompactKey<'a> {
    TableMetadata {
        table_id: TableStorageId,
    },
    TableNameLookup {
        table_name: &'a [u8],
    },
    PrimaryItem {
        table_id: TableStorageId,
        key: &'a [u8],
    },
    GsiItem {
        table_id: TableStorageId,
        index_id: IndexStorageId,
        suffix: &'a [u8],
    },
    GsiTombstone {
        table_id: TableStorageId,
        index_id: IndexStorageId,
        suffix: &'a [u8],
    },
    GsiBackfill {
        table_id: TableStorageId,
        index_id: IndexStorageId,
    },
    TtlConfig {
        table_id: TableStorageId,
    },
    TtlDueIndex {
        table_id: TableStorageId,
        ttl_seconds: u64,
        key: &'a [u8],
    },
    SystemStreamRow {
        stream_item_id: &'a [u8],
    },
    TableStreamRow {
        table_id: TableStorageId,
        stream_item_id: &'a [u8],
    },
    ItemStreamRow {
        table_id: TableStorageId,
        item_scope: &'a [u8],
    },
    StreamPointerTableIndex {
        table_id: TableStorageId,
        stream_item_id: &'a [u8],
    },
    StreamPointerItemIndex {
        table_id: TableStorageId,
        item_scope: &'a [u8],
    },
    StreamTrimState {
        scope_key: &'a [u8],
    },
    StreamTrimDue {
        due_millis: i64,
        scope_key: &'a [u8],
        policy_version: u64,
    },
    PartitionControl {
        kind: PartitionControlKind,
        resource_id: StreamStorageId,
        suffix: &'a [u8],
    },
    OrderedLogData {
        bucket: u8,
        stream_id: StreamStorageId,
        partition_id: u16,
        suffix: &'a [u8],
    },
    PartitionedQueueData {
        bucket: u8,
        queue_id: QueueStorageId,
        partition_id: u16,
        kind: QueueRecordKind,
        suffix: &'a [u8],
    },
    QueueMetadata {
        queue_id: QueueStorageId,
    },
    QueueLookup {
        kind: QueueMetadataKind,
        lookup_key: &'a [u8],
    },
    PubsubRecord {
        kind: PubsubRecordKind,
        left_id: U48,
        right_id: Option<U48>,
        suffix: &'a [u8],
    },
    SyncRecord {
        kind: SystemRecordKind,
        suffix: &'a [u8],
    },
    Other {
        family: KeyFamily,
        payload: &'a [u8],
    },
}

impl ParsedCompactKey<'_> {
    #[must_use]
    pub fn debug_without_metadata(&self) -> CompactKeyDebug {
        self.debug_with_metadata(&CompactKeyMetadata::default())
    }

    #[must_use]
    pub fn debug_with_metadata(&self, metadata: &CompactKeyMetadata<'_>) -> CompactKeyDebug {
        match self {
            Self::TableMetadata { table_id } => CompactKeyDebug::new(
                KeyFamily::TableMetadata,
                format!("table={}", metadata.table_label(*table_id)),
            ),
            Self::TableNameLookup { table_name } => CompactKeyDebug::new(
                KeyFamily::TableNameLookup,
                format!("name={}", printable_bytes(table_name)),
            ),
            Self::PrimaryItem { table_id, key } => CompactKeyDebug::new(
                KeyFamily::PrimaryItem,
                format!(
                    "table={},key={}",
                    metadata.table_label(*table_id),
                    hex_bytes(key)
                ),
            ),
            Self::GsiItem {
                table_id,
                index_id,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::GsiItem,
                format!(
                    "table={},index={},suffix={}",
                    metadata.table_label(*table_id),
                    metadata.index_label(*index_id),
                    hex_bytes(suffix)
                ),
            ),
            Self::GsiTombstone {
                table_id,
                index_id,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::GsiTombstone,
                format!(
                    "table={},index={},suffix={}",
                    metadata.table_label(*table_id),
                    metadata.index_label(*index_id),
                    hex_bytes(suffix)
                ),
            ),
            Self::GsiBackfill { table_id, index_id } => CompactKeyDebug::new(
                KeyFamily::GsiBackfill,
                format!(
                    "table={},index={}",
                    metadata.table_label(*table_id),
                    metadata.index_label(*index_id)
                ),
            ),
            Self::TtlConfig { table_id } => CompactKeyDebug::new(
                KeyFamily::TtlConfig,
                format!("table={}", metadata.table_label(*table_id)),
            ),
            Self::TtlDueIndex {
                table_id,
                ttl_seconds,
                key,
            } => CompactKeyDebug::new(
                KeyFamily::TtlDueIndex,
                format!(
                    "table={},ttl={},key={}",
                    metadata.table_label(*table_id),
                    ttl_seconds,
                    hex_bytes(key)
                ),
            ),
            Self::SystemStreamRow { stream_item_id } => CompactKeyDebug::new(
                KeyFamily::SystemStreamRow,
                format!("id={}", hex_bytes(stream_item_id)),
            ),
            Self::TableStreamRow {
                table_id,
                stream_item_id,
            } => CompactKeyDebug::new(
                KeyFamily::TableStreamRow,
                format!(
                    "table={},id={}",
                    metadata.table_label(*table_id),
                    hex_bytes(stream_item_id)
                ),
            ),
            Self::ItemStreamRow {
                table_id,
                item_scope,
            } => CompactKeyDebug::new(
                KeyFamily::ItemStreamRow,
                format!(
                    "table={},scope={}",
                    metadata.table_label(*table_id),
                    hex_bytes(item_scope)
                ),
            ),
            Self::StreamPointerTableIndex {
                table_id,
                stream_item_id,
            } => CompactKeyDebug::new(
                KeyFamily::StreamPointerTableIndex,
                format!(
                    "table={},id={}",
                    metadata.table_label(*table_id),
                    hex_bytes(stream_item_id)
                ),
            ),
            Self::StreamPointerItemIndex {
                table_id,
                item_scope,
            } => CompactKeyDebug::new(
                KeyFamily::StreamPointerItemIndex,
                format!(
                    "table={},scope={}",
                    metadata.table_label(*table_id),
                    hex_bytes(item_scope)
                ),
            ),
            Self::StreamTrimState { scope_key } => CompactKeyDebug::new(
                KeyFamily::StreamTrimState,
                format!("scope={}", hex_bytes(scope_key)),
            ),
            Self::StreamTrimDue {
                due_millis,
                scope_key,
                policy_version,
            } => CompactKeyDebug::new(
                KeyFamily::StreamTrimDue,
                format!(
                    "due_ms={},scope={},policy={}",
                    due_millis,
                    hex_bytes(scope_key),
                    policy_version
                ),
            ),
            Self::PartitionControl {
                kind,
                resource_id,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::PartitionControl,
                format!(
                    "kind={},resource={},suffix={}",
                    kind.label(),
                    resource_id.get(),
                    hex_bytes(suffix)
                ),
            ),
            Self::OrderedLogData {
                bucket,
                stream_id,
                partition_id,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::OrderedLogData,
                format!(
                    "bucket={},stream={},partition={},suffix={}",
                    bucket,
                    stream_id.get(),
                    partition_id,
                    hex_bytes(suffix)
                ),
            ),
            Self::PartitionedQueueData {
                bucket,
                queue_id,
                partition_id,
                kind,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::PartitionedQueueData,
                format!(
                    "bucket={},queue={},partition={},kind={},suffix={}",
                    bucket,
                    metadata.queue_label(*queue_id),
                    partition_id,
                    kind.label(),
                    hex_bytes(suffix)
                ),
            ),
            Self::QueueMetadata { queue_id } => CompactKeyDebug::new(
                KeyFamily::QueueMetadata,
                format!("queue={}", metadata.queue_label(*queue_id)),
            ),
            Self::QueueLookup { kind, lookup_key } => CompactKeyDebug::new(
                KeyFamily::QueueMetadata,
                format!(
                    "kind={},lookup={}",
                    kind.label(),
                    printable_bytes(lookup_key)
                ),
            ),
            Self::PubsubRecord {
                kind,
                left_id,
                right_id,
                suffix,
            } => CompactKeyDebug::new(
                KeyFamily::PubsubRecord,
                format!(
                    "kind={},left={},right={},suffix={}",
                    kind.label(),
                    left_id.get(),
                    right_id
                        .map(|id| id.get().to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    hex_bytes(suffix)
                ),
            ),
            Self::SyncRecord { kind, suffix } => CompactKeyDebug::new(
                KeyFamily::SyncRecord,
                format!("kind={},suffix={}", kind.label(), hex_bytes(suffix)),
            ),
            Self::Other { family, payload } => {
                CompactKeyDebug::new(*family, format!("payload={}", hex_bytes(payload)))
            }
        }
    }
}

#[derive(Default)]
pub struct CompactKeyMetadata<'a> {
    pub table_name: Option<(TableStorageId, &'a str)>,
    pub index_name: Option<(IndexStorageId, &'a str)>,
    pub queue_name: Option<(QueueStorageId, &'a str)>,
}

impl CompactKeyMetadata<'_> {
    fn table_label(&self, table_id: TableStorageId) -> String {
        self.table_name
            .filter(|(id, _)| *id == table_id)
            .map(|(_, name)| format!("{}:{name}", table_id.get()))
            .unwrap_or_else(|| table_id.get().to_string())
    }

    fn index_label(&self, index_id: IndexStorageId) -> String {
        self.index_name
            .filter(|(id, _)| *id == index_id)
            .map(|(_, name)| format!("{}:{name}", index_id.get()))
            .unwrap_or_else(|| index_id.get().to_string())
    }

    fn queue_label(&self, queue_id: QueueStorageId) -> String {
        self.queue_name
            .filter(|(id, _)| *id == queue_id)
            .map(|(_, name)| format!("{}:{name}", queue_id.get()))
            .unwrap_or_else(|| queue_id.get().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactKeyDebug {
    family: KeyFamily,
    body: String,
}

impl CompactKeyDebug {
    #[must_use]
    pub fn new(family: KeyFamily, body: String) -> Self {
        Self { family, body }
    }
}

impl fmt::Display for CompactKeyDebug {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}({})", self.family.code() as char, self.body)
    }
}

#[must_use]
pub fn table_metadata_key(table_id: TableStorageId) -> Vec<u8> {
    fixed_table_key(KeyFamily::TableMetadata, table_id)
}

#[must_use]
pub fn table_metadata_prefix() -> KeyRange {
    range_for_prefix(vec![KeyFamily::TableMetadata.code()])
}

#[must_use]
pub fn table_name_lookup_key(table_name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + table_name.len());
    key.push(KeyFamily::TableNameLookup.code());
    key.extend_from_slice(table_name);
    key
}

#[must_use]
pub fn primary_item_key(table_id: TableStorageId, encoded_key: &[u8]) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::PrimaryItem, table_id);
    key.extend_from_slice(encoded_key);
    key
}

#[must_use]
pub fn gsi_item_key(table_id: TableStorageId, index_id: IndexStorageId, suffix: &[u8]) -> Vec<u8> {
    let mut key = fixed_table_index_key(KeyFamily::GsiItem, table_id, index_id);
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn gsi_tombstone_key(
    table_id: TableStorageId,
    index_id: IndexStorageId,
    suffix: &[u8],
) -> Vec<u8> {
    let mut key = fixed_table_index_key(KeyFamily::GsiTombstone, table_id, index_id);
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn gsi_backfill_key(table_id: TableStorageId, index_id: IndexStorageId) -> Vec<u8> {
    fixed_table_index_key(KeyFamily::GsiBackfill, table_id, index_id)
}

#[must_use]
pub fn ttl_config_key(table_id: TableStorageId) -> Vec<u8> {
    fixed_table_key(KeyFamily::TtlConfig, table_id)
}

#[must_use]
pub fn ttl_due_key(table_id: TableStorageId, ttl_seconds: u64, encoded_key: &[u8]) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::TtlDueIndex, table_id);
    put_u64(&mut key, ttl_seconds);
    key.extend_from_slice(encoded_key);
    key
}

#[must_use]
pub fn system_stream_key(stream_item_id: &[u8; STREAM_ITEM_ID_LEN]) -> Vec<u8> {
    let mut key = vec![KeyFamily::SystemStreamRow.code()];
    key.extend_from_slice(stream_item_id);
    key
}

#[must_use]
pub fn table_stream_key(
    table_id: TableStorageId,
    stream_item_id: &[u8; STREAM_ITEM_ID_LEN],
) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::TableStreamRow, table_id);
    key.extend_from_slice(stream_item_id);
    key
}

#[must_use]
pub fn item_stream_key(table_id: TableStorageId, item_scope_and_id: &[u8]) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::ItemStreamRow, table_id);
    key.extend_from_slice(item_scope_and_id);
    key
}

#[must_use]
pub fn stream_pointer_table_key(
    table_id: TableStorageId,
    stream_item_id: &[u8; STREAM_ITEM_ID_LEN],
) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::StreamPointerTableIndex, table_id);
    key.extend_from_slice(stream_item_id);
    key
}

#[must_use]
pub fn stream_pointer_item_key(table_id: TableStorageId, item_scope_and_id: &[u8]) -> Vec<u8> {
    let mut key = fixed_table_key(KeyFamily::StreamPointerItemIndex, table_id);
    key.extend_from_slice(item_scope_and_id);
    key
}

#[must_use]
pub fn stream_trim_state_key(scope_key: &[u8]) -> Vec<u8> {
    let mut key = vec![KeyFamily::StreamTrimState.code()];
    key.extend_from_slice(scope_key);
    key
}

#[must_use]
pub fn stream_trim_due_key(due_millis: i64, scope_key: &[u8], policy_version: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + scope_key.len() + 8);
    key.push(KeyFamily::StreamTrimDue.code());
    key.extend_from_slice(&due_millis.to_be_bytes());
    key.extend_from_slice(scope_key);
    key.extend_from_slice(&policy_version.to_be_bytes());
    key
}

#[must_use]
pub fn stream_trim_due_prefix() -> KeyRange {
    range_for_prefix(vec![KeyFamily::StreamTrimDue.code()])
}

#[must_use]
pub fn stream_trim_due_upper_bound(due_before: i64) -> Vec<u8> {
    let mut key = vec![KeyFamily::StreamTrimDue.code()];
    key.extend_from_slice(&due_before.to_be_bytes());
    key.push(0);
    prefix_range_end(&key)
}

#[must_use]
pub fn ordered_log_key(
    bucket: u8,
    stream_id: StreamStorageId,
    partition_id: u16,
    suffix: &[u8],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 6 + 2 + suffix.len());
    key.push(KeyFamily::OrderedLogData.code());
    key.push(bucket);
    put_u48(&mut key, stream_id.get());
    put_u16(&mut key, partition_id);
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn queue_record_key(
    bucket: u8,
    queue_id: QueueStorageId,
    partition_id: u16,
    kind: QueueRecordKind,
    suffix: &[u8],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 6 + 2 + 1 + suffix.len());
    key.push(KeyFamily::PartitionedQueueData.code());
    key.push(bucket);
    put_u48(&mut key, queue_id.get());
    put_u16(&mut key, partition_id);
    key.push(kind.code());
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn pubsub_record_key(
    kind: PubsubRecordKind,
    left_id: U48,
    right_id: Option<U48>,
    suffix: &[u8],
) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(1 + 1 + 6 + right_id.map(|_| 6).unwrap_or_default() + suffix.len());
    key.push(KeyFamily::PubsubRecord.code());
    key.push(kind.code());
    put_u48(&mut key, left_id.get());
    if let Some(right_id) = right_id {
        put_u48(&mut key, right_id.get());
    }
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn pubsub_record_prefix(kind: PubsubRecordKind, left_id: U48) -> KeyRange {
    let mut prefix = Vec::with_capacity(1 + 1 + 6);
    prefix.push(KeyFamily::PubsubRecord.code());
    prefix.push(kind.code());
    put_u48(&mut prefix, left_id.get());
    range_for_prefix(prefix)
}

#[must_use]
pub fn pubsub_kind_prefix(kind: PubsubRecordKind) -> KeyRange {
    range_for_prefix(vec![KeyFamily::PubsubRecord.code(), kind.code()])
}

#[must_use]
pub fn pubsub_global_record_key(kind: PubsubRecordKind, suffix: &[u8]) -> Vec<u8> {
    pubsub_record_key(kind, U48::masked(0), None, suffix)
}

#[must_use]
pub fn stream_high_water_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::StreamHighWater.code(),
    ]
}

#[must_use]
pub fn idempotency_token_key(token: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 8);
    key.push(KeyFamily::SyncRecord.code());
    key.push(SystemRecordKind::IdempotencyToken.code());
    put_u64(&mut key, stable_token_hash(token.as_bytes()));
    key
}

#[must_use]
pub fn sync_apply_marker_key(mutation_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 8);
    key.push(KeyFamily::SyncRecord.code());
    key.push(SystemRecordKind::SyncApplyMutation.code());
    put_u64(&mut key, stable_token_hash(mutation_id.as_bytes()));
    key
}

#[must_use]
pub fn sync_last_applied_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::SyncLastApplied.code(),
    ]
}

#[must_use]
pub fn sync_log_entry_prefix() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::SyncLogEntry.code(),
    ]
}

#[must_use]
pub fn sync_log_entry_key(term: u64, index: u64) -> Vec<u8> {
    let mut key = sync_log_entry_prefix();
    put_u64(&mut key, term);
    put_u64(&mut key, index);
    key
}

#[must_use]
pub fn item_revision_prefix() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::ItemRevision.code(),
    ]
}

#[must_use]
pub fn item_revision_key(table_name: &str, key_json: &str) -> Vec<u8> {
    let mut hash_input = Vec::with_capacity(table_name.len() + 1 + key_json.len());
    hash_input.extend_from_slice(table_name.as_bytes());
    hash_input.push(0);
    hash_input.extend_from_slice(key_json.as_bytes());

    let mut key = item_revision_prefix();
    put_u64(&mut key, stable_token_hash(&hash_input));
    key
}

#[must_use]
pub fn queue_id_allocator_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::QueueIdAllocator.code(),
    ]
}

#[must_use]
pub fn topic_id_allocator_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::TopicIdAllocator.code(),
    ]
}

#[must_use]
pub fn subscription_id_allocator_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::SubscriptionIdAllocator.code(),
    ]
}

#[must_use]
pub fn delivery_id_allocator_key() -> Vec<u8> {
    vec![
        KeyFamily::SyncRecord.code(),
        SystemRecordKind::DeliveryIdAllocator.code(),
    ]
}

#[must_use]
pub fn queue_metadata_key(queue_id: QueueStorageId) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 6);
    key.push(KeyFamily::QueueMetadata.code());
    key.push(QueueMetadataKind::Metadata.code());
    put_u48(&mut key, queue_id.get());
    key
}

#[must_use]
pub fn queue_metadata_prefix() -> KeyRange {
    range_for_prefix(vec![
        KeyFamily::QueueMetadata.code(),
        QueueMetadataKind::Metadata.code(),
    ])
}

#[must_use]
pub fn queue_url_lookup_key(queue_url: &str) -> Vec<u8> {
    queue_lookup_key(QueueMetadataKind::UrlLookup, queue_url.as_bytes())
}

#[must_use]
pub fn queue_name_lookup_key(queue_name: &str) -> Vec<u8> {
    queue_lookup_key(QueueMetadataKind::NameLookup, queue_name.as_bytes())
}

fn queue_lookup_key(kind: QueueMetadataKind, lookup_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + lookup_key.len());
    key.push(KeyFamily::QueueMetadata.code());
    key.push(kind.code());
    key.extend_from_slice(lookup_key);
    key
}

fn stable_token_hash(token: &[u8]) -> u64 {
    let digest = Uuid::new_v5(&Uuid::NAMESPACE_OID, token).into_bytes();
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

#[must_use]
pub fn partition_control_key(
    kind: PartitionControlKind,
    resource_id: StreamStorageId,
    suffix: &[u8],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 1 + 6 + suffix.len());
    key.push(KeyFamily::PartitionControl.code());
    key.push(kind.code());
    put_u48(&mut key, resource_id.get());
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn primary_item_prefix(table_id: TableStorageId) -> KeyRange {
    range_for_prefix(fixed_table_key(KeyFamily::PrimaryItem, table_id))
}

#[must_use]
pub fn gsi_prefix(table_id: TableStorageId, index_id: IndexStorageId) -> KeyRange {
    range_for_prefix(fixed_table_index_key(
        KeyFamily::GsiItem,
        table_id,
        index_id,
    ))
}

#[must_use]
pub fn gsi_tombstone_prefix(table_id: TableStorageId, index_id: IndexStorageId) -> KeyRange {
    range_for_prefix(fixed_table_index_key(
        KeyFamily::GsiTombstone,
        table_id,
        index_id,
    ))
}

#[must_use]
pub fn system_stream_prefix() -> KeyRange {
    range_for_prefix(vec![KeyFamily::SystemStreamRow.code()])
}

#[must_use]
pub fn table_stream_prefix(table_id: TableStorageId) -> KeyRange {
    range_for_prefix(fixed_table_key(KeyFamily::TableStreamRow, table_id))
}

#[must_use]
pub fn item_stream_prefix(table_id: TableStorageId, item_scope: &[u8]) -> KeyRange {
    let mut prefix = fixed_table_key(KeyFamily::ItemStreamRow, table_id);
    prefix.extend_from_slice(item_scope);
    range_for_prefix(prefix)
}

#[must_use]
pub fn item_stream_table_prefix(table_id: TableStorageId) -> KeyRange {
    range_for_prefix(fixed_table_key(KeyFamily::ItemStreamRow, table_id))
}

#[must_use]
pub fn stream_pointer_table_prefix(table_id: TableStorageId) -> KeyRange {
    range_for_prefix(fixed_table_key(
        KeyFamily::StreamPointerTableIndex,
        table_id,
    ))
}

#[must_use]
pub fn stream_pointer_item_prefix(table_id: TableStorageId, item_scope: &[u8]) -> KeyRange {
    let mut prefix = fixed_table_key(KeyFamily::StreamPointerItemIndex, table_id);
    prefix.extend_from_slice(item_scope);
    range_for_prefix(prefix)
}

#[must_use]
pub fn stream_pointer_item_table_prefix(table_id: TableStorageId) -> KeyRange {
    range_for_prefix(fixed_table_key(KeyFamily::StreamPointerItemIndex, table_id))
}

#[must_use]
pub fn queue_ready_prefix(bucket: u8, queue_id: QueueStorageId, partition_id: u16) -> KeyRange {
    let mut prefix = Vec::with_capacity(1 + 1 + 6 + 2 + 1);
    prefix.push(KeyFamily::PartitionedQueueData.code());
    prefix.push(bucket);
    put_u48(&mut prefix, queue_id.get());
    put_u16(&mut prefix, partition_id);
    prefix.push(QueueRecordKind::Ready.code());
    range_for_prefix(prefix)
}

pub fn parse_compact_key(key: &[u8]) -> Result<ParsedCompactKey<'_>, CompactKeyError> {
    let Some((&family_byte, payload)) = key.split_first() else {
        return Err(CompactKeyError::EmptyKey);
    };
    let family = KeyFamily::from_code(family_byte)?;

    match family {
        KeyFamily::TableMetadata => {
            require_len(family, key, 5)?;
            Ok(ParsedCompactKey::TableMetadata {
                table_id: TableStorageId(read_u32(&payload[..4])),
            })
        }
        KeyFamily::TableNameLookup => Ok(ParsedCompactKey::TableNameLookup {
            table_name: payload,
        }),
        KeyFamily::PrimaryItem => {
            require_len(family, key, 5)?;
            Ok(ParsedCompactKey::PrimaryItem {
                table_id: TableStorageId(read_u32(&payload[..4])),
                key: &payload[4..],
            })
        }
        KeyFamily::GsiItem | KeyFamily::GsiTombstone => {
            require_len(family, key, 7)?;
            let parsed = (
                TableStorageId(read_u32(&payload[..4])),
                IndexStorageId(read_u16(&payload[4..6])),
                &payload[6..],
            );
            if family == KeyFamily::GsiItem {
                Ok(ParsedCompactKey::GsiItem {
                    table_id: parsed.0,
                    index_id: parsed.1,
                    suffix: parsed.2,
                })
            } else {
                Ok(ParsedCompactKey::GsiTombstone {
                    table_id: parsed.0,
                    index_id: parsed.1,
                    suffix: parsed.2,
                })
            }
        }
        KeyFamily::GsiBackfill => {
            require_len(family, key, 7)?;
            Ok(ParsedCompactKey::GsiBackfill {
                table_id: TableStorageId(read_u32(&payload[..4])),
                index_id: IndexStorageId(read_u16(&payload[4..6])),
            })
        }
        KeyFamily::TtlConfig => {
            require_len(family, key, 5)?;
            Ok(ParsedCompactKey::TtlConfig {
                table_id: TableStorageId(read_u32(&payload[..4])),
            })
        }
        KeyFamily::TtlDueIndex => {
            require_len(family, key, 13)?;
            Ok(ParsedCompactKey::TtlDueIndex {
                table_id: TableStorageId(read_u32(&payload[..4])),
                ttl_seconds: read_u64(&payload[4..12]),
                key: &payload[12..],
            })
        }
        KeyFamily::SystemStreamRow => {
            require_len(family, key, 13)?;
            Ok(ParsedCompactKey::SystemStreamRow {
                stream_item_id: payload,
            })
        }
        KeyFamily::TableStreamRow | KeyFamily::StreamPointerTableIndex => {
            require_len(family, key, 17)?;
            let parsed = (
                TableStorageId(read_u32(&payload[..4])),
                &payload[4..(4 + STREAM_ITEM_ID_LEN)],
            );
            if family == KeyFamily::TableStreamRow {
                Ok(ParsedCompactKey::TableStreamRow {
                    table_id: parsed.0,
                    stream_item_id: parsed.1,
                })
            } else {
                Ok(ParsedCompactKey::StreamPointerTableIndex {
                    table_id: parsed.0,
                    stream_item_id: parsed.1,
                })
            }
        }
        KeyFamily::ItemStreamRow | KeyFamily::StreamPointerItemIndex => {
            require_len(family, key, 5)?;
            let parsed = (TableStorageId(read_u32(&payload[..4])), &payload[4..]);
            if family == KeyFamily::ItemStreamRow {
                Ok(ParsedCompactKey::ItemStreamRow {
                    table_id: parsed.0,
                    item_scope: parsed.1,
                })
            } else {
                Ok(ParsedCompactKey::StreamPointerItemIndex {
                    table_id: parsed.0,
                    item_scope: parsed.1,
                })
            }
        }
        KeyFamily::StreamTrimState => Ok(ParsedCompactKey::StreamTrimState { scope_key: payload }),
        KeyFamily::StreamTrimDue => {
            require_len(family, key, 17)?;
            Ok(ParsedCompactKey::StreamTrimDue {
                due_millis: read_i64(&payload[..8]),
                scope_key: &payload[8..payload.len() - 8],
                policy_version: read_u64(&payload[payload.len() - 8..]),
            })
        }
        KeyFamily::PartitionControl => {
            require_len(family, key, 8)?;
            Ok(ParsedCompactKey::PartitionControl {
                kind: PartitionControlKind::from_code(payload[0])?,
                resource_id: StreamStorageId::from(U48(read_u48(&payload[1..7]))),
                suffix: &payload[7..],
            })
        }
        KeyFamily::OrderedLogData => {
            require_len(family, key, 10)?;
            Ok(ParsedCompactKey::OrderedLogData {
                bucket: payload[0],
                stream_id: StreamStorageId::from(U48(read_u48(&payload[1..7]))),
                partition_id: read_u16(&payload[7..9]),
                suffix: &payload[9..],
            })
        }
        KeyFamily::PartitionedQueueData => {
            require_len(family, key, 11)?;
            Ok(ParsedCompactKey::PartitionedQueueData {
                bucket: payload[0],
                queue_id: QueueStorageId::from(U48(read_u48(&payload[1..7]))),
                partition_id: read_u16(&payload[7..9]),
                kind: QueueRecordKind::from_code(payload[9])?,
                suffix: &payload[10..],
            })
        }
        KeyFamily::QueueMetadata => {
            require_len(family, key, 2)?;
            let kind = QueueMetadataKind::from_code(payload[0])?;
            match kind {
                QueueMetadataKind::Metadata => {
                    require_len(family, key, 8)?;
                    Ok(ParsedCompactKey::QueueMetadata {
                        queue_id: QueueStorageId::from(U48(read_u48(&payload[1..7]))),
                    })
                }
                QueueMetadataKind::UrlLookup | QueueMetadataKind::NameLookup => {
                    Ok(ParsedCompactKey::QueueLookup {
                        kind,
                        lookup_key: &payload[1..],
                    })
                }
            }
        }
        KeyFamily::PubsubRecord => {
            require_len(family, key, 8)?;
            let kind = PubsubRecordKind::from_code(payload[0])?;
            let left_id = U48(read_u48(&payload[1..7]));
            let (right_id, suffix) = if payload.len() >= 13 {
                (Some(U48(read_u48(&payload[7..13]))), &payload[13..])
            } else {
                (None, &payload[7..])
            };
            Ok(ParsedCompactKey::PubsubRecord {
                kind,
                left_id,
                right_id,
                suffix,
            })
        }
        KeyFamily::SyncRecord => {
            require_len(family, key, 2)?;
            Ok(ParsedCompactKey::SyncRecord {
                kind: SystemRecordKind::from_code(payload[0])?,
                suffix: &payload[1..],
            })
        }
    }
}

fn fixed_table_key(family: KeyFamily, table_id: TableStorageId) -> Vec<u8> {
    let mut key = Vec::with_capacity(5);
    key.push(family.code());
    put_u32(&mut key, table_id.get());
    key
}

fn fixed_table_index_key(
    family: KeyFamily,
    table_id: TableStorageId,
    index_id: IndexStorageId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(7);
    key.push(family.code());
    put_u32(&mut key, table_id.get());
    put_u16(&mut key, index_id.get());
    key
}

fn range_for_prefix(prefix: Vec<u8>) -> KeyRange {
    let end = prefix_range_end(&prefix);
    KeyRange { start: prefix, end }
}

fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] = end[index].saturating_add(1);
            end.truncate(index + 1);
            return end;
        }
    }
    end.push(0);
    end
}

fn require_len(
    family: KeyFamily,
    key: &[u8],
    expected_at_least: usize,
) -> Result<(), CompactKeyError> {
    if key.len() >= expected_at_least {
        Ok(())
    } else {
        Err(CompactKeyError::Truncated {
            family,
            expected_at_least,
            actual: key.len(),
        })
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u48(out: &mut Vec<u8>, value: u64) {
    let bytes = value.to_be_bytes();
    out.extend_from_slice(&bytes[2..]);
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_be_bytes([bytes[0], bytes[1]])
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn read_i64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn read_u48(bytes: &[u8]) -> u64 {
    u64::from_be_bytes([
        0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
    ])
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

fn printable_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}
