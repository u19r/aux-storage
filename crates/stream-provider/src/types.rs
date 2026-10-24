use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use storage_types::{
    DurationSeconds, ItemRevision, ItemStreamVersion, ItemWriteVersions, StreamItemId, StreamName,
    SystemStreamPosition, TableName, TimestampMillis, UserStreamName,
};
use utoipa::ToSchema;

use crate::newtypes::CursorName;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionDestination {
    pub protocol: String,
    pub endpoint: String,
    #[serde(default)]
    pub extra_json: serde_json::Value,
}

impl SubscriptionDestination {
    #[must_use]
    pub fn new(protocol: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            endpoint: endpoint.into(),
            extra_json: serde_json::Value::Null,
        }
    }

    #[must_use]
    pub fn with_extra_json(mut self, extra_json: serde_json::Value) -> Self {
        self.extra_json = extra_json;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionMessage {
    pub subscription_id: String,
    pub message_id: String,
    pub destination: SubscriptionDestination,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl SubscriptionMessage {
    #[must_use]
    pub fn new(
        subscription_id: impl Into<String>,
        message_id: impl Into<String>,
        destination: SubscriptionDestination,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            message_id: message_id.into(),
            destination,
            payload: payload.into(),
            attributes: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionSendOutcome {
    Delivered,
    AcceptedForDelivery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub name: UserStreamName,
    pub internal_id: StreamName,
    pub ttl_seconds: Option<DurationSeconds>,
    #[serde(default)]
    pub partitioning_mode: StreamPartitioningMode,
    pub created_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPointer {
    pub stream_name: StreamName,
    pub table_name: TableName,
    pub item_stream_version: ItemStreamVersion,
    pub stream_item_id: StreamItemId,
    pub indexers: Vec<String>,
    pub old_indexers: Option<Vec<String>>,
    /// Logical SystemStream identity, when the storage backend has persisted
    /// shard lineage.  Physical pointers without this metadata are not valid
    /// public SystemStream records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_position: Option<SystemStreamPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedStreamItem {
    pub data: Vec<u8>,
    pub data_type: StreamDataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoredStreamPointer {
    Pointer {
        history_id: storage_types::TableHistoryId,
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
        item_revision: ItemRevision,
        indexers: Vec<String>,
        old_indexers: Option<Vec<String>>,
    },
    Embedded {
        history_id: storage_types::TableHistoryId,
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
        item_revision: ItemRevision,
        items: Vec<EmbeddedStreamItem>,
        indexers: Vec<String>,
        old_indexers: Option<Vec<String>>,
    },
}

impl StoredStreamPointer {
    pub fn history_address(&self) -> storage_types::ItemHistoryAddress {
        let (Self::Pointer {
            history_id,
            stream_name,
            ..
        }
        | Self::Embedded {
            history_id,
            stream_name,
            ..
        }) = self;
        storage_types::ItemHistoryAddress {
            history_id: *history_id,
            stream_name: stream_name.clone(),
        }
    }

    pub fn matches_table_history(&self, target: storage_types::TableHistoryId) -> bool {
        let (Self::Pointer { history_id, .. } | Self::Embedded { history_id, .. }) = self;
        *history_id == target
    }

    #[must_use]
    pub fn pointer(
        history: storage_types::ItemHistoryAddress,
        table_name: TableName,
        versions: ItemWriteVersions,
    ) -> Self {
        Self::Pointer {
            history_id: history.history_id,
            stream_name: history.stream_name,
            table_name,
            item_stream_version: versions.item_stream_version,
            item_revision: versions.item_revision,
            indexers: Vec::new(),
            old_indexers: None,
        }
    }

    #[must_use]
    pub fn embedded(
        history: storage_types::ItemHistoryAddress,
        table_name: TableName,
        versions: ItemWriteVersions,
        items: Vec<EmbeddedStreamItem>,
    ) -> Self {
        Self::Embedded {
            history_id: history.history_id,
            stream_name: history.stream_name,
            table_name,
            item_stream_version: versions.item_stream_version,
            item_revision: versions.item_revision,
            items,
            indexers: Vec::new(),
            old_indexers: None,
        }
    }

    #[must_use]
    pub fn with_indexers(mut self, indexers: Vec<String>) -> Self {
        match &mut self {
            Self::Pointer {
                indexers: stored, ..
            }
            | Self::Embedded {
                indexers: stored, ..
            } => *stored = indexers,
        }
        self
    }

    #[must_use]
    pub fn with_old_indexers(mut self, old_indexers: Option<Vec<String>>) -> Self {
        match &mut self {
            Self::Pointer {
                old_indexers: stored,
                ..
            }
            | Self::Embedded {
                old_indexers: stored,
                ..
            } => *stored = old_indexers,
        }
        self
    }

    #[must_use]
    pub fn stream_name(&self) -> &StreamName {
        match self {
            StoredStreamPointer::Pointer { stream_name, .. } => stream_name,
            StoredStreamPointer::Embedded { stream_name, .. } => stream_name,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &TableName {
        match self {
            StoredStreamPointer::Pointer { table_name, .. } => table_name,
            StoredStreamPointer::Embedded { table_name, .. } => table_name,
        }
    }

    #[must_use]
    pub fn indexers(&self) -> &[String] {
        match self {
            Self::Pointer { indexers, .. } | Self::Embedded { indexers, .. } => indexers,
        }
    }

    #[must_use]
    pub fn old_indexers(&self) -> Option<&[String]> {
        match self {
            Self::Pointer { old_indexers, .. } | Self::Embedded { old_indexers, .. } => {
                old_indexers.as_deref()
            }
        }
    }

    #[must_use]
    pub fn embedded_items(&self) -> Option<&[EmbeddedStreamItem]> {
        match self {
            StoredStreamPointer::Embedded { items, .. } => Some(items),
            StoredStreamPointer::Pointer { .. } => None,
        }
    }

    #[must_use]
    pub fn target_item_stream_version(&self) -> ItemStreamVersion {
        match self {
            StoredStreamPointer::Pointer {
                item_stream_version,
                ..
            }
            | StoredStreamPointer::Embedded {
                item_stream_version,
                ..
            } => *item_stream_version,
        }
    }
}

pub struct PointerRecordsResult {
    pub records: Vec<(StreamPointer, Vec<StreamItem>)>,
    pub last_evaluated_key: Option<StreamItemId>,
    pub last_scanned_key: Option<StreamItemId>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamItem {
    pub id: StreamItemId,
    pub stream_name: Option<StreamName>,
    pub data: Vec<u8>,
    pub data_type: StreamDataType,
    pub created_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamDataType {
    Binary = 0,
    Json = 1,
    Text = 2,
    DynamoDbJson = 3,
    DeleteMarker = 4,
    StreamPointer = 5,
}

impl From<i32> for StreamDataType {
    fn from(value: i32) -> Self {
        match value {
            0 => StreamDataType::Binary,
            1 => StreamDataType::Json,
            2 => StreamDataType::Text,
            3 => StreamDataType::DynamoDbJson,
            4 => StreamDataType::DeleteMarker,
            5 => StreamDataType::StreamPointer,
            _ => StreamDataType::Text, // Default to Text for unknown values
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCursor {
    pub name: CursorName,
    pub stream_name: StreamName,
    pub position: StreamItemId,
    pub created_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamPartitioningMode {
    #[default]
    Single,
    KeyOrdered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPage {
    pub items: Vec<StreamItem>,
    pub last_evaluated_key: Option<StreamItemId>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorPage {
    pub items: Vec<StreamItem>,
    pub cursor_position: StreamItemId,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum CursorPosition {
    #[serde(rename = "head")]
    Head,
    #[serde(rename = "tail")]
    Tail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ReadDirection {
    #[serde(rename = "forward")]
    Forward,
    #[serde(rename = "backward")]
    Backward,
}
