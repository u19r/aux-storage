use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use storage_types::{
    DurationSeconds, ItemStreamVersion, ReplicationEventMetadata, StreamItemId, StreamName,
    TableName, TimestampMillis, UserStreamName,
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
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replication: Option<ReplicationEventMetadata>,
    },
    Embedded {
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
        items: Vec<EmbeddedStreamItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replication: Option<ReplicationEventMetadata>,
    },
}

impl StoredStreamPointer {
    #[must_use]
    pub fn pointer(
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
    ) -> Self {
        Self::Pointer {
            stream_name,
            table_name,
            item_stream_version,
            replication: None,
        }
    }

    #[must_use]
    pub fn embedded(
        stream_name: StreamName,
        table_name: TableName,
        item_stream_version: ItemStreamVersion,
        items: Vec<EmbeddedStreamItem>,
    ) -> Self {
        Self::Embedded {
            stream_name,
            table_name,
            item_stream_version,
            items,
            replication: None,
        }
    }

    #[must_use]
    pub fn with_replication_metadata(mut self, replication: ReplicationEventMetadata) -> Self {
        match &mut self {
            StoredStreamPointer::Pointer {
                replication: pointer_replication,
                ..
            }
            | StoredStreamPointer::Embedded {
                replication: pointer_replication,
                ..
            } => {
                *pointer_replication = Some(replication);
            }
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

    #[must_use]
    pub fn into_stream_pointer(self, pointer_stream_item_id: StreamItemId) -> StreamPointer {
        let (stream_name, table_name, item_stream_version) = match self {
            StoredStreamPointer::Pointer {
                stream_name,
                table_name,
                item_stream_version,
                ..
            } => (stream_name, table_name, item_stream_version),
            StoredStreamPointer::Embedded {
                stream_name,
                table_name,
                item_stream_version,
                ..
            } => (stream_name, table_name, item_stream_version),
        };
        StreamPointer {
            stream_name,
            table_name,
            item_stream_version,
            stream_item_id: pointer_stream_item_id,
        }
    }

    #[must_use]
    pub fn replication_metadata(&self) -> Option<&ReplicationEventMetadata> {
        match self {
            StoredStreamPointer::Pointer { replication, .. }
            | StoredStreamPointer::Embedded { replication, .. } => replication.as_ref(),
        }
    }
}

pub struct PointerRecordsResult {
    pub records: Vec<(StreamPointer, Vec<StreamItem>)>,
    pub last_evaluated_key: Option<StreamItemId>,
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

#[derive(Debug, Clone)]
pub struct StreamPage {
    pub items: Vec<StreamItem>,
    pub last_evaluated_key: Option<StreamItemId>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateStreamRequest {
    #[serde(rename = "StreamName")]
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = "^[a-zA-Z0-9][a-zA-Z0-9_.-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        example = "user-events"
    )]
    pub stream_name: String,

    #[serde(rename = "StreamPrefix")]
    #[schema(min_length = 1, max_length = 1024, example = "app/users/events/")]
    pub stream_prefix: String,

    #[serde(rename = "TTLSeconds", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1, maximum = 31_536_000, example = 3600)]
    pub ttl_seconds: Option<u64>,

    #[serde(rename = "PartitioningMode", skip_serializing_if = "Option::is_none")]
    #[schema(example = "single")]
    pub partitioning_mode: Option<StreamPartitioningMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateStreamResponse {
    #[serde(rename = "StreamName")]
    #[schema(example = "user-events")]
    pub stream_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AppendItemRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,

    #[serde(rename = "Data")]
    #[schema(format = "binary", example = "base64-encoded-data")]
    pub data: Vec<u8>,

    #[serde(rename = "PartitionKey", skip_serializing_if = "Option::is_none")]
    #[schema(example = "tenant-123")]
    pub partition_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AppendItemResponse {
    #[serde(rename = "ItemId")]
    #[schema(value_type = String, example = "5fea7756-0ea4-451a-a703-a558b933e274")]
    pub item_id: StreamItemId,

    #[serde(rename = "Timestamp")]
    #[schema(value_type = String, example = "2024-01-01T12:00:00Z")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadStreamRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,

    #[serde(rename = "PageToken", skip_serializing_if = "Option::is_none")]
    #[schema(example = "eyJ0aW1lc3RhbXAiOiIyMDI0LTAxLTAxVDEyOjAwOjAwWiJ9")]
    pub page_token: Option<String>,

    #[serde(rename = "Limit", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1, maximum = 1000, example = 100)]
    pub limit: Option<usize>,

    #[serde(rename = "Direction")]
    #[schema(example = "forward")]
    pub direction: ReadDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ReadDirection {
    #[serde(rename = "forward")]
    Forward,
    #[serde(rename = "backward")]
    Backward,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadStreamResponse {
    #[serde(rename = "Items")]
    pub items: Vec<StreamItemResponse>,

    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    #[schema(example = "eyJ0aW1lc3RhbXAiOiIyMDI0LTAxLTAxVDEyOjAwOjAwWiJ9")]
    pub next_token: Option<String>,

    #[serde(rename = "HasMore")]
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StreamItemResponse {
    #[serde(rename = "ItemId")]
    #[schema(value_type = String, example = "5fea7756-0ea4-451a-a703-a558b933e274")]
    pub item_id: StreamItemId,

    #[serde(rename = "Data")]
    #[schema(format = "binary", example = "base64-encoded-data")]
    pub data: Vec<u8>,

    #[serde(rename = "Timestamp")]
    #[schema(value_type = String, example = "2024-01-01T12:00:00Z")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateCursorRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,

    #[serde(rename = "CursorName")]
    #[schema(
        min_length = 1,
        max_length = 64,
        pattern = "^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$",
        example = "consumer1"
    )]
    pub cursor_name: String,

    #[serde(rename = "Position")]
    #[schema(example = "head")]
    pub position: CursorPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateCursorResponse {
    #[serde(rename = "CursorName")]
    #[schema(example = "consumer1")]
    pub cursor_name: String,

    #[serde(rename = "Position")]
    #[schema(example = "head")]
    pub position: CursorPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadFromCursorRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,

    #[serde(rename = "CursorName")]
    #[schema(min_length = 1, max_length = 64, example = "consumer1")]
    pub cursor_name: String,

    #[serde(rename = "Limit", skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1, maximum = 1000, example = 100)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReadFromCursorResponse {
    #[serde(rename = "Items")]
    pub items: Vec<StreamItemResponse>,

    #[serde(rename = "CursorPosition")]
    #[schema(value_type = String, example = "5fea7756-0ea4-451a-a703-a558b933e274")]
    pub cursor_position: StreamItemId,

    #[serde(rename = "HasMore")]
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteStreamRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeleteCursorRequest {
    #[serde(rename = "StreamName")]
    #[schema(min_length = 1, max_length = 255, example = "user-events")]
    pub stream_name: String,

    #[serde(rename = "CursorName")]
    #[schema(min_length = 1, max_length = 64, example = "consumer1")]
    pub cursor_name: String,
}
