use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use storage_types::{DurationSeconds, StreamItemId, StreamName, TimestampMillis, UserStreamName};
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
