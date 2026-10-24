use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    IndexName, KeyAttributeType, KeyAttributes, KeySchemaElement, MaxIndexers, StorageError,
    StorageResult, StreamRetentionDuration, TableName, TimestampMillis, WireItem,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredTableInfo {
    pub table_name: TableName,
    pub table_status: TableStatus,
    #[schema(value_type = i64, example = 1_700_000_000_000i64)]
    pub created_at: TimestampMillis,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub key_schema: Vec<KeySchemaElement>,
    pub max_indexers: MaxIndexers,
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>>,
    pub table_size_bytes: u64,
    pub item_count: u64,
    pub stream_specification: Option<StreamSpecification>,
    #[serde(default = "default_table_stream_duration")]
    pub table_stream_duration: StreamRetentionDuration,
    #[serde(default = "default_item_stream_duration")]
    pub default_item_stream_duration: StreamRetentionDuration,
    #[serde(default)]
    pub deletion_protection_enabled: bool,
}

impl StoredTableInfo {
    /// Extracts the complete primary key from a wire item in one bounded
    /// decode.
    pub fn table_key_from_wire(&self, item: &WireItem) -> StorageResult<KeyAttributes> {
        let fields = self
            .key_schema
            .iter()
            .map(|element| element.attribute_name.as_str())
            .collect::<smallvec::SmallVec<[&str; 2]>>();
        let values = item.select_attribute_values(&fields)?;
        let mut key = KeyAttributes::with_capacity(self.key_schema.len());
        for (element, value) in self.key_schema.iter().zip(values) {
            key.insert(
                element.attribute_name.clone(),
                value.ok_or_else(|| {
                    StorageError::internal("wire item is missing a table key attribute")
                })?,
            );
        }
        Ok(key)
    }

    /// Whether a table mutation must be retained in the stream for a public
    /// stream or deferred secondary-index projection.
    #[must_use]
    pub fn requires_stream_entry(&self, immediate_gsi_consistency: bool) -> bool {
        let stream_enabled = self
            .stream_specification
            .as_ref()
            .is_some_and(|specification| specification.stream_enabled);
        let has_deferred_index = !immediate_gsi_consistency
            && self
                .global_secondary_indexes
                .as_ref()
                .is_some_and(|indexes| {
                    indexes.iter().any(|index| !index.index_name.is_ttl_index())
                });
        stream_enabled || has_deferred_index
    }
}

fn default_table_stream_duration() -> StreamRetentionDuration {
    StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION
}

fn default_item_stream_duration() -> StreamRetentionDuration {
    StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AttributeDefinition {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,

    #[serde(rename = "AttributeType")]
    pub attribute_type: KeyAttributeType,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GlobalSecondaryIndex {
    #[serde(rename = "IndexName")]
    pub index_name: IndexName,

    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,

    #[serde(rename = "Projection")]
    pub projection: Projection,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StreamSpecification {
    #[serde(rename = "StreamEnabled")]
    pub stream_enabled: bool,

    #[serde(rename = "StreamViewType", skip_serializing_if = "Option::is_none")]
    pub stream_view_type: Option<StreamViewType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Projection {
    #[serde(rename = "ProjectionType", skip_serializing_if = "Option::is_none")]
    pub projection_type: Option<ProjectionType>,

    #[serde(rename = "NonKeyAttributes", skip_serializing_if = "Option::is_none")]
    pub non_key_attributes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectionType {
    KeysOnly,
    Include,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamViewType {
    KeysOnly,
    NewImage,
    OldImage,
    NewAndOldImages,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub enum TableStatus {
    #[serde(rename = "CREATING")]
    Creating,
    #[serde(rename = "ACTIVE")]
    Active,
    #[serde(rename = "DELETING")]
    Deleting,
    #[serde(rename = "UPDATING")]
    Updating,
    #[serde(rename = "INACCESSIBLE_ENCRYPTION_CREDENTIALS")]
    InaccessibleEncryptionCredentials,
    #[serde(rename = "ARCHIVING")]
    Archiving,
    #[serde(rename = "ARCHIVED")]
    Archived,
    #[serde(rename = "REPLICATION_NOT_AUTHORIZED")]
    ReplicationNotAuthorized,
}

impl From<&str> for TableStatus {
    fn from(status: &str) -> Self {
        match status {
            "CREATING" => TableStatus::Creating,
            "DELETING" => TableStatus::Deleting,
            "UPDATING" => TableStatus::Updating,
            "INACCESSIBLE_ENCRYPTION_CREDENTIALS" => TableStatus::InaccessibleEncryptionCredentials,
            "ARCHIVING" => TableStatus::Archiving,
            "ARCHIVED" => TableStatus::Archived,
            "REPLICATION_NOT_AUTHORIZED" => TableStatus::ReplicationNotAuthorized,
            _ => TableStatus::Active, // Default to Active if unknown
        }
    }
}

impl From<TableStatus> for String {
    fn from(status: TableStatus) -> Self {
        match status {
            TableStatus::Creating => "CREATING".to_string(),
            TableStatus::Active => "ACTIVE".to_string(),
            TableStatus::Deleting => "DELETING".to_string(),
            TableStatus::Updating => "UPDATING".to_string(),
            TableStatus::InaccessibleEncryptionCredentials => {
                "INACCESSIBLE_ENCRYPTION_CREDENTIALS".to_string()
            }
            TableStatus::Archiving => "ARCHIVING".to_string(),
            TableStatus::Archived => "ARCHIVED".to_string(),
            TableStatus::ReplicationNotAuthorized => "REPLICATION_NOT_AUTHORIZED".to_string(),
        }
    }
}

impl From<&TableStatus> for String {
    fn from(status: &TableStatus) -> Self {
        match status {
            TableStatus::Creating => "CREATING".to_string(),
            TableStatus::Active => "ACTIVE".to_string(),
            TableStatus::Deleting => "DELETING".to_string(),
            TableStatus::Updating => "UPDATING".to_string(),
            TableStatus::InaccessibleEncryptionCredentials => {
                "INACCESSIBLE_ENCRYPTION_CREDENTIALS".to_string()
            }
            TableStatus::Archiving => "ARCHIVING".to_string(),
            TableStatus::Archived => "ARCHIVED".to_string(),
            TableStatus::ReplicationNotAuthorized => "REPLICATION_NOT_AUTHORIZED".to_string(),
        }
    }
}
