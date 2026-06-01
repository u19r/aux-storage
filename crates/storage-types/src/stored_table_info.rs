use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{IndexName, KeyAttributeType, KeySchemaElement, TableName, TimestampMillis};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredTableInfo {
    pub table_name: TableName,
    pub table_status: TableStatus,
    #[schema(value_type = i64, example = 1_700_000_000_000i64)]
    pub created_at: TimestampMillis,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub key_schema: Vec<KeySchemaElement>,
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndex>>,
    pub table_size_bytes: u64,
    pub item_count: u64,
    pub stream_specification: Option<StreamSpecification>,
    #[serde(default)]
    pub deletion_protection_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub enum GsiBackfillStatus {
    Backfilling,
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GsiBackfillRecord {
    pub table_name: String,
    pub index_name: String,
    pub status: GsiBackfillStatus,
    pub scan_lek: Option<String>,
    pub captured_stream_tail: Option<String>,
    #[schema(value_type = i64, example = 1_700_000_000_000i64)]
    pub created_at: TimestampMillis,
    #[schema(value_type = i64, example = 1_700_000_001_234i64)]
    pub updated_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Projection {
    #[serde(rename = "ProjectionType", skip_serializing_if = "Option::is_none")]
    pub projection_type: Option<ProjectionType>,

    #[serde(rename = "NonKeyAttributes", skip_serializing_if = "Option::is_none")]
    pub non_key_attributes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
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
