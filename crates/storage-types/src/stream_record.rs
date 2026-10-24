use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{AttributeMap, KeyAttributes, SystemStreamPosition, TableName};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamRecord {
    #[serde(skip)]
    pub cursor: Option<String>,
    /// Ordinary table name for a table stream, or the validated logical
    /// source table identity for a SystemStream record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_table_name: Option<TableName>,
    pub keys: KeyAttributes,
    pub sequence_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_image: Option<AttributeMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_image: Option<AttributeMap>,
    /// Logical shard, lineage, and opaque offset for a SystemStream record.
    /// Ordinary table-stream records leave this absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_position: Option<SystemStreamPosition>,
}
