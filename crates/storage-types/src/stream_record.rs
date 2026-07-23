use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{AttributeValue, TableName};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamRecord {
    #[serde(skip)]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_table_name: Option<TableName>,
    pub keys: HashMap<String, AttributeValue>,
    pub sequence_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_image: Option<HashMap<String, AttributeValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_image: Option<HashMap<String, AttributeValue>>,
}
