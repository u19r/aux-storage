use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::KeyType;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,

    #[serde(rename = "KeyType")]
    pub key_type: KeyType,
}
