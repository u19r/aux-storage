use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{AttributeMap, ReadSequenceConsistency, TableName};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceResponse {
    pub responses: Vec<ReadSequenceRootResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<ReadSequenceWarning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_sequence_token: Option<String>,
    pub read_consistency: ReadSequenceConsistency,
    pub partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceRootResponse {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<AttributeMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<ReadSequenceItemResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub joins: Option<std::collections::BTreeMap<String, ReadSequenceJoinResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceItemResult {
    pub item: AttributeMap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub joins: Option<std::collections::BTreeMap<String, ReadSequenceJoinResult>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceJoinResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<AttributeMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<AttributeMap>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceWarning {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_gsi: Option<ReadSequenceSuggestedGsi>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceSuggestedGsi {
    pub table_name: TableName,
    pub partition_key: ReadSequenceSuggestedGsiKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_key: Option<ReadSequenceSuggestedGsiKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceSuggestedGsiKey {
    pub attribute_name: String,
    pub source: String,
}
