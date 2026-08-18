use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{BatchGetItemResponse, GetItemResponse, QueryResponse, ReadSequenceConsistency};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceResponse {
    pub nodes: Vec<ReadSequenceNodeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_sequence_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumed_capacity: Option<serde_json::Value>,
    pub read_consistency: ReadSequenceConsistency,
    pub partial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceNodeResult {
    pub name: String,
    pub invocations: Vec<ReadSequenceInvocationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceInvocationResult {
    pub ordinal: u32,
    pub input_refs: std::collections::BTreeMap<String, ReadSequenceInputReference>,
    pub result: ReadSequenceInvocationPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub enum ReadSequenceInvocationPayload {
    Get(GetItemResponse),
    BatchGet(BatchGetItemResponse),
    Query(QueryResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ReadSequenceInputReference {
    pub node: String,
    pub invocation_ordinal: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_ordinal: Option<u32>,
}

impl ReadSequenceInvocationPayload {
    #[must_use]
    pub fn item_count(&self) -> u32 {
        match self {
            Self::Get(response) => u32::from(response.item.is_some()),
            Self::BatchGet(response) => response
                .responses
                .as_ref()
                .map(|tables| tables.values().map(Vec::len).sum::<usize>() as u32)
                .unwrap_or(0),
            Self::Query(response) => response.count,
        }
    }
}
