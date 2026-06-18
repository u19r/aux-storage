use serde::{Deserialize, Serialize};
use storage_types::TableName;

pub const CHANGE_INDEX_MARKER_RETENTION_MS: i64 = 6 * 60 * 60 * 1000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeIndexMarker {
    pub slot: u16,
    pub versionstamp: String,
    pub table_id: TableName,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListChangeIndexMarkersRequest {
    pub slot: u16,
    pub after_versionstamp: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListChangeIndexMarkersResponse {
    pub markers: Vec<ChangeIndexMarker>,
}
