use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeDefinition, GlobalSecondaryIndexUpdate, MaxIndexers, StreamRetentionDuration,
    TableName, UpdateTableRequest,
};

use super::table::StreamPolicyChange;

/// Only fields with maintained table semantics cross the control boundary.
/// Dynamo compatibility fields remain ignored by the ordinary shared executor.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableMetadataChange {
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    pub indexes: Option<Vec<GlobalSecondaryIndexUpdate>>,
    pub stream: Option<StreamPolicyChange>,
    pub max_indexers: Option<MaxIndexers>,
    pub deletion_protection: Option<bool>,
    pub stream_duration: Option<StreamRetentionDuration>,
    pub default_item_duration: Option<StreamRetentionDuration>,
}

impl From<&UpdateTableRequest> for TableMetadataChange {
    fn from(request: &UpdateTableRequest) -> Self {
        Self {
            attribute_definitions: request.attribute_definitions.clone(),
            indexes: request.global_secondary_index_updates.clone(),
            stream: request.stream_specification.clone().map(StreamPolicyChange),
            max_indexers: request.max_indexers,
            deletion_protection: request.deletion_protection_enabled,
            stream_duration: request.aux_stream_duration_hours,
            default_item_duration: request.aux_default_item_stream_duration_hours,
        }
    }
}

impl TableMetadataChange {
    pub fn changes_schema(&self) -> bool {
        self.max_indexers.is_some()
            || self
                .indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|update| update.create.is_some() || update.delete.is_some())
    }

    pub fn created_indexes(&self) -> impl Iterator<Item = &storage_types::IndexName> {
        self.indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|update| update.create.as_ref().map(|create| &create.index_name))
    }

    pub fn to_request(&self, table_name: TableName) -> UpdateTableRequest {
        UpdateTableRequest {
            max_indexers: self.max_indexers,
            attribute_definitions: self.attribute_definitions.clone(),
            global_secondary_index_updates: self.indexes.clone(),
            stream_specification: self.stream.as_ref().map(|stream| stream.0.clone()),
            deletion_protection_enabled: self.deletion_protection,
            aux_stream_duration_hours: self.stream_duration,
            aux_default_item_stream_duration_hours: self.default_item_duration,
            ..UpdateTableRequest::new(table_name)
        }
    }
}

impl std::fmt::Debug for TableMetadataChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMetadataChange")
            .finish_non_exhaustive()
    }
}
