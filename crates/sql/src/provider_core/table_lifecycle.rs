use storage_types::{
    CreateTableRequest, MaxIndexers, StorageError, StorageResult, StreamRetentionDuration,
    TimestampMillis,
};

use crate::constants::MAX_GSI_COUNT;

#[derive(Debug, Clone)]
pub(crate) struct PreparedTableMetadata {
    pub(crate) created_at: TimestampMillis,
    pub(crate) attribute_definitions_json: String,
    pub(crate) key_schema_json: String,
    pub(crate) max_indexers: MaxIndexers,
    pub(crate) global_secondary_indexes_json: Option<String>,
    pub(crate) stream_specification_json: Option<String>,
    pub(crate) deletion_protection_enabled: bool,
    pub(crate) table_stream_duration: StreamRetentionDuration,
    pub(crate) default_item_stream_duration: StreamRetentionDuration,
}

pub(crate) fn validate_create_table_request(request: &CreateTableRequest) -> StorageResult<()> {
    storage_common::validate_create_table(request)?;
    validate_key_schema_attributes(request)?;
    validate_gsi_attributes(request)?;
    Ok(())
}

pub(crate) fn prepare_table_metadata(
    request: &CreateTableRequest,
) -> StorageResult<PreparedTableMetadata> {
    let created_at = TimestampMillis::now();
    let attribute_definitions_json = serde_json::to_string(&request.attribute_definitions)?;
    let key_schema_json = serde_json::to_string(&request.key_schema)?;
    let global_secondary_indexes_json = request
        .global_secondary_indexes
        .as_ref()
        .map(|indexes| {
            let storage_indexes: Vec<storage_types::GlobalSecondaryIndex> =
                indexes.iter().cloned().map(Into::into).collect();
            serde_json::to_string(&storage_indexes)
        })
        .transpose()?;
    let stream_specification_json = request
        .stream_specification
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let deletion_protection_enabled = request.deletion_protection_enabled.unwrap_or(false);
    let table_stream_duration = request.aux_stream_duration_hours.unwrap_or_default();
    let default_item_stream_duration = request
        .aux_default_item_stream_duration_hours
        .unwrap_or(table_stream_duration);

    Ok(PreparedTableMetadata {
        created_at,
        attribute_definitions_json,
        key_schema_json,
        max_indexers: request.max_indexers,
        global_secondary_indexes_json,
        stream_specification_json,
        deletion_protection_enabled,
        table_stream_duration,
        default_item_stream_duration,
    })
}

pub(crate) fn requested_capacity_increase(
    current: MaxIndexers,
    requested: Option<MaxIndexers>,
) -> StorageResult<Option<MaxIndexers>> {
    let Some(requested) = requested else {
        return Ok(None);
    };
    if requested < current {
        return Err(StorageError::validation("MaxIndexers:cannot_decrease"));
    }
    Ok((requested > current).then_some(requested))
}

fn validate_key_schema_attributes(request: &CreateTableRequest) -> StorageResult<()> {
    let mut seen = std::collections::HashSet::new();
    for key_schema in &request.key_schema {
        if !seen.insert(&key_schema.attribute_name) {
            return Err(StorageError::validation(format!(
                "Duplicate key schema attribute: {}",
                key_schema.attribute_name
            )));
        }
        if !request
            .attribute_definitions
            .iter()
            .any(|definition| definition.attribute_name == key_schema.attribute_name)
        {
            return Err(StorageError::validation(format!(
                "Key attribute '{}' missing from attribute definitions",
                key_schema.attribute_name
            )));
        }
    }
    Ok(())
}

fn validate_gsi_attributes(request: &CreateTableRequest) -> StorageResult<()> {
    let Some(gsis) = &request.global_secondary_indexes else {
        return Ok(());
    };
    if gsis.len() > MAX_GSI_COUNT {
        return Err(StorageError::validation(format!(
            "Too many global secondary indexes: {} (max {MAX_GSI_COUNT})",
            gsis.len()
        )));
    }
    for gsi in gsis {
        for key_schema in &gsi.key_schema {
            if !request
                .attribute_definitions
                .iter()
                .any(|definition| definition.attribute_name == key_schema.attribute_name)
            {
                return Err(StorageError::validation(format!(
                    "GSI '{}' key attribute '{}' missing from attribute definitions",
                    gsi.index_name, key_schema.attribute_name
                )));
            }
        }
    }
    Ok(())
}
