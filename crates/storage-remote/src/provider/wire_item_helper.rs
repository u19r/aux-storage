use std::collections::HashMap;

use serde_json::value::RawValue;
use storage_types::{
    KeysAndAttributes, StorageEnum, StorageError, StorageResult, TableName, WireItem,
};

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetItemWireResponse<'a> {
    #[serde(default, borrow)]
    item: Option<&'a RawValue>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ScanQueryWireResponse<'a> {
    #[serde(default, borrow)]
    items: Option<Vec<&'a RawValue>>,
    #[serde(default, borrow)]
    last_evaluated_key: Option<&'a RawValue>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BatchGetWireResponse<'a> {
    #[serde(default, borrow)]
    responses: Option<HashMap<TableName, Vec<&'a RawValue>>>,
    #[serde(default)]
    unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,
}

pub(super) struct BatchGetWireParsed {
    pub(super) responses: Option<HashMap<TableName, Vec<WireItem>>>,
    pub(super) unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,
}

pub(super) fn parse_get_item_wire(bytes: &[u8]) -> StorageResult<Option<WireItem>> {
    let response: GetItemWireResponse<'_> = serde_json::from_slice(bytes)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))?;
    response.item.map(raw_item_to_wire_item).transpose()
}

pub(super) fn parse_scan_query_wire(
    bytes: &[u8],
) -> StorageResult<(Vec<WireItem>, Option<String>)> {
    let response: ScanQueryWireResponse<'_> = serde_json::from_slice(bytes)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))?;

    let items = response
        .items
        .unwrap_or_default()
        .into_iter()
        .map(raw_item_to_wire_item)
        .collect::<StorageResult<Vec<_>>>()?;

    let last_evaluated_key = response
        .last_evaluated_key
        .map(raw_json_key_to_string)
        .transpose()?;

    Ok((items, last_evaluated_key))
}

pub(super) fn parse_batch_get_wire(bytes: &[u8]) -> StorageResult<BatchGetWireParsed> {
    let response: BatchGetWireResponse<'_> = serde_json::from_slice(bytes)
        .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)))?;

    let responses = if let Some(tables) = response.responses {
        let mut decoded = HashMap::with_capacity(tables.len());
        for (table_name, table_items) in tables {
            let mut wire_items = Vec::with_capacity(table_items.len());
            for raw_item in table_items {
                wire_items.push(raw_item_to_wire_item(raw_item)?);
            }
            decoded.insert(table_name, wire_items);
        }
        Some(decoded)
    } else {
        None
    };

    Ok(BatchGetWireParsed {
        responses,
        unprocessed_keys: response.unprocessed_keys,
    })
}

fn raw_item_to_wire_item(raw_item: &RawValue) -> StorageResult<WireItem> {
    let raw_item_json = raw_item.get();
    let trimmed = raw_item_json.trim_start();
    if !trimmed.starts_with('{') {
        return Err(StorageError::internal(
            "remote storage item payload is not a JSON object",
        ));
    }
    Ok(WireItem::dynamo_json(raw_item_json.as_bytes().to_vec()))
}

fn raw_json_key_to_string(raw_key: &RawValue) -> StorageResult<String> {
    let raw_json = raw_key.get();
    if raw_json.trim_start().starts_with('"') {
        return serde_json::from_str::<String>(raw_json)
            .map_err(|err| StorageError::Base(StorageEnum::Serialization(err)));
    }
    Ok(raw_json.to_string())
}
