use std::collections::HashMap;

use serde_json::value::RawValue;
use storage_provider::{RangeAdmission, RangeReadBudget, RangeReadPage, RangeReadProgress};
use storage_types::{
    IndexName, KeyAttributes, KeysAndAttributes, StorageError, StorageResult, StoredTableInfo,
    TableName, WireItem,
};

use super::provider_helpers::remote_key_to_page_token;

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
    #[serde(rename = "Count")]
    count: Option<u32>,
    #[serde(rename = "ScannedCount")]
    scanned_count: Option<u32>,
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
    pub responses: Option<HashMap<TableName, Vec<WireItem>>>,
    pub unprocessed_keys: Option<HashMap<TableName, KeysAndAttributes>>,
}

#[derive(Debug)]
pub(super) struct ScanQueryWireParsed {
    pub items: Vec<WireItem>,
    pub count: u32,
    pub scanned_count: u32,
    pub last_evaluated_key: Option<String>,
}

impl ScanQueryWireParsed {
    pub(super) fn into_page(
        self,
        table_info: Option<&StoredTableInfo>,
        index_name: &Option<IndexName>,
        budget: RangeReadBudget,
    ) -> StorageResult<RangeReadPage> {
        self.validate_source_counts()?;
        let continuation = self.continuation(table_info, index_name)?;
        let progress = self.source_progress(table_info, index_name, budget)?;
        let ScanQueryWireParsed { items, .. } = self;
        RangeReadPage {
            items,
            progress,
            continuation,
        }
        .finish()
    }

    fn validate_source_counts(&self) -> StorageResult<()> {
        let item_count = u32::try_from(self.items.len()).map_err(|_| {
            StorageError::internal("remote scan/query item count exceeds its count type")
        })?;
        if self.count != item_count || self.scanned_count != item_count {
            return Err(StorageError::internal(
                "remote scan/query counts must equal the complete source image count",
            ));
        }

        Ok(())
    }

    fn continuation(
        &self,
        table_info: Option<&StoredTableInfo>,
        index_name: &Option<IndexName>,
    ) -> StorageResult<Option<String>> {
        self.last_evaluated_key
            .as_deref()
            .map(|raw_key| normalize_last_evaluated_key(raw_key, table_info, index_name))
            .transpose()
    }

    fn source_progress(
        &self,
        table_info: Option<&StoredTableInfo>,
        index_name: &Option<IndexName>,
        budget: RangeReadBudget,
    ) -> StorageResult<RangeReadProgress> {
        let mut progress = RangeReadProgress::default();
        if !self.items.is_empty() {
            let table_info = table_info.ok_or_else(|| {
                StorageError::internal("remote range source items require table metadata")
            })?;
            for item in &self.items {
                let key = item.item_key(table_info, index_name)?.ok_or_else(|| {
                    StorageError::internal("remote range source item is missing its key")
                })?;
                let size = item.logical_size().map_err(|error| {
                    StorageError::internal(&format!(
                        "remote range source logical size is invalid: {error}"
                    ))
                })?;
                if budget.admit(&mut progress, key, size)? == RangeAdmission::Stop {
                    return Err(StorageError::internal(
                        "remote range source page exceeds its bounded read budget",
                    ));
                }
            }
        }
        Ok(progress)
    }
}

fn normalize_last_evaluated_key(
    raw_key: &str,
    table_info: Option<&StoredTableInfo>,
    index_name: &Option<IndexName>,
) -> StorageResult<String> {
    let key = serde_json::from_str::<KeyAttributes>(raw_key).map_err(|error| {
        StorageError::internal(&format!(
            "remote scan/query LastEvaluatedKey must be a DynamoDB key object: {error}"
        ))
    })?;
    let table_info = table_info
        .ok_or_else(|| StorageError::internal("remote range cursor requires table metadata"))?;
    remote_key_to_page_token(&key, table_info, index_name.as_ref())
}

pub(super) fn parse_get_item_wire(bytes: &[u8]) -> StorageResult<Option<WireItem>> {
    let response: GetItemWireResponse<'_> =
        serde_json::from_slice(bytes).map_err(StorageError::from)?;
    response.item.map(raw_item_to_wire_item).transpose()
}

pub(super) fn parse_scan_query_wire(bytes: &[u8]) -> StorageResult<ScanQueryWireParsed> {
    let response: ScanQueryWireResponse<'_> =
        serde_json::from_slice(bytes).map_err(StorageError::from)?;

    let items = response
        .items
        .unwrap_or_default()
        .into_iter()
        .map(raw_item_to_wire_item)
        .collect::<StorageResult<Vec<_>>>()?;

    let count = response
        .count
        .ok_or_else(|| StorageError::internal("remote scan/query response omitted Count"))?;
    let scanned_count = response
        .scanned_count
        .ok_or_else(|| StorageError::internal("remote scan/query response omitted ScannedCount"))?;
    let last_evaluated_key = response
        .last_evaluated_key
        .map(raw_json_key_to_string)
        .transpose()?;

    Ok(ScanQueryWireParsed {
        items,
        count,
        scanned_count,
        last_evaluated_key,
    })
}

pub(super) fn parse_batch_get_wire(bytes: &[u8]) -> StorageResult<BatchGetWireParsed> {
    let response: BatchGetWireResponse<'_> =
        serde_json::from_slice(bytes).map_err(StorageError::from)?;

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
    if !raw_json.trim_start().starts_with('{') {
        return Err(StorageError::internal(
            "remote scan/query LastEvaluatedKey must be a DynamoDB key object",
        ));
    }
    Ok(raw_json.to_string())
}
