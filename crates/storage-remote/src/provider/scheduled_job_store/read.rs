use std::{collections::HashMap, sync::Arc, time::Instant};

use bytes::Bytes;
use scheduled_bg_jobs::store::{RangeDirection, RangeEntry, RangePage, RangeRequest, StoreKey};
use storage_provider::RawOrderedJobStoreError;
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetItemResponse, ExclusiveStartKey, GetItemRequest,
    GetItemResponse, KeyAttributes, KeysAndAttributes, QueryRequest, QueryResponse, TableName,
    VecMap,
};

use crate::provider::{
    implementation::RemoteStorageProvider,
    scheduled_job_store::{constants::*, encoding, provider::job_table},
};

pub(crate) async fn get(
    provider: Arc<RemoteStorageProvider>,
    key: StoreKey,
) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
    let mut request = GetItemRequest::new(
        job_table(),
        encoding::key_attributes(encoding::data_sort_key(&key)),
    );
    request.consistent_read = Some(true);
    let response: GetItemResponse = provider
        .invoke("DynamoDB_20120810.GetItem", &request)
        .await
        .map_err(RawOrderedJobStoreError::Backend)?;
    response
        .item
        .map(storage_types::AttributeMap::try_from)
        .transpose()
        .map_err(RawOrderedJobStoreError::Backend)?
        .as_ref()
        .map(encoding::decode_data_item)
        .transpose()
        .map(|item| item.map(|(_, value)| value))
}

pub(crate) async fn get_many(
    provider: Arc<RemoteStorageProvider>,
    keys: &[StoreKey],
) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
    get_many_until(provider, keys, None).await
}

pub(crate) async fn get_many_until(
    provider: Arc<RemoteStorageProvider>,
    keys: &[StoreKey],
    deadline: Option<Instant>,
) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let unique = unique_keys(keys);
    let mut pending = unique;
    let mut values = HashMap::with_capacity(pending.len());
    for _ in 0..MAX_BATCH_GET_ATTEMPTS {
        if pending.is_empty() {
            break;
        }
        let response = batch_get_once(Arc::clone(&provider), &pending, deadline).await?;
        collect_batch_values(&mut values, response.responses)?;
        pending = unprocessed_keys(response.unprocessed_keys)?;
    }
    if !pending.is_empty() {
        return Err(RawOrderedJobStoreError::Backend(
            storage_types::StorageError::service_unavailable(1),
        ));
    }
    Ok(keys.iter().map(|key| values.get(key).cloned()).collect())
}

async fn batch_get_once(
    provider: Arc<RemoteStorageProvider>,
    keys: &[StoreKey],
    deadline: Option<Instant>,
) -> Result<BatchGetItemResponse, RawOrderedJobStoreError> {
    let table = job_table();
    let request = BatchGetItemRequest {
        request_items: [(
            table,
            KeysAndAttributes {
                keys: keys
                    .iter()
                    .map(|key| encoding::key_attributes(encoding::data_sort_key(key)))
                    .collect(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: Some(true),
            },
        )]
        .into_iter()
        .collect(),
        return_consumed_capacity: None,
    };
    match deadline {
        Some(deadline) => {
            crate::provider::scheduled_job_store::deadline::invoke_with_deadline(
                provider,
                "DynamoDB_20120810.BatchGetItem",
                &request,
                deadline,
            )
            .await
        }
        None => provider
            .invoke("DynamoDB_20120810.BatchGetItem", &request)
            .await
            .map_err(RawOrderedJobStoreError::Backend),
    }
}

fn collect_batch_values(
    values: &mut HashMap<StoreKey, Bytes>,
    responses: Option<VecMap<TableName, Vec<storage_types::WireItem>>>,
) -> Result<(), RawOrderedJobStoreError> {
    let Some(responses) = responses else {
        return Ok(());
    };
    if responses.keys().any(|table| table != &job_table()) {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled batch response contains an unknown table",
        ));
    }
    let Some(items) = responses.get(&job_table()) else {
        return Ok(());
    };
    for item in items {
        let attributes = storage_types::AttributeMap::try_from(item)
            .map_err(RawOrderedJobStoreError::Backend)?;
        let (key, value) = encoding::decode_data_item(&attributes)?;
        if values.insert(key, value).is_some() {
            return Err(RawOrderedJobStoreError::Corrupt(
                "scheduled batch response contains a duplicate key",
            ));
        }
    }
    Ok(())
}

fn unprocessed_keys(
    unprocessed: Option<VecMap<TableName, KeysAndAttributes>>,
) -> Result<Vec<StoreKey>, RawOrderedJobStoreError> {
    let Some(entries) = unprocessed else {
        return Ok(Vec::new());
    };
    if entries.keys().any(|table| table != &job_table()) {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled batch response contains an unknown table",
        ));
    }
    let Some(keys) = entries.get(&job_table()) else {
        return Ok(Vec::new());
    };
    keys.keys
        .iter()
        .map(encoding::decode_continuation)
        .collect()
}

pub(crate) async fn range(
    provider: Arc<RemoteStorageProvider>,
    request: &RangeRequest,
) -> Result<RangePage, RawOrderedJobStoreError> {
    let (lower, upper) = range_bounds(request);
    let mut query = QueryRequest::new(
        job_table(),
        "#pk = :pk AND #sk BETWEEN :lower AND :upper".to_string(),
    );
    query.expression_attribute_names = Some(storage_types::VecMap::from_iter([
        ("#pk".to_string(), KEY_ATTRIBUTE.to_string()),
        ("#sk".to_string(), SORT_ATTRIBUTE.to_string()),
    ]));
    query.expression_attribute_values = Some(storage_types::AttributeMap::from_iter([
        (
            ":pk".to_string(),
            AttributeValue::S(PARTITION_KEY.to_string()),
        ),
        (":lower".to_string(), AttributeValue::S(lower)),
        (":upper".to_string(), AttributeValue::S(upper)),
    ]));
    query.limit = Some(request.item_limit().get());
    query.consistent_read = Some(true);
    query.scan_index_forward = Some(matches!(request.direction(), RangeDirection::Forward));
    query.exclusive_start_key = range_query_start(request)
        .map(|key| ExclusiveStartKey::Key(encoding::key_attributes(encoding::data_sort_key(key))));
    let response: QueryResponse = provider
        .invoke("DynamoDB_20120810.Query", &query)
        .await
        .map_err(RawOrderedJobStoreError::Backend)?;
    make_page(request, response)
}

pub(super) fn unique_keys(keys: &[StoreKey]) -> Vec<StoreKey> {
    let mut unique = Vec::with_capacity(keys.len());
    for key in keys {
        if !unique.iter().any(|candidate| candidate == key) {
            unique.push(key.clone());
        }
    }
    unique
}

fn range_bounds(request: &RangeRequest) -> (String, String) {
    let lower = request
        .start()
        .map(encoding::data_sort_key)
        .unwrap_or_else(|| DATA_KEY_PREFIX.to_string());
    let upper = request
        .end()
        .map(encoding::data_sort_key)
        .unwrap_or_else(|| DATA_KEY_END.to_string());
    (lower, upper)
}

fn make_page(
    request: &RangeRequest,
    response: QueryResponse,
) -> Result<RangePage, RawOrderedJobStoreError> {
    let response_items = response.items.unwrap_or_default();
    validate_query_item_limit(response_items.len(), request)?;
    let (entries, has_unreturned, end_seen) = collect_entries(response_items, request)?;
    let continuation = page_continuation(
        request,
        &entries,
        has_unreturned,
        end_seen,
        response.last_evaluated_key.as_ref(),
    )?;
    RangePage::new(entries, continuation)
        .map_err(|_| RawOrderedJobStoreError::Corrupt("invalid scheduled range page"))
}

fn validate_query_item_limit(
    item_count: usize,
    request: &RangeRequest,
) -> Result<(), RawOrderedJobStoreError> {
    if item_count > request.item_limit().get() as usize {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled range response exceeded its item limit",
        ));
    }
    Ok(())
}

fn collect_entries(
    response_items: Vec<storage_types::WireItem>,
    request: &RangeRequest,
) -> Result<(Vec<RangeEntry>, bool, bool), RawOrderedJobStoreError> {
    let mut entries = Vec::new();
    let mut returned_bytes = 0usize;
    let mut has_unreturned = false;
    let mut end_seen = false;
    let mut stopped = false;
    for item in response_items {
        let attributes = storage_types::AttributeMap::try_from(item)
            .map_err(RawOrderedJobStoreError::Backend)?;
        let (key, value) = encoding::decode_data_item(&attributes)?;
        if request.end().is_some_and(|end| key == *end) {
            if end_seen {
                return Err(RawOrderedJobStoreError::Corrupt(
                    "scheduled range response contains duplicate end row",
                ));
            }
            end_seen = true;
            continue;
        }
        if stopped {
            has_unreturned = true;
            continue;
        }
        if entries.len() >= request.item_limit().get() as usize {
            has_unreturned = true;
            stopped = true;
            continue;
        }
        let entry_bytes = key.as_bytes().len().saturating_add(value.len());
        if entry_bytes > request.byte_limit().get() && entries.is_empty() {
            return Err(RawOrderedJobStoreError::Backend(
                storage_types::StorageError::validation(
                    "range byte limit cannot fit one scheduled value",
                ),
            ));
        }
        if returned_bytes.saturating_add(entry_bytes) > request.byte_limit().get() {
            has_unreturned = true;
            stopped = true;
            continue;
        }
        returned_bytes = returned_bytes.saturating_add(entry_bytes);
        entries.push(RangeEntry { key, value });
    }
    Ok((entries, has_unreturned, end_seen))
}

fn range_query_start(request: &RangeRequest) -> Option<&StoreKey> {
    request.continuation().or_else(|| {
        (request.direction() == RangeDirection::Reverse)
            .then(|| request.end())
            .flatten()
    })
}

fn page_continuation(
    request: &RangeRequest,
    entries: &[RangeEntry],
    has_unreturned: bool,
    end_seen: bool,
    last_evaluated_key: Option<&KeyAttributes>,
) -> Result<Option<StoreKey>, RawOrderedJobStoreError> {
    if entries.is_empty() && last_evaluated_key.is_some() {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled range response returned a cursor without rows",
        ));
    }
    if entries.is_empty() {
        return Ok(None);
    }
    if has_unreturned {
        return Ok(entries.last().map(|entry| entry.key.clone()));
    }
    if end_seen && request.direction() == RangeDirection::Forward {
        return Ok(None);
    }
    last_evaluated_key
        .map(encoding::decode_continuation)
        .transpose()
}
