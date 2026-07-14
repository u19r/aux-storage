use std::collections::HashMap;

use storage_types::{
    BatchGetItemRequest, BatchGetWireItemResponse, KeysAndAttributes, TableName, WireItem,
};

#[derive(Debug, Clone)]
pub struct BatchGetCachePlan {
    pub cacheable_request: BatchGetItemRequest,
    pub db_request: BatchGetItemRequest,
    pub total_cacheable_keys: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RuntimeBatchGetCacheOutcome {
    Hit,
    Miss,
    HitPartial,
}

#[derive(Debug, Clone)]
pub struct PreparedBatchGetExecution {
    pub db_request: BatchGetItemRequest,
    pub cached_responses: HashMap<TableName, Vec<WireItem>>,
    pub cache_outcome: Option<RuntimeBatchGetCacheOutcome>,
}

pub fn plan_batch_get_request(request: &BatchGetItemRequest) -> BatchGetCachePlan {
    plan_batch_get_request_with_options(request, BatchGetCachePlanOptions::default())
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct BatchGetCachePlanOptions {
    pub authoritative_strong_point_reads: bool,
}

pub fn plan_batch_get_request_with_options(
    request: &BatchGetItemRequest,
    options: BatchGetCachePlanOptions,
) -> BatchGetCachePlan {
    let mut cacheable_request_items = HashMap::new();
    let mut bypass_request_items = HashMap::new();

    for (table_name, keys_and_attributes) in &request.request_items {
        if keys_and_attributes.consistent_read.unwrap_or(false)
            && !options.authoritative_strong_point_reads
        {
            bypass_request_items.insert(table_name.clone(), keys_and_attributes.clone());
        } else {
            cacheable_request_items.insert(table_name.clone(), keys_and_attributes.clone());
        }
    }

    let cacheable_request = BatchGetItemRequest {
        request_items: cacheable_request_items,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    };
    let db_request = BatchGetItemRequest {
        request_items: bypass_request_items,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    };
    let total_cacheable_keys =
        batch_get_keys_and_attributes_count_map(&cacheable_request.request_items);

    BatchGetCachePlan {
        cacheable_request,
        db_request,
        total_cacheable_keys,
    }
}

pub fn batch_request_has_items(request: &BatchGetItemRequest) -> bool {
    !request.request_items.is_empty()
}

pub fn batch_get_keys_and_attributes_count_map(
    request_items: &HashMap<TableName, KeysAndAttributes>,
) -> usize {
    request_items
        .values()
        .map(|keys_and_attributes| keys_and_attributes.keys.len())
        .sum()
}

pub fn merge_cached_batch_get_response(
    response: &mut BatchGetWireItemResponse,
    cached_responses: HashMap<TableName, Vec<WireItem>>,
) {
    if cached_responses.is_empty() {
        return;
    }

    let Some(merged) = response.responses.as_mut() else {
        response.responses = Some(cached_responses);
        return;
    };
    for (table_name, mut items) in cached_responses {
        merged.entry(table_name).or_default().append(&mut items);
    }
}

pub fn finish_batch_get_request(
    batch_get_plan: BatchGetCachePlan,
    unresolved_request_items: HashMap<TableName, KeysAndAttributes>,
    cached_responses: HashMap<TableName, Vec<WireItem>>,
) -> PreparedBatchGetExecution {
    let mut db_request = batch_get_plan.db_request;
    let unresolved_cacheable_keys =
        batch_get_keys_and_attributes_count_map(&unresolved_request_items);
    let cache_hit_keys = batch_get_plan
        .total_cacheable_keys
        .saturating_sub(unresolved_cacheable_keys);
    db_request.request_items.extend(unresolved_request_items);
    let cache_outcome = runtime_batch_get_cache_outcome(
        batch_get_plan.total_cacheable_keys,
        cache_hit_keys,
        batch_request_has_items(&db_request),
    );

    PreparedBatchGetExecution {
        db_request,
        cached_responses,
        cache_outcome,
    }
}

fn runtime_batch_get_cache_outcome(
    total_cacheable_keys: usize,
    cache_hits: usize,
    still_needs_db: bool,
) -> Option<RuntimeBatchGetCacheOutcome> {
    if total_cacheable_keys == 0 {
        return None;
    }
    if cache_hits == 0 {
        return still_needs_db.then_some(RuntimeBatchGetCacheOutcome::Miss);
    }
    if still_needs_db {
        return Some(RuntimeBatchGetCacheOutcome::HitPartial);
    }
    Some(RuntimeBatchGetCacheOutcome::Hit)
}
