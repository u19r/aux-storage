use std::collections::HashMap;

use storage_cache::{
    BatchGetCachePlanOptions, PreparedBatchGetExecution, RuntimeBatchGetCacheOutcome,
    batch_request_has_items, finish_batch_get_request, merge_cached_batch_get_response,
    plan_batch_get_request_with_options,
};
use storage_types::{
    BatchGetItemRequest, DurableBatchPointReadProof, KeysAndAttributes, StorageResult, TableName,
    WireItem,
};

use crate::{
    cache_coordinator::StorageCacheServices,
    cache_read_observability::{
        StorageCacheReadOperation, StorageCacheReadOutcome, record_storage_cache_read_outcome,
    },
    point_read_cache::{AuthoritativePointReadPurpose, PointReadBatchGetResult},
};

pub(crate) type PreparedBatchGetCacheRead = PreparedBatchGetExecution;

pub(crate) struct StorageBatchGetCacheRuntime<'a> {
    services: &'a StorageCacheServices,
}

impl<'a> StorageBatchGetCacheRuntime<'a> {
    pub(crate) fn new(services: &'a StorageCacheServices) -> Self {
        Self { services }
    }

    pub(crate) async fn prepare(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<PreparedBatchGetCacheRead> {
        if !self.services.point_read_enabled() {
            return Ok(PreparedBatchGetCacheRead {
                db_request: request,
                cached_responses: HashMap::new(),
                cache_outcome: None,
            });
        }

        let batch_get_plan = plan_batch_get_request_with_options(
            &request,
            BatchGetCachePlanOptions {
                authoritative_strong_point_reads: self
                    .services
                    .authoritative_strong_point_reads_enabled(),
            },
        );
        let cache_result = if batch_request_has_items(&batch_get_plan.cacheable_request) {
            if request_contains_consistent_reads(&batch_get_plan.cacheable_request) {
                self.services
                    .batch_get_authoritative_point_reads(
                        &batch_get_plan.cacheable_request,
                        AuthoritativePointReadPurpose::StrongBatchGet,
                    )
                    .await?
            } else {
                self.services
                    .batch_get_eventual_point_reads(&batch_get_plan.cacheable_request)
                    .await?
            }
        } else {
            PointReadBatchGetResult {
                responses: HashMap::new(),
                unresolved_request_items: HashMap::new(),
            }
        };
        Ok(finish_batch_get_request(
            batch_get_plan,
            cache_result.unresolved_request_items,
            cache_result.responses,
        ))
    }

    pub(crate) fn merge_cached_responses(
        &self,
        response: &mut storage_types::BatchGetWireItemResponse,
        cached_responses: HashMap<TableName, Vec<WireItem>>,
    ) {
        merge_cached_batch_get_response(response, cached_responses);
    }

    pub(crate) fn record_outcome(&self, outcome: Option<RuntimeBatchGetCacheOutcome>) {
        if let Some(outcome) = outcome {
            let outcome = match outcome {
                RuntimeBatchGetCacheOutcome::Hit => StorageCacheReadOutcome::Hit,
                RuntimeBatchGetCacheOutcome::Miss => StorageCacheReadOutcome::Miss,
                RuntimeBatchGetCacheOutcome::HitPartial => StorageCacheReadOutcome::HitPartial,
            };
            record_storage_cache_read_outcome(StorageCacheReadOperation::BatchGetItem, outcome);
        }
    }

    pub(crate) fn strong_read_through_warming_enabled(&self) -> bool {
        self.services.strong_read_through_warming_enabled()
    }

    pub(crate) async fn warm_authoritative_batch(
        &self,
        proof: DurableBatchPointReadProof,
    ) -> StorageResult<()> {
        self.services
            .warm_authoritative_batch_point_reads(proof)
            .await
    }
}

pub(crate) fn request_contains_consistent_reads(request: &BatchGetItemRequest) -> bool {
    request
        .request_items
        .values()
        .any(|keys_and_attributes| keys_and_attributes.consistent_read.unwrap_or(false))
}

pub(crate) fn strong_only_batch_get_request(
    request: &BatchGetItemRequest,
) -> Option<BatchGetItemRequest> {
    let request_items = request
        .request_items
        .iter()
        .filter(|(_, keys_and_attributes)| keys_and_attributes.consistent_read.unwrap_or(false))
        .map(|(table_name, keys_and_attributes)| {
            (
                table_name.clone(),
                clone_keys_and_attributes(keys_and_attributes),
            )
        })
        .collect::<HashMap<_, _>>();

    (!request_items.is_empty()).then(|| BatchGetItemRequest {
        request_items,
        return_consumed_capacity: request.return_consumed_capacity.clone(),
    })
}

fn clone_keys_and_attributes(keys_and_attributes: &KeysAndAttributes) -> KeysAndAttributes {
    KeysAndAttributes {
        keys: keys_and_attributes.keys.clone(),
        attributes_to_get: keys_and_attributes.attributes_to_get.clone(),
        projection_expression: keys_and_attributes.projection_expression.clone(),
        expression_attribute_names: keys_and_attributes.expression_attribute_names.clone(),
        consistent_read: keys_and_attributes.consistent_read,
    }
}
