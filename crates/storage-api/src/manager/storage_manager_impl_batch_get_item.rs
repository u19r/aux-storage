use std::collections::{HashMap, HashSet};

use http_error::HttpApiError;
use storage_types::{
    AttributeMap, BatchGetItemRequest, BatchGetItemResponse, BatchGetWireItemResponse,
    KeyAttributes, KeysAndAttributes, StorageEnum, StorageError, StoredTableInfo,
    context::WrappedError as _, validate_key_attributes_for_schema, validate_transact_key,
};

use crate::{
    batch_get_wire_response::BatchGetWireResponse,
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_expression::{
            apply_projection_expression_refs, project_attribute_map,
        },
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn batch_get_item_internal(
        &self,
        request: BatchGetItemRequest,
    ) -> Result<Response, HttpApiError> {
        let request_shape = request.clone();
        self.validate_batch_get_keys(&request_shape).await?;

        let needs_barrier = request
            .request_items
            .values()
            .any(|keys| keys.consistent_read.unwrap_or(false));
        self.ensure_sync_read_barrier(needs_barrier).await?;
        let wire_response = self.db().batch_get_item(request).await?;

        if batch_get_needs_decoded_response(&request_shape) {
            let response = project_batch_get_response(wire_response, &request_shape)?;
            return Ok(Response::BatchGetItem(response));
        }

        let wire_response = add_empty_batch_get_response_tables(wire_response, &request_shape);
        Ok(Response::BatchGetWire(BatchGetWireResponse::from(
            wire_response,
        )))
    }

    async fn validate_batch_get_keys(
        &self,
        request: &BatchGetItemRequest,
    ) -> Result<(), HttpApiError> {
        for (table_name, keys_and_attributes) in &request.request_items {
            let table_info = self.db().get_table_info(table_name).await?;
            let mut seen = HashSet::with_capacity(keys_and_attributes.keys.len());
            for key in &keys_and_attributes.keys {
                validate_batch_get_key(&table_info, key)?;
                let fingerprint = key
                    .canonical_dynamo_json()
                    .map_err(|err| StorageError::internal(&err.to_string()))?;
                if !seen.insert(fingerprint) {
                    return Err(StorageError::validation(
                        "Provided list of item keys contains duplicates",
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

fn batch_get_needs_decoded_response(request: &BatchGetItemRequest) -> bool {
    request.request_items.values().any(|keys| {
        keys.projection_expression.is_some()
            || keys
                .attributes_to_get
                .as_ref()
                .is_some_and(|attrs| !attrs.is_empty())
    })
}

fn validate_batch_get_key(
    table_info: &StoredTableInfo,
    key: &KeyAttributes,
) -> Result<(), HttpApiError> {
    if key.len() != table_info.key_schema.len() {
        return Err(key_schema_validation_error());
    }
    validate_transact_key(table_info, key).map_err(key_validation_error)?;
    validate_key_attributes_for_schema(&table_info.key_schema, key).map_err(HttpApiError::from)
}

fn key_schema_validation_error() -> HttpApiError {
    HttpApiError::from(StorageError::validation(
        "The provided key element does not match the schema",
    ))
}

fn key_validation_error(error: StorageError) -> HttpApiError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return HttpApiError::from(error);
    };
    if message == "The provided key element does not match the schema" {
        return key_schema_validation_error();
    }
    HttpApiError::from(StorageError::validation(message.clone()))
}

fn project_batch_get_response(
    wire_response: BatchGetWireItemResponse,
    request: &BatchGetItemRequest,
) -> Result<BatchGetItemResponse, HttpApiError> {
    let mut response = BatchGetWireResponse::from(wire_response).into_batch_get_response()?;
    let mut projected_responses = HashMap::with_capacity(request.request_items.len());
    let responses = response.responses.take().unwrap_or_default();

    for (table_name, keys_and_attributes) in &request.request_items {
        let items = responses.get(table_name).cloned().unwrap_or_default();
        let projected_items = project_batch_get_items(items, keys_and_attributes);
        projected_responses.insert(table_name.clone(), projected_items);
    }

    Ok(BatchGetItemResponse {
        responses: Some(projected_responses),
        unprocessed_keys: Some(response.unprocessed_keys.unwrap_or_default()),
        consumed_capacity: response.consumed_capacity,
    })
}

fn project_batch_get_items(
    items: Vec<AttributeMap>,
    keys_and_attributes: &KeysAndAttributes,
) -> Vec<AttributeMap> {
    match keys_and_attributes.projection_expression.as_deref() {
        Some(projection_expression) => {
            let item_maps = items
                .into_iter()
                .map(AttributeMap::into_hashmap)
                .collect::<Vec<_>>();
            let item_refs = item_maps.iter().collect::<Vec<_>>();
            apply_projection_expression_refs(
                &item_refs,
                projection_expression,
                keys_and_attributes.expression_attribute_names.as_ref(),
            )
            .into_iter()
            .map(AttributeMap::from)
            .collect()
        }
        None => items
            .into_iter()
            .map(|item| {
                project_attribute_map(
                    item,
                    None,
                    keys_and_attributes.attributes_to_get.as_deref(),
                    keys_and_attributes.expression_attribute_names.as_ref(),
                )
            })
            .collect(),
    }
}

fn add_empty_batch_get_response_tables(
    mut response: BatchGetWireItemResponse,
    request: &BatchGetItemRequest,
) -> BatchGetWireItemResponse {
    let responses = response.responses.get_or_insert_with(HashMap::new);
    for table_name in request.request_items.keys() {
        responses.entry(table_name.clone()).or_default();
    }
    response.unprocessed_keys.get_or_insert_with(HashMap::new);
    response
}
