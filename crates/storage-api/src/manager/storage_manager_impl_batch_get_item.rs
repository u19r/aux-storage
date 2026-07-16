use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};

use futures::future::join_all;
use http_error::HttpApiError;
use storage_types::{
    AttributeMap, BatchGetItemRequest, BatchGetItemResponse, BatchGetWireItemResponse,
    AttributeValue, KeyAttributes, KeySchemaElement, KeysAndAttributes, StorageEnum, StorageError,
    StoredTableInfo, context::WrappedError as _, normalize_dynamodb_number_for_write,
    validate_key_attributes_for_schema, validate_transact_key,
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
        let resolutions = join_all(request_shape.request_items.keys().cloned().map(|table_name| {
            async move {
                let operation = self.db().resolve_storage_operation(table_name.clone()).await;
                (table_name, operation)
            }
        }))
        .await;
        let mut operations = Vec::with_capacity(resolutions.len());
        for (table_name, operation) in resolutions {
            let operation = operation?;
            let keys_and_attributes = request_shape
                .request_items
                .get(&table_name)
                .ok_or_else(|| StorageError::internal("resolved BatchGet table is missing"))?;
            self.validate_batch_get_keys(keys_and_attributes, operation.table_info())?;
            operations.push(operation);
        }
        let plan = storage::ResolvedBatchGetPlan::new(operations);

        let needs_barrier = request
            .request_items
            .values()
            .any(|keys| keys.consistent_read.unwrap_or(false));
        self.ensure_sync_read_barrier(needs_barrier).await?;
        let wire_response = self.db().batch_get_item_resolved(request, plan).await?;

        if batch_get_needs_decoded_response(&request_shape) {
            let response = project_batch_get_response(wire_response, &request_shape)?;
            return Ok(Response::BatchGetItem(response));
        }

        let wire_response = add_empty_batch_get_response_tables(wire_response, &request_shape);
        Ok(Response::BatchGetWire(BatchGetWireResponse::from(
            wire_response,
        )))
    }

    fn validate_batch_get_keys(
        &self,
        keys_and_attributes: &KeysAndAttributes,
        table_info: &StoredTableInfo,
    ) -> Result<(), HttpApiError> {
        let mut seen = HashSet::with_capacity(keys_and_attributes.keys.len());
        for key in &keys_and_attributes.keys {
            validate_batch_get_key(table_info, key)?;
            if !seen.insert(BatchGetKeyIdentity::new(&table_info.key_schema, key)) {
                return Err(StorageError::validation(
                    "Provided list of item keys contains duplicates",
                )
                .into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct BatchGetKeyIdentity<'a> {
    key_schema: &'a [KeySchemaElement],
    key: &'a KeyAttributes,
}

impl<'a> BatchGetKeyIdentity<'a> {
    pub(super) fn new(key_schema: &'a [KeySchemaElement], key: &'a KeyAttributes) -> Self {
        Self { key_schema, key }
    }
}

impl PartialEq for BatchGetKeyIdentity<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key_schema.len() == other.key_schema.len()
            && self
                .key_schema
                .iter()
                .zip(other.key_schema)
                .all(|(left_schema, right_schema)| {
                    left_schema.attribute_name == right_schema.attribute_name
                        && match (
                            self.key.get(&left_schema.attribute_name),
                            other.key.get(&right_schema.attribute_name),
                        ) {
                            (Some(left), Some(right)) => canonical_key_value_eq(left, right),
                            (None, None) => true,
                            _ => false,
                        }
                })
    }
}

impl Eq for BatchGetKeyIdentity<'_> {}

impl Hash for BatchGetKeyIdentity<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key_schema.len().hash(state);
        for schema in self.key_schema {
            schema.attribute_name.hash(state);
            if let Some(value) = self.key.get(&schema.attribute_name) {
                hash_canonical_key_value(value, state);
            }
        }
    }
}

fn canonical_key_value_eq(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            normalize_dynamodb_number_for_write(left) == normalize_dynamodb_number_for_write(right)
        }
        _ => left == right,
    }
}

fn hash_canonical_key_value<H: Hasher>(value: &AttributeValue, state: &mut H) {
    match value {
        AttributeValue::S(value) => {
            0_u8.hash(state);
            value.hash(state);
        }
        AttributeValue::N(value) => {
            1_u8.hash(state);
            normalize_dynamodb_number_for_write(value).hash(state);
        }
        AttributeValue::B(value) => {
            2_u8.hash(state);
            value.hash(state);
        }
        value => {
            3_u8.hash(state);
            std::mem::discriminant(value).hash(state);
        }
    }
}

pub(super) fn batch_get_needs_decoded_response(request: &BatchGetItemRequest) -> bool {
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

pub(super) fn project_batch_get_response(
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

pub(super) fn add_empty_batch_get_response_tables(
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
