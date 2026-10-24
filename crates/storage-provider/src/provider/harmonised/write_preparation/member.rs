//! Shared table-write membership (phase 4, §4.1).
//!
//! Closed write alternatives with retained identity. Native adapters build
//! these from already-framed requests; the shared preparation owner converts
//! them.

use storage_types::{
    DeleteItemRequest, PutItemRequest, StorageError, StorageResult, TableName, TransactWriteItem,
    TransactWriteItemsRequest, UpdateItemRequest, WriteRequest,
};

use super::condition_check::TableCheckMember;
use crate::{OrderedOperationContexts, PhysicalWrite, TtlWriteContext};

/// One table write member with its retained request, context and ordinal.
#[derive(Clone)]
pub enum TableWriteMember {
    Put(PhysicalWrite<PutItemRequest, TtlWriteContext>),
    Update(PhysicalWrite<UpdateItemRequest, TtlWriteContext>),
    Delete(PhysicalWrite<DeleteItemRequest, TtlWriteContext>),
    Check(TableCheckMember),
}

impl TableWriteMember {
    /// Convert transact items into shared members while retaining the source
    /// ordinal and per-item TTL context.
    pub fn from_transact_request(
        request: &TransactWriteItemsRequest,
        contexts: &OrderedOperationContexts,
    ) -> StorageResult<Vec<Self>> {
        request
            .transact_items
            .iter()
            .enumerate()
            .map(|(ordinal, item)| Self::from_transact_item(item, contexts, ordinal))
            .collect()
    }

    /// Convert one canonical `BatchWriteItem` member while retaining its
    /// source ordinal and TTL context. Batch adapters use this shared
    /// conversion so put/delete request framing cannot drift by provider.
    pub fn from_batch_write(
        operation: &PhysicalWrite<WriteRequest, TtlWriteContext>,
    ) -> StorageResult<Self> {
        let table = operation.physical_table().clone();
        let context = operation.context().clone();
        match operation.request() {
            WriteRequest {
                put_request: Some(put),
                delete_request: None,
                ..
            } => {
                let request = PutItemRequest {
                    indexers: put.indexers.clone(),
                    aux_item_stream_ttl_hours: put.aux_item_stream_ttl_hours,
                    ..PutItemRequest::new(table.clone(), put.item.clone())
                };
                Ok(Self::Put(PhysicalWrite::try_new(
                    operation.ordinal(),
                    table,
                    request,
                    context,
                )?))
            }
            WriteRequest {
                put_request: None,
                delete_request: Some(delete),
                ..
            } => {
                let request = DeleteItemRequest {
                    aux_item_stream_ttl_hours: delete.aux_item_stream_ttl_hours,
                    ..DeleteItemRequest::new(table.clone(), delete.key.clone())
                };
                Ok(Self::Delete(PhysicalWrite::try_new(
                    operation.ordinal(),
                    table,
                    request,
                    context,
                )?))
            }
            _ => Err(StorageError::validation(
                "batch write request must contain exactly one of a put or delete action",
            )),
        }
    }

    fn from_transact_item(
        item: &TransactWriteItem,
        contexts: &OrderedOperationContexts,
        ordinal: usize,
    ) -> StorageResult<Self> {
        let table = storage_types::transact_item_table_name(item).ok_or_else(|| {
            StorageError::validation("transaction operation must contain one table-bound action")
        })?;
        let context = contexts.operations()[ordinal].context().clone();
        match item {
            TransactWriteItem {
                put: Some(request),
                update: None,
                delete: None,
                condition_check: None,
            } => {
                let request = PutItemRequest {
                    condition_expression: request.condition_expression.clone(),
                    expression_attribute_names: request.expression_attribute_names.clone(),
                    expression_attribute_values: request.expression_attribute_values.clone(),
                    indexers: request.indexers.clone(),
                    return_values: None,
                    return_values_on_condition_check_failure: request
                        .return_values_on_condition_check_failure
                        .clone(),
                    aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
                    ..PutItemRequest::new(table.clone(), request.item.clone())
                };
                Ok(Self::Put(PhysicalWrite::try_new(
                    ordinal,
                    table.clone(),
                    request,
                    context,
                )?))
            }
            TransactWriteItem {
                put: None,
                update: Some(request),
                delete: None,
                condition_check: None,
            } => {
                let request = UpdateItemRequest {
                    key: request.key.clone(),
                    indexers: request.indexers.clone(),
                    update_expression: Some(request.update_expression.clone()),
                    attribute_updates: None,
                    condition_expression: request.condition_expression.clone(),
                    expression_attribute_names: request.expression_attribute_names.clone(),
                    expression_attribute_values: request.expression_attribute_values.clone(),
                    expected: None,
                    conditional_operator: None,
                    return_values: None,
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                    return_values_on_condition_check_failure: request
                        .return_values_on_condition_check_failure
                        .clone(),
                    aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
                    table_name: table.clone(),
                };
                Ok(Self::Update(PhysicalWrite::try_new(
                    ordinal,
                    table.clone(),
                    request,
                    context,
                )?))
            }
            TransactWriteItem {
                put: None,
                update: None,
                delete: Some(request),
                condition_check: None,
            } => {
                let request = DeleteItemRequest {
                    key: request.key.clone(),
                    condition_expression: request.condition_expression.clone(),
                    expression_attribute_names: request.expression_attribute_names.clone(),
                    expression_attribute_values: request.expression_attribute_values.clone(),
                    expected: None,
                    conditional_operator: None,
                    return_values: None,
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                    return_values_on_condition_check_failure: request
                        .return_values_on_condition_check_failure
                        .clone(),
                    aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
                    table_name: table.clone(),
                };
                Ok(Self::Delete(PhysicalWrite::try_new(
                    ordinal,
                    table.clone(),
                    request,
                    context,
                )?))
            }
            TransactWriteItem {
                put: None,
                update: None,
                delete: None,
                condition_check: Some(request),
            } => Ok(Self::Check(TableCheckMember::new(ordinal, request.clone()))),
            _ => Err(StorageError::validation(
                "Each TransactWriteItem must contain exactly one of Put, Update, Delete, or \
                 ConditionCheck",
            )),
        }
    }

    /// Physical table retained by the member's request/context.
    pub fn table_name(&self) -> &TableName {
        match self {
            Self::Put(write) => write.physical_table(),
            Self::Update(write) => write.physical_table(),
            Self::Delete(write) => write.physical_table(),
            Self::Check(check) => &check.request().table_name,
        }
    }

    /// Source ordinal retained by the member carrier, never a per-verb
    /// position.
    pub fn ordinal(&self) -> usize {
        match self {
            Self::Put(write) => write.ordinal(),
            Self::Update(write) => write.ordinal(),
            Self::Delete(write) => write.ordinal(),
            Self::Check(check) => check.ordinal(),
        }
    }
}
