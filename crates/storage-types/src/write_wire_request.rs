use std::collections::HashMap;

use typed_builder::TypedBuilder;

use crate::{
    AllOld, AttributeValue, BatchWriteItemRequest, DeleteRequest, PutRequest,
    StreamRetentionDuration, TableName, TransactConditionCheckRequest, TransactDeleteRequest,
    TransactPutRequest, TransactUpdateRequest, TransactWriteItem, TransactWriteItemsRequest,
    WireItem, WriteRequest,
};

#[derive(Debug, Clone)]
pub struct PutItemEncodeRequest {
    pub table_name: TableName,
    pub item: WireItem,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<AllOld>,
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRetryPolicy {
    max_attempts: u32,
    delay: std::time::Duration,
}

impl WriteRetryPolicy {
    #[must_use]
    pub const fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            delay: std::time::Duration::ZERO,
        }
    }

    #[must_use]
    pub const fn new(max_attempts: u32, delay: std::time::Duration) -> Self {
        Self {
            max_attempts: if max_attempts == 0 { 1 } else { max_attempts },
            delay,
        }
    }

    #[must_use]
    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    #[must_use]
    pub const fn delay(self) -> std::time::Duration {
        self.delay
    }
}

#[allow(clippy::extra_unused_lifetimes)]
#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct BatchWriteItemEncodeRequest {
    #[builder(!default, setter(!strip_option))]
    pub request_items: HashMap<TableName, Vec<EncodeWriteRequest>>,
    pub return_consumed_capacity: Option<String>,
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct EncodeWriteRequest {
    pub put_request: Option<EncodePutRequest>,
    pub delete_request: Option<DeleteRequest>,
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct EncodePutRequest {
    #[builder(setter(!strip_option))]
    pub item: WireItem,
    #[builder(default, setter(strip_option))]
    pub aux_item_stream_ttl_hours: Option<crate::StreamRetentionDuration>,
}

impl TryFrom<BatchWriteItemEncodeRequest> for BatchWriteItemRequest {
    type Error = crate::StorageError;

    fn try_from(value: BatchWriteItemEncodeRequest) -> Result<Self, Self::Error> {
        let mut request_items = HashMap::with_capacity(value.request_items.len());
        for (table_name, write_requests) in value.request_items {
            let mut converted = Vec::with_capacity(write_requests.len());
            for write_request in write_requests {
                let put_request = match write_request.put_request {
                    Some(put_request) => Some(PutRequest {
                        item: put_request.item.into_attribute_map()?,
                        aux_item_stream_ttl_hours: put_request.aux_item_stream_ttl_hours,
                    }),
                    None => None,
                };

                converted.push(WriteRequest {
                    put_request,
                    delete_request: write_request.delete_request,
                });
            }
            request_items.insert(table_name, converted);
        }

        Ok(BatchWriteItemRequest {
            request_items,
            return_consumed_capacity: value.return_consumed_capacity,
            return_item_collection_metrics: value.return_item_collection_metrics,
        })
    }
}

impl TryFrom<BatchWriteItemRequest> for BatchWriteItemEncodeRequest {
    type Error = crate::StorageError;

    fn try_from(value: BatchWriteItemRequest) -> Result<Self, Self::Error> {
        let mut request_items = HashMap::with_capacity(value.request_items.len());
        for (table_name, write_requests) in value.request_items {
            let mut converted = Vec::with_capacity(write_requests.len());
            for write_request in write_requests {
                let put_request = match write_request.put_request {
                    Some(put_request) => Some(EncodePutRequest {
                        item: WireItem::from_attribute_map(&put_request.item)?,
                        aux_item_stream_ttl_hours: put_request.aux_item_stream_ttl_hours,
                    }),
                    None => None,
                };

                converted.push(EncodeWriteRequest {
                    put_request,
                    delete_request: write_request.delete_request,
                });
            }
            request_items.insert(table_name, converted);
        }

        Ok(BatchWriteItemEncodeRequest {
            request_items,
            return_consumed_capacity: value.return_consumed_capacity,
            return_item_collection_metrics: value.return_item_collection_metrics,
        })
    }
}

#[allow(clippy::extra_unused_lifetimes)]
#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactWriteItemsEncodeRequest {
    #[builder(!default, setter(!strip_option))]
    pub transact_items: Vec<TransactEncodeItem>,
    pub client_request_token: Option<String>,
    pub return_consumed_capacity: Option<String>,
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Debug, Clone, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactEncodeItem {
    pub put: Option<TransactEncodePutRequest>,
    pub update: Option<TransactUpdateRequest>,
    pub delete: Option<TransactDeleteRequest>,
    pub condition_check: Option<TransactConditionCheckRequest>,
}

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactEncodePutRequest {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub item: WireItem,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values_on_condition_check_failure: Option<String>,
    pub aux_item_stream_ttl_hours: Option<crate::StreamRetentionDuration>,
}

impl TryFrom<TransactWriteItemsEncodeRequest> for TransactWriteItemsRequest {
    type Error = crate::StorageError;

    fn try_from(value: TransactWriteItemsEncodeRequest) -> Result<Self, Self::Error> {
        let mut transact_items = Vec::with_capacity(value.transact_items.len());
        for item in value.transact_items {
            let put = match item.put {
                Some(put_request) => Some(TransactPutRequest {
                    table_name: put_request.table_name,
                    item: put_request.item.into_attribute_map()?,
                    condition_expression: put_request.condition_expression,
                    expression_attribute_names: put_request.expression_attribute_names,
                    expression_attribute_values: put_request.expression_attribute_values,
                    return_values_on_condition_check_failure: put_request
                        .return_values_on_condition_check_failure,
                    aux_item_stream_ttl_hours: put_request.aux_item_stream_ttl_hours,
                }),
                None => None,
            };

            transact_items.push(TransactWriteItem {
                put,
                update: item.update,
                delete: item.delete,
                condition_check: item.condition_check,
            });
        }

        Ok(TransactWriteItemsRequest {
            transact_items,
            client_request_token: value.client_request_token,
            return_consumed_capacity: value.return_consumed_capacity,
            return_item_collection_metrics: value.return_item_collection_metrics,
        })
    }
}
