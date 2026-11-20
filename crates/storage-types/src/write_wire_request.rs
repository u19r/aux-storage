use typed_builder::TypedBuilder;

use crate::{
    AttributeMap, BatchWriteItemRequest, DeleteRequest, PutRequest, ReturnConsumedCapacity,
    TableName, TransactConditionCheckRequest, TransactDeleteRequest, TransactPutRequest,
    TransactUpdateRequest, TransactWriteItem, TransactWriteItemsRequest, VecMap, WriteRequest,
    single_table_entity::WireEntity,
};

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

    /// Delay after a known-aborted conflict; attempt numbers start at one.
    #[must_use]
    pub fn delay_after_conflict(self, attempt: u32) -> std::time::Duration {
        let multiplier = 1_u32 << attempt.saturating_sub(1).min(31);
        self.delay
            .saturating_mul(multiplier)
            .min(std::time::Duration::from_secs(1))
    }
}

#[allow(clippy::extra_unused_lifetimes)]
#[derive(Clone, Default, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct BatchWriteItemEncodeRequest {
    #[builder(!default, setter(!strip_option))]
    pub request_items: VecMap<TableName, Vec<EncodeWriteRequest>>,
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Clone, Default, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct EncodeWriteRequest {
    pub put_request: Option<EncodePutRequest>,
    pub delete_request: Option<DeleteRequest>,
}

#[derive(Clone, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct EncodePutRequest {
    #[builder(setter(!strip_option))]
    pub item: WireEntity,
}

impl TryFrom<BatchWriteItemEncodeRequest> for BatchWriteItemRequest {
    type Error = crate::StorageError;

    fn try_from(value: BatchWriteItemEncodeRequest) -> Result<Self, Self::Error> {
        let mut request_items = VecMap::with_capacity(value.request_items.len());
        for (table_name, write_requests) in value.request_items {
            let mut converted = Vec::with_capacity(write_requests.len());
            for write_request in write_requests {
                let put_request = match write_request.put_request {
                    Some(put_request) => {
                        let (item, indexers) = put_request.item.into_write_parts();
                        Some(PutRequest { item, indexers })
                    }
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
        let mut request_items = VecMap::with_capacity(value.request_items.len());
        for (table_name, write_requests) in value.request_items {
            let mut converted = Vec::with_capacity(write_requests.len());
            for write_request in write_requests {
                let put_request = match write_request.put_request {
                    Some(put_request) => Some(EncodePutRequest {
                        item: WireEntity::from_write_parts(put_request.item, put_request.indexers),
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
#[derive(Clone, Default, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactWriteItemsEncodeRequest {
    #[builder(!default, setter(!strip_option))]
    pub transact_items: Vec<TransactEncodeItem>,
    pub client_request_token: Option<String>,
    pub return_consumed_capacity: Option<ReturnConsumedCapacity>,
    pub return_item_collection_metrics: Option<String>,
}

#[derive(Clone, Default, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactEncodeItem {
    pub put: Option<TransactEncodePutRequest>,
    pub update: Option<TransactUpdateRequest>,
    pub delete: Option<TransactDeleteRequest>,
    pub condition_check: Option<TransactConditionCheckRequest>,
}

#[derive(Clone, TypedBuilder)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct TransactEncodePutRequest {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub item: WireEntity,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,
    pub expression_attribute_values: Option<AttributeMap>,
    pub return_values_on_condition_check_failure: Option<String>,
}

impl TryFrom<TransactWriteItemsEncodeRequest> for TransactWriteItemsRequest {
    type Error = crate::StorageError;

    fn try_from(value: TransactWriteItemsEncodeRequest) -> Result<Self, Self::Error> {
        let mut transact_items = Vec::with_capacity(value.transact_items.len());
        for item in value.transact_items {
            let put = match item.put {
                Some(put_request) => {
                    let (item, indexers) = put_request.item.into_write_parts();
                    Some(TransactPutRequest {
                        table_name: put_request.table_name,
                        item,
                        indexers,
                        condition_expression: put_request.condition_expression,
                        expression_attribute_names: put_request.expression_attribute_names,
                        expression_attribute_values: put_request.expression_attribute_values,
                        return_values_on_condition_check_failure: put_request
                            .return_values_on_condition_check_failure,
                    })
                }
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
