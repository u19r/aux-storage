#![allow(non_snake_case)]

use std::collections::HashMap;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{
    AttributeValue, BatchWriteItemRequest, KeyAttributes, PutRequest, StreamRetentionDuration,
    TableName, TransactPutRequest, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsRequest, UpdateTableRequest, WriteRequest,
};

use crate::{
    ItemStreamTtlIntent, batch_write_request_has_custom_item_stream_ttl,
    transaction_request_has_custom_item_stream_ttl,
    update_table_request_has_custom_stream_duration, validate_transaction_item_ttl_intents,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ApiCase {
    operation: String,
    #[serde(rename = "tableExtension")]
    table_extension: bool,
    #[serde(rename = "defaultItemExtension")]
    default_item_extension: bool,
    #[serde(rename = "putTtl")]
    put_ttl: bool,
    #[serde(rename = "updateTtl")]
    update_ttl: bool,
    #[serde(rename = "conflictingTxnTtl")]
    conflicting_txn_ttl: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct CustomStreamDurationApiState {
    #[serde(rename = "lastCase")]
    last_case: ApiCase,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<CustomStreamDurationApiDriver> for CustomStreamDurationApiState {
    fn from_driver(driver: &CustomStreamDurationApiDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct CustomStreamDurationApiDriver {
    last_case: ApiCase,
    last_decision: String,
}

impl Default for CustomStreamDurationApiDriver {
    fn default() -> Self {
        Self {
            last_case: ApiCase {
                operation: "update_table".to_string(),
                table_extension: false,
                default_item_extension: false,
                put_ttl: false,
                update_ttl: false,
                conflicting_txn_ttl: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for CustomStreamDurationApiDriver {
    type State = CustomStreamDurationApiState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                operation: String,
                tableExtension: bool,
                defaultItemExtension: bool,
                putTtl: bool,
                updateTtl: bool,
                conflictingTxnTtl: bool
            ) => {
                self.check(ApiCase {
                    operation,
                    table_extension: tableExtension,
                    default_item_extension: defaultItemExtension,
                    put_ttl: putTtl,
                    update_ttl: updateTtl,
                    conflicting_txn_ttl: conflictingTxnTtl,
                });
            },
            step(
                operation: String?,
                tableExtension: bool?,
                defaultItemExtension: bool?,
                putTtl: bool?,
                updateTtl: bool?,
                conflictingTxnTtl: bool?
            ) => {
                if let (
                    Some(operation),
                    Some(table_extension),
                    Some(default_item_extension),
                    Some(put_ttl),
                    Some(update_ttl),
                    Some(conflicting_txn_ttl),
                ) = (
                    operation,
                    tableExtension,
                    defaultItemExtension,
                    putTtl,
                    updateTtl,
                    conflictingTxnTtl,
                ) {
                    self.check(ApiCase {
                        operation,
                        table_extension,
                        default_item_extension,
                        put_ttl,
                        update_ttl,
                        conflicting_txn_ttl,
                    });
                }
            },
        })
    }
}

impl CustomStreamDurationApiDriver {
    fn check(&mut self, api_case: ApiCase) {
        self.last_decision = api_decision(&api_case).to_string();
        self.last_case = api_case;
    }
}

fn api_decision(api_case: &ApiCase) -> &'static str {
    match api_case.operation.as_str() {
        "update_table" => {
            if update_table_request_has_custom_stream_duration(&update_table_request(api_case)) {
                "extension_present"
            } else {
                "standard"
            }
        }
        "batch_write" => {
            if batch_write_request_has_custom_item_stream_ttl(&batch_write_request(api_case)) {
                "extension_present"
            } else {
                "standard"
            }
        }
        "transact_write" => {
            if validate_transaction_item_ttl_intents(&transaction_ttl_intents(api_case)).is_err() {
                "conflicting_ttl_rejected"
            } else if transaction_request_has_custom_item_stream_ttl(&transaction_request(api_case))
            {
                "extension_present"
            } else {
                "standard"
            }
        }
        _ => "standard",
    }
}

fn update_table_request(api_case: &ApiCase) -> UpdateTableRequest {
    UpdateTableRequest {
        table_name: TableName::new("ApiMbt"),
        max_indexers: None,
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        replica_updates: None,
        sse_specification: None,
        stream_specification: None,
        aux_stream_duration_hours: api_case
            .table_extension
            .then_some(StreamRetentionDuration::FiniteHours(24)),
        aux_default_item_stream_duration_hours: api_case
            .default_item_extension
            .then_some(StreamRetentionDuration::Forever),
        table_class: None,
    }
}

fn batch_write_request(api_case: &ApiCase) -> BatchWriteItemRequest {
    BatchWriteItemRequest {
        request_items: HashMap::from([(
            TableName::new("ApiMbt"),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: item("item"),
                    indexers: None,
                    aux_item_stream_ttl_hours: api_case
                        .put_ttl
                        .then_some(StreamRetentionDuration::FiniteHours(24)),
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn transaction_request(api_case: &ApiCase) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: TableName::new("ApiMbt"),
                    item: item("item"),
                    indexers: None,
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: api_case
                        .put_ttl
                        .then_some(StreamRetentionDuration::FiniteHours(24)),
                }),
                ..TransactWriteItem::default()
            },
            TransactWriteItem {
                update: Some(TransactUpdateRequest {
                    table_name: TableName::new("ApiMbt"),
                    key: key("item"),
                    update_expression: "SET #value = :value".to_string(),
                    indexers: None,
                    condition_expression: None,
                    expression_attribute_names: Some(HashMap::from([(
                        "#value".to_string(),
                        "value".to_string(),
                    )])),
                    expression_attribute_values: Some(HashMap::from([(
                        ":value".to_string(),
                        AttributeValue::S("next".to_string()),
                    )])),
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: api_case.update_ttl.then_some(
                        if api_case.conflicting_txn_ttl {
                            StreamRetentionDuration::FiniteHours(48)
                        } else {
                            StreamRetentionDuration::FiniteHours(24)
                        },
                    ),
                }),
                ..TransactWriteItem::default()
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn transaction_ttl_intents(api_case: &ApiCase) -> Vec<ItemStreamTtlIntent> {
    let request = transaction_request(api_case);
    let mut intents = Vec::new();
    for item in request.transact_items {
        if let Some(put) = item.put
            && let Some(retention) = put.aux_item_stream_ttl_hours
        {
            intents.push(ItemStreamTtlIntent {
                table_name: put.table_name,
                item_key: key("item"),
                retention,
            });
        }
        if let Some(update) = item.update
            && let Some(retention) = update.aux_item_stream_ttl_hours
        {
            intents.push(ItemStreamTtlIntent {
                table_name: update.table_name,
                item_key: update.key,
                retention,
            });
        }
    }
    intents
}

fn item(id: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("id".to_string(), AttributeValue::S(id.to_string())),
        ("value".to_string(), AttributeValue::S("value".to_string())),
    ])
}

fn key(id: &str) -> KeyAttributes {
    HashMap::from([("id".to_string(), AttributeValue::S(id.to_string()))]).into()
}

#[quint_run(
    spec = "../../quint/custom_stream_duration_api_mbt.qnt",
    max_samples = 64,
    max_steps = 8,
    seed = "0xc57d1a9"
)]
fn custom_stream_duration_api_mbt_matches_planner() -> impl Driver {
    CustomStreamDurationApiDriver::default()
}
