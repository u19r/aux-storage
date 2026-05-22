use serde_json::json;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, TableName,
};

use crate::{
    routes::routes_test_support::{
        default_conformance_backends, handle_batch_get_item, handle_batch_write_item,
    },
    types::Response,
};

#[tokio::test]
async fn batch_write_put_delete_round_trips_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_batch_table(db.as_ref()).await;

        let put_response = handle_batch_write_item(
            db.clone(),
            json!({
                "RequestItems": {
                    "BatchWriteRouteTable": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "item-1"},
                                    "payload": {"S": "value-1"}
                                }
                            }
                        },
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "item-2"},
                                    "payload": {"S": "value-2"}
                                }
                            }
                        }
                    ]
                }
            })
            .try_into()
            .expect("batch write put request"),
        )
        .await
        .unwrap_or_else(|err| panic!("{} batch write put: {err:?}", backend.name));
        assert_empty_batch_write_response(&put_response, backend.name);

        let items = batch_get_payloads(db.clone(), backend.name).await;
        assert_eq!(items, vec!["value-1".to_string(), "value-2".to_string()]);

        let delete_response = handle_batch_write_item(
            db.clone(),
            json!({
                "RequestItems": {
                    "BatchWriteRouteTable": [
                        {
                            "DeleteRequest": {
                                "Key": {"pk": {"S": "item-1"}}
                            }
                        }
                    ]
                }
            })
            .try_into()
            .expect("batch write delete request"),
        )
        .await
        .unwrap_or_else(|err| panic!("{} batch write delete: {err:?}", backend.name));
        assert_empty_batch_write_response(&delete_response, backend.name);

        let items = batch_get_payloads(db, backend.name).await;
        assert_eq!(items, vec!["value-2".to_string()], "{}", backend.name);
    }
}

#[tokio::test]
async fn batch_write_duplicate_keys_reject_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_batch_table(db.as_ref()).await;

        for payload in [
            json!({
                "RequestItems": {
                    "BatchWriteRouteTable": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "dup"},
                                    "payload": {"S": "first"}
                                }
                            }
                        },
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "dup"},
                                    "payload": {"S": "second"}
                                }
                            }
                        }
                    ]
                }
            }),
            json!({
                "RequestItems": {
                    "BatchWriteRouteTable": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "dup-delete"},
                                    "payload": {"S": "first"}
                                }
                            }
                        },
                        {
                            "DeleteRequest": {
                                "Key": {"pk": {"S": "dup-delete"}}
                            }
                        }
                    ]
                }
            }),
        ] {
            let err = handle_batch_write_item(
                db.clone(),
                payload.try_into().expect("batch write request"),
            )
            .await
            .expect_err("duplicate batch write keys should fail");

            assert_eq!(err.status_code, 400, "{}", backend.name);
            assert_eq!(
                err.error_type, "com.amazon.coral.validate#ValidationException",
                "{}",
                backend.name
            );
            assert_eq!(
                err.message, "Provided list of item keys contains duplicates",
                "{}",
                backend.name
            );
        }
    }
}

#[tokio::test]
async fn batch_write_key_validation_priority_matches_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_batch_table(db.as_ref()).await;
        create_binary_batch_table(db.as_ref()).await;

        for (case_name, payload, expected_message) in [
            (
                "missing table before empty string key",
                json!({
                    "RequestItems": {
                        "MissingBatchWriteRouteTable": [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": "missing"}
                                    }
                                }
                            }
                        ],
                        "BatchWriteRouteTable": [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": ""}
                                    }
                                }
                            }
                        ]
                    }
                }),
                "Requested resource not found",
            ),
            (
                "empty string hash key",
                json!({
                    "RequestItems": {
                        "BatchWriteRouteTable": [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": ""}
                                    }
                                }
                            }
                        ]
                    }
                }),
                "One or more parameter values are not valid. The AttributeValue for a key \
                 attribute cannot contain an empty string value. Key: pk",
            ),
            (
                "empty binary hash key",
                json!({
                    "RequestItems": {
                        "BatchWriteBinaryRouteTable": [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"B": ""}
                                    }
                                }
                            }
                        ]
                    }
                }),
                "One or more parameter values are not valid. The AttributeValue for a key \
                 attribute cannot contain an empty binary value. Key: pk",
            ),
            (
                "duplicate before later oversized hash key",
                json!({
                    "RequestItems": {
                        "BatchWriteRouteTable": [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": "dup-later-big"},
                                        "payload": {"S": "first"}
                                    }
                                }
                            },
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": "dup-later-big"},
                                        "payload": {"S": "second"}
                                    }
                                }
                            },
                            {
                                "PutRequest": {
                                    "Item": {
                                        "pk": {"S": "p".repeat(storage_types::MAX_PARTITION_KEY_BYTES + 1)}
                                    }
                                }
                            }
                        ]
                    }
                }),
                "Provided list of item keys contains duplicates",
            ),
        ] {
            let err = handle_batch_write_item(
                db.clone(),
                payload.try_into().expect("batch write request"),
            )
            .await
            .expect_err("invalid batch write should fail");

            assert_eq!(err.status_code, 400, "{}: {case_name}", backend.name);
            if expected_message == "Requested resource not found" {
                assert_eq!(
                    err.error_type, "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
                    "{}: {case_name}",
                    backend.name
                );
            } else {
                assert_eq!(
                    err.error_type, "com.amazon.coral.validate#ValidationException",
                    "{}: {case_name}",
                    backend.name
                );
            }
            assert_eq!(
                err.message, expected_message,
                "{}: {case_name}",
                backend.name
            );
        }
    }
}

async fn create_batch_table(db: &storage::DatabaseManager) {
    db.create_table(&CreateTableRequest::new(
        TableName::new("BatchWriteRouteTable"),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create batch write route table");
}

async fn create_binary_batch_table(db: &storage::DatabaseManager) {
    db.create_table(&CreateTableRequest::new(
        TableName::new("BatchWriteBinaryRouteTable"),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::B,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create binary batch write route table");
}

async fn batch_get_payloads(
    db: std::sync::Arc<storage::DatabaseManager>,
    backend_name: &str,
) -> Vec<String> {
    let response = handle_batch_get_item(
        db,
        json!({
            "RequestItems": {
                "BatchWriteRouteTable": {
                    "Keys": [
                        {"pk": {"S": "item-1"}},
                        {"pk": {"S": "item-2"}}
                    ]
                }
            }
        })
        .try_into()
        .expect("batch get request"),
    )
    .await
    .unwrap_or_else(|err| panic!("{backend_name} batch get after batch write: {err:?}"));
    let Response::BatchGetWire(response) = response else {
        panic!("{backend_name} expected batch get wire response");
    };
    let response = response
        .into_batch_get_response()
        .unwrap_or_else(|err| panic!("{backend_name} decode batch get response: {err}"));
    let mut payloads = response
        .responses
        .and_then(|mut responses| responses.remove(&TableName::new("BatchWriteRouteTable")))
        .unwrap_or_default()
        .into_iter()
        .map(|item| {
            let Some(AttributeValue::S(payload)) = item.get("payload") else {
                panic!("{backend_name} batch get item missing payload: {item:?}");
            };
            payload.clone()
        })
        .collect::<Vec<_>>();
    payloads.sort();
    payloads
}

fn assert_empty_batch_write_response(response: &Response, backend_name: &str) {
    let Response::BatchWriteItem(response) = response else {
        panic!("{backend_name} expected batch write response");
    };
    assert!(
        response
            .unprocessed_items
            .as_ref()
            .is_none_or(std::collections::HashMap::is_empty),
        "{}",
        backend_name
    );
}
