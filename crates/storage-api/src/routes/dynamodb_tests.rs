use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use http_error::HttpApiError;
use serde_json::json;
use storage_sync::{
    SYNC_LEADER_HINT_HEADER, SYNC_NOT_LEADER_ERROR_TYPE, SyncProposalResponse,
    SyncWriteProposalRequest,
};
use storage_types::{DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, StorageEnum, StorageError};

use crate::{
    manager::{StorageApiManagerOptions, SyncWriteProposer},
    routes::{
        dynamodb::{dynamodb_endpoint, is_conditional_check_failed_api_error},
        routes_test_support::{create_test_db, default_conformance_backends},
    },
    types::AppState,
};

struct NotLeaderProposer {
    leader_hint: String,
}

#[async_trait]
impl SyncWriteProposer for NotLeaderProposer {
    async fn propose_sync_write(
        &self,
        _request: SyncWriteProposalRequest,
    ) -> Result<SyncProposalResponse, HttpApiError> {
        let message = format!(
            "storage sync node is not the current leader; retry against {}",
            self.leader_hint
        );
        Err(
            HttpApiError::dynamodb_error(SYNC_NOT_LEADER_ERROR_TYPE, message, 500)
                .with_response_header(SYNC_LEADER_HINT_HEADER, self.leader_hint.clone()),
        )
    }
}

#[test]
fn conditional_check_failed_api_errors_are_expected_operation_errors() {
    let storage_error: StorageError = StorageEnum::ConditionalCheckFailed.into();
    let conditional_error: HttpApiError = storage_error.into();
    let validation_error = HttpApiError::validation_error("bad request");

    assert_eq!(
        conditional_error.message,
        DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE
    );
    assert!(is_conditional_check_failed_api_error(&conditional_error));
    assert!(!is_conditional_check_failed_api_error(&validation_error));
}

#[tokio::test]
async fn get_item_rejects_unknown_fields() {
    assert_rejects_unknown_fields(
        "DynamoDB_20120810.GetItem",
        json!({
            "TableName": "TestTable",
            "Key": {"id": {"S": "test123"}},
            "InvalidField": "invalid",
        }),
    )
    .await;
}

#[tokio::test]
async fn put_item_rejects_unknown_fields() {
    assert_rejects_unknown_fields(
        "DynamoDB_20120810.PutItem",
        json!({
            "TableName": "TestTable",
            "Item": {"id": {"S": "test123"}},
            "InvalidField": "invalid",
        }),
    )
    .await;
}

async fn assert_rejects_unknown_fields(target: &'static str, payload: serde_json::Value) {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut headers = HeaderMap::new();
    headers.insert("x-amz-target", HeaderValue::from_static(target));

    let body = Bytes::from(serde_json::to_vec(&payload).expect("Payload should serialize"));

    let result = dynamodb_endpoint(State(app_state), headers, body).await;
    let (status, _headers, error) = result.expect_err("Request should be rejected").into_parts();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert!(
        error.0.message.contains("unknown field"),
        "unexpected error message: {}",
        error.0.message
    );
}

#[tokio::test]
async fn transact_write_runtime_update_validation_uses_cancellation_reasons() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        let app_state = Arc::new(AppState::new_with_manager_options(
            db,
            StorageApiManagerOptions::default(),
        ));

        execute_dynamodb_json(
            app_state.clone(),
            "DynamoDB_20120810.CreateTable",
            json!({
                "TableName": "TxnRuntime",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
            }),
        )
        .await
        .unwrap_or_else(|err| panic!("{} create table: {err:?}", backend.name));
        execute_dynamodb_json(
            app_state.clone(),
            "DynamoDB_20120810.PutItem",
            json!({
                "TableName": "TxnRuntime",
                "Item": {
                    "pk": {"S": "p"},
                    "n": {"N": "3"},
                    "s": {"S": "old"}
                }
            }),
        )
        .await
        .unwrap_or_else(|err| panic!("{} put item: {err:?}", backend.name));

        let (status, error) = execute_dynamodb_json(
            app_state,
            "DynamoDB_20120810.TransactWriteItems",
            json!({
                "TransactItems": [{
                    "Update": {
                        "TableName": "TxnRuntime",
                        "Key": {"pk": {"S": "p"}},
                        "UpdateExpression": "SET n = n + s"
                    }
                }]
            }),
        )
        .await
        .expect_err("runtime update error should cancel the transaction");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{}", backend.name);
        assert_eq!(
            error.0.error_type, "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "{}",
            backend.name
        );
        assert_eq!(error.0.message, "", "{}", backend.name);
        assert_eq!(
            error.0.transaction_message.as_deref(),
            Some(
                "Transaction cancelled, please refer cancellation reasons for specific reasons \
                 [ValidationError]"
            ),
            "{}",
            backend.name
        );
        let reasons = error
            .0
            .cancellation_reasons
            .as_ref()
            .expect("cancellation reasons");
        assert_eq!(reasons.len(), 1, "{}", backend.name);
        assert_eq!(reasons[0].code, "ValidationError", "{}", backend.name);
        assert_eq!(
            reasons[0].message.as_deref(),
            Some("An operand in the update expression has an incorrect data type"),
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn transact_write_expression_validation_takes_priority_over_missing_tables_and_duplicates() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        let app_state = Arc::new(AppState::new_with_manager_options(
            db,
            StorageApiManagerOptions::default(),
        ));

        execute_dynamodb_json(
            app_state.clone(),
            "DynamoDB_20120810.CreateTable",
            json!({
                "TableName": "TxnExpressionPriority",
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ]
            }),
        )
        .await
        .unwrap_or_else(|err| panic!("{} create table: {err:?}", backend.name));
        execute_dynamodb_json(
            app_state.clone(),
            "DynamoDB_20120810.PutItem",
            json!({
                "TableName": "TxnExpressionPriority",
                "Item": {
                    "pk": {"S": "p"},
                    "sk": {"S": "s1"},
                    "note": {"S": "old"},
                    "n": {"N": "1"}
                }
            }),
        )
        .await
        .unwrap_or_else(|err| panic!("{} put item: {err:?}", backend.name));

        let arn = "arn:aws:dynamodb:eu-central-1:123456789012:table/TxnExpressionPriority";
        for (case_name, payload, expected_message) in [
            (
                "invalid condition name before missing table",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "#c = :v",
                                "ExpressionAttributeNames": {"c": "note"},
                                "ExpressionAttributeValues": {":v": {"S": "old"}}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "ExpressionAttributeNames contains invalid key: Syntax error; key: \"c\"",
            ),
            (
                "missing table before invalid condition name",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "#c = :v",
                                "ExpressionAttributeNames": {"c": "note"},
                                "ExpressionAttributeValues": {":v": {"S": "old"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeNames contains invalid key: Syntax error; key: \"c\"",
            ),
            (
                "invalid update grammar before missing table",
                json!({
                    "TransactItems": [
                        {
                            "Update": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "UpdateExpression": "SET note = :v +",
                                "ExpressionAttributeValues": {":v": {"N": "1"}}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"+\"",
            ),
            (
                "missing table before invalid update grammar",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Update": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "UpdateExpression": "SET note = :v +",
                                "ExpressionAttributeValues": {":v": {"N": "1"}}
                            }
                        }
                    ]
                }),
                "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"+\"",
            ),
            (
                "duplicate arn item before invalid update grammar",
                json!({
                    "TransactItems": [
                        {
                            "Update": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "UpdateExpression": "SET n = n + :inc",
                                "ExpressionAttributeValues": {":inc": {"N": "1"}}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": arn,
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Update": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "missing"}},
                                "UpdateExpression": "SET note = :v +",
                                "ExpressionAttributeValues": {":v": {"N": "1"}}
                            }
                        }
                    ]
                }),
                "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"+\"",
            ),
            (
                "arn invalid condition name",
                json!({
                    "TransactItems": [{
                        "ConditionCheck": {
                            "TableName": arn,
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                            "ConditionExpression": "#c = :v",
                            "ExpressionAttributeNames": {"c": "note"},
                            "ExpressionAttributeValues": {":v": {"S": "old"}}
                        }
                    }]
                }),
                "ExpressionAttributeNames contains invalid key: Syntax error; key: \"c\"",
            ),
            (
                "arn missing expression name",
                json!({
                    "TransactItems": [{
                        "ConditionCheck": {
                            "TableName": arn,
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                            "ConditionExpression": "#missing = :v",
                            "ExpressionAttributeValues": {":v": {"S": "old"}}
                        }
                    }]
                }),
                "Invalid ConditionExpression: An expression attribute name used in the document \
                 path is not defined; attribute name: #missing",
            ),
            (
                "put unused expression name before missing table",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s2"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "attribute_not_exists(pk)",
                                "ExpressionAttributeNames": {"#unused": "note"}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
            ),
            (
                "missing table before put unused expression name",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s2"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "attribute_not_exists(pk)",
                                "ExpressionAttributeNames": {"#unused": "note"}
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
            ),
            (
                "delete unused expression value before key mismatch",
                json!({
                    "TransactItems": [
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeValues": {":unused": {"S": "x"}}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"N": "1"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeValues unused in expressions: keys: \
                 {:unused}",
            ),
            (
                "key mismatch before delete unused expression value",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"N": "1"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeValues": {":unused": {"S": "x"}}
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeValues unused in expressions: keys: \
                 {:unused}",
            ),
            (
                "condition invalid value key before missing table",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "note = :v",
                                "ExpressionAttributeValues": {"v": {"S": "old"}}
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "missing table before condition invalid value key",
                json!({
                    "TransactItems": [
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "note = :v",
                                "ExpressionAttributeValues": {"v": {"S": "old"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "arn delete invalid expression value key",
                json!({
                    "TransactItems": [{
                        "Delete": {
                            "TableName": arn,
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                            "ConditionExpression": "note = :v",
                            "ExpressionAttributeValues": {"v": {"S": "old"}}
                        }
                    }]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "arn put unused expression value",
                json!({
                    "TransactItems": [{
                        "Put": {
                            "TableName": arn,
                            "Item": {
                                "pk": {"S": "p"},
                                "sk": {"S": "s3"},
                                "note": {"S": "new"}
                            },
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ExpressionAttributeValues": {":unused": {"S": "x"}}
                        }
                    }]
                }),
                "Value provided in ExpressionAttributeValues unused in expressions: keys: \
                 {:unused}",
            ),
            (
                "delete missing expression name before put key mismatch",
                json!({
                    "TransactItems": [
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "#missing = :v",
                                "ExpressionAttributeValues": {":v": {"S": "old"}}
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"N": "1"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "x"}
                                }
                            }
                        }
                    ]
                }),
                "Invalid ConditionExpression: An expression attribute name used in the document \
                 path is not defined; attribute name: #missing",
            ),
            (
                "put invalid expression name before delete key mismatch",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s4"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "#c = :v",
                                "ExpressionAttributeNames": {"c": "note"},
                                "ExpressionAttributeValues": {":v": {"S": "old"}}
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"N": "1"}, "sk": {"S": "s1"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeNames contains invalid key: Syntax error; key: \"c\"",
            ),
            (
                "invalid return consumed capacity before duplicate item",
                json!({
                    "ReturnConsumedCapacity": "BOGUS",
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "invalid return item collection metrics before duplicate item",
                json!({
                    "ReturnItemCollectionMetrics": "BOGUS",
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' \
                 failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]",
            ),
            (
                "invalid return values on condition failure before missing table",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s2"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "attribute_not_exists(pk)",
                                "ReturnValuesOnConditionCheckFailure": "BOGUS"
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "1 validation error detected: Value 'BOGUS' at \
                 'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to \
                 satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
            ),
            (
                "empty expression names before duplicate item",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeNames": {}
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "ExpressionAttributeNames must not be empty",
            ),
            (
                "empty expression values before missing table",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeValues": {}
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriorityMissing",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues must not be empty",
            ),
            (
                "empty transaction item object",
                json!({
                    "TransactItems": [{}]
                }),
                "Invalid Request: TransactWriteRequest should contain Delete or Put or Update \
                 request",
            ),
            (
                "two operations in one transaction item object",
                json!({
                    "TransactItems": [{
                        "Put": {
                            "TableName": "TxnExpressionPriority",
                            "Item": {
                                "pk": {"S": "p"},
                                "sk": {"S": "s2"}
                            }
                        },
                        "Delete": {
                            "TableName": "TxnExpressionPriority",
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}}
                        }
                    }]
                }),
                "TransactItems can only contain one of Check, Put, Update or Delete",
            ),
            (
                "duplicate item before unused expression name",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s2"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "attribute_not_exists(pk)",
                                "ExpressionAttributeNames": {"#unused": "note"}
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
            ),
            (
                "unused expression name before duplicate item",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s2"},
                                    "note": {"S": "new"}
                                },
                                "ConditionExpression": "attribute_not_exists(pk)",
                                "ExpressionAttributeNames": {"#unused": "note"}
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
            ),
            (
                "duplicate item before invalid expression value key",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "note = :v",
                                "ExpressionAttributeValues": {"v": {"S": "old"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "invalid expression value key before duplicate item",
                json!({
                    "TransactItems": [
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "note = :v",
                                "ExpressionAttributeValues": {"v": {"S": "old"}}
                            }
                        },
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "duplicate item before missing expression name",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "#missing = :v",
                                "ExpressionAttributeValues": {":v": {"S": "old"}}
                            }
                        }
                    ]
                }),
                "Invalid ConditionExpression: An expression attribute name used in the document \
                 path is not defined; attribute name: #missing",
            ),
            (
                "duplicate item before unused expression value",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeValues": {":unused": {"S": "x"}}
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeValues unused in expressions: keys: \
                 {:unused}",
            ),
            (
                "arn duplicate item before invalid expression value key",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": arn,
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "note = :v",
                                "ExpressionAttributeValues": {"v": {"S": "old"}}
                            }
                        }
                    ]
                }),
                "ExpressionAttributeValues contains invalid key: Syntax error; key: \"v\"",
            ),
            (
                "arn duplicate item before unused expression value",
                json!({
                    "TransactItems": [
                        {
                            "Put": {
                                "TableName": "TxnExpressionPriority",
                                "Item": {
                                    "pk": {"S": "p"},
                                    "sk": {"S": "s1"},
                                    "note": {"S": "dup"}
                                }
                            }
                        },
                        {
                            "ConditionCheck": {
                                "TableName": arn,
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)"
                            }
                        },
                        {
                            "Delete": {
                                "TableName": "TxnExpressionPriority",
                                "Key": {"pk": {"S": "p"}, "sk": {"S": "s1"}},
                                "ConditionExpression": "attribute_exists(pk)",
                                "ExpressionAttributeValues": {":unused": {"S": "x"}}
                            }
                        }
                    ]
                }),
                "Value provided in ExpressionAttributeValues unused in expressions: keys: \
                 {:unused}",
            ),
        ] {
            let (status, error) = execute_dynamodb_json(
                app_state.clone(),
                "DynamoDB_20120810.TransactWriteItems",
                payload,
            )
            .await
            .expect_err("expression validation should reject request");

            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{}: {case_name}",
                backend.name
            );
            assert_eq!(
                error.0.error_type, "com.amazon.coral.validate#ValidationException",
                "{}: {case_name}",
                backend.name
            );
            assert_eq!(
                error.0.message, expected_message,
                "{}: {case_name}",
                backend.name
            );
            assert!(
                error.0.cancellation_reasons.is_none(),
                "{}: {case_name}",
                backend.name
            );
        }
    }
}

#[tokio::test]
async fn request_parameter_validation_takes_priority_before_backend_lookup() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        let app_state = Arc::new(AppState::new_with_manager_options(
            db,
            StorageApiManagerOptions::default(),
        ));

        for (case_name, target, payload, expected_message) in [
            (
                "put empty expression values before table lookup",
                "DynamoDB_20120810.PutItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Item": {"pk": {"S": "p"}},
                    "ConditionExpression": "attribute_not_exists(pk)",
                    "ExpressionAttributeValues": {}
                }),
                "1 validation error detected: ExpressionAttributeValues must not be empty",
            ),
            (
                "put invalid expression value key before table lookup",
                "DynamoDB_20120810.PutItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Item": {"pk": {"S": "p"}},
                    "ConditionExpression": "note = :v",
                    "ExpressionAttributeValues": {"v": {"S": "old"}}
                }),
                "1 validation error detected: ExpressionAttributeValues contains invalid key: \
                 Syntax error; key: \"v\"",
            ),
            (
                "put invalid return values on condition check failure",
                "DynamoDB_20120810.PutItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Item": {"pk": {"S": "p"}},
                    "ReturnValuesOnConditionCheckFailure": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at \
                 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must \
                 satisfy enum value set: [ALL_OLD, NONE]",
            ),
            (
                "put invalid return item collection metrics",
                "DynamoDB_20120810.PutItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Item": {"pk": {"S": "p"}},
                    "ReturnItemCollectionMetrics": "BOGUS"
                }),
                "1 validation error detected: ReturnItemCollectionMetrics can only be SIZE or NONE",
            ),
            (
                "get empty expression names before table lookup",
                "DynamoDB_20120810.GetItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Key": {"pk": {"S": "p"}},
                    "ProjectionExpression": "note",
                    "ExpressionAttributeNames": {}
                }),
                "ExpressionAttributeNames must not be empty",
            ),
            (
                "transact get invalid return consumed capacity",
                "DynamoDB_20120810.TransactGetItems",
                json!({
                    "ReturnConsumedCapacity": "BOGUS",
                    "TransactItems": [{
                        "Get": {
                            "TableName": "MissingRequestValidation",
                            "Key": {"pk": {"S": "p"}}
                        }
                    }]
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "update invalid return consumed capacity",
                "DynamoDB_20120810.UpdateItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Key": {"pk": {"S": "p"}},
                    "UpdateExpression": "SET note = :v",
                    "ExpressionAttributeValues": {":v": {"S": "x"}},
                    "ReturnConsumedCapacity": "BOGUS"
                }),
                "Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy constraint: Member \
                 must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "update invalid return values on condition check failure",
                "DynamoDB_20120810.UpdateItem",
                json!({
                    "TableName": "MissingRequestValidation",
                    "Key": {"pk": {"S": "p"}},
                    "UpdateExpression": "SET note = :v",
                    "ExpressionAttributeValues": {":v": {"S": "x"}},
                    "ReturnValuesOnConditionCheckFailure": "BOGUS"
                }),
                "Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy \
                 constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
            ),
            (
                "query invalid return consumed capacity",
                "DynamoDB_20120810.Query",
                json!({
                    "TableName": "MissingRequestValidation",
                    "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": {":p": {"S": "p"}},
                    "ReturnConsumedCapacity": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "scan invalid return consumed capacity",
                "DynamoDB_20120810.Scan",
                json!({
                    "TableName": "MissingRequestValidation",
                    "ReturnConsumedCapacity": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "batch get invalid return consumed capacity",
                "DynamoDB_20120810.BatchGetItem",
                json!({
                    "RequestItems": {
                        "MissingRequestValidation": {
                            "Keys": [{"pk": {"S": "p"}}]
                        }
                    },
                    "ReturnConsumedCapacity": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "batch get empty expression names before table lookup",
                "DynamoDB_20120810.BatchGetItem",
                json!({
                    "RequestItems": {
                        "MissingRequestValidation": {
                            "Keys": [{"pk": {"S": "p"}}],
                            "ProjectionExpression": "note",
                            "ExpressionAttributeNames": {}
                        }
                    }
                }),
                "ExpressionAttributeNames must not be empty",
            ),
            (
                "batch write invalid return consumed capacity",
                "DynamoDB_20120810.BatchWriteItem",
                json!({
                    "RequestItems": {
                        "MissingRequestValidation": [{
                            "PutRequest": {
                                "Item": {"pk": {"S": "p"}}
                            }
                        }]
                    },
                    "ReturnConsumedCapacity": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
            ),
            (
                "batch write invalid return item collection metrics",
                "DynamoDB_20120810.BatchWriteItem",
                json!({
                    "RequestItems": {
                        "MissingRequestValidation": [{
                            "PutRequest": {
                                "Item": {"pk": {"S": "p"}}
                            }
                        }]
                    },
                    "ReturnItemCollectionMetrics": "BOGUS"
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnItemCollectionMetrics' \
                 failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]",
            ),
        ] {
            let (status, error) = execute_dynamodb_json(app_state.clone(), target, payload)
                .await
                .expect_err("request validation should reject request before backend lookup");

            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{}: {case_name}",
                backend.name
            );
            assert_eq!(
                error.0.error_type, "com.amazon.coral.validate#ValidationException",
                "{}: {case_name}",
                backend.name
            );
            assert_eq!(
                error.0.message, expected_message,
                "{}: {case_name}",
                backend.name
            );
        }
    }
}

#[allow(clippy::result_large_err)]
async fn execute_dynamodb_json(
    app_state: Arc<AppState>,
    target: &'static str,
    payload: serde_json::Value,
) -> Result<axum::response::Response, (StatusCode, axum::response::Json<http_error::ErrorResponse>)>
{
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-target", HeaderValue::from_static(target));
    let body = Bytes::from(serde_json::to_vec(&payload).expect("Payload should serialize"));
    dynamodb_endpoint(State(app_state), headers, body)
        .await
        .map_err(|error| {
            let (status, _headers, body) = error.into_parts();
            (status, body)
        })
}

async fn execute_dynamodb_json_body(
    app_state: Arc<AppState>,
    target: &'static str,
    payload: serde_json::Value,
) -> serde_json::Value {
    let response = execute_dynamodb_json(app_state, target, payload)
        .await
        .expect("request should succeed");
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&body).expect("response json")
}

#[tokio::test]
async fn dynamodb_streams_operations_read_table_stream_records() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDB_20120810.CreateTable",
        json!({
            "TableName": "StreamsCompat",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }),
    )
    .await;

    let table = execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDB_20120810.DescribeTable",
        json!({"TableName": "StreamsCompat"}),
    )
    .await;
    let stream_arn = table["Table"]["LatestStreamArn"]
        .as_str()
        .expect("latest stream arn")
        .to_string();

    let streams = execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDBStreams_20120810.ListStreams",
        json!({"TableName": "StreamsCompat"}),
    )
    .await;
    assert_eq!(streams["Streams"][0]["StreamArn"], stream_arn);

    let description = execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDBStreams_20120810.DescribeStream",
        json!({"StreamArn": stream_arn}),
    )
    .await;
    let shard_id = description["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .expect("shard id")
        .to_string();
    assert_eq!(
        description["StreamDescription"]["TableName"],
        "StreamsCompat"
    );

    execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDB_20120810.PutItem",
        json!({
            "TableName": "StreamsCompat",
            "Item": {
                "pk": {"S": "p1"},
                "value": {"S": "created"}
            }
        }),
    )
    .await;

    let iterator = execute_dynamodb_json_body(
        app_state.clone(),
        "DynamoDBStreams_20120810.GetShardIterator",
        json!({
            "StreamArn": stream_arn,
            "ShardId": shard_id,
            "ShardIteratorType": "TRIM_HORIZON"
        }),
    )
    .await;
    let shard_iterator = iterator["ShardIterator"]
        .as_str()
        .expect("shard iterator")
        .to_string();

    let records = execute_dynamodb_json_body(
        app_state,
        "DynamoDBStreams_20120810.GetRecords",
        json!({"ShardIterator": shard_iterator, "Limit": 10}),
    )
    .await;
    assert_eq!(records["Records"][0]["eventName"], "INSERT");
    assert_eq!(records["Records"][0]["eventSource"], "aws:dynamodb");
    assert_eq!(records["Records"][0]["dynamodb"]["Keys"]["pk"]["S"], "p1");
    assert_eq!(
        records["Records"][0]["dynamodb"]["StreamViewType"],
        "NEW_AND_OLD_IMAGES"
    );
    assert!(records["NextShardIterator"].is_string());
}

#[tokio::test]
async fn dynamodb_streams_get_records_rejects_invalid_iterator() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let (status, error) = execute_dynamodb_json(
        app_state,
        "DynamoDBStreams_20120810.GetRecords",
        json!({"ShardIterator": "not-an-iterator"}),
    )
    .await
    .expect_err("invalid iterator should fail");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazonaws.dynamodb.v20120810#TrimmedDataAccessException"
    );
}

#[tokio::test]
async fn sync_not_leader_error_includes_leader_hint_header() {
    let db = create_test_db().await;
    db.create_table(
        &json!({
            "TableName": "LeaderHintTable",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create table");
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions {
            sync_write_proposer: Some(Arc::new(NotLeaderProposer {
                leader_hint: "http://127.0.0.1:19002".to_string(),
            })),
            ..StorageApiManagerOptions::default()
        },
    ));

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.PutItem"),
    );
    let body = Bytes::from(
        json!({
            "TableName": "LeaderHintTable",
            "Item": {"pk": {"S": "p"}}
        })
        .to_string(),
    );

    let error = dynamodb_endpoint(State(app_state), headers, body)
        .await
        .expect_err("not-leader proposer should reject");
    let (status, response_headers, body) = error.into_parts();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response_headers
            .get(SYNC_LEADER_HINT_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("http://127.0.0.1:19002")
    );
    assert_eq!(body.0.error_type, SYNC_NOT_LEADER_ERROR_TYPE);
}

#[tokio::test]
async fn update_continuous_backups_returns_explicit_unsupported_error() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.UpdateContinuousBackups"),
    );

    let payload = json!({
        "TableName": "sys",
        "PointInTimeRecoverySpecification": {
            "PointInTimeRecoveryEnabled": true
        }
    });

    let body = Bytes::from(serde_json::to_vec(&payload).expect("Payload should serialize"));

    let (status, _headers, error) = dynamodb_endpoint(State(app_state), headers, body)
        .await
        .expect_err("Request should be rejected")
        .into_parts();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error.0.error_type, "ValidationException");
    assert_eq!(
        error.0.message,
        "UpdateContinuousBackups is not yet supported on the AuxFn storage compatibility surface"
    );
}

#[tokio::test]
async fn update_continuous_backups_rejects_unknown_fields() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.UpdateContinuousBackups"),
    );

    let payload = json!({
        "TableName": "sys",
        "PointInTimeRecoverySpecification": {
            "PointInTimeRecoveryEnabled": true
        },
        "Unexpected": true
    });

    let body = Bytes::from(serde_json::to_vec(&payload).expect("Payload should serialize"));

    let (status, _headers, error) = dynamodb_endpoint(State(app_state), headers, body)
        .await
        .expect_err("Request should be rejected")
        .into_parts();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert!(
        error.0.message.contains("unknown field"),
        "unexpected error message: {}",
        error.0.message
    );
}
