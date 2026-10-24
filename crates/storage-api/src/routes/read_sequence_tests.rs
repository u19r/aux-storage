use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde_json::json;
#[cfg(feature = "remote")]
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings};

use crate::{
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_tests::{for_each_conformance_backend, handle_create_table, handle_put_item},
    },
    types::AppState,
};

#[tokio::test]
async fn given_graph_roots_when_read_sequence_is_called_then_flat_nodes_are_returned() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "items").await;
        put_item(&db, "items", "a", "A").await;
        put_item(&db, "items", "b", "B").await;

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "ReadConsistency": "EVENTUAL",
                "Nodes": [
                    {"Name": "a", "Operation": {"Get": {"TableName": "items", "Key": {"id": {"S": "a"}}}}, "Inputs": {}, "After": []},
                    {"Name": "b", "Operation": {"Get": {"TableName": "items", "Key": {"id": {"S": "b"}}}}, "Inputs": {}, "After": []}
                ],
                "Outputs": ["a", "b"]
            }),
        )
        .await;
        let payload = response_body(response).await;
        assert_eq!(payload["Nodes"].as_array().expect("nodes").len(), 2);
        assert_eq!(payload["Nodes"][0]["Name"], "a");
        assert_eq!(
            payload["Nodes"][0]["Invocations"][0]["Result"]["Get"]["Item"]["value"]["S"],
            "A"
        );
    })
    .await;
}

#[tokio::test]
async fn given_invalid_numeric_key_when_read_sequence_is_called_then_it_fails_validation() {
    for_each_conformance_backend(|_, db| async move {
        handle_create_table(
            db.clone(),
            json!({
                "TableName": "read-sequence-invalid-number",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "N"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
            })
            .try_into()
            .expect("create numeric read-sequence table request"),
        )
        .await
        .expect("create numeric read-sequence table");

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [{
                    "Name": "invalid",
                    "Operation": {
                        "Get": {
                            "TableName": "read-sequence-invalid-number",
                            "Key": {"pk": {"N": "1E-131"}}
                        }
                    }
                }],
                "Outputs": ["invalid"]
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_body(response).await;
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|message| message.contains("supported range")),
            "unexpected invalid numeric read-sequence response: {payload}"
        );
    })
    .await;
}

#[tokio::test]
async fn given_batch_get_keys_when_whole_plan_executes_then_membership_and_request_order_are_exact()
{
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "batch-order").await;
        put_item(&db, "batch-order", "a", "A").await;
        put_item(&db, "batch-order", "b", "B").await;

        let payload = response_body(
            execute_read_sequence(
                app_state(db),
                json!({
                    "Nodes": [{
                        "Name": "records",
                        "Operation": {"BatchGet": {"RequestItems": {
                            "batch-order": {"Keys": [
                                {"id": {"S": "b"}},
                                {"id": {"S": "missing"}},
                                {"id": {"S": "a"}}
                            ]}
                        }}}
                    }],
                    "Outputs": ["records"]
                }),
            )
            .await,
        )
        .await;

        let items = payload["Nodes"][0]["Invocations"][0]["Result"]["BatchGet"]["Responses"]
            ["batch-order"]
            .as_array()
            .expect("batch response items");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"]["S"], "b");
        assert_eq!(items[1]["id"]["S"], "a");
    })
    .await;
}

#[tokio::test]
async fn given_missing_child_item_when_dependent_get_runs_then_empty_child_result_is_published() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "missing-parent").await;
        seed_table(&db, "missing-child").await;
        put_item_with_child(&db, "missing-parent", "parent", "P", "does-not-exist").await;

        let payload = response_body(
            execute_read_sequence(
                app_state(db),
                json!({
                    "Nodes": [
                        {"Name": "parent", "Operation": {"Get": {
                            "TableName": "missing-parent",
                            "Key": {"id": {"S": "parent"}}
                        }}},
                        {"Name": "child", "Operation": {"Get": {
                            "TableName": "missing-child",
                            "Key": {"id": {"FromInput": "child_id"}}
                        }}, "Inputs": {
                            "child_id": {
                                "From": {"Node": "parent", "Select": "$.Get.Item.child_id"},
                                "Cardinality": "ONE",
                                "OnMissing": "ERROR"
                            }
                        }}
                    ],
                    "Outputs": ["parent", "child"]
                }),
            )
            .await,
        )
        .await;

        assert_eq!(payload["Nodes"].as_array().expect("nodes").len(), 2);
        assert_eq!(payload["Nodes"][0]["Name"], "parent");
        assert_eq!(payload["Nodes"][1]["Name"], "child");
        assert_eq!(
            payload["Nodes"][1]["Invocations"].as_array().unwrap().len(),
            1
        );
        assert!(
            payload["Nodes"][1]["Invocations"][0]["Result"]["Get"]
                .get("Item")
                .is_none()
        );
    })
    .await;
}

#[tokio::test]
async fn given_required_child_input_is_missing_when_sequence_runs_then_no_partial_response_is_published()
 {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "required-parent").await;
        seed_table(&db, "required-child").await;
        put_item_without_child(&db, "required-parent", "parent", "P").await;

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [
                    {"Name": "parent", "Operation": {"Get": {
                        "TableName": "required-parent",
                        "Key": {"id": {"S": "parent"}}
                    }}},
                    {"Name": "child", "Operation": {"Get": {
                        "TableName": "required-child",
                        "Key": {"id": {"FromInput": "child_id"}}
                    }}, "Inputs": {
                        "child_id": {
                            "From": {"Node": "parent", "Select": "$.Get.Item.child_id"},
                            "Cardinality": "ONE",
                            "OnMissing": "ERROR"
                        }
                    }}
                ],
                "Outputs": ["parent", "child"]
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_body(response).await;
        assert!(
            payload["Nodes"].is_null(),
            "unexpected partial response: {payload}"
        );
        assert_eq!(
            payload["message"],
            "ReadSequence node 'child' input 'child_id' expected one value(s), got zero"
        );
    })
    .await;
}

#[tokio::test]
async fn given_duplicate_child_keys_when_dependent_get_runs_then_one_child_invocation_is_published()
{
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "duplicate-parents").await;
        seed_table(&db, "duplicate-children").await;
        put_composite_item_with_child(&db, "duplicate-parents", "acct", "a", "first", "same").await;
        put_composite_item_with_child(&db, "duplicate-parents", "acct", "b", "second", "same")
            .await;
        put_item(&db, "duplicate-children", "same", "child").await;

        let payload = response_body(
            execute_read_sequence(
                app_state(db),
                json!({
                    "Nodes": [
                        {"Name": "parents", "Operation": {"Query": {
                            "TableName": "duplicate-parents",
                            "KeyConditionExpression": "pk = :pk",
                            "ExpressionAttributeValues": {":pk": {"S": "acct"}}
                        }}},
                        {"Name": "children", "Operation": {"Get": {
                            "TableName": "duplicate-children",
                            "Key": {"id": {"FromInput": "child_id"}}
                        }}, "Inputs": {
                            "child_id": {
                                "From": {"Node": "parents", "Select": "$.Query.Items[*].child_id"},
                                "MappedKeySource": {"AttributeName": "child_id", "Indexer": 0},
                                "Cardinality": "MANY",
                                "OnMissing": "ERROR"
                            }
                        }, "Iterate": "child_id"}
                    ],
                    "Outputs": ["children"]
                }),
            )
            .await,
        )
        .await;

        let invocations = payload["Nodes"][0]["Invocations"]
            .as_array()
            .expect("child invocations");
        assert_eq!(
            invocations.len(),
            1,
            "duplicate child work was amplified: {payload}"
        );
        assert_eq!(invocations[0]["Ordinal"], 0);
        assert_eq!(
            invocations[0]["Result"]["Get"]["Item"]["value"]["S"],
            "child"
        );
    })
    .await;
}

#[tokio::test]
async fn given_child_fanout_exceeds_limit_when_sequence_runs_then_no_child_reads_are_published() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "fanout-parents").await;
        seed_table(&db, "fanout-children").await;
        put_composite_item_with_child(&db, "fanout-parents", "acct", "a", "A", "a").await;
        put_composite_item_with_child(&db, "fanout-parents", "acct", "b", "B", "b").await;
        put_item(&db, "fanout-children", "a", "child-a").await;
        put_item(&db, "fanout-children", "b", "child-b").await;

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "MaxFanoutPerStep": 1,
                "Nodes": [
                    {"Name": "parents", "Operation": {"Query": {
                        "TableName": "fanout-parents",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"S": "acct"}}
                    }}},
                    {"Name": "children", "Operation": {"Get": {
                        "TableName": "fanout-children",
                        "Key": {"id": {"FromInput": "child_id"}}
                    }}, "Inputs": {
                        "child_id": {
                            "From": {"Node": "parents", "Select": "$.Query.Items[*].child_id"},
                            "Cardinality": "MANY",
                            "OnMissing": "ERROR"
                        }
                    }, "Iterate": "child_id"}
                ],
                "Outputs": ["parents", "children"]
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = response_body(response).await;
        assert!(
            payload["Nodes"].is_null(),
            "unexpected partial response: {payload}"
        );
        assert!(
            payload["message"].as_str().is_some_and(|message| {
                message.contains("fanout") || message.contains("Fanout")
            }),
            "unexpected fanout payload: {payload}"
        );
    })
    .await;
}

#[tokio::test]
async fn given_total_budget_when_read_sequence_is_called_then_wave_suffix_is_deferred() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "budgeted").await;
        put_item(&db, "budgeted", "a", "A").await;
        put_item(&db, "budgeted", "b", "B").await;

        let request = json!({
            "MaxTotalReadItems": 1,
            "Nodes": [
                {"Name": "a", "Operation": {"Get": {"TableName": "budgeted", "Key": {"id": {"S": "a"}}}}, "Inputs": {}, "After": []},
                {"Name": "b", "Operation": {"Get": {"TableName": "budgeted", "Key": {"id": {"S": "b"}}}}, "Inputs": {}, "After": []}
            ],
            "Outputs": ["a", "b"]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
        assert_eq!(first["Nodes"].as_array().expect("first nodes").len(), 1);
        assert_eq!(first["Partial"], true);
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("budget continuation")
            .to_string();

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
        assert_eq!(second["Nodes"][0]["Name"], "b");
        assert_eq!(second["Partial"], false);
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_explicit_response_budget_when_read_sequence_is_called_then_wave_suffix_is_deferred()
{
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "byte-budgeted").await;
        let value = "x".repeat(1024);
        put_item(&db, "byte-budgeted", "a", &value).await;
        put_item(&db, "byte-budgeted", "b", &value).await;

        let request = json!({
            "MaxResponseBytes": 2_000,
            "Nodes": [
                {"Name": "a", "Operation": {"Get": {"TableName": "byte-budgeted", "Key": {"id": {"S": "a"}}}}, "Inputs": {}, "After": []},
                {"Name": "b", "Operation": {"Get": {"TableName": "byte-budgeted", "Key": {"id": {"S": "b"}}}}, "Inputs": {}, "After": []}
            ],
            "Outputs": ["a", "b"]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
        assert_eq!(first["Nodes"].as_array().expect("first nodes").len(), 1);
        assert_eq!(first["Partial"], true);
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("byte-budget continuation")
            .to_string();

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
        assert_eq!(second["Nodes"][0]["Name"], "b");
        assert_eq!(second["Partial"], false);
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_dependent_response_budget_when_page_would_overflow_then_child_is_deferred() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "dependent-byte-budget").await;
        let value = "x".repeat(1800);
        put_item_with_child(&db, "dependent-byte-budget", "parent", &value, "child").await;
        put_item(&db, "dependent-byte-budget", "child", &value).await;

        let request = json!({
        "MaxResponseBytes": 4_000,
        "Nodes": [
            {
                "Name": "parent",
                "Operation": {"Get": {
                    "TableName": "dependent-byte-budget",
                    "Key": {"id": {"S": "parent"}}
                }},
                "Inputs": {},
                "After": []
            },
            {
                "Name": "child",
                "Operation": {"Get": {
                    "TableName": "dependent-byte-budget",
                    "Key": {"id": {"FromInput": "child_id"}}
                }},
                "Inputs": {"child_id": {
                    "From": {"Node": "parent", "Select": "$.Get.Item.child_id"},
                    "Cardinality": "ONE",
                    "OnMissing": "ERROR"
                }},
                "After": []
            }
        ],
        "Outputs": ["parent", "child"]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        assert_eq!(first["Nodes"].as_array().expect("first nodes").len(), 1);
        assert_eq!(first["Nodes"][0]["Name"], "parent");
        assert_eq!(first["Partial"], true);
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("dependent byte continuation")
            .to_string();

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
        assert_eq!(second["Nodes"][0]["Name"], "child");
        assert_eq!(second["Partial"], false);
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_input_binding_when_parent_is_declared_later_then_child_is_bound() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "parents").await;
        seed_table(&db, "children").await;
        put_item_with_child(&db, "parents", "p1", "c1", "c1").await;
        put_item(&db, "children", "c1", "child").await;

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [
                    {"Name": "child", "Operation": {"Get": {"TableName": "children", "Key": {"id": {"FromInput": "child_id"}}}}, "Inputs": {"child_id": {"From": {"Node": "parent", "Select": "$.Get.Item.child_id"}, "Cardinality": "ONE", "OnMissing": "ERROR"}}, "After": []},
                    {"Name": "parent", "Operation": {"Get": {"TableName": "parents", "Key": {"id": {"S": "p1"}}}}, "Inputs": {}, "After": []}
                ],
                "Outputs": ["parent", "child"]
            }),
        )
        .await;
        let payload = response_body(response).await;
        assert_eq!(payload["Nodes"][0]["Name"], "child");
        assert_eq!(
            payload["Nodes"][0]["Invocations"][0]["Result"]["Get"]["Item"]["value"]["S"],
            "child"
        );
        assert_eq!(
            payload["Nodes"][0]["Invocations"][0]["InputRefs"]["child_id"]["Node"],
            "parent"
        );
    })
    .await;
}

#[tokio::test]
async fn given_string_template_when_read_sequence_is_called_then_composite_key_is_bound() {
    for_each_conformance_backend(|_, db| async move {
        seed_table(&db, "contexts").await;
        seed_table(&db, "models").await;
        put_context_item(&db, "contexts", "request#1", "42", "7").await;
        put_item(&db, "models", "entity#42#sub_model#7#v1", "versioned model").await;

        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [
                    {
                        "Name": "context",
                        "Operation": {"Get": {
                            "TableName": "contexts",
                            "Key": {"id": {"S": "request#1"}}
                        }}
                    },
                    {
                        "Name": "model",
                        "Operation": {"Get": {
                            "TableName": "models",
                            "Key": {
                                "id": {
                                    "StringTemplate": "entity#{id}#sub_model#{sub_id}#v1"
                                }
                            }
                        }},
                        "Inputs": {
                            "id": {
                                "From": {"Node": "context", "Select": "$.Get.Item.entity_id.S"},
                                "Cardinality": "ONE",
                                "OnMissing": "ERROR"
                            },
                            "sub_id": {
                                "From": {"Node": "context", "Select": "$.Get.Item.sub_id.S"},
                                "Cardinality": "ONE",
                                "OnMissing": "ERROR"
                            }
                        }
                    }
                ],
                "Outputs": ["context", "model"]
            }),
        )
        .await;
        let payload = response_body(response).await;

        assert_eq!(
            payload["Nodes"][1]["Invocations"][0]["Result"]["Get"]["Item"]["value"]["S"],
            "versioned model"
        );
        assert_eq!(
            payload["Nodes"][1]["Invocations"][0]["InputRefs"]["id"]["Node"],
            "context"
        );
        assert_eq!(
            payload["Nodes"][1]["Invocations"][0]["InputRefs"]["sub_id"]["Node"],
            "context"
        );
    })
    .await;
}

#[tokio::test]
async fn given_ordered_sequence_payload_when_called_then_breaking_contract_rejects_it() {
    for_each_conformance_backend(|_, db| async move {
        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Sequence": [{"Name": "old", "Get": {"TableName": "items", "Key": {"id": {"S": "a"}}}}]
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

#[tokio::test]
async fn given_invalid_node_operation_when_called_then_validation_precedes_reads() {
    for_each_conformance_backend(|_, db| async move {
        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [{
                    "Name": "invalid",
                    "Operation": {
                        "Query": {
                            "TableName": "items",
                            "KeyConditionExpression": ""
                        }
                    },
                    "Inputs": {},
                    "After": []
                }]
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

#[tokio::test]
async fn given_cycle_when_called_then_no_read_is_started_and_validation_is_returned() {
    for_each_conformance_backend(|_, db| async move {
        let response = execute_read_sequence(
            app_state(db),
            json!({
                "Nodes": [
                    {"Name": "a", "Operation": {"Get": {"TableName": "items", "Key": {"id": {"S": "a"}}}}, "Inputs": {}, "After": ["b"]},
                    {"Name": "b", "Operation": {"Get": {"TableName": "items", "Key": {"id": {"S": "b"}}}}, "Inputs": {}, "After": ["a"]}
                ]
            }),
        )
        .await;
        let payload = response_body(response).await;
        assert!(
            payload["__type"]
                .as_str()
                .unwrap_or_default()
                .ends_with("ValidationException")
        );
    })
    .await;
}

#[tokio::test]
async fn given_query_page_when_resumed_then_cursor_advances_the_same_invocation() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "paged").await;
        put_composite_item(&db, "paged", "acct", "a", "first").await;
        put_composite_item(&db, "paged", "acct", "b", "second").await;

        let request = json!({
            "Nodes": [{
                "Name": "page",
                "Operation": {
                    "Query": {
                        "TableName": "paged",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"S": "acct"}},
                        "Limit": 1
                    }
                },
                "Inputs": {},
                "After": []
            }]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        assert_eq!(
            first["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
            1
        );
        assert_eq!(
            first["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "a"
        );
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("first page has a continuation token");

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(
            second["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
            1
        );
        assert_eq!(
            second["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "b"
        );
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_explicit_query_item_budget_when_resumed_then_optimized_frontier_advances() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "bounded_query").await;
        put_composite_item(&db, "bounded_query", "acct", "a", "first").await;
        put_composite_item(&db, "bounded_query", "acct", "b", "second").await;

        let request = json!({
            "MaxRootItems": 1,
            "Nodes": [{
                "Name": "page",
                "Operation": {"Query": {
                    "TableName": "bounded_query",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "acct"}},
                    "Limit": 2
                }},
                "Inputs": {},
                "After": []
            }]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        assert_eq!(
            first["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
            1
        );
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("bounded query continuation")
            .to_string();

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(
            second["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "b"
        );
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_query_without_limit_when_called_then_default_page_bound_is_applied() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "default_query_limit").await;
        for index in 0..101 {
            put_composite_item(
                &db,
                "default_query_limit",
                "acct",
                &format!("{index:03}"),
                "value",
            )
            .await;
        }

        let payload = response_body(
            execute_read_sequence(
                app_state(db),
                json!({
                    "Nodes": [{
                        "Name": "page",
                        "Operation": {"Query": {
                            "TableName": "default_query_limit",
                            "KeyConditionExpression": "pk = :pk",
                            "ExpressionAttributeValues": {":pk": {"S": "acct"}}
                        }},
                        "Inputs": {},
                        "After": []
                    }]
                }),
            )
            .await,
        )
        .await;

        assert_eq!(
            payload["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
            100
        );
        assert!(payload["NextSequenceToken"].as_str().is_some());
    })
    .await;
}

#[tokio::test]
async fn given_independent_query_pages_when_resumed_then_each_frontier_advances() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "parallel-pages").await;
        put_composite_item(&db, "parallel-pages", "acct-a", "a", "a-first").await;
        put_composite_item(&db, "parallel-pages", "acct-a", "b", "a-second").await;
        put_composite_item(&db, "parallel-pages", "acct-b", "a", "b-first").await;
        put_composite_item(&db, "parallel-pages", "acct-b", "b", "b-second").await;

        let request = json!({
            "Nodes": [
                {
                    "Name": "a",
                    "Operation": {"Query": {
                        "TableName": "parallel-pages",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"S": "acct-a"}},
                        "Limit": 1
                    }},
                    "Inputs": {},
                    "After": []
                },
                {
                    "Name": "b",
                    "Operation": {"Query": {
                        "TableName": "parallel-pages",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"S": "acct-b"}},
                        "Limit": 1
                    }},
                    "Inputs": {},
                    "After": []
                }
            ],
            "Outputs": ["a", "b"]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        assert!(first["NextSequenceToken"].as_str().is_some());

        let token = first["NextSequenceToken"].as_str().unwrap().to_string();
        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        assert_eq!(
            second["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "b"
        );
        assert_eq!(
            second["Nodes"][1]["Invocations"][0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "b"
        );
        assert!(second.get("NextSequenceToken").is_none());
    })
    .await;
}

#[tokio::test]
async fn given_iterated_query_pages_when_resumed_then_prior_invocations_are_not_replayed() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "iterated").await;
        put_composite_item(&db, "iterated", "acct-a", "a", "a-first").await;
        put_composite_item(&db, "iterated", "acct-a", "b", "a-second").await;
        put_composite_item(&db, "iterated", "acct-b", "a", "b-first").await;
        put_composite_item(&db, "iterated", "acct-b", "b", "b-second").await;

        let request = json!({
        "Nodes": [
            {
                "Name": "parents",
                "Operation": {"Query": {
                    "TableName": "iterated",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "acct-a"}},
                    "Limit": 2
                }},
                "Inputs": {},
                "After": []
            },
            {
                "Name": "children",
                "Operation": {"Query": {
                    "TableName": "iterated",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"FromInput": "pk"}},
                    "Limit": 1
                }},
                "Inputs": {"pk": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].pk"},
                    "Cardinality": "MANY",
                    "OnMissing": "ERROR"
                }},
                "Iterate": "pk",
                "After": []
            }
        ],
        "Outputs": ["children"]
        });

        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        assert_eq!(
            first["Nodes"][0]["Invocations"].as_array().unwrap().len(),
            1
        );
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("first child invocation has a continuation")
            .to_string();

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(token);
        let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
        let invocations = second["Nodes"][0]["Invocations"]
            .as_array()
            .expect("child invocations");
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0]["Ordinal"], 0);
        assert_eq!(
            invocations[0]["Result"]["Query"]["Items"][0]["sk"]["S"],
            "b"
        );
        assert_eq!(invocations[1]["Ordinal"], 1);
    })
    .await;
}

#[tokio::test]
async fn given_tampered_query_token_when_resumed_then_request_is_rejected() {
    for_each_conformance_backend(|_, db| async move {
        seed_composite_table(&db, "tampered").await;
        put_composite_item(&db, "tampered", "acct", "a", "first").await;
        put_composite_item(&db, "tampered", "acct", "b", "second").await;

        let request = json!({
        "Nodes": [{
            "Name": "page",
            "Operation": {"Query": {
                "TableName": "tampered",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "acct"}},
                "Limit": 1
            }},
            "Inputs": {},
            "After": []
        }]
        });
        let first =
            response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await)
                .await;
        let token = first["NextSequenceToken"]
            .as_str()
            .expect("first page has a continuation token")
            .to_string();
        let mut bytes = token.into_bytes();
        let last = bytes.len() - 1;
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };
        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(String::from_utf8(bytes).expect("hex token"));
        let response = execute_read_sequence(app_state(db), resumed).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    })
    .await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_service_when_dependent_get_fans_out_then_native_deduplication_and_projection_are_preserved()
 {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_table(&db, "parents").await;
    seed_composite_table(&db, "children").await;
    for (id, child) in [
        ("one", "a"),
        ("two", "b"),
        ("three", "missing"),
        ("four", "a"),
    ] {
        insert_remote_fixture(
            &db,
            "parents",
            json!({"id": {"S": id}, "child_pk": {"S": "account"}, "child_sk": {"S": child}}),
        )
        .await;
    }
    put_composite_item(&db, "children", "account", "a", "A").await;
    put_composite_item(&db, "children", "account", "b", "B").await;
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [
                {"Name": "parents", "Operation": {"BatchGet": {
                    "RequestItems": {"parents": {"Keys": [
                        {"id": {"S": "one"}}, {"id": {"S": "two"}},
                        {"id": {"S": "three"}}, {"id": {"S": "four"}}
                    ]}}
                }}},
                {
                    "Name": "children",
                    "Operation": {"Get": {
                        "TableName": "children",
                        "ProjectionExpression": "#value",
                        "ExpressionAttributeNames": {"#value": "value"},
                        "Key": {
                            "pk": {"FromInput": "child_pk"},
                            "sk": {"FromInput": "child_sk"}
                        }
                    }},
                    "Inputs": {
                        "child_pk": {
                            "From": {"Node": "parents", "Select": "$.BatchGet.Items[0].child_pk"},
                            "Cardinality": "ONE",
                            "OnMissing": "ERROR"
                        },
                        "child_sk": {
                            "From": {"Node": "parents", "Select": "$.BatchGet.Items[*].child_sk"},
                            "Cardinality": "MANY",
                            "OnMissing": "ERROR"
                        }
                    },
                    "Iterate": "child_sk"
                }
            ],
            "Outputs": ["children"]
        }),
    )
    .await;
    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    assert_eq!(
        payload["Nodes"][0]["Invocations"][0]["Result"]["Get"]["Item"]["value"]["S"],
        "A"
    );
    assert_eq!(
        payload["Nodes"][0]["Invocations"][1]["Result"]["Get"]["Item"]["value"]["S"],
        "B"
    );
    let invocations = payload["Nodes"][0]["Invocations"]
        .as_array()
        .expect("invocations");
    assert_eq!(invocations.len(), 3);
    assert!(invocations[0]["Result"]["Get"]["Item"].get("pk").is_none());
    assert!(invocations[2]["Result"]["Get"].get("Item").is_none());
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_service_when_batch_get_node_spans_tables_then_one_graph_request_is_used() {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_table(&db, "accounts").await;
    seed_table(&db, "profiles").await;
    put_item(&db, "accounts", "account-1", "account").await;
    put_item(&db, "profiles", "profile-1", "profile").await;
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [{
                "Name": "records",
                "Operation": {"BatchGet": {"RequestItems": {
                    "accounts": {"Keys": [{"id": {"S": "account-1"}}]},
                    "profiles": {"Keys": [{"id": {"S": "profile-1"}}]}
                }}}
            }],
            "Outputs": ["records"]
        }),
    )
    .await;
    let status = response.status();
    let payload = response_body(response).await;

    assert_eq!(status, StatusCode::OK, "{payload}");
    let responses = &payload["Nodes"][0]["Invocations"][0]["Result"]["BatchGet"]["Responses"];
    assert_eq!(responses["accounts"][0]["id"]["S"], "account-1");
    assert_eq!(responses["profiles"][0]["id"]["S"], "profile-1");
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_service_when_strong_dependent_query_fans_out_then_each_input_is_queried() {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_table(&db, "parents").await;
    seed_composite_table(&db, "events").await;
    for (id, account) in [("one", "acct-a"), ("two", "acct-b")] {
        insert_remote_fixture(
            &db,
            "parents",
            json!({"id": {"S": id}, "account_id": {"S": account}}),
        )
        .await;
    }
    put_composite_item(&db, "events", "acct-a", "1", "event-a").await;
    put_composite_item(&db, "events", "acct-b", "1", "event-b").await;
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "STRONG",
            "Nodes": [
                {"Name": "parents", "Operation": {"BatchGet": {
                    "RequestItems": {"parents": {"Keys": [
                        {"id": {"S": "one"}}, {"id": {"S": "two"}}
                    ]}}
                }}},
                {
                    "Name": "events",
                    "Operation": {"Query": {
                        "TableName": "events",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"FromInput": "account_id"}},
                        "Limit": 10
                    }},
                    "Inputs": {"account_id": {
                        "From": {"Node": "parents", "Select": "$.BatchGet.Items[*].account_id"},
                        "Cardinality": "MANY",
                        "OnMissing": "ERROR"
                    }},
                    "Iterate": "account_id"
                }
            ],
            "Outputs": ["events"]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_body(response).await;
    assert_eq!(
        payload["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0]["value"]["S"],
        "event-a"
    );
    assert_eq!(
        payload["Nodes"][0]["Invocations"][1]["Result"]["Query"]["Items"][0]["value"]["S"],
        "event-b"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_service_when_transactional_sequence_is_requested_then_native_result_is_returned()
 {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_table(&db, "items").await;
    put_item(&db, "items", "one", "native").await;
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Nodes": [{
                "Name": "item",
                "Operation": {"Get": {
                    "TableName": "items",
                    "Key": {"id": {"S": "one"}}
                }}
            }],
            "Outputs": ["item"]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_body(response).await;
    assert_eq!(payload["ReadConsistency"], "TRANSACTIONAL");
    assert_eq!(
        payload["Nodes"][0]["Invocations"][0]["Result"]["Get"]["Item"]["value"]["S"],
        "native"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_service_gsi_query_when_graph_executes_then_query_options_and_projection_are_preserved()
 {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_remote_index(&db, "ALL").await;
    for (id, state) in [("002", "drop"), ("003", "keep")] {
        insert_remote_fixture(
            &db,
            "events",
            json!({
                "pk": {"S": "tenant"}, "sk": {"S": id}, "gpk": {"S": "status"},
                "gsk": {"S": id}, "state": {"S": state}, "value": {"S": "visible"}
            }),
        )
        .await;
    }
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [{
                "Name": "events",
                "Operation": {"Query": {
                    "TableName": "events",
                    "IndexName": "by_status",
                    "KeyConditionExpression": "gpk = :gpk",
                    "FilterExpression": "#state = :wanted",
                    "ProjectionExpression": "pk, sk, #value",
                    "ExpressionAttributeNames": {"#state": "state", "#value": "value"},
                    "ExpressionAttributeValues": {
                        ":gpk": {"S": "status"},
                        ":wanted": {"S": "keep"}
                    },
                    "ExclusiveStartKey": {
                        "pk": {"S": "tenant"},
                        "sk": {"S": "004"},
                        "gpk": {"S": "status"},
                        "gsk": {"S": "004"}
                    },
                    "ScanIndexForward": false,
                    "Limit": 2
                }}
            }],
            "Outputs": ["events"]
        }),
    )
    .await;
    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    let item = &payload["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"][0];
    assert_eq!(item["value"]["S"], "visible");
    assert!(item.get("state").is_none());
    assert!(item.get("gpk").is_none());
    assert_eq!(
        payload["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
        1
    );
    assert_eq!(
        payload["Nodes"][0]["Invocations"][0]["Result"]["Query"]["ScannedCount"],
        2
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_gsi_missing_a_filter_attribute_when_read_sequence_executes_then_it_fails_without_a_base_lookup()
 {
    let db = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    seed_remote_index(&db, "KEYS_ONLY").await;
    insert_remote_fixture(
        &db,
        "events",
        json!({
            "pk": {"S": "tenant"}, "sk": {"S": "001"}, "gpk": {"S": "status"},
            "gsk": {"S": "001"}, "private_note": {"S": "hidden"}
        }),
    )
    .await;
    let (state, server, calls) = remote_app_state(db).await;

    let response = execute_read_sequence(
        state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [{
                "Name": "events",
                "Operation": {"Query": {
                    "TableName": "events",
                    "IndexName": "by_status",
                    "KeyConditionExpression": "gpk = :gpk",
                    "FilterExpression": "#private = :private",
                    "ExpressionAttributeNames": {"#private": "private_note"},
                    "ExpressionAttributeValues": {
                        ":gpk": {"S": "status"},
                        ":private": {"S": "hidden"}
                    }
                }}
            }],
            "Outputs": ["events"]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert!(
        payload["message"]
            .as_str()
            .is_some_and(|message| message.contains("does not project [private_note]")),
        "{payload}"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    server.abort();
}

#[cfg(feature = "remote")]
async fn remote_app_state(
    db: Arc<storage::DatabaseManager>,
) -> (
    Arc<AppState>,
    tokio::task::JoinHandle<()>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let router = crate::server_router(app_state(db), false).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let observed = Arc::clone(&observed);
            async move {
                assert_eq!(
                    request.headers().get("x-amz-target").unwrap(),
                    "DynamoDB_20120810.ReadSequence"
                );
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                next.run(request).await
            }
        },
    ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let provider = storage::RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: vec![endpoint],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::Static(storage_provider::RemoteStaticCredentials {
            access_key_id: "test-access".into(),
            secret_access_key: "test-secret".into(),
            session_token: None,
        }),
        timeouts: None,
    })
    .await
    .unwrap();
    (
        app_state(Arc::new(
            storage::DatabaseManager::new_with_mocks(Arc::new(provider)).unwrap(),
        )),
        server,
        calls,
    )
}

#[cfg(feature = "remote")]
async fn insert_remote_fixture(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    item: serde_json::Value,
) {
    handle_put_item(
        db.clone(),
        json!({"TableName": table, "Item": item})
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
}

#[cfg(feature = "remote")]
async fn seed_remote_index(db: &Arc<storage::DatabaseManager>, projection: &str) {
    handle_create_table(db.clone(), json!({
        "TableName": "events",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"}, {"AttributeName": "gsk", "AttributeType": "S"}
        ],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "GlobalSecondaryIndexes": [{"IndexName": "by_status", "KeySchema": [
            {"AttributeName": "gpk", "KeyType": "HASH"}, {"AttributeName": "gsk", "KeyType": "RANGE"}
        ], "Projection": {"ProjectionType": projection}}]
    }).try_into().unwrap()).await.unwrap();
}

async fn seed_table(db: &Arc<storage::DatabaseManager>, table: &str) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": table,
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create table");
}

async fn seed_composite_table(db: &Arc<storage::DatabaseManager>, table: &str) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": table,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "MaxIndexers": 1
        })
        .try_into()
        .expect("create composite table request"),
    )
    .await
    .expect("create composite table");
}

async fn put_composite_item(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    pk: &str,
    sk: &str,
    value: &str,
) {
    handle_put_item(
        db.clone(),
        json!({
            "TableName": table,
            "Item": {
                "pk": {"S": pk},
                "sk": {"S": sk},
                "value": {"S": value}
            }
        })
        .try_into()
        .expect("put composite item request"),
    )
    .await
    .expect("put composite item");
}

async fn put_composite_item_with_child(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    pk: &str,
    sk: &str,
    value: &str,
    child_id: &str,
) {
    let mut request: storage_types::PutItemRequest = json!({
        "TableName": table,
        "Item": {
            "pk": {"S": pk},
            "sk": {"S": sk},
            "value": {"S": value},
            "child_id": {"S": child_id}
        }
    })
    .try_into()
    .expect("put composite item with child request");
    request.indexers = Some(vec!["child_id".to_string()]);
    handle_put_item(db.clone(), request)
        .await
        .expect("put composite item with child");
}

async fn put_item(db: &Arc<storage::DatabaseManager>, table: &str, id: &str, value: &str) {
    put_item_with_child(db, table, id, value, id).await;
}

async fn put_item_without_child(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    id: &str,
    value: &str,
) {
    handle_put_item(
        db.clone(),
        json!({
            "TableName": table,
            "Item": {"id": {"S": id}, "value": {"S": value}}
        })
        .try_into()
        .expect("put item without child request"),
    )
    .await
    .expect("put item without child");
}

async fn put_item_with_child(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    id: &str,
    value: &str,
    child_id: &str,
) {
    handle_put_item(
        db.clone(),
        json!({
            "TableName": table,
            "Item": {"id": {"S": id}, "value": {"S": value}, "child_id": {"S": child_id}}
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put item");
}

async fn put_context_item(
    db: &Arc<storage::DatabaseManager>,
    table: &str,
    id: &str,
    entity_id: &str,
    sub_id: &str,
) {
    handle_put_item(
        db.clone(),
        json!({
            "TableName": table,
            "Item": {
                "id": {"S": id},
                "entity_id": {"S": entity_id},
                "sub_id": {"S": sub_id}
            }
        })
        .try_into()
        .expect("put context item request"),
    )
    .await
    .expect("put context item");
}

fn app_state(db: Arc<storage::DatabaseManager>) -> Arc<AppState> {
    Arc::new(AppState::new(db))
}

async fn execute_read_sequence(
    app_state: Arc<AppState>,
    payload: serde_json::Value,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.ReadSequence"),
    );
    let body = Bytes::from(serde_json::to_vec(&payload).expect("serialize request"));
    dynamodb_endpoint(State(app_state), headers, body)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn response_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response");
    serde_json::from_slice(&bytes).expect("json response")
}
