use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
#[cfg(feature = "remote")]
use httpmock::prelude::*;
use serde_json::json;
#[cfg(feature = "remote")]
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings};

use crate::{
    manager::StorageApiManagerOptions,
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_test_support::{create_test_db, handle_create_table, handle_put_item},
    },
    types::AppState,
};

#[tokio::test]
async fn given_graph_roots_when_read_sequence_is_called_then_flat_nodes_are_returned() {
    let db = create_test_db().await;
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
}

#[tokio::test]
async fn given_total_budget_when_read_sequence_is_called_then_wave_suffix_is_deferred() {
    let db = create_test_db().await;
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
    let token = first["NextSequenceToken"]
        .as_str()
        .expect("budget continuation")
        .to_string();

    let mut resumed = request;
    resumed["NextSequenceToken"] = json!(token);
    let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
    assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
    assert_eq!(second["Nodes"][0]["Name"], "b");
    assert!(second.get("NextSequenceToken").is_none());
}

#[tokio::test]
async fn given_explicit_response_budget_when_read_sequence_is_called_then_wave_suffix_is_deferred()
{
    let db = create_test_db().await;
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
    let token = first["NextSequenceToken"]
        .as_str()
        .expect("byte-budget continuation")
        .to_string();

    let mut resumed = request;
    resumed["NextSequenceToken"] = json!(token);
    let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
    assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
    assert_eq!(second["Nodes"][0]["Name"], "b");
    assert!(second.get("NextSequenceToken").is_none());
}

#[tokio::test]
async fn given_dependent_response_budget_when_page_would_overflow_then_child_is_deferred() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
    assert_eq!(first["Nodes"].as_array().expect("first nodes").len(), 1);
    assert_eq!(first["Nodes"][0]["Name"], "parent");
    let token = first["NextSequenceToken"]
        .as_str()
        .expect("dependent byte continuation")
        .to_string();

    let mut resumed = request;
    resumed["NextSequenceToken"] = json!(token);
    let second = response_body(execute_read_sequence(app_state(db), resumed).await).await;
    assert_eq!(second["Nodes"].as_array().expect("second nodes").len(), 1);
    assert_eq!(second["Nodes"][0]["Name"], "child");
    assert!(second.get("NextSequenceToken").is_none());
}

#[tokio::test]
async fn given_input_binding_when_parent_is_declared_later_then_child_is_bound() {
    let db = create_test_db().await;
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
}

#[tokio::test]
async fn given_string_template_when_read_sequence_is_called_then_composite_key_is_bound() {
    let db = create_test_db().await;
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
}

#[tokio::test]
async fn given_ordered_sequence_payload_when_called_then_breaking_contract_rejects_it() {
    let response = execute_read_sequence(
        app_state(create_test_db().await),
        json!({
            "Sequence": [{"Name": "old", "Get": {"TableName": "items", "Key": {"id": {"S": "a"}}}}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn given_invalid_node_operation_when_called_then_validation_precedes_reads() {
    let response = execute_read_sequence(
        app_state(create_test_db().await),
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
}

#[tokio::test]
async fn given_cycle_when_called_then_no_read_is_started_and_validation_is_returned() {
    let response = execute_read_sequence(
        app_state(create_test_db().await),
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
}

#[tokio::test]
async fn given_query_page_when_resumed_then_cursor_advances_the_same_invocation() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
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
}

#[tokio::test]
async fn given_explicit_query_item_budget_when_resumed_then_optimized_frontier_advances() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
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
}

#[tokio::test]
async fn given_query_without_limit_when_called_then_default_page_bound_is_applied() {
    let db = create_test_db().await;
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
}

#[tokio::test]
async fn given_independent_query_pages_when_resumed_then_each_frontier_advances() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
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
}

#[tokio::test]
async fn given_iterated_query_pages_when_resumed_then_prior_invocations_are_not_replayed() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
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
}

#[tokio::test]
async fn given_tampered_query_token_when_resumed_then_request_is_rejected() {
    let db = create_test_db().await;
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
        response_body(execute_read_sequence(app_state(db.clone()), request.clone()).await).await;
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
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_standard_remote_table_when_dependent_get_fans_out_then_batch_get_is_used() {
    let server = MockServer::start_async().await;
    let describe_parents = mock_standard_table(&server, "parents", false).await;
    let describe_children = mock_standard_table(&server, "children", true).await;
    let root_batch = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"parents\"");
            then.status(200).json_body(json!({
                "Responses": {"parents": [
                    {"id": {"S": "one"}, "child_pk": {"S": "account"}, "child_sk": {"S": "a"}},
                    {"id": {"S": "two"}, "child_pk": {"S": "account"}, "child_sk": {"S": "b"}}
                ]},
                "UnprocessedKeys": {}
            }));
        })
        .await;
    let child_batch = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"children\"");
            then.status(200).json_body(json!({
                "Responses": {"children": [
                    {"pk": {"S": "account"}, "sk": {"S": "b"}, "value": {"S": "B"}},
                    {"pk": {"S": "account"}, "sk": {"S": "a"}, "value": {"S": "A"}}
                ]},
                "UnprocessedKeys": {}
            }));
        })
        .await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.status(500);
        })
        .await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [
                {"Name": "parents", "Operation": {"BatchGet": {
                    "RequestItems": {"parents": {"Keys": [
                        {"id": {"S": "one"}}, {"id": {"S": "two"}}
                    ]}}
                }}},
                {
                    "Name": "children",
                    "Operation": {"Get": {
                        "TableName": "children",
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
    describe_parents.assert_calls_async(1).await;
    describe_children.assert_calls_async(1).await;
    root_batch.assert_calls_async(1).await;
    child_batch.assert_calls_async(1).await;
    get.assert_calls_async(0).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_standard_remote_tables_when_batch_get_node_spans_tables_then_one_standard_request_is_used()
 {
    let server = MockServer::start_async().await;
    let describe_accounts = mock_standard_table(&server, "accounts", false).await;
    let describe_profiles = mock_standard_table(&server, "profiles", false).await;
    let batch = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"accounts\"")
                .body_includes("\"profiles\"");
            then.status(200).json_body(json!({
                "Responses": {
                    "accounts": [{"id": {"S": "account-1"}}],
                    "profiles": [{"id": {"S": "profile-1"}}]
                },
                "UnprocessedKeys": {}
            }));
        })
        .await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
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
    describe_accounts.assert_calls_async(1).await;
    describe_profiles.assert_calls_async(1).await;
    batch.assert_calls_async(1).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_standard_remote_table_when_strong_dependent_query_fans_out_then_each_input_is_queried()
 {
    let server = MockServer::start_async().await;
    let describe_parents = mock_standard_table(&server, "parents", false).await;
    let describe_events = mock_standard_table(&server, "events", true).await;
    let root_batch = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"parents\"")
                .body_includes("\"ConsistentRead\":true");
            then.status(200).json_body(json!({
                "Responses": {"parents": [
                    {"id": {"S": "one"}, "account_id": {"S": "acct-a"}},
                    {"id": {"S": "two"}, "account_id": {"S": "acct-b"}}
                ]},
                "UnprocessedKeys": {}
            }));
        })
        .await;
    let query_a = mock_standard_query(&server, "acct-a", "event-a").await;
    let query_b = mock_standard_query(&server, "acct-b", "event-b").await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
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
    describe_parents.assert_calls_async(1).await;
    describe_events.assert_calls_async(1).await;
    root_batch.assert_calls_async(1).await;
    query_a.assert_calls_async(1).await;
    query_b.assert_calls_async(1).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_standard_remote_table_when_transactional_sequence_is_requested_then_no_remote_read_is_started()
 {
    let server = MockServer::start_async().await;
    let describe = mock_standard_table(&server, "items", false).await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.status(500);
        })
        .await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
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

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    describe.assert_calls_async(0).await;
    get.assert_calls_async(0).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_standard_remote_gsi_query_when_read_sequence_executes_then_query_options_and_local_post_processing_are_preserved()
 {
    let server = MockServer::start_async().await;
    let describe_events = mock_standard_gsi_table(&server, "events", "ALL").await;
    let query = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"TableName\":\"events\"")
                .body_includes("\"IndexName\":\"by_status\"")
                .body_includes("\"ScanIndexForward\":false")
                .body_includes("\"ExclusiveStartKey\"")
                .body_excludes("\"FilterExpression\"")
                .body_excludes("\"ProjectionExpression\"");
            then.status(200).json_body(json!({
                "Items": [
                    {
                        "pk": {"S": "tenant"},
                        "sk": {"S": "002"},
                        "gpk": {"S": "status"},
                        "gsk": {"S": "002"},
                        "state": {"S": "discard"},
                        "value": {"S": "hidden"}
                    },
                    {
                        "pk": {"S": "tenant"},
                        "sk": {"S": "003"},
                        "gpk": {"S": "status"},
                        "gsk": {"S": "003"},
                        "state": {"S": "keep"},
                        "value": {"S": "visible"}
                    }
                ],
                "Count": 2,
                "ScannedCount": 2
            }));
        })
        .await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
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
                        "sk": {"S": "001"},
                        "gpk": {"S": "status"},
                        "gsk": {"S": "001"}
                    },
                    "ScanIndexForward": false,
                    "Limit": 10
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
    describe_events.assert_calls_async(1).await;
    query.assert_calls_async(1).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_gsi_missing_a_filter_attribute_when_read_sequence_executes_then_it_fails_without_a_base_lookup()
 {
    let server = MockServer::start_async().await;
    let describe_events = mock_standard_gsi_table(&server, "events", "KEYS_ONLY").await;
    let query = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.Query");
            then.status(500);
        })
        .await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.GetItem");
            then.status(500);
        })
        .await;
    let read_sequence = mock_remote_read_sequence(&server).await;

    let response = execute_read_sequence(
        remote_app_state(&server).await,
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
    describe_events.assert_calls_async(1).await;
    query.assert_calls_async(0).await;
    get.assert_calls_async(0).await;
    read_sequence.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
async fn remote_app_state(server: &MockServer) -> Arc<AppState> {
    let provider = storage::RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: vec![server.url("/")],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("remote provider");
    app_state(Arc::new(
        storage::DatabaseManager::new_with_mocks(Arc::new(provider))
            .expect("create remote mock database manager"),
    ))
}

#[cfg(feature = "remote")]
async fn mock_standard_table<'a>(
    server: &'a MockServer,
    table_name: &'a str,
    has_range_key: bool,
) -> httpmock::Mock<'a> {
    let mut attribute_definitions = vec![
        json!({"AttributeName": if has_range_key { "pk" } else { "id" }, "AttributeType": "S"}),
    ];
    let mut key_schema =
        vec![json!({"AttributeName": if has_range_key { "pk" } else { "id" }, "KeyType": "HASH"})];
    if has_range_key {
        attribute_definitions.push(json!({"AttributeName": "sk", "AttributeType": "S"}));
        key_schema.push(json!({"AttributeName": "sk", "KeyType": "RANGE"}));
    }
    server
        .mock_async(move |when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes(format!("\"TableName\":\"{table_name}\""));
            then.status(200).json_body(json!({
                "Table": {
                    "TableName": table_name,
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": attribute_definitions,
                    "KeySchema": key_schema,
                    "TableArn": format!("arn:aws:dynamodb:local:table/{table_name}"),
                    "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await
}

#[cfg(feature = "remote")]
async fn mock_standard_gsi_table<'a>(
    server: &'a MockServer,
    table_name: &'a str,
    projection_type: &'a str,
) -> httpmock::Mock<'a> {
    server
        .mock_async(move |when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes(format!("\"TableName\":\"{table_name}\""));
            then.status(200).json_body(json!({
                "Table": {
                    "TableName": table_name,
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"},
                        {"AttributeName": "gpk", "AttributeType": "S"},
                        {"AttributeName": "gsk", "AttributeType": "S"}
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"}
                    ],
                    "GlobalSecondaryIndexes": [{
                        "IndexName": "by_status",
                        "KeySchema": [
                            {"AttributeName": "gpk", "KeyType": "HASH"},
                            {"AttributeName": "gsk", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": projection_type},
                        "IndexStatus": "ACTIVE",
                        "IndexArn": "arn:aws:dynamodb:local:table/events/index/by_status",
                        "ItemCount": 0,
                        "IndexSizeBytes": 0
                    }],
                    "TableArn": format!("arn:aws:dynamodb:local:table/{table_name}"),
                    "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await
}

#[cfg(feature = "remote")]
async fn mock_standard_query<'a>(
    server: &'a MockServer,
    partition_key: &'a str,
    value: &'a str,
) -> httpmock::Mock<'a> {
    server
        .mock_async(move |when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes(format!("\"S\":\"{partition_key}\""))
                .body_includes("\"ConsistentRead\":true");
            then.status(200).json_body(json!({
                "Items": [{
                    "pk": {"S": partition_key},
                    "sk": {"S": "1"},
                    "value": {"S": value}
                }],
                "Count": 1,
                "ScannedCount": 1
            }));
        })
        .await
}

#[cfg(feature = "remote")]
async fn mock_remote_read_sequence(server: &MockServer) -> httpmock::Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "DynamoDB_20120810.ReadSequence");
            then.status(500);
        })
        .await
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
            ]
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

async fn put_item(db: &Arc<storage::DatabaseManager>, table: &str, id: &str, value: &str) {
    put_item_with_child(db, table, id, value, id).await;
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
    Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ))
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
