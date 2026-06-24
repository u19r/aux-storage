use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};

use crate::{
    manager::{ReadSequenceAfterRootStepHook, StorageApiManagerOptions, SyncReadBarrier},
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_test_support::{
            create_test_db, default_conformance_backends, handle_create_table, handle_put_item,
        },
    },
    types::AppState,
};

#[derive(Debug, Default)]
struct CountingReadBarrier {
    calls: AtomicUsize,
}

struct UpdateOrgAfterRootStepHook {
    db: Arc<storage::DatabaseManager>,
}

#[async_trait::async_trait]
impl ReadSequenceAfterRootStepHook for UpdateOrgAfterRootStepHook {
    async fn after_root_step(&self) -> Result<(), http_error::HttpApiError> {
        handle_put_item(
            self.db.clone(),
            json!({
                "TableName": "Organizations",
                "Item": {
                    "pk": {"S": "org#1"},
                    "name": {"S": "Updated Org"}
                }
            })
            .try_into()
            .expect("put item request"),
        )
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SyncReadBarrier for CountingReadBarrier {
    async fn ensure_linearizable_read(&self) -> Result<(), http_error::HttpApiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn read_sequence_root_get_returns_item_response() {
    let response = execute_read_sequence(
        seeded_user_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "ReturnConsumedCapacity": "TOTAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_body(response).await;
    assert_eq!(payload["ReadConsistency"], "EVENTUAL");
    assert_eq!(payload["Partial"], false);
    assert!(payload["NextSequenceToken"].is_null());
    assert_eq!(payload["Responses"][0]["Name"], "user");
    assert_eq!(payload["Responses"][0]["Item"]["pk"]["S"], "user#1");
    assert_eq!(payload["Responses"][0]["Item"]["org_id"]["S"], "org#1");
    assert_eq!(payload["ConsumedCapacity"]["TableName"], "ReadSequence");
    assert_eq!(payload["ConsumedCapacity"]["ReadCapacityUnits"], 1.0);
}

#[tokio::test]
async fn read_sequence_dependent_get_attaches_joined_item() {
    let response = execute_read_sequence(
        seeded_user_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "user.Item.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": "Organizations",
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "user",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let org = &payload["Responses"][0]["Joins"]["org"]["Item"];
    assert_eq!(org["pk"]["S"], "org#1");
    assert_eq!(org["name"]["S"], "Example Org");
}

#[tokio::test]
async fn read_sequence_root_batch_get_returns_item_results() {
    let response = execute_read_sequence(
        seeded_user_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "users",
                    "BatchGet": {
                        "RequestItems": {
                            "Users": {
                                "Keys": [
                                    {"pk": {"S": "user#1"}},
                                    {"pk": {"S": "user#2"}}
                                ]
                            }
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    let pks = items
        .iter()
        .map(|item| item["Item"]["pk"]["S"].as_str().expect("pk"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(pks, std::collections::BTreeSet::from(["user#1", "user#2"]));
}

#[tokio::test]
async fn read_sequence_root_query_and_shared_child_get_returns_warning_and_joins() {
    let response = execute_read_sequence(
        seeded_team_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": "Organizations",
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "members",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(payload["Warning"]["Code"], "BetterModeledAsGsi");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    for item in items {
        assert_eq!(item["Joins"]["org"]["Item"]["pk"]["S"], "org#1");
    }
}

#[tokio::test]
async fn read_sequence_dependent_batch_get_attaches_array_join() {
    let response = execute_read_sequence(
        seeded_team_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "orgs",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "BatchGet": {
                            "RequestItems": {
                                "Organizations": {
                                    "Keys": [
                                        {"pk": {"S": "${org_id}"}}
                                    ]
                                }
                            }
                        },
                        "Join": {
                            "To": "members",
                            "As": "orgs",
                            "Type": "ARRAY"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    for item in items {
        let joined = item["Joins"]["orgs"]["Items"].as_array().expect("orgs");
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0]["pk"]["S"], "org#1");
    }
}

#[tokio::test]
async fn read_sequence_dependent_batch_get_expands_string_set_selector() {
    let response = execute_read_sequence(
        seeded_team_app_state_with_related_sks().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "member",
                    "Get": {
                        "TableName": "TeamUsers",
                        "Key": {
                            "pk": {"S": "tenant#1"},
                            "sk": {"S": "user#1"}
                        }
                    },
                    "Select": {
                        "tenant_pk": "$.pk"
                    }
                },
                {
                    "Name": "related_members",
                    "ForEach": {
                        "From": "member.Item.related_sks",
                        "As": "related_sk",
                        "OnMissing": "ERROR",
                        "BatchGet": {
                            "RequestItems": {
                                "TeamUsers": {
                                    "Keys": [
                                        {
                                            "pk": {"S": "${tenant_pk}"},
                                            "sk": {"S": "${related_sk}"}
                                        }
                                    ]
                                }
                            }
                        },
                        "Join": {
                            "To": "member",
                            "As": "related",
                            "Type": "ARRAY"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let joined = payload["Responses"][0]["Joins"]["related"]["Items"]
        .as_array()
        .expect("related items");
    let names = joined
        .iter()
        .map(|item| item["name"]["S"].as_str().expect("name"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names, std::collections::BTreeSet::from(["Ava", "Ben"]));
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn read_sequence_remote_eventual_query_child_get_uses_bounded_remote_calls() {
    let server = MockServer::start_async().await;
    let app_state = remote_app_state(&server).await;
    let members_describe = remote_describe_table_mock(
        &server,
        "TeamUsers",
        &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
    )
    .await;
    let orgs_describe =
        remote_describe_table_mock(&server, "Organizations", &[("pk", "S", "HASH")]).await;
    let query = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.Query")
                .body_includes("\"TableName\":\"TeamUsers\"")
                .path("/");
            then.status(200).json_body(json!({
                "Items": [
                    {
                        "pk": {"S": "tenant#1"},
                        "sk": {"S": "user#1"},
                        "org_id": {"S": "org#1"}
                    },
                    {
                        "pk": {"S": "tenant#1"},
                        "sk": {"S": "user#2"},
                        "org_id": {"S": "org#1"}
                    }
                ]
            }));
        })
        .await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes("\"TableName\":\"Organizations\"")
                .body_includes("\"org#1\"")
                .path("/");
            then.status(200).json_body(json!({
                "Item": {
                    "pk": {"S": "org#1"},
                    "name": {"S": "Remote Org"}
                }
            }));
        })
        .await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": "Organizations",
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "members",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    for item in items {
        assert_eq!(item["Joins"]["org"]["Item"]["name"]["S"], "Remote Org");
    }
    members_describe.assert_calls_async(1).await;
    orgs_describe.assert_calls_async(1).await;
    query.assert_calls_async(1).await;
    get.assert_calls_async(1).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn read_sequence_remote_strong_get_child_batch_get_uses_consistent_remote_reads() {
    let server = MockServer::start_async().await;
    let app_state = remote_app_state(&server).await;
    let users_describe = remote_describe_table_mock(&server, "Users", &[("pk", "S", "HASH")]).await;
    let orgs_describe =
        remote_describe_table_mock(&server, "Organizations", &[("pk", "S", "HASH")]).await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes("\"TableName\":\"Users\"")
                .body_includes("\"ConsistentRead\":true")
                .path("/");
            then.status(200).json_body(json!({
                "Item": {
                    "pk": {"S": "user#1"},
                    "org_id": {"S": "org#1"}
                }
            }));
        })
        .await;
    let batch_get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"Organizations\"")
                .body_includes("\"ConsistentRead\":true")
                .body_includes("\"org#1\"")
                .path("/");
            then.status(200).json_body(json!({
                "Responses": {
                    "Organizations": [
                        {
                            "pk": {"S": "org#1"},
                            "name": {"S": "Remote Org"}
                        }
                    ]
                }
            }));
        })
        .await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "STRONG",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "orgs",
                    "ForEach": {
                        "From": "user.Item.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "BatchGet": {
                            "RequestItems": {
                                "Organizations": {
                                    "Keys": [
                                        {"pk": {"S": "${org_id}"}}
                                    ]
                                }
                            }
                        },
                        "Join": {
                            "To": "user",
                            "As": "orgs",
                            "Type": "ARRAY"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(
        payload["Responses"][0]["Joins"]["orgs"]["Items"][0]["name"]["S"],
        "Remote Org"
    );
    users_describe.assert_calls_async(1).await;
    orgs_describe.assert_calls_async(1).await;
    get.assert_calls_async(1).await;
    batch_get.assert_calls_async(1).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn read_sequence_remote_rejects_transactional_before_remote_call() {
    let server = MockServer::start_async().await;
    let app_state = remote_app_state(&server).await;
    let remote_call = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(200).json_body(json!({}));
        })
        .await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "payload: {payload}");
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("Transactional is not supported")
    );
    remote_call.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn read_sequence_remote_rejects_strong_gsi_before_remote_call() {
    let server = MockServer::start_async().await;
    let app_state = remote_app_state(&server).await;
    let remote_call = server
        .mock_async(|when, then| {
            when.method(POST).path("/");
            then.status(200).json_body(json!({}));
        })
        .await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "STRONG",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "payload: {payload}");
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("STRONG consistency cannot read GSIs")
    );
    remote_call.assert_calls_async(0).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn read_sequence_remote_batch_get_unprocessed_keys_returns_retryable_error() {
    let server = MockServer::start_async().await;
    let app_state = remote_app_state(&server).await;
    let users_describe = remote_describe_table_mock(&server, "Users", &[("pk", "S", "HASH")]).await;
    let orgs_describe =
        remote_describe_table_mock(&server, "Organizations", &[("pk", "S", "HASH")]).await;
    let get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.GetItem")
                .body_includes("\"TableName\":\"Users\"")
                .path("/");
            then.status(200).json_body(json!({
                "Item": {
                    "pk": {"S": "user#1"},
                    "org_id": {"S": "org#1"}
                }
            }));
        })
        .await;
    let batch_get = server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.BatchGetItem")
                .body_includes("\"Organizations\"")
                .path("/");
            then.status(200).json_body(json!({
                "Responses": {},
                "UnprocessedKeys": {
                    "Organizations": {
                        "Keys": [
                            {"pk": {"S": "org#1"}}
                        ]
                    }
                }
            }));
        })
        .await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "orgs",
                    "ForEach": {
                        "From": "user.Item.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "BatchGet": {
                            "RequestItems": {
                                "Organizations": {
                                    "Keys": [
                                        {"pk": {"S": "${org_id}"}}
                                    ]
                                }
                            }
                        },
                        "Join": {
                            "To": "user",
                            "As": "orgs",
                            "Type": "ARRAY"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "payload: {payload}");
    assert_eq!(payload["__type"], "ThrottlingException");
    users_describe.assert_calls_async(1).await;
    orgs_describe.assert_calls_async(1).await;
    get.assert_calls_async(1).await;
    batch_get.assert_calls_async(1).await;
}

#[cfg(feature = "remote")]
#[tokio::test]
#[ignore = "requires AUX_STORAGE_READ_SEQUENCE_REMOTE_ENDPOINT pointing at a running storage-api \
            /storage route"]
async fn read_sequence_remote_live_endpoint_eventual_query_child_get_round_trips() {
    let endpoint_url = std::env::var("AUX_STORAGE_READ_SEQUENCE_REMOTE_ENDPOINT")
        .expect("AUX_STORAGE_READ_SEQUENCE_REMOTE_ENDPOINT");
    let db = remote_db_for_endpoint(endpoint_url).await;
    let app_state = remote_app_state_for_db(db.clone());
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let team_users_table = format!("ReadSequenceLiveTeamUsers{suffix}");
    let organizations_table = format!("ReadSequenceLiveOrganizations{suffix}");

    seed_live_remote_team_tables(db, &team_users_table, &organizations_table).await;

    let response = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": team_users_table,
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        },
                        "Limit": 10
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": organizations_table,
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "members",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    for item in items {
        assert_eq!(item["Joins"]["org"]["Item"]["name"]["S"], "Live Remote Org");
    }
}

#[tokio::test]
async fn read_sequence_dependent_query_attaches_array_join() {
    let response = execute_read_sequence(
        seeded_team_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "activity",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Query": {
                            "TableName": "OrgActivity",
                            "KeyConditionExpression": "pk = :org_id",
                            "ExpressionAttributeValues": {
                                ":org_id": {"S": "${org_id}"}
                            },
                            "Limit": 10
                        },
                        "Join": {
                            "To": "members",
                            "As": "activity",
                            "Type": "ARRAY"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    let items = payload["Responses"][0]["Items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    for item in items {
        let joined = item["Joins"]["activity"]["Items"]
            .as_array()
            .expect("activity");
        assert_eq!(joined.len(), 2);
        assert_eq!(joined[0]["pk"]["S"], "org#1");
    }
}

#[tokio::test]
async fn read_sequence_total_read_budget_stops_root_query() {
    let response = execute_read_sequence(
        seeded_team_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "MaxTotalReadItems": 1,
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("total read limit")
    );
}

#[tokio::test]
async fn read_sequence_root_query_token_resumes_next_page() {
    let app_state = seeded_team_app_state().await;
    let first_request = json!({
        "ReadConsistency": "EVENTUAL",
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    },
                    "Limit": 1
                }
            }
        ]
    });
    let first = execute_read_sequence(app_state.clone(), first_request.clone()).await;
    let first_status = first.status();
    let first_payload = response_body(first).await;
    assert_eq!(first_status, StatusCode::OK, "payload: {first_payload}");
    assert_eq!(first_payload["Partial"], true);
    assert_eq!(
        first_payload["Responses"][0]["Items"][0]["Item"]["sk"]["S"],
        "user#1"
    );
    let token = first_payload["NextSequenceToken"]
        .as_str()
        .expect("next sequence token")
        .to_string();

    let mut second_request = first_request;
    second_request["NextSequenceToken"] = serde_json::Value::String(token);
    let second = execute_read_sequence(app_state, second_request).await;
    let second_status = second.status();
    let second_payload = response_body(second).await;
    assert_eq!(second_status, StatusCode::OK, "payload: {second_payload}");
    assert_eq!(second_payload["Partial"], false);
    assert!(second_payload["NextSequenceToken"].is_null());
    assert_eq!(
        second_payload["Responses"][0]["Items"][0]["Item"]["sk"]["S"],
        "user#2"
    );
}

#[tokio::test]
async fn read_sequence_root_query_token_rejects_request_digest_mismatch() {
    let app_state = seeded_team_app_state().await;
    let first_request = json!({
        "ReadConsistency": "EVENTUAL",
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    },
                    "Limit": 1
                }
            }
        ]
    });
    let first = execute_read_sequence(app_state.clone(), first_request).await;
    let first_payload = response_body(first).await;
    let token = first_payload["NextSequenceToken"]
        .as_str()
        .expect("next sequence token")
        .to_string();

    let stale_resume = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "NextSequenceToken": token,
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#2"}
                        },
                        "Limit": 1
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(stale_resume.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(stale_resume).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("token is stale")
    );
}

#[tokio::test]
async fn read_sequence_root_query_token_rejects_metadata_digest_mismatch() {
    let app_state = seeded_team_app_state().await;
    let first_request = json!({
        "ReadConsistency": "EVENTUAL",
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    },
                    "Limit": 1
                }
            }
        ]
    });
    let first = execute_read_sequence(app_state.clone(), first_request.clone()).await;
    let first_payload = response_body(first).await;
    let mut token: serde_json::Value = serde_json::from_str(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token"),
    )
    .expect("token json");
    token["metadataDigest"] = serde_json::Value::String("stale".to_string());

    let mut stale_request = first_request;
    stale_request["NextSequenceToken"] = serde_json::Value::String(token.to_string());
    let stale_resume = execute_read_sequence(app_state, stale_request).await;

    assert_eq!(stale_resume.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(stale_resume).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("token is stale")
    );
}

#[tokio::test]
async fn read_sequence_root_query_token_rejects_expired_token() {
    let app_state = seeded_team_app_state().await;
    let first = execute_read_sequence(
        app_state.clone(),
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        },
                        "Limit": 1
                    }
                }
            ]
        }),
    )
    .await;
    let first_payload = response_body(first).await;
    let mut token: serde_json::Value = serde_json::from_str(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token"),
    )
    .expect("token json");
    token["expiresAtEpochSeconds"] = serde_json::Value::from(0);

    let expired_resume = execute_read_sequence(
        app_state,
        json!({
            "ReadConsistency": "EVENTUAL",
            "NextSequenceToken": token.to_string(),
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        },
                        "Limit": 1
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(expired_resume.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(expired_resume).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("snapshot expired")
    );
}

#[tokio::test]
async fn read_sequence_child_query_token_resumes_without_repeating_child_items() {
    let app_state = seeded_user_app_state().await;
    let request = json!({
        "ReadConsistency": "EVENTUAL",
        "Sequence": [
            {
                "Name": "user",
                "Get": {
                    "TableName": "Users",
                    "Key": {"pk": {"S": "user#1"}}
                },
                "Select": {
                    "org_id": "$.org_id"
                }
            },
            {
                "Name": "activity",
                "ForEach": {
                    "From": "user.Item.org_id",
                    "As": "org_id",
                    "OnMissing": "ERROR",
                    "Query": {
                        "TableName": "OrgActivity",
                        "KeyConditionExpression": "pk = :org_id",
                        "ExpressionAttributeValues": {
                            ":org_id": {"S": "${org_id}"}
                        },
                        "Limit": 1
                    },
                    "Join": {
                        "To": "user",
                        "As": "activity",
                        "Type": "ARRAY"
                    }
                }
            }
        ]
    });
    let first = execute_read_sequence(app_state.clone(), request.clone()).await;
    let first_status = first.status();
    let first_payload = response_body(first).await;
    assert_eq!(first_status, StatusCode::OK, "payload: {first_payload}");
    assert_eq!(first_payload["Partial"], true);
    assert_eq!(
        first_payload["Responses"][0]["Joins"]["activity"]["Items"][0]["sk"]["S"],
        "activity#1"
    );
    assert_eq!(
        first_payload["Responses"][0]["Joins"]["activity"]["Partial"],
        true
    );

    let mut resume_request = request;
    resume_request["NextSequenceToken"] = serde_json::Value::String(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token")
            .to_string(),
    );
    let second = execute_read_sequence(app_state, resume_request).await;
    let second_status = second.status();
    let second_payload = response_body(second).await;
    assert_eq!(second_status, StatusCode::OK, "payload: {second_payload}");
    assert_eq!(second_payload["Partial"], false);
    assert!(second_payload["NextSequenceToken"].is_null());
    assert_eq!(
        second_payload["Responses"][0]["Joins"]["activity"]["Items"][0]["sk"]["S"],
        "activity#2"
    );
    assert!(second_payload["Responses"][0]["Joins"]["activity"]["Partial"].is_null());
}

#[tokio::test]
async fn read_sequence_fanout_token_resumes_at_next_parent() {
    let app_state = seeded_team_app_state().await;
    let request = json!({
        "ReadConsistency": "EVENTUAL",
        "MaxFanoutPerStep": 1,
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    }
                },
                "Select": {
                    "org_id": "$.org_id"
                }
            },
            {
                "Name": "org",
                "ForEach": {
                    "From": "members.Items.org_id",
                    "As": "org_id",
                    "OnMissing": "ERROR",
                    "Get": {
                        "TableName": "Organizations",
                        "Key": {"pk": {"S": "${org_id}"}}
                    },
                    "Join": {
                        "To": "members",
                        "As": "org",
                        "Type": "REQUIRED_ONE"
                    }
                }
            }
        ]
    });

    let first = execute_read_sequence(app_state.clone(), request.clone()).await;
    let first_status = first.status();
    let first_payload = response_body(first).await;
    assert_eq!(first_status, StatusCode::OK, "payload: {first_payload}");
    assert_eq!(first_payload["Partial"], true);
    let first_items = first_payload["Responses"][0]["Items"]
        .as_array()
        .expect("items");
    assert_eq!(first_items[0]["Joins"]["org"]["Item"]["pk"]["S"], "org#1");
    assert!(first_items[1]["Joins"].is_null());

    let mut resume_request = request;
    resume_request["NextSequenceToken"] = serde_json::Value::String(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token")
            .to_string(),
    );
    let second = execute_read_sequence(app_state, resume_request).await;
    let second_status = second.status();
    let second_payload = response_body(second).await;
    assert_eq!(second_status, StatusCode::OK, "payload: {second_payload}");
    assert_eq!(second_payload["Partial"], false);
    assert!(second_payload["NextSequenceToken"].is_null());
    let second_items = second_payload["Responses"][0]["Items"]
        .as_array()
        .expect("items");
    assert!(second_items[0]["Joins"].is_null());
    assert_eq!(second_items[1]["Joins"]["org"]["Item"]["pk"]["S"], "org#1");
}

#[tokio::test]
async fn read_sequence_response_byte_token_resumes_at_next_parent() {
    let app_state = seeded_team_app_state().await;
    let request = json!({
        "ReadConsistency": "EVENTUAL",
        "MaxResponseBytes": 350,
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    }
                },
                "Select": {
                    "org_id": "$.org_id"
                }
            },
            {
                "Name": "org",
                "ForEach": {
                    "From": "members.Items.org_id",
                    "As": "org_id",
                    "OnMissing": "ERROR",
                    "Get": {
                        "TableName": "Organizations",
                        "Key": {"pk": {"S": "${org_id}"}}
                    },
                    "Join": {
                        "To": "members",
                        "As": "org",
                        "Type": "REQUIRED_ONE"
                    }
                }
            }
        ]
    });

    let first = execute_read_sequence(app_state.clone(), request.clone()).await;
    let first_status = first.status();
    let first_payload = response_body(first).await;
    assert_eq!(first_status, StatusCode::OK, "payload: {first_payload}");
    assert_eq!(first_payload["Partial"], true);
    let first_items = first_payload["Responses"][0]["Items"]
        .as_array()
        .expect("items");
    assert_eq!(first_items[0]["Joins"]["org"]["Item"]["pk"]["S"], "org#1");
    assert!(first_items[1]["Joins"].is_null());

    let mut resume_request = request;
    resume_request["NextSequenceToken"] = serde_json::Value::String(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token")
            .to_string(),
    );
    let second = execute_read_sequence(app_state, resume_request).await;
    let second_status = second.status();
    let second_payload = response_body(second).await;
    assert_eq!(second_status, StatusCode::OK, "payload: {second_payload}");
    assert_eq!(second_payload["Partial"], false);
    let second_items = second_payload["Responses"][0]["Items"]
        .as_array()
        .expect("items");
    assert!(second_items[0]["Joins"].is_null());
    assert_eq!(second_items[1]["Joins"]["org"]["Item"]["pk"]["S"], "org#1");
}

#[tokio::test]
async fn read_sequence_response_byte_budget_fails_when_next_child_cannot_fit() {
    let response = execute_read_sequence(
        seeded_team_app_state().await,
        json!({
            "ReadConsistency": "EVENTUAL",
            "MaxResponseBytes": 250,
            "Sequence": [
                {
                    "Name": "members",
                    "Query": {
                        "TableName": "TeamUsers",
                        "KeyConditionExpression": "pk = :tenant",
                        "ExpressionAttributeValues": {
                            ":tenant": {"S": "tenant#1"}
                        }
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "members.Items.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": "Organizations",
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "members",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("cannot fit the next child result")
    );
}

#[tokio::test]
async fn read_sequence_transactional_gsi_rejected_before_manager_execution() {
    let response = execute_read_sequence(
        app_state().await,
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    let message = payload["message"].as_str().expect("message");
    assert!(
        message.contains("TRANSACTIONAL consistency cannot read GSIs"),
        "unexpected message: {message}"
    );
    assert!(
        !message.contains("execution is not yet supported"),
        "request should fail validation before manager execution: {message}"
    );
}

#[tokio::test]
async fn read_sequence_immediate_gsi_transactional_rejected_without_snapshot_support() {
    let db = create_test_db().await;
    seed_orders_gsi_table(db.clone()).await;
    let response = execute_read_sequence(
        app_state_with_db_and_options(
            db,
            StorageApiManagerOptions {
                read_sequence_capabilities: Some(storage_types::ReadSequenceProviderCapabilities {
                    immediate_gsi_consistency: true,
                    ..storage_types::ReadSequenceProviderCapabilities::default()
                }),
                ..StorageApiManagerOptions::default()
            },
        ),
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("consistency Transactional is not supported")
    );
}

#[tokio::test]
async fn read_sequence_transactional_immediate_gsi_succeeds_on_file_backed_sqlite_snapshot() {
    let db = create_file_backed_sqlite_test_db_with_immediate_gsi(true).await;
    seed_orders_gsi_table(db.clone()).await;
    let response = execute_read_sequence(
        app_state_with_db(db),
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(payload["ReadConsistency"], "TRANSACTIONAL");
    assert_eq!(
        payload["Responses"][0]["Items"][0]["Item"]["pk"]["S"],
        "order#1"
    );
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn read_sequence_foundationdb_transactional_immediate_gsi_succeeds() {
    let Some(db) = create_foundationdb_test_db_with_immediate_gsi().await else {
        eprintln!(
            "Skipping FoundationDB ReadSequence immediate-GSI test: 127.0.0.1:4689 is unavailable"
        );
        return;
    };
    seed_orders_gsi_table(db.clone()).await;
    let response = execute_read_sequence(
        app_state_with_db(db),
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(payload["ReadConsistency"], "TRANSACTIONAL");
    assert_eq!(
        payload["Responses"][0]["Items"][0]["Item"]["pk"]["S"],
        "order#1"
    );
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn read_sequence_foundationdb_transactional_token_resumes_under_response_pressure() {
    let Some(db) = create_foundationdb_test_db_with_immediate_gsi().await else {
        eprintln!(
            "Skipping FoundationDB ReadSequence token-pressure test: 127.0.0.1:4689 is unavailable"
        );
        return;
    };
    seed_team_tables(db.clone(), false).await;
    let app_state = app_state_with_db(db);
    let request = json!({
        "ReadConsistency": "TRANSACTIONAL",
        "MaxResponseBytes": 350,
        "Sequence": [
            {
                "Name": "members",
                "Query": {
                    "TableName": "TeamUsers",
                    "KeyConditionExpression": "pk = :tenant",
                    "ExpressionAttributeValues": {
                        ":tenant": {"S": "tenant#1"}
                    }
                },
                "Select": {
                    "org_id": "$.org_id"
                }
            },
            {
                "Name": "org",
                "ForEach": {
                    "From": "members.Items.org_id",
                    "As": "org_id",
                    "OnMissing": "ERROR",
                    "Get": {
                        "TableName": "Organizations",
                        "Key": {"pk": {"S": "${org_id}"}}
                    },
                    "Join": {
                        "To": "members",
                        "As": "org",
                        "Type": "REQUIRED_ONE"
                    }
                }
            }
        ]
    });

    let first = execute_read_sequence(app_state.clone(), request.clone()).await;
    let first_status = first.status();
    let first_payload = response_body(first).await;
    assert_eq!(first_status, StatusCode::OK, "payload: {first_payload}");
    assert_eq!(first_payload["Partial"], true);
    assert!(first_payload["NextSequenceToken"].as_str().is_some());

    let mut resume_request = request;
    resume_request["NextSequenceToken"] = serde_json::Value::String(
        first_payload["NextSequenceToken"]
            .as_str()
            .expect("next sequence token")
            .to_string(),
    );
    let second = execute_read_sequence(app_state, resume_request).await;
    let second_status = second.status();
    let second_payload = response_body(second).await;
    assert_eq!(second_status, StatusCode::OK, "payload: {second_payload}");
    assert_eq!(second_payload["Partial"], false);
}

#[tokio::test]
async fn read_sequence_strong_gsi_rejected_before_execution() {
    let response = execute_read_sequence(
        app_state().await,
        json!({
            "ReadConsistency": "STRONG",
            "Sequence": [
                {
                    "Name": "orders",
                    "Query": {
                        "TableName": "Orders",
                        "IndexName": "by_customer",
                        "KeyConditionExpression": "customer_id = :customer",
                        "ExpressionAttributeValues": {
                            ":customer": {"S": "customer#1"}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("STRONG consistency cannot read GSIs")
    );
}

#[tokio::test]
async fn read_sequence_transactional_base_table_rejected_without_snapshot_support() {
    let response = execute_read_sequence(
        seeded_user_app_state().await,
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = response_body(response).await;
    assert_validation_error_type(&payload);
    assert!(
        payload["message"]
            .as_str()
            .expect("message")
            .contains("consistency Transactional is not supported")
    );
}

#[tokio::test]
async fn read_sequence_transactional_file_backed_sqlite_child_reads_root_snapshot() {
    let db = create_file_backed_sqlite_test_db().await;
    seed_user_tables(db.clone()).await;
    seed_org_table(db.clone()).await;
    let response = execute_read_sequence(
        app_state_with_db_and_options(
            db.clone(),
            StorageApiManagerOptions {
                read_sequence_after_root_step_hook: Some(Arc::new(UpdateOrgAfterRootStepHook {
                    db: db.clone(),
                })),
                ..StorageApiManagerOptions::default()
            },
        ),
        json!({
            "ReadConsistency": "TRANSACTIONAL",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    },
                    "Select": {
                        "org_id": "$.org_id"
                    }
                },
                {
                    "Name": "org",
                    "ForEach": {
                        "From": "user.Item.org_id",
                        "As": "org_id",
                        "OnMissing": "ERROR",
                        "Get": {
                            "TableName": "Organizations",
                            "Key": {"pk": {"S": "${org_id}"}}
                        },
                        "Join": {
                            "To": "user",
                            "As": "org",
                            "Type": "REQUIRED_ONE"
                        }
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(payload["ReadConsistency"], "TRANSACTIONAL");
    assert_eq!(
        payload["Responses"][0]["Joins"]["org"]["Item"]["name"]["S"], "Example Org",
        "child read should stay on the root read snapshot"
    );

    let follow_up = execute_read_sequence(
        app_state_with_db(db),
        json!({
            "ReadConsistency": "EVENTUAL",
            "Sequence": [
                {
                    "Name": "org",
                    "Get": {
                        "TableName": "Organizations",
                        "Key": {"pk": {"S": "org#1"}}
                    }
                }
            ]
        }),
    )
    .await;
    let follow_up_status = follow_up.status();
    let follow_up_payload = response_body(follow_up).await;
    assert_eq!(
        follow_up_status,
        StatusCode::OK,
        "payload: {follow_up_payload}"
    );
    assert_eq!(
        follow_up_payload["Responses"][0]["Item"]["name"]["S"], "Updated Org",
        "the interleaved write should be committed outside the snapshot"
    );
}

#[tokio::test]
async fn read_sequence_strong_base_table_get_uses_consistent_read_barrier() {
    let db = create_test_db().await;
    seed_user_tables(db.clone()).await;
    let barrier = Arc::new(CountingReadBarrier::default());
    let response = execute_read_sequence(
        app_state_with_db_and_options(
            db,
            StorageApiManagerOptions {
                sync_read_barrier: Some(barrier.clone()),
                ..StorageApiManagerOptions::default()
            },
        ),
        json!({
            "ReadConsistency": "STRONG",
            "Sequence": [
                {
                    "Name": "user",
                    "Get": {
                        "TableName": "Users",
                        "Key": {"pk": {"S": "user#1"}}
                    }
                }
            ]
        }),
    )
    .await;

    let status = response.status();
    let payload = response_body(response).await;
    assert_eq!(status, StatusCode::OK, "payload: {payload}");
    assert_eq!(barrier.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn read_sequence_eventual_root_and_child_get_matches_across_conformance_backends() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        seed_user_tables(db.clone()).await;
        seed_org_table(db.clone()).await;
        let response = execute_read_sequence(
            app_state_with_db(db),
            json!({
                "ReadConsistency": "EVENTUAL",
                "Sequence": [
                    {
                        "Name": "user",
                        "Get": {
                            "TableName": "Users",
                            "Key": {"pk": {"S": "user#1"}}
                        },
                        "Select": {
                            "org_id": "$.org_id"
                        }
                    },
                    {
                        "Name": "org",
                        "ForEach": {
                            "From": "user.Item.org_id",
                            "As": "org_id",
                            "OnMissing": "ERROR",
                            "Get": {
                                "TableName": "Organizations",
                                "Key": {"pk": {"S": "${org_id}"}}
                            },
                            "Join": {
                                "To": "user",
                                "As": "org",
                                "Type": "REQUIRED_ONE"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let status = response.status();
        let payload = response_body(response).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} payload: {payload}",
            backend.name
        );
        assert_eq!(
            payload["Responses"][0]["Joins"]["org"]["Item"]["name"]["S"], "Example Org",
            "{}",
            backend.name
        );
    }
}

async fn app_state() -> Arc<AppState> {
    app_state_with_db(create_test_db().await)
}

async fn create_file_backed_sqlite_test_db() -> Arc<storage::DatabaseManager> {
    create_file_backed_sqlite_test_db_with_immediate_gsi(false).await
}

async fn create_file_backed_sqlite_test_db_with_immediate_gsi(
    immediate_gsi_consistency: bool,
) -> Arc<storage::DatabaseManager> {
    let database_path = std::env::temp_dir().join(format!(
        "aux-storage-read-sequence-{}.db",
        uuid::Uuid::new_v4()
    ));
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(database_path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            immediate_gsi_consistency,
            force_file_backed_database: true,
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    Arc::new(
        storage::DatabaseManager::new_for_test_with_config(config)
            .await
            .expect("file-backed sqlite db"),
    )
}

#[cfg(feature = "foundationdb")]
async fn create_foundationdb_test_db_with_immediate_gsi() -> Option<Arc<storage::DatabaseManager>> {
    if !foundationdb_live_port_available().await {
        return None;
    }
    let config = StorageConfig {
        backend_type: StorageBackend::FoundationDb,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: Some(storage_provider::FoundationDbSettings {
            subspace_prefix: Some(format!("tests/storage-api/{}/", uuid::Uuid::new_v4())),
            immediate_gsi_consistency: true,
            ..storage_provider::FoundationDbSettings::default()
        }),
        remote: None,
    };
    match storage::DatabaseManager::new_for_test_with_config(config).await {
        Ok(db) => Some(Arc::new(db)),
        Err(error) => {
            eprintln!("Skipping FoundationDB ReadSequence test: {error}");
            None
        }
    }
}

#[cfg(feature = "foundationdb")]
async fn foundationdb_live_port_available() -> bool {
    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        tokio::net::TcpStream::connect("127.0.0.1:4689"),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

#[cfg(feature = "remote")]
async fn remote_app_state(server: &MockServer) -> Arc<AppState> {
    remote_app_state_for_db(remote_db_for_endpoint(server.url("/")).await)
}

#[cfg(feature = "remote")]
async fn remote_db_for_endpoint(endpoint_url: String) -> Arc<storage::DatabaseManager> {
    let provider = Arc::new(
        storage::RemoteStorageProvider::new(RemoteStorageSettings {
            endpoint_urls: vec![endpoint_url],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        })
        .await
        .expect("remote provider"),
    );
    Arc::new(storage::DatabaseManager::new_with_mocks(provider))
}

#[cfg(feature = "remote")]
fn remote_app_state_for_db(db: Arc<storage::DatabaseManager>) -> Arc<AppState> {
    app_state_with_db_and_options(
        db,
        StorageApiManagerOptions {
            read_sequence_capabilities: Some(storage_types::ReadSequenceProviderCapabilities {
                eventual_reads: true,
                strong_reads: true,
                transactional_reads: false,
                transactional_snapshots: false,
                immediate_gsi_consistency: false,
            }),
            ..StorageApiManagerOptions::default()
        },
    )
}

#[cfg(feature = "remote")]
async fn seed_live_remote_team_tables(
    db: Arc<storage::DatabaseManager>,
    team_users_table: &str,
    organizations_table: &str,
) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": team_users_table,
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
        .expect("create live remote team table request"),
    )
    .await
    .expect("create live remote team table");
    for (sk, name) in [("user#1", "Ava"), ("user#2", "Ben")] {
        handle_put_item(
            db.clone(),
            json!({
                "TableName": team_users_table,
                "Item": {
                    "pk": {"S": "tenant#1"},
                    "sk": {"S": sk},
                    "org_id": {"S": "org#1"},
                    "name": {"S": name}
                }
            })
            .try_into()
            .expect("put live remote team item request"),
        )
        .await
        .expect("put live remote team item");
    }
    handle_create_table(
        db.clone(),
        json!({
            "TableName": organizations_table,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create live remote org table request"),
    )
    .await
    .expect("create live remote org table");
    handle_put_item(
        db,
        json!({
            "TableName": organizations_table,
            "Item": {
                "pk": {"S": "org#1"},
                "name": {"S": "Live Remote Org"}
            }
        })
        .try_into()
        .expect("put live remote org item request"),
    )
    .await
    .expect("put live remote org item");
}

#[cfg(feature = "remote")]
async fn remote_describe_table_mock<'a>(
    server: &'a MockServer,
    table_name: &'static str,
    key_schema: &[(&'static str, &'static str, &'static str)],
) -> httpmock::Mock<'a> {
    let attribute_definitions = key_schema
        .iter()
        .map(|(attribute_name, attribute_type, _)| {
            json!({
                "AttributeName": attribute_name,
                "AttributeType": attribute_type
            })
        })
        .collect::<Vec<_>>();
    let key_schema = key_schema
        .iter()
        .map(|(attribute_name, _, key_type)| {
            json!({
                "AttributeName": attribute_name,
                "KeyType": key_type
            })
        })
        .collect::<Vec<_>>();

    server
        .mock_async(|when, then| {
            when.method(POST)
                .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                .body_includes(format!("\"TableName\":\"{table_name}\""))
                .path("/");
            then.status(200).json_body(json!({
                "Table": {
                    "TableName": table_name,
                    "TableStatus": "ACTIVE",
                    "CreationDateTime": 0,
                    "AttributeDefinitions": attribute_definitions,
                    "KeySchema": key_schema,
                    "TableArn": format!("arn:aws:dynamodb:local:table/{table_name}"),
                    "BillingModeSummary": {
                        "BillingMode": "PAY_PER_REQUEST"
                    },
                    "ItemCount": 0,
                    "TableSizeBytes": 0
                }
            }));
        })
        .await
}

async fn seed_user_tables(db: Arc<storage::DatabaseManager>) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "Users",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create Users table");
    handle_put_item(
        db,
        json!({
            "TableName": "Users",
            "Item": {
                "pk": {"S": "user#1"},
                "org_id": {"S": "org#1"},
                "name": {"S": "Ava"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put user item");
}

async fn seeded_user_app_state() -> Arc<AppState> {
    let db = create_test_db().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "Users",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create Users table");
    handle_put_item(
        db.clone(),
        json!({
            "TableName": "Users",
            "Item": {
                "pk": {"S": "user#1"},
                "org_id": {"S": "org#1"},
                "name": {"S": "Ava"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put user item");
    handle_put_item(
        db.clone(),
        json!({
            "TableName": "Users",
            "Item": {
                "pk": {"S": "user#2"},
                "org_id": {"S": "org#1"},
                "name": {"S": "Ben"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put second user item");
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "Organizations",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create Organizations table");
    handle_put_item(
        db.clone(),
        json!({
            "TableName": "Organizations",
            "Item": {
                "pk": {"S": "org#1"},
                "name": {"S": "Example Org"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put org item");
    seed_activity_table(db.clone()).await;
    app_state_with_db(db)
}

async fn seeded_team_app_state() -> Arc<AppState> {
    seeded_team_app_state_with_options(false).await
}

async fn seeded_team_app_state_with_related_sks() -> Arc<AppState> {
    seeded_team_app_state_with_options(true).await
}

async fn seeded_team_app_state_with_options(include_related_sks: bool) -> Arc<AppState> {
    let db = create_test_db().await;
    seed_team_tables(db.clone(), include_related_sks).await;
    app_state_with_db(db)
}

async fn seed_team_tables(db: Arc<storage::DatabaseManager>, include_related_sks: bool) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "TeamUsers",
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
        .expect("create table request"),
    )
    .await
    .expect("create TeamUsers table");
    for (sk, name) in [("user#1", "Ava"), ("user#2", "Ben")] {
        let mut item = json!({
            "pk": {"S": "tenant#1"},
            "sk": {"S": sk},
            "org_id": {"S": "org#1"},
            "name": {"S": name}
        });
        if include_related_sks && sk == "user#1" {
            item["related_sks"] = json!({"SS": ["user#1", "user#2"]});
        }
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "TeamUsers",
                "Item": item
            })
            .try_into()
            .expect("put item request"),
        )
        .await
        .expect("put team user item");
    }
    seed_org_table(db.clone()).await;
    seed_activity_table(db.clone()).await;
}

async fn seed_org_table(db: Arc<storage::DatabaseManager>) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "Organizations",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create Organizations table");
    handle_put_item(
        db,
        json!({
            "TableName": "Organizations",
            "Item": {
                "pk": {"S": "org#1"},
                "name": {"S": "Example Org"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put org item");
}

async fn seed_activity_table(db: Arc<storage::DatabaseManager>) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "OrgActivity",
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
        .expect("create table request"),
    )
    .await
    .expect("create OrgActivity table");
    for sk in ["activity#1", "activity#2"] {
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "OrgActivity",
                "Item": {
                    "pk": {"S": "org#1"},
                    "sk": {"S": sk}
                }
            })
            .try_into()
            .expect("put item request"),
        )
        .await
        .expect("put activity item");
    }
}

fn app_state_with_db(db: Arc<storage::DatabaseManager>) -> Arc<AppState> {
    app_state_with_db_and_options(db, StorageApiManagerOptions::default())
}

fn app_state_with_db_and_options(
    db: Arc<storage::DatabaseManager>,
    options: StorageApiManagerOptions,
) -> Arc<AppState> {
    Arc::new(AppState::new_with_manager_options(db, options))
}

async fn seed_orders_gsi_table(db: Arc<storage::DatabaseManager>) {
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "Orders",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "customer_id", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"}
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "by_customer",
                    "KeySchema": [
                        {"AttributeName": "customer_id", "KeyType": "HASH"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ]
        })
        .try_into()
        .expect("create table request"),
    )
    .await
    .expect("create Orders table");
    handle_put_item(
        db,
        json!({
            "TableName": "Orders",
            "Item": {
                "pk": {"S": "order#1"},
                "customer_id": {"S": "customer#1"}
            }
        })
        .try_into()
        .expect("put item request"),
    )
    .await
    .expect("put order item");
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
    match dynamodb_endpoint(State(app_state), headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn response_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response");
    serde_json::from_slice(&bytes).expect("json response")
}

fn assert_validation_error_type(payload: &serde_json::Value) {
    let error_type = payload["__type"].as_str().expect("error type");
    assert!(
        error_type.ends_with("ValidationException"),
        "unexpected error type: {error_type}"
    );
}
