use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage_types::{BillingMode, KeyAttributeType, KeyType, TableName};

use crate::{
    constants::STORAGE_GATEWAY_API_KEY_HEADER,
    manager::StorageApiManagerOptions,
    routes::{dynamodb::dynamodb_endpoint, internal, routes_support_tests::create_test_db},
    types::AppState,
};

const REPLICATION_SERVICE_TOKEN: &str = "replication-service-token";

fn replication_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static(REPLICATION_SERVICE_TOKEN),
    );
    headers
}

fn create_table_body(table_name: &str) -> serde_json::Value {
    serde_json::json!({
        "TableName": table_name,
        "AttributeDefinitions": [
            {
                "AttributeName": "pk",
                "AttributeType": KeyAttributeType::S,
            }
        ],
        "KeySchema": [
            {
                "AttributeName": "pk",
                "KeyType": KeyType::Hash,
            }
        ],
        "BillingMode": BillingMode::PayPerRequest,
    })
}

fn create_stream_table_body(table_name: &str) -> serde_json::Value {
    serde_json::json!({
        "TableName": table_name,
        "AttributeDefinitions": [
            {
                "AttributeName": "pk",
                "AttributeType": KeyAttributeType::S,
            }
        ],
        "KeySchema": [
            {
                "AttributeName": "pk",
                "KeyType": KeyType::Hash,
            }
        ],
        "BillingMode": BillingMode::PayPerRequest,
        "StreamSpecification": {
            "StreamEnabled": true,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        }
    })
}

#[tokio::test]
async fn public_dispatch_rejects_internal_helper_targets() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("TestUtil.RunBackgroundJob"),
    );

    let error = dynamodb_endpoint(
        State(app_state),
        headers,
        Bytes::from(json!({"JobName": "gsi-update"}).to_string()),
    )
    .await
    .expect_err("request should fail");

    let (status, _headers, error) = error.into_parts();
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(
        error.0.message,
        "Unknown operation: TestUtil.RunBackgroundJob"
    );
}

#[tokio::test]
async fn internal_clear_all_tables_route_clears_existing_tables() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_table_body("clear-me").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let clear_response = internal::clear_all_tables_endpoint(State(app_state.clone()))
        .await
        .expect("clear response");
    assert_eq!(clear_response.status(), StatusCode::OK);

    let mut list_headers = HeaderMap::new();
    list_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.ListTables"),
    );
    let list_response = dynamodb_endpoint(State(app_state), list_headers, Bytes::from("{}"))
        .await
        .expect("list response");
    assert_eq!(list_response.status(), StatusCode::OK);
    let body = body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let table_names = json["TableNames"].as_array().expect("table names array");
    assert!(table_names.is_empty(), "expected no tables after clear");
}

#[tokio::test]
async fn append_table_stream_record_route_accepts_payload() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let append_response = internal::append_table_stream_record_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "TableName": "orders",
                "Data": "stream-payload"
            })
            .to_string(),
        ),
    )
    .await
    .expect("append response");
    assert_eq!(append_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn internal_stream_reader_returns_records_from_real_table_writes() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(
            json!({
                "TableName": "orders",
                "AttributeDefinitions": [
                    {
                        "AttributeName": "pk",
                        "AttributeType": KeyAttributeType::S,
                    }
                ],
                "KeySchema": [
                    {
                        "AttributeName": "pk",
                        "KeyType": KeyType::Hash,
                    }
                ],
                "BillingMode": BillingMode::PayPerRequest,
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            })
            .to_string(),
        ),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let mut put_headers = HeaderMap::new();
    put_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.PutItem"),
    );
    let put_response = dynamodb_endpoint(
        State(app_state.clone()),
        put_headers,
        Bytes::from(
            json!({
                "TableName": "orders",
                "Item": {
                    "pk": { "S": "order#1" },
                    "status": { "S": "pending" }
                }
            })
            .to_string(),
        ),
    )
    .await
    .expect("put response");
    assert_eq!(put_response.status(), StatusCode::OK);

    let background_job_response = internal::run_background_job_endpoint(
        State(app_state.clone()),
        Path("gsi-update".to_string()),
    )
    .await
    .expect("background job response");
    assert_eq!(background_job_response.status(), StatusCode::OK);

    let get_response = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "TableName": "orders",
                "Limit": 10
            })
            .to_string(),
        ),
    )
    .await
    .expect("get stream response");
    assert_eq!(get_response.status(), StatusCode::OK);
    let body = body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let records = json["Records"].as_array().expect("records array");
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn given_mixed_table_writes_when_system_stream_is_read_then_records_resume_globally() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
    for table_name in ["system-orders", "system-users"] {
        let mut create_headers = HeaderMap::new();
        create_headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
        );
        dynamodb_endpoint(
            State(app_state.clone()),
            create_headers,
            Bytes::from(create_stream_table_body(table_name).to_string()),
        )
        .await
        .expect("create response");

        let mut put_headers = HeaderMap::new();
        put_headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.PutItem"),
        );
        dynamodb_endpoint(
            State(app_state.clone()),
            put_headers,
            Bytes::from(
                json!({
                    "TableName": table_name,
                    "Item": {
                        "pk": { "S": format!("{table_name}#1") },
                        "status": { "S": "pending" }
                    }
                })
                .to_string(),
            ),
        )
        .await
        .expect("put response");
    }

    let first = internal::get_stream_records_endpoint(
        State(app_state.clone()),
        Bytes::from(json!({"SystemStream": true, "Limit": 1}).to_string()),
    )
    .await
    .expect("first system stream response");
    let first_body = body::to_bytes(first.into_body(), usize::MAX)
        .await
        .expect("first body");
    let first_json: serde_json::Value =
        serde_json::from_slice(&first_body).expect("first response json");
    assert_eq!(first_json["Records"].as_array().expect("records").len(), 1);
    let cursor = first_json["LastEvaluatedKey"]
        .as_str()
        .expect("continuation cursor");

    let second = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "SystemStream": true,
                "LastEvaluatedKey": cursor,
                "Limit": 8192
            })
            .to_string(),
        ),
    )
    .await
    .expect("second system stream response");
    let second_body = body::to_bytes(second.into_body(), usize::MAX)
        .await
        .expect("second body");
    let second_json: serde_json::Value =
        serde_json::from_slice(&second_body).expect("second response json");
    let second_records = second_json["Records"].as_array().expect("records");
    assert_eq!(second_records.len(), 1);
    assert!(second_records[0]["SourceTableName"].is_string());
    assert!(second_records[0]["Keys"]["pk"]["S"].is_string());
    assert!(second_json.get("LastEvaluatedKey").is_none());
}

#[tokio::test]
async fn internal_stream_reader_omits_last_evaluated_key_on_final_non_empty_page() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_stream_table_body("paged-orders").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let mut put_headers = HeaderMap::new();
    put_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.PutItem"),
    );

    for order_id in ["order#1", "order#2", "order#3"] {
        let put_response = dynamodb_endpoint(
            State(app_state.clone()),
            put_headers.clone(),
            Bytes::from(
                json!({
                    "TableName": "paged-orders",
                    "Item": {
                        "pk": { "S": order_id },
                        "status": { "S": "pending" }
                    }
                })
                .to_string(),
            ),
        )
        .await
        .expect("put response");
        assert_eq!(put_response.status(), StatusCode::OK);
    }

    let background_job_response = internal::run_background_job_endpoint(
        State(app_state.clone()),
        Path("gsi-update".to_string()),
    )
    .await
    .expect("background job response");
    assert_eq!(background_job_response.status(), StatusCode::OK);

    let first_response = internal::get_stream_records_endpoint(
        State(app_state.clone()),
        Bytes::from(
            json!({
                "TableName": "paged-orders",
                "Limit": 2
            })
            .to_string(),
        ),
    )
    .await
    .expect("first stream response");
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_body = body::to_bytes(first_response.into_body(), usize::MAX)
        .await
        .expect("first body");
    let first_json: serde_json::Value =
        serde_json::from_slice(&first_body).expect("first response json");
    let first_records = first_json["Records"].as_array().expect("first records");
    assert_eq!(first_records.len(), 2);
    let first_lek = first_json["LastEvaluatedKey"]
        .as_str()
        .expect("first page token")
        .to_string();

    let second_response = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "TableName": "paged-orders",
                "Limit": 2,
                "LastEvaluatedKey": first_lek
            })
            .to_string(),
        ),
    )
    .await
    .expect("second stream response");
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_body = body::to_bytes(second_response.into_body(), usize::MAX)
        .await
        .expect("second body");
    let second_json: serde_json::Value =
        serde_json::from_slice(&second_body).expect("second response json");
    let second_records = second_json["Records"].as_array().expect("second records");
    assert_eq!(second_records.len(), 1);
    assert!(
        second_json["LastEvaluatedKey"].is_null(),
        "final non-empty page should not emit a token"
    );
}

#[tokio::test]
async fn public_dispatch_returns_stream_records_from_real_table_writes() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_stream_table_body("public-orders").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let mut put_headers = HeaderMap::new();
    put_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.PutItem"),
    );
    let put_response = dynamodb_endpoint(
        State(app_state.clone()),
        put_headers,
        Bytes::from(
            json!({
                "TableName": "public-orders",
                "Item": {
                    "pk": { "S": "order#1" },
                    "status": { "S": "pending" }
                }
            })
            .to_string(),
        ),
    )
    .await
    .expect("put response");
    assert_eq!(put_response.status(), StatusCode::OK);

    let background_job_response = internal::run_background_job_endpoint(
        State(app_state.clone()),
        Path("gsi-update".to_string()),
    )
    .await
    .expect("background job response");
    assert_eq!(background_job_response.status(), StatusCode::OK);

    let mut stream_headers = HeaderMap::new();
    stream_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.GetStreamRecords"),
    );
    let stream_response = dynamodb_endpoint(
        State(app_state),
        stream_headers,
        Bytes::from(
            json!({
                "TableName": "public-orders",
                "Limit": 10
            })
            .to_string(),
        ),
    )
    .await
    .expect("get stream response");
    assert_eq!(stream_response.status(), StatusCode::OK);
    let body = body::to_bytes(stream_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let records = json["Records"].as_array().expect("records array");
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn replication_internal_routes_require_configured_service_token() {
    let db = create_test_db().await;
    let app_state = Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ));
    let body = Bytes::from("{}");

    let (status, error) = internal::apply_replication_endpoint(
        State(app_state.clone()),
        HeaderMap::new(),
        body.clone(),
    )
    .await
    .expect_err("missing token config should reject");
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error.0.error_type, "AccessDeniedException");

    let app_state = Arc::new(
        AppState::new_with_manager_options(
            app_state.db_manager.clone(),
            StorageApiManagerOptions::default(),
        )
        .with_replication_service_tokens([REPLICATION_SERVICE_TOKEN]),
    );
    let (status, error) = internal::apply_replication_endpoint(
        State(app_state.clone()),
        HeaderMap::new(),
        body.clone(),
    )
    .await
    .expect_err("missing header should reject");
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error.0.error_type, "UnrecognizedClientException");

    let mut sync_headers = HeaderMap::new();
    sync_headers.insert(
        STORAGE_GATEWAY_API_KEY_HEADER,
        HeaderValue::from_static("sync-secret"),
    );
    let (status, error) =
        internal::replication_heartbeat_endpoint(State(app_state), sync_headers, body)
            .await
            .expect_err("sync token should not authorize replication endpoint");
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error.0.error_type, "AccessDeniedException");
}

#[tokio::test]
async fn replication_apply_route_accepts_valid_mutation_batches() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(db.clone(), StorageApiManagerOptions::default())
            .with_replication_service_tokens([REPLICATION_SERVICE_TOKEN]),
    );

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_stream_table_body("replication-orders").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let apply_response = internal::apply_replication_endpoint(
        State(app_state.clone()),
        replication_headers(),
        Bytes::from(
            json!({
                "SourceRegion": "us-east-1",
                "Mutations": [
                    {
                        "TableName": "replication-orders",
                        "Key": {
                            "pk": { "S": "order#1" }
                        },
                        "NewImage": {
                            "pk": { "S": "order#1" },
                            "status": { "S": "pending" }
                        },
                        "Metadata": {
                            "origin_region": "us-east-1",
                            "origin_sequence": "000000000000000000000001",
                            "origin_hlc": {
                                "physical_ms": 2000000000000_i64,
                                "logical": 0
                            },
                            "origin_commit_ts": 2000000000000_i64,
                            "table_replica_epoch": 1,
                            "write_source": "replicated"
                        }
                    }
                ]
            })
            .to_string(),
        ),
    )
    .await
    .expect("apply response");
    assert_eq!(apply_response.status(), StatusCode::OK);
    let body = body::to_bytes(apply_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["ReceivedMutations"], 1);
    assert_eq!(json["AppliedMutations"], 1);
    assert_eq!(json["SkippedMutations"], 0);

    let item = db
        .get_item_map(
            TableName::new("replication-orders"),
            std::collections::HashMap::from([(
                "pk".to_string(),
                storage_types::AttributeValue::S("order#1".to_string()),
            )]),
        )
        .await
        .expect("load item");
    assert!(item.is_some(), "replicated mutation should write item");
}

#[tokio::test]
async fn replication_apply_route_rejects_origin_region_mismatch() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(db, StorageApiManagerOptions::default())
            .with_replication_service_tokens([REPLICATION_SERVICE_TOKEN]),
    );

    let error = internal::apply_replication_endpoint(
        State(app_state),
        replication_headers(),
        Bytes::from(
            json!({
                "SourceRegion": "us-east-1",
                "Mutations": [
                    {
                        "TableName": "orders",
                        "Key": {
                            "pk": { "S": "order#1" }
                        },
                        "Metadata": {
                            "origin_region": "eu-west-1",
                            "origin_sequence": "000000000000000000000001",
                            "origin_hlc": {
                                "physical_ms": 2000000000000_i64,
                                "logical": 0
                            },
                            "origin_commit_ts": 2000000000000_i64,
                            "table_replica_epoch": 1,
                            "write_source": "replicated"
                        }
                    }
                ]
            })
            .to_string(),
        ),
    )
    .await
    .expect_err("mismatched source region should fail");

    let (status, _error) = error;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn replication_logical_backfill_import_route_imports_valid_chunk() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(
            db,
            StorageApiManagerOptions {
                self_region: Some("region-a".to_string()),
                ..StorageApiManagerOptions::default()
            },
        )
        .with_replication_service_tokens([REPLICATION_SERVICE_TOKEN]),
    );

    let response = internal::import_replication_logical_backfill_endpoint(
        State(app_state),
        replication_headers(),
        Bytes::from(
            json!({
                "SourceRegion": "region-a",
                "Manifest": {
                    "id": "bootstrap-1",
                    "caller": "multi_region_bootstrap",
                    "activation_gate": "replica_activation_cursor",
                    "conflict_policy": "item_stream_version_only",
                    "tombstone_cleanup": "after_final_catchup_drain",
                    "source_backend": "sqlite",
                    "destination_backend": "sqlite",
                    "domains": ["item_records"],
                    "protected_stream_cursor": null,
                    "source_log_boundary": null,
                    "chunks": [{
                        "id": "chunk-1",
                        "domain": "item_records",
                        "record_count": 0,
                        "checksum": "empty"
                    }]
                },
                "Chunk": {
                    "summary": {
                        "id": "chunk-1",
                        "domain": "item_records",
                        "record_count": 0,
                        "checksum": "empty"
                    },
                    "records": []
                }
            })
            .to_string(),
        ),
    )
    .await
    .expect("logical import response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["Result"], "chunk_imported");
}

#[tokio::test]
async fn replication_heartbeat_and_health_routes_return_internal_shapes() {
    let db = create_test_db().await;
    let app_state = Arc::new(
        AppState::new_with_manager_options(
            db,
            StorageApiManagerOptions {
                self_region: Some("region-a".to_string()),
                ..StorageApiManagerOptions::default()
            },
        )
        .with_replication_service_tokens([REPLICATION_SERVICE_TOKEN]),
    );

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_stream_table_body("health-table").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let heartbeat_response = internal::replication_heartbeat_endpoint(
        State(app_state.clone()),
        replication_headers(),
        Bytes::from(
            json!({
                "SourceRegion": "us-east-1",
                "SentAt": 2000000000000_i64,
                "SourceLatestCommitTs": 2000000000000_i64
            })
            .to_string(),
        ),
    )
    .await
    .expect("heartbeat response");
    assert_eq!(heartbeat_response.status(), StatusCode::OK);

    let apply_response = internal::apply_replication_endpoint(
        State(app_state.clone()),
        replication_headers(),
        Bytes::from(
            json!({
                "SourceRegion": "us-east-1",
                "Mutations": [
                    {
                        "TableName": "health-table",
                        "Key": {
                            "pk": { "S": "item#1" }
                        },
                        "NewImage": {
                            "pk": { "S": "item#1" },
                            "status": { "S": "replicated" }
                        },
                        "Metadata": {
                            "origin_region": "us-east-1",
                            "origin_sequence": "000000000000000000000001",
                            "origin_hlc": {
                                "physical_ms": 2000000000000_i64,
                                "logical": 0
                            },
                            "origin_commit_ts": 2000000000000_i64,
                            "table_replica_epoch": 1,
                            "write_source": "replicated"
                        }
                    }
                ]
            })
            .to_string(),
        ),
    )
    .await
    .expect("apply response");
    assert_eq!(apply_response.status(), StatusCode::OK);

    let health_response =
        internal::replication_health_endpoint(State(app_state), replication_headers())
            .await
            .expect("health response");
    assert_eq!(health_response.status(), StatusCode::OK);
    let body = body::to_bytes(health_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["SelfRegion"], "region-a");
    let peers = json["Peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0]["RegionName"], "us-east-1");
    assert_eq!(peers[0]["Healthy"], true);
    assert!(peers[0]["LastHeartbeatAt"].is_number());
    assert!(peers[0]["HeartbeatStalenessMs"].is_number());
    assert_eq!(peers[0]["SourceLatestCommitTs"], 2000000000000_i64);
    assert!(peers[0]["LastReceivedCommitTs"].is_number());
    assert_eq!(peers[0]["ReplicationLagMs"], 0);
}
