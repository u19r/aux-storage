use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage::{AdmissionClass, DatabaseManager};
#[cfg(feature = "sqlite")]
use storage_types::SystemStreamCheckpointToken;
use storage_types::{BillingMode, KeyAttributeType, KeyType};
use tower::ServiceExt as _;

#[cfg(feature = "sqlite")]
use crate::routes::routes_tests::create_sqlite_test_db;
use crate::{
    routes::{dynamodb::dynamodb_endpoint, internal, routes_tests::for_each_conformance_backend},
    types::AppState,
};

async fn system_scope_json(db: &Arc<DatabaseManager>) -> serde_json::Value {
    let scope = db
        .get_system_stream_scope_for_control()
        .await
        .expect("initialized store owns a persisted logical SystemStream scope");
    json!({
        "Version": scope.version(),
        "ScopeId": scope.scope_id().as_str(),
        "Generation": scope.generation(),
    })
}

async fn system_split_json(db: &Arc<DatabaseManager>) -> serde_json::Value {
    let scope = db
        .get_system_stream_scope_for_control()
        .await
        .expect("initialized store owns a persisted logical SystemStream scope");
    let catalog = db
        .run_stream_admitted("default", AdmissionClass::RangeRead, |stream| {
            let scope = scope.clone();
            async move {
                stream
                    .system_stream_lineage_catalog(scope)
                    .await
                    .map_err(|error| storage_types::StorageError::from(error.into_storage_enum()))
            }
        })
        .await
        .expect("initialized store exposes its logical SystemStream catalog");
    let parent = catalog
        .iter()
        .find(|lineage| !lineage.sealed())
        .expect("SQLite owns an active SystemStream lineage");
    json!({
        "SourceScope": {
            "Version": scope.version(),
            "ScopeId": scope.scope_id().as_str(),
            "Generation": scope.generation(),
        },
        "OperationId": "storage-api-system-stream-split",
        "Parent": {
            "ShardId": parent.shard_id().as_str(),
            "LineageId": parent.lineage_id().as_str(),
            "ParentLineageId": parent
                .parent_lineage_id()
                .map(|lineage_id| lineage_id.as_str().to_owned()),
            "Sealed": parent.sealed(),
        },
        "Children": [
            {
                "Lineage": {
                    "ShardId": "storage-api-system-stream-left",
                    "LineageId": "storage-api-system-stream-left-lineage",
                    "ParentLineageId": parent.lineage_id().as_str(),
                    "Sealed": false,
                },
                "InitialSequence": 1,
            },
            {
                "Lineage": {
                    "ShardId": "storage-api-system-stream-right",
                    "LineageId": "storage-api-system-stream-right-lineage",
                    "ParentLineageId": parent.lineage_id().as_str(),
                    "Sealed": false,
                },
                "InitialSequence": 1,
            },
        ],
    })
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_split_retry_with_same_logical_request_replays() {
    let db = create_sqlite_test_db().await;
    let request = Bytes::from(system_split_json(&db).await.to_string());
    let app_state = Arc::new(AppState::new(db));

    let first = internal::split_system_stream_endpoint(State(app_state.clone()), request.clone())
        .await
        .expect("first split should apply");
    let retry = internal::split_system_stream_endpoint(State(app_state), request)
        .await
        .expect("same split request should replay");

    assert_eq!(first.0["Disposition"], "Applied");
    assert_eq!(retry.0["Disposition"], "Replayed");
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
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));
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
    })
    .await;
}

#[tokio::test]
async fn internal_ttl_sweep_route_requires_a_direct_table_target() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

        let error = internal::run_background_job_endpoint(
            State(app_state),
            Path("ttl-sweep".to_string()),
            Bytes::from(
                json!({
                    "TTLAttribute": "expires_at",
                    "ScanTime": 1_700_000_000_000_i64,
                    "Limit": 10,
                })
                .to_string(),
            ),
        )
        .await
        .expect_err("ttl sweep without a target must fail closed");

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(error.1.0.message.contains("TableName"));
    })
    .await;
}

#[tokio::test]
async fn internal_background_job_route_rejects_job_name_only_without_provider_effects() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

        let error = internal::run_background_job_endpoint(
            State(app_state.clone()),
            Path("gsi-update".to_string()),
            Bytes::from_static(b"{}"),
        )
        .await
        .expect_err("job-name-only dispatch must be rejected");

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(error.1.0.message.contains("invalid background job payload"));
    })
    .await;
}

#[tokio::test]
async fn internal_ttl_sweep_route_rejects_missing_body_before_provider_effects() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

        let error = internal::run_background_job_endpoint(
            State(app_state),
            Path("ttl-sweep".to_string()),
            Bytes::new(),
        )
        .await
        .expect_err("ttl sweep without a scoped body must fail closed");

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(error.1.0.message.contains("body is required"));
    })
    .await;
}

#[tokio::test]
async fn internal_ttl_sweep_route_rejects_non_object_body_before_provider_effects() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

        let error = internal::run_background_job_endpoint(
            State(app_state),
            Path("ttl-sweep".to_string()),
            Bytes::from_static(b"[]"),
        )
        .await
        .expect_err("non-object ttl sweep body must fail closed");

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(error.1.0.message.contains("JSON object"));
    })
    .await;
}

#[tokio::test]
async fn internal_clear_all_tables_route_clears_existing_tables() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

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

        let clear_response = crate::router::internal_helper_router(app_state.clone())
            .oneshot(
                axum::http::Request::post("/_internal/test/clear-all-tables")
                    .body(body::Body::empty())
                    .expect("clear request"),
            )
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
    })
    .await;
}

#[tokio::test]
async fn append_table_stream_record_route_accepts_payload() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-target",
            HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
        );
        let create_response = dynamodb_endpoint(
            State(app_state.clone()),
            headers,
            Bytes::from(create_stream_table_body("orders").to_string()),
        )
        .await
        .expect("create stream-enabled table");
        assert_eq!(create_response.status(), StatusCode::OK);

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
    })
    .await;
}

#[tokio::test]
async fn internal_stream_reader_returns_records_from_real_table_writes() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

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
    })
    .await;
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_without_source_scope_is_rejected_before_backend_reads() {
    let db = create_sqlite_test_db().await;
    let app_state = Arc::new(AppState::new(db));
    let error = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(json!({"SystemStream": true, "Limit": 1}).to_string()),
    )
    .await
    .expect_err("missing source scope must fail before the backend is read");
    let (status, error) = error;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(error.0.message, "SystemStream SourceScope is required");
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_rejects_legacy_raw_resume_before_backend_reads() {
    let db = create_sqlite_test_db().await;
    let app_state = Arc::new(AppState::new(db));
    let error = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "SystemStream": true,
                "SourceScope": {
                    "Version": 1,
                    "ScopeId": "catalog",
                    "Generation": 1,
                },
                "LastEvaluatedKey": "legacy-raw-token",
            })
            .to_string(),
        ),
    )
    .await
    .expect_err("legacy raw resume must fail before the backend is read");
    let (status, error) = error;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(
        error.0.message,
        "LastEvaluatedKey is not supported for SystemStream"
    );
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_with_valid_scope_returns_empty_terminal_page() {
    let db = create_sqlite_test_db().await;
    let source_scope = system_scope_json(&db).await;
    let app_state = Arc::new(AppState::new(db));
    let response = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "SystemStream": true,
                "SourceScope": source_scope,
                "Limit": 10,
            })
            .to_string(),
        ),
    )
    .await
    .expect("SQLite owns the logical lineage page");
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["Records"], serde_json::json!([]));
    assert_eq!(json["SourceScope"], source_scope);
    storage_types::SystemStreamCheckpointToken::try_from(
        json["Checkpoint"].as_str().expect("resumable empty page"),
    )
    .expect("valid opaque checkpoint");
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_reads_sqlite_logical_records_and_resumes_with_checkpoint() {
    let db = create_sqlite_test_db().await;
    let source_scope = system_scope_json(&db).await;
    let app_state = Arc::new(AppState::new(db));

    let mut create_headers = HeaderMap::new();
    create_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.CreateTable"),
    );
    let create_response = dynamodb_endpoint(
        State(app_state.clone()),
        create_headers,
        Bytes::from(create_stream_table_body("system-orders").to_string()),
    )
    .await
    .expect("create response");
    assert_eq!(create_response.status(), StatusCode::OK);

    let mut put_headers = HeaderMap::new();
    put_headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.PutItem"),
    );
    for order_id in ["order#1", "order#2"] {
        let put_response = dynamodb_endpoint(
            State(app_state.clone()),
            put_headers.clone(),
            Bytes::from(
                json!({
                    "TableName": "system-orders",
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

    let first_response = internal::get_stream_records_endpoint(
        State(app_state.clone()),
        Bytes::from(
            json!({
                "SystemStream": true,
                "SourceScope": source_scope.clone(),
                "Limit": 1,
            })
            .to_string(),
        ),
    )
    .await
    .expect("first logical page");
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_body = body::to_bytes(first_response.into_body(), usize::MAX)
        .await
        .expect("first body");
    let first_json: serde_json::Value = serde_json::from_slice(&first_body).expect("first json");
    assert_eq!(first_json["Records"].as_array().map(Vec::len), Some(1));
    assert_eq!(first_json["Records"][0]["Keys"]["pk"]["S"], "order#1");
    assert!(first_json["Records"][0]["SystemPosition"].is_object());
    let checkpoint = first_json["Checkpoint"]
        .as_str()
        .expect("issued checkpoint")
        .to_string();

    let second_response = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "SystemStream": true,
                "SourceScope": source_scope,
                "Checkpoint": checkpoint,
                "Limit": 1,
            })
            .to_string(),
        ),
    )
    .await
    .expect("resumed logical page");
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_body = body::to_bytes(second_response.into_body(), usize::MAX)
        .await
        .expect("second body");
    let second_json: serde_json::Value = serde_json::from_slice(&second_body).expect("second json");
    assert_eq!(second_json["Records"].as_array().map(Vec::len), Some(1));
    assert_eq!(second_json["Records"][0]["Keys"]["pk"]["S"], "order#2");
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn system_stream_rejects_forged_well_formed_checkpoint_before_backend_reads() {
    let db = create_sqlite_test_db().await;
    let source_scope = system_scope_json(&db).await;
    let app_state = Arc::new(AppState::new(db));
    let forged = SystemStreamCheckpointToken::try_from(format!("v1.{}", "43".repeat(16)))
        .expect("syntactic token");
    let error = internal::get_stream_records_endpoint(
        State(app_state),
        Bytes::from(
            json!({
                "SystemStream": true,
                "SourceScope": source_scope,
                "Checkpoint": forged.as_str(),
            })
            .to_string(),
        ),
    )
    .await
    .expect_err("unknown checkpoint must not reach a provider read");
    let (status, error) = error;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.0.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(
        error.0.message,
        "SystemStream checkpoint token is not issued for this source"
    );
}

#[tokio::test]
async fn internal_stream_reader_omits_last_evaluated_key_on_final_non_empty_page() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

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
    })
    .await;
}

#[tokio::test]
async fn public_dispatch_returns_stream_records_from_real_table_writes() {
    for_each_conformance_backend(|_, db| async move {
        let app_state = Arc::new(AppState::new(db));

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
    })
    .await;
}
