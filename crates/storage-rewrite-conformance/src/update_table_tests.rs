//! Customer-visible UpdateTable rules retained from the legacy provider suites.

use std::{
    num::NonZeroU64,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use scheduled_bg_jobs::{JobId, Namespace};
use serde_json::json;
use storage_backfill::GsiPageLimit;
use storage_observability_test_support::observation::observe;
use storage_provider::maintenance::GsiBuildRequest;
use storage_types::{
    CreateTableRequest, DeleteTableRequest, DescribeTableRequest, GetItemRequest, PutItemRequest,
    QueryRequest, TimestampMillis, UpdateItemRequest, UpdateTableRequest,
    context::WrappedError as _, maintenance::StepResult,
};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

async fn complete_gsi_build(store: &TestStore, request: &GsiBuildRequest) {
    for _ in 0..32 {
        let step = observe(
            store
                .gsi_build_repository
                .advance_gsi_build(request.clone()),
        )
        .await;
        step.assert_no_warning_or_error();
        match step
            .result
            .unwrap_or_else(|error| panic!("advance GSI build: {:?}", error.to_enum()))
        {
            StepResult::Advanced(()) | StepResult::Superseded => {}
            StepResult::Complete => return,
        }
    }
    panic!("GSI build must complete within 32 bounded steps");
}

// Validation and negative rules: a malformed GSI action blocks the entire
// update at request parsing.
#[tokio::test]
async fn given_malformed_gsi_actions_when_update_table_is_parsed_then_protection_and_catalog_remain_unchanged()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let before = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .expect("describe table before malformed update");
        let before = serde_json::to_value(before).expect("serialize original description");

        for action in [
            json!({}),
            json!({
                "Update": {"IndexName": "ByGroup"},
                "Delete": {"IndexName": "ByGroup"}
            }),
        ] {
            let parsed = observe(async {
                UpdateTableRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "DeletionProtectionEnabled": true,
                    "GlobalSecondaryIndexUpdates": [action]
                }))
            })
            .await;
            parsed.assert_no_warning_or_error();
            parsed.assert_database_calls_exact(&[]);
            assert_eq!(
                parsed
                    .result
                    .expect_err("malformed GSI action must reject the whole update"),
                "GlobalSecondaryIndexUpdates[0] must contain exactly one of Create, Update, or \
                 Delete"
            );
            let after = store
                .operations
                .describe_table(DescribeTableRequest {
                    table_name: table.clone(),
                })
                .await
                .expect("describe table after rejected update");
            assert_eq!(serde_json::to_value(after).unwrap(), before);
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and negative rules: protection appears in responses and blocks
// deletion until disabled.
#[tokio::test]
async fn given_existing_table_when_protection_is_enabled_then_delete_fails_until_disabled() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;

        let enable = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "DeletionProtectionEnabled": true
        })).expect("valid protection update");
        let updated = observe(store.operations.update_table(enable)).await;
        updated.assert_no_warning_or_error();
        assert!(updated.result.expect("enable protection").table_description.deletion_protection_enabled);

        let describe = DescribeTableRequest::try_from(json!({"TableName": table.as_ref()}))
            .expect("valid describe request");
        let described = store.operations.describe_table(describe)
            .await.expect("describe protected table");
        assert!(described.table.deletion_protection_enabled);

        let delete = DeleteTableRequest::try_from(json!({"TableName": table.as_ref()}))
            .expect("valid delete request");
        let rejected = observe(store.operations.delete_table(delete)).await;
        rejected.assert_no_warning_or_error();
        let error = rejected.result.expect_err("protected table must reject deletion");
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(status.as_u16(), 400);
        assert_eq!(serde_json::to_value(body).unwrap(), json!({
            "__type": "com.amazon.coral.validate#ValidationException",
            "message": "Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first."
        }));

        let disable = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "DeletionProtectionEnabled": false
        })).expect("valid protection update");
        let updated = observe(store.operations.update_table(disable)).await;
        updated.assert_no_warning_or_error();
        assert!(!updated.result.expect("disable protection").table_description.deletion_protection_enabled);

        let delete = DeleteTableRequest::try_from(json!({"TableName": table.as_ref()}))
            .expect("valid delete request");
        store.operations.delete_table(delete).await.expect("unprotected table deletes");
        let describe = DescribeTableRequest::try_from(json!({"TableName": table.as_ref()}))
            .expect("valid describe request");
        assert!(store.operations.describe_table(describe).await.is_err());
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule: ordered-indexer capacity may increase, but a lower value is
// rejected.
#[tokio::test]
async fn given_indexer_capacity_when_increased_then_decrease_is_rejected_without_changing_it() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 2, "AuxStreamDurationHours": 1
        })).expect("valid table capacity");
        let created = store.operations.create_table(create).await.expect("create capacity table");
        assert_eq!(created.table_description.max_indexers.get(), 2);

        let increase = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "MaxIndexers": 4
        })).expect("valid capacity increase");
        let increased = observe(store.operations.update_table(increase)).await;
        increased.assert_no_warning_or_error();
        assert_eq!(increased.result.expect("capacity increases").table_description.max_indexers.get(), 4);

        let describe_request = DescribeTableRequest::try_from(json!({"TableName": table.as_ref()})).unwrap();
        let before = store.operations.describe_table(describe_request.clone()).await.unwrap();
        let due_at = store.now() + 8 * 60 * 60 * 1000;
        let markers = store.stream_retention_repository.list_due_stream_trim_markers(due_at, 100).await.unwrap();
        let scopes = markers.iter().map(|marker| marker.scope.clone()).collect::<Vec<_>>();
        let states = store.stream_retention_repository.load_stream_trim_states(&scopes).await.unwrap();
        let decrease = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "MaxIndexers": 1, "DeletionProtectionEnabled": true, "AuxStreamDurationHours": 7,
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "rejected-index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        })).expect("valid decrease request shape");
        let rejected = observe(store.operations.update_table(decrease)).await;
        rejected.assert_no_warning_or_error();
        let error = rejected.result.expect_err("capacity cannot decrease");
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(status.as_u16(), 400);
        assert_eq!(serde_json::to_value(body).unwrap(), json!({
            "__type": "com.amazon.coral.validate#ValidationException",
            "message": "MaxIndexers:cannot_decrease"
        }));

        let describe = DescribeTableRequest::try_from(json!({"TableName": table.as_ref()}))
            .expect("valid describe request");
        let described = store.operations.describe_table(describe)
            .await.expect("describe capacity after rejection");
        assert_eq!(serde_json::to_value(described).unwrap(), serde_json::to_value(before).unwrap());
        assert!(store.stream_retention_repository.load_stream_trim_states(&scopes).await.unwrap() == states, "rejected update changed retention states");
        assert!(store.stream_retention_repository.list_due_stream_trim_markers(due_at, 100).await.unwrap() == markers, "rejected update changed due markers");
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: a newly added GSI remains visibly backfilling until it can
// serve queries.
#[tokio::test]
async fn given_existing_table_when_gsi_is_added_then_describe_preserves_creating_status() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;

        let request = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "gpk", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        })).expect("valid GSI creation request");
        let updated = observe(store.operations.update_table(request)).await;
        updated.assert_no_warning_or_error();
        let updated = updated.result.expect("add GSI").table_description;
        let index = &updated.global_secondary_indexes.as_ref().expect("GSI in update response")[0];
        assert_eq!(serde_json::to_value(index).unwrap()["IndexStatus"], "CREATING");
        assert_eq!(index.backfilling, Some(true));

        let described = store.operations.describe_table(DescribeTableRequest {
            table_name: table.clone(),
        }).await.expect("describe building GSI");
        assert_eq!(
            serde_json::to_value(described.table.global_secondary_indexes).unwrap(),
            serde_json::to_value(updated.global_secondary_indexes).unwrap(),
            "DescribeTable must report the same durable GSI readiness as UpdateTable",
        );

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "group-1"}}
        })).expect("valid GSI Query request");
        let rejected = observe(store.operations.query(query)).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.expect_err("building GSI must reject queries"),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"Global secondary index ByGroup is not ready for queries"}"#,
        );
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and negative rules: deleting a building GSI removes access without
// deleting base items.
#[tokio::test]
async fn given_building_gsi_when_index_is_deleted_then_base_items_remain_and_index_is_rejected() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "gpk": {"S": "group-1"}}
        })).expect("valid base item");
        store.operations.put_item(put).await.expect("put base item");

        let create = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "gpk", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        })).expect("valid GSI creation request");
        let created = observe(store.operations.update_table(create)).await;
        created.assert_no_warning_or_error();
        let indexes = created.result.expect("start GSI build").table_description
            .global_secondary_indexes.expect("building GSI in update response");
        assert_eq!(indexes[0].backfilling, Some(true));

        let delete = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "ByGroup"}}]
        })).expect("valid GSI deletion request");
        let deleted = observe(store.operations.update_table(delete)).await;
        deleted.assert_no_warning_or_error();
        let updated = deleted.result.expect("delete GSI").table_description;
        assert!(updated.global_secondary_indexes.as_deref().unwrap_or_default().is_empty());

        let described = store.operations.describe_table(DescribeTableRequest {
            table_name: table.clone(),
        }).await.expect("describe table after GSI deletion");
        assert!(described.table.global_secondary_indexes.as_deref().unwrap_or_default().is_empty());

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        })).expect("valid GetItem request");
        let item = store.operations.get_item(get).await.expect("base item survives GSI deletion");
        assert_eq!(serde_json::to_value(item).unwrap(), json!({
            "Item": {"pk": {"S": "customer-1"}, "gpk": {"S": "group-1"}}
        }));

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "group-1"}}
        })).expect("valid GSI Query request");
        let rejected = observe(store.operations.query(query)).await;
        rejected.assert_no_warning_or_error();
        let expected = format!(
            r#"{{"__type":"com.amazon.coral.validate#ValidationException","message":"The table '{}' does not have the specified index: ByGroup"}}"#,
            table,
        );
        assert_dynamo_error_response_exact(
            rejected.result.expect_err("deleted GSI must reject queries"),
            axum::http::StatusCode::BAD_REQUEST,
            &expected,
        );
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Deleting and recreating a GSI name must use the replacement's physical
// incarnation.
#[tokio::test]
async fn given_deleted_gsi_when_same_name_is_recreated_then_new_index_queries_only_new_keys() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        for attribute in ["old_key", "new_key"] {
            let create = UpdateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [{"AttributeName": attribute, "AttributeType": "S"}],
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "ByGroup",
                    "KeySchema": [{"AttributeName": attribute, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}]
            })).expect("valid GSI creation");
            if attribute == "old_key" {
                store.operations.update_table(create).await.expect("create first GSI incarnation");
                let delete = UpdateTableRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "ByGroup"}}]
                })).expect("valid GSI deletion");
                store.operations.update_table(delete).await.expect("delete first GSI incarnation");
            } else {
                let reserved_error = r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"GSI 'ByGroup' name is reserved"}"#;
                let rejected = observe(store.operations.update_table(create.clone())).await;
                rejected.assert_no_warning_or_error();
                assert_dynamo_error_response_exact(
                    rejected.result.expect_err("deleted GSI name remains reserved for 30 seconds"),
                    axum::http::StatusCode::BAD_REQUEST,
                    reserved_error,
                );
                store.advance_time(std::time::Duration::from_secs(29));
                let still_reserved = observe(store.operations.update_table(create.clone())).await;
                still_reserved.assert_no_warning_or_error();
                assert_dynamo_error_response_exact(
                    still_reserved.result.expect_err("deleted GSI name remains reserved before 30 seconds"),
                    axum::http::StatusCode::BAD_REQUEST,
                    reserved_error,
                );
                let described = store.operations.describe_table(DescribeTableRequest {
                    table_name: table.clone(),
                }).await.expect("describe table while GSI name is reserved");
                assert!(described.table.global_secondary_indexes.as_deref().unwrap_or_default().is_empty());
                store.advance_time(std::time::Duration::from_secs(1));
                store.operations.update_table(create).await.expect("create replacement GSI incarnation");
            }
        }
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "item-1"}, "old_key": {"S": "old"},
                "new_key": {"S": "new"}}
        })).expect("valid item for replacement index");
        store.operations.put_item(put).await.expect("write replacement GSI source");
        let request = GsiBuildRequest {
            destination: Namespace::try_new("rewrite-conformance-gsi")
                .expect("valid GSI job destination"),
            job: JobId::from([7; 16]),
            fence: NonZeroU64::new(1).expect("positive fence"),
            page_limit: GsiPageLimit::try_from(1usize).expect("positive page limit"),
            captured_at: TimestampMillis::now(),
        };
        complete_gsi_build(&store, &request).await;
        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(), "IndexName": "ByGroup",
            "KeyConditionExpression": "new_key = :value",
            "ExpressionAttributeValues": {":value": {"S": "new"}}
        })).expect("valid replacement GSI query");
        let response = store.operations.query(query).await.expect("query replacement GSI");
        assert_eq!(response.count, 1);
        assert_eq!(serde_json::to_value(response).unwrap()["Items"][0]["pk"],
            json!({"S": "item-1"}));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: a new GSI includes writes before and during its build, but
// omits sparse items.
#[tokio::test]
async fn given_writes_around_gsi_registration_when_build_completes_then_only_indexed_items_are_queryable()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {
                "pk": {"S": "customer-1"},
                "gpk": {"S": "group-1"},
                "gsk": {"S": "before"},
                "payload": {"S": "before-build"}
            }
        }))
        .expect("valid item before GSI registration");
        store
            .operations
            .put_item(put)
            .await
            .expect("put item before GSI registration");

        let create = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "ByGroup",
                "KeySchema": [
                    {"AttributeName": "gpk", "KeyType": "HASH"},
                    {"AttributeName": "gsk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .expect("valid GSI creation request");
        let created = observe(store.operations.update_table(create)).await;
        created.assert_no_warning_or_error();
        let indexes = created
            .result
            .expect("register GSI build")
            .table_description
            .global_secondary_indexes
            .expect("GSI in update response");
        assert_eq!(indexes[0].backfilling, Some(true));

        let during_build = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {
                "pk": {"S": "customer-2"},
                "gpk": {"S": "group-1"},
                "gsk": {"S": "during"},
                "payload": {"S": "during-build"}
            }
        }))
        .expect("valid item during GSI build");
        let written = observe(store.operations.put_item(during_build)).await;
        written.assert_no_warning_or_error();
        written.result.expect("put item during GSI build");
        let sparse_item = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-3"}, "payload": {"S": "unindexed"}}
        }))
        .expect("valid item without GSI keys");
        store
            .operations
            .put_item(sparse_item)
            .await
            .expect("put sparse item");

        let request = GsiBuildRequest {
            destination: Namespace::try_new("rewrite-conformance-gsi")
                .expect("valid GSI job destination"),
            job: JobId::from([7; 16]),
            fence: NonZeroU64::new(1).expect("positive fence"),
            page_limit: GsiPageLimit::try_from(1usize).expect("positive page limit"),
            captured_at: TimestampMillis::now(),
        };
        complete_gsi_build(&store, &request).await;

        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .expect("describe completed GSI");
        let indexes = described
            .table
            .global_secondary_indexes
            .expect("GSI after build");
        assert_eq!(
            serde_json::to_value(&indexes[0]).unwrap()["IndexStatus"],
            "ACTIVE"
        );
        assert_eq!(indexes[0].backfilling, Some(false));

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "group-1"}}
        }))
        .expect("valid GSI Query request");
        let queried = observe(store.operations.query(query)).await;
        queried.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(queried.result.expect("query active GSI")).unwrap(),
            json!({
                "Items": [
                    {
                        "pk": {"S": "customer-1"},
                        "gpk": {"S": "group-1"},
                        "gsk": {"S": "before"},
                        "payload": {"S": "before-build"}
                    },
                    {
                        "pk": {"S": "customer-2"},
                        "gpk": {"S": "group-1"},
                        "gsk": {"S": "during"},
                        "payload": {"S": "during-build"}
                    }
                ],
                "Count": 2,
                "ScannedCount": 2
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule: catch-up removes every stale GSI key after repeated customer
// updates.
#[tokio::test]
async fn given_active_gsi_when_key_moves_twice_then_only_latest_projection_is_queryable() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("valid table with GSI");
        store
            .operations
            .create_table(create)
            .await
            .expect("create table with GSI");

        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {
                "pk": {"S": "customer-1"},
                "gpk": {"S": "original"},
                "payload": {"S": "before-move"}
            }
        }))
        .expect("valid indexed item");
        store
            .operations
            .put_item(put)
            .await
            .expect("put original item");
        let request = GsiBuildRequest {
            destination: Namespace::try_new("rewrite-conformance-gsi")
                .expect("valid GSI job destination"),
            job: JobId::from([7; 16]),
            fence: NonZeroU64::new(1).expect("positive fence"),
            page_limit: GsiPageLimit::try_from(1usize).expect("positive page limit"),
            captured_at: TimestampMillis::now(),
        };
        complete_gsi_build(&store, &request).await;

        let original = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "original"}}
        }))
        .expect("valid query for original GSI key");
        let indexed = observe(store.operations.query(original)).await;
        indexed.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(indexed.result.expect("query original GSI key")).unwrap(),
            json!({
                "Items": [{
                    "pk": {"S": "customer-1"},
                    "gpk": {"S": "original"},
                    "payload": {"S": "before-move"}
                }],
                "Count": 1,
                "ScannedCount": 1
            })
        );

        for (group, payload) in [("intermediate", "during-move"), ("latest", "after-move")] {
            let update = UpdateItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": "customer-1"}},
                "UpdateExpression": "SET gpk = :group, payload = :payload",
                "ExpressionAttributeValues": {
                    ":group": {"S": group},
                    ":payload": {"S": payload}
                }
            }))
            .expect("valid GSI key move");
            let moved = observe(store.operations.update_item(update)).await;
            moved.assert_no_warning_or_error();
            moved.result.expect("update indexed item");
        }
        complete_gsi_build(&store, &request).await;

        for group in ["original", "intermediate"] {
            let query = QueryRequest::try_from(json!({
                "TableName": table.as_ref(),
                "IndexName": "ByGroup",
                "KeyConditionExpression": "gpk = :group",
                "ExpressionAttributeValues": {":group": {"S": group}}
            }))
            .expect("valid query for old GSI key");
            let result = observe(store.operations.query(query)).await;
            result.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(result.result.expect("query old GSI key")).unwrap(),
                json!({
                    "Items": [], "Count": 0, "ScannedCount": 0
                }),
                "stale index entry for {group}"
            );
        }
        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "latest"}}
        }))
        .expect("valid query for latest GSI key");
        let result = observe(store.operations.query(query)).await;
        result.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(result.result.expect("query latest GSI key")).unwrap(),
            json!({
                "Items": [{
                    "pk": {"S": "customer-1"},
                    "gpk": {"S": "latest"},
                    "payload": {"S": "after-move"}
                }],
                "Count": 1,
                "ScannedCount": 1
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_combined_table_updates_when_owner_reopens_then_catalog_and_retention_survive() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("table-updates.db");
    let table = unique_table_name();
    let store = TestStore::open_sqlite_file(&path).await;
    store.operations.create_table(CreateTableRequest::try_from(json!({
        "TableName": table, "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "BillingMode": "PAY_PER_REQUEST",
        "MaxIndexers": 1, "AuxStreamDurationHours": 1
    })).unwrap()).await.unwrap();
    store.finish().await;
    for (capacity, duration, enabled, gsi) in [
        (
            3,
            7,
            true,
            json!({"Create": {"IndexName": "new-index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}}),
        ),
        (4, 9, false, json!({"Delete": {"IndexName": "new-index"}})),
    ] {
        let store = TestStore::open_sqlite_file(&path).await;
        let mut specification = json!({"StreamEnabled": enabled});
        if enabled {
            specification["StreamViewType"] = json!("NEW_AND_OLD_IMAGES");
        }
        let response = store
            .operations
            .update_table(
                UpdateTableRequest::try_from(json!({
                    "TableName": table, "MaxIndexers": capacity, "AuxStreamDurationHours": duration,
                    "DeletionProtectionEnabled": enabled, "StreamSpecification": specification,
                    "GlobalSecondaryIndexUpdates": [gsi]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.table_description.max_indexers.get(), capacity);
        assert_eq!(
            response.table_description.deletion_protection_enabled,
            enabled
        );
        let describe = DescribeTableRequest::try_from(json!({"TableName": table})).unwrap();
        let before = store
            .operations
            .describe_table(describe.clone())
            .await
            .unwrap();
        let due_at = store.now() + 10 * 60 * 60 * 1000;
        let markers = store
            .stream_retention_repository
            .list_due_stream_trim_markers(due_at, 100)
            .await
            .unwrap();
        assert!(
            !markers.is_empty(),
            "updated retention policy has no marker"
        );
        let scopes = markers
            .iter()
            .map(|marker| marker.scope.clone())
            .collect::<Vec<_>>();
        let states = store
            .stream_retention_repository
            .load_stream_trim_states(&scopes)
            .await
            .unwrap();
        store.finish().await;
        let reopened = TestStore::open_sqlite_file(&path).await;
        assert_eq!(
            serde_json::to_value(reopened.operations.describe_table(describe).await.unwrap())
                .unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert!(
            reopened
                .stream_retention_repository
                .load_stream_trim_states(&scopes)
                .await
                .unwrap()
                == states
        );
        assert!(
            reopened
                .stream_retention_repository
                .list_due_stream_trim_markers(due_at, 100)
                .await
                .unwrap()
                == markers
        );
        reopened.finish().await;
    }
}
