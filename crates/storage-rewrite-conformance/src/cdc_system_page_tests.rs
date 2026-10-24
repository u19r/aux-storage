//! Logical CDC records and resumable checkpoints through replacement providers.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest, UpdateItemRequest,
};
use stream_provider::SystemStreamPageRequest;

use super::test_support::{
    ConformanceFixtures, TestStore, assert_dynamo_error_response_exact, unique_table_name,
};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

// Business and boundary rules: bounded CDC pages retain exact logical images
// and a checkpoint resumes after the last delivered record.
#[tokio::test]
async fn given_three_committed_writes_when_system_cdc_is_paged_then_checkpoint_resumes_without_duplicates()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        let created = observe(store.operations.create_table(create)).await;
        created.assert_no_warning_or_error();
        created.one_span_with("dynamo_operation", &[("operation", "CreateTable")]);
        created.result.unwrap();

        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        for key in ["first", "second", "third"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}, "value": {"S": key}}
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
            written.result.unwrap();
        }

        let first = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert_eq!(first.source_scope(), &scope);
        assert_eq!(first.records().len(), 2);
        assert!(!first.terminal());
        let checkpoint = first.issued_checkpoint().token().clone();

        let second = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint), 2).unwrap(),
        ))
        .await;
        second.assert_no_warning_or_error();
        let second = second.result.unwrap();
        assert_eq!(second.records().len(), 1);
        assert!(second.terminal());
        let checkpoint = second.issued_checkpoint().token().clone();

        for (record, key) in first
            .records()
            .iter()
            .chain(second.records())
            .zip(["first", "second", "third"])
        {
            assert_eq!(record.source_table_name.as_ref(), Some(&table));
            assert!(record.cursor.is_none());
            assert!(!record.sequence_number.is_empty());
            assert!(record.system_position.is_some());
            assert!(record.old_image.is_none());
            assert_eq!(
                serde_json::to_value(&record.keys).unwrap(),
                json!({"pk": {"S": key}}),
            );
            assert_eq!(
                serde_json::to_value(&record.new_image).unwrap(),
                json!({"pk": {"S": key}, "value": {"S": key}}),
            );
        }

        let expected_records = first
            .records()
            .iter()
            .zip(["first", "second"])
            .map(|(record, key)| {
                json!({
                    "SourceTableName": &table,
                    "Keys": {"pk": {"S": key}},
                    "SequenceNumber": record.sequence_number.as_str(),
                    "NewImage": {"pk": {"S": key}, "value": {"S": key}},
                    "SystemPosition": record.system_position.as_ref().unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let response = first.clone().into_response().unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "Records": expected_records,
                "SourceScope": &scope,
                "Checkpoint": first.issued_checkpoint().token(),
            }),
        );

        let exhausted = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint.clone()), 2).unwrap(),
        ))
        .await;
        exhausted.assert_no_warning_or_error();
        let exhausted = exhausted.result.unwrap();
        assert!(exhausted.records().is_empty());
        assert!(exhausted.terminal());
        assert_eq!(exhausted.issued_checkpoint().token(), &checkpoint);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and format rules: the logical CDC boundary exposes the exact
// before and after images of each committed item transition.
#[tokio::test]
async fn given_item_put_update_delete_when_logical_cdc_is_read_then_images_match_each_transition() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        let original = json!({"pk": {"S": "item-1"}, "status": {"S": "original"}});
        let updated_image = json!({"pk": {"S": "item-1"}, "status": {"S": "updated"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": original
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
        written.result.unwrap();

        let update = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "item-1"}},
            "UpdateExpression": "SET #status = :next",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":next": {"S": "updated"}}
        }))
        .unwrap();
        let changed = observe(store.operations.update_item(update)).await;
        changed.assert_no_warning_or_error();
        changed.one_span_with("dynamo_operation", &[("operation", "UpdateItem")]);
        changed.result.unwrap();

        let delete = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "item-1"}}
        }))
        .unwrap();
        let removed = observe(store.operations.delete_item(delete)).await;
        removed.assert_no_warning_or_error();
        removed.one_span_with("dynamo_operation", &[("operation", "DeleteItem")]);
        removed.result.unwrap();

        let read =
            observe(store.stream_operations.get_system_stream_page(
                SystemStreamPageRequest::try_new(scope, None, 10).unwrap(),
            ))
            .await;
        read.assert_no_warning_or_error();
        let page = read.result.unwrap();
        assert_eq!(page.records().len(), 3);
        assert!(page.terminal());
        let records = page.records();
        for record in records {
            assert_eq!(record.source_table_name.as_ref(), Some(&table));
            assert_eq!(
                serde_json::to_value(&record.keys).unwrap(),
                json!({"pk": {"S": "item-1"}}),
            );
            assert!(record.system_position.is_some());
        }
        assert!(records[0].old_image.is_none());
        assert_eq!(
            serde_json::to_value(&records[0].new_image).unwrap(),
            original
        );
        assert_eq!(
            serde_json::to_value(&records[1].old_image).unwrap(),
            original
        );
        assert_eq!(
            serde_json::to_value(&records[1].new_image).unwrap(),
            updated_image
        );
        assert_eq!(
            serde_json::to_value(&records[2].old_image).unwrap(),
            updated_image
        );
        assert!(records[2].new_image.is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative rule: a rejected write cannot advance the durable CDC frontier.
#[tokio::test]
async fn given_cdc_checkpoint_when_conditional_put_fails_then_no_new_logical_record_is_visible() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        let existing = json!({"pk": {"S": "item-1"}, "status": {"S": "existing"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": existing
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();
        let before = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap(),
        ))
        .await;
        before.assert_no_warning_or_error();
        let before = before.result.unwrap();
        assert_eq!(before.records().len(), 1);
        assert!(before.terminal());
        let checkpoint = before.issued_checkpoint().token().clone();

        let rejected = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "item-1"}, "status": {"S": "replacement"}},
            "ConditionExpression": "attribute_not_exists(pk)"
        }))
        .unwrap();
        let failure = observe(store.operations.put_item(rejected)).await;
        failure.assert_no_warning_or_error();
        failure.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
        let diagnostics = format!("{:?}{:?}", failure.events, failure.spans);
        assert!(!diagnostics.contains("item-1"));
        assert!(!diagnostics.contains("replacement"));
        let case = FIXTURES.case(
            include_str!("../fixtures/put_item_cases.jsonl"),
            "field-condition_expression/400/attribute_not_exists_failure.json",
        );
        assert_dynamo_error_response_exact(
            failure.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &case["expected"]["body"].to_string(),
        );

        let after = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint), 10).unwrap(),
        ))
        .await;
        after.assert_no_warning_or_error();
        let after = after.result.unwrap();
        assert!(after.records().is_empty());
        assert!(after.terminal());

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "item-1"}}
        }))
        .unwrap();
        let current = store.operations.get_item(get).await.unwrap();
        let item = storage_types::AttributeMap::try_from(current.item.expect("existing item"))
            .expect("decode item");
        assert_eq!(serde_json::to_value(item).unwrap(), existing);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and boundary rules: a system-wide checkpoint spans independent
// tables without confusing equal item keys or replaying a record.
#[tokio::test]
async fn given_equal_keys_in_two_tables_when_system_cdc_is_paged_then_each_record_keeps_its_source_table()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let first_table = unique_table_name();
        let second_table = unique_table_name();
        for table in [&first_table, &second_table] {
            let create = CreateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }))
            .unwrap();
            store.operations.create_table(create).await.unwrap();
        }
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        for (table, value) in [
            (&first_table, "first-1"),
            (&second_table, "second-1"),
            (&first_table, "first-2"),
        ] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": "same-key"}, "value": {"S": value}}
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
            written.result.unwrap();
        }

        let expected = [
            (first_table.as_ref().to_string(), "first-1".to_string()),
            (second_table.as_ref().to_string(), "second-1".to_string()),
            (first_table.as_ref().to_string(), "first-2".to_string()),
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        let mut observed = std::collections::HashSet::new();
        let mut positions = std::collections::HashSet::new();
        let mut checkpoint = None;
        for page_number in 0..3 {
            let read = observe(store.stream_operations.get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), checkpoint, 1).unwrap(),
            ))
            .await;
            read.assert_no_warning_or_error();
            let page = read.result.unwrap();
            assert_eq!(page.source_scope(), &scope);
            assert_eq!(page.records().len(), 1);
            assert_eq!(page.terminal(), page_number == 2);
            let record = &page.records()[0];
            let table: &str = record.source_table_name.as_ref().unwrap().as_ref();
            let image = serde_json::to_value(&record.new_image).unwrap();
            let value = image["value"]["S"].as_str().unwrap();
            assert_eq!(
                image,
                json!({
                    "pk": {"S": "same-key"}, "value": {"S": value}
                })
            );
            assert_eq!(
                serde_json::to_value(&record.keys).unwrap(),
                json!({"pk": {"S": "same-key"}}),
            );
            assert!(record.cursor.is_none());
            if table == first_table.as_ref() && value == "first-2" {
                assert_eq!(
                    serde_json::to_value(&record.old_image).unwrap(),
                    json!({"pk": {"S": "same-key"}, "value": {"S": "first-1"}}),
                );
            } else {
                assert!(record.old_image.is_none());
            }
            assert!(positions.insert(record.system_position.clone().unwrap()));
            assert!(observed.insert((table.to_string(), value.to_string())));
            checkpoint = Some(page.issued_checkpoint().token().clone());
        }
        assert_eq!(observed, expected);

        let exhausted = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, checkpoint, 1).unwrap(),
        ))
        .await;
        exhausted.assert_no_warning_or_error();
        let exhausted = exhausted.result.unwrap();
        assert!(exhausted.records().is_empty());
        assert!(exhausted.terminal());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule: an empty page's checkpoint remains usable when a later
// committed write extends the logical stream.
#[tokio::test]
async fn given_empty_system_cdc_checkpoint_when_item_is_put_then_resume_and_retry_return_the_new_record()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        let empty = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap(),
        ))
        .await;
        empty.assert_no_warning_or_error();
        let empty = empty.result.unwrap();
        assert!(empty.records().is_empty());
        assert!(empty.terminal());
        let checkpoint = empty.issued_checkpoint().token().clone();
        assert_eq!(
            serde_json::to_value(empty.into_response().unwrap()).unwrap(),
            json!({"Records": [], "SourceScope": &scope, "Checkpoint": &checkpoint}),
        );

        let idle = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        idle.assert_no_warning_or_error();
        idle.one_span_with(
            "system_stream_operation",
            &[
                ("operation", "GetSystemStreamPage"),
                ("stream_name", "system"),
            ],
        );
        let idle = idle.result.unwrap();
        assert!(idle.records().is_empty());
        assert!(idle.terminal());
        assert_eq!(idle.issued_checkpoint().token(), &checkpoint);

        let image = json!({"pk": {"S": "after-empty"}, "status": {"S": "created"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": image
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.one_span_with("dynamo_operation", &[("operation", "PutItem")]);
        written.result.unwrap();

        let read = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        read.assert_no_warning_or_error();
        let page = read.result.unwrap();
        assert_eq!(page.source_scope(), &scope);
        assert_eq!(page.records().len(), 1);
        assert!(page.terminal());
        let record = &page.records()[0];
        assert_eq!(record.source_table_name.as_ref(), Some(&table));
        assert_eq!(serde_json::to_value(&record.new_image).unwrap(), image);
        assert!(record.old_image.is_none());
        assert!(record.cursor.is_none());
        assert!(record.system_position.is_some());

        let retry = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint), 1).unwrap(),
        ))
        .await;
        retry.assert_no_warning_or_error();
        let retry = retry.result.unwrap();
        assert_eq!(
            serde_json::to_value(retry.records()).unwrap(),
            serde_json::to_value(page.records()).unwrap(),
        );
        assert_eq!(retry.issued_checkpoint(), page.issued_checkpoint());
        assert_eq!(retry.terminal(), page.terminal());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and boundary rules: deleting an already absent item succeeds but
// cannot publish a second customer-visible removal.
#[tokio::test]
async fn given_deleted_item_when_deleted_again_then_logical_cdc_contains_only_one_removal() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        let image = json!({"pk": {"S": "one"}, "status": {"S": "present"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": image,
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.unwrap();

        let delete = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "one"}},
        }))
        .unwrap();
        for _ in 0..2 {
            let removed = observe(store.operations.delete_item(delete.clone())).await;
            removed.assert_no_warning_or_error();
            removed.one_span_with("dynamo_operation", &[("operation", "DeleteItem")]);
            assert_eq!(
                serde_json::to_value(removed.result.unwrap()).unwrap(),
                json!({})
            );
        }

        let read =
            observe(store.stream_operations.get_system_stream_page(
                SystemStreamPageRequest::try_new(scope, None, 10).unwrap(),
            ))
            .await;
        read.assert_no_warning_or_error();
        let page = read.result.unwrap();
        assert!(page.terminal());
        assert_eq!(page.records().len(), 2);
        assert_eq!(page.records()[0].source_table_name.as_ref(), Some(&table));
        assert_eq!(page.records()[1].source_table_name.as_ref(), Some(&table));
        assert_eq!(
            serde_json::to_value(&page.records()[1].keys).unwrap(),
            json!({"pk": {"S": "one"}}),
        );
        assert_eq!(
            serde_json::to_value(&page.records()[0].new_image).unwrap(),
            image
        );
        assert!(page.records()[0].old_image.is_none());
        assert_eq!(
            serde_json::to_value(&page.records()[1].old_image).unwrap(),
            image
        );
        assert!(page.records()[1].new_image.is_none());

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "one"}},
        }))
        .unwrap();
        let current = observe(store.operations.get_item(get)).await;
        current.assert_no_warning_or_error();
        assert!(current.result.unwrap().item.is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
