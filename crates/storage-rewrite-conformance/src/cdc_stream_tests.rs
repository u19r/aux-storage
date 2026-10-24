//! CDC rows written by the replacement Dynamo operation and read by the
//! replacement stream operation.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, CreateTableRequest, DeleteItemRequest, DeleteTableRequest, GetItemRequest,
    ItemHistoryAddress, PutItemRequest, StreamName, UpdateItemRequest, storage_serde,
};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamRowKind};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Business rule: a successful Dynamo write is present in the durable change
// stream.
#[tokio::test]
async fn given_stream_enabled_table_when_item_is_put_then_cdc_exposes_the_exact_new_image() {
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
        .expect("valid stream-enabled table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create stream-enabled table");

        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store
            .stream_operations
            .read_forward(system_stream.clone(), None, 10)
            .await
            .expect("read empty CDC stream");
        assert!(before.items.is_empty());

        let item = json!({"pk": {"S": "customer-1"}, "status": {"S": "active"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": item
        }))
        .expect("valid PutItem request");
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written
            .result
            .expect("write item through replacement store");

        let cdc = observe(
            store
                .stream_operations
                .read_forward(system_stream, None, 10),
        )
        .await;
        cdc.assert_no_warning_or_error();
        let cdc = cdc.result.expect("read CDC after write");
        assert_eq!(
            cdc.items.len(),
            1,
            "one committed write has one CDC pointer"
        );
        assert!(!cdc.has_more);
        let row = &cdc.items[0];
        assert_eq!(row.data_type, StreamDataType::StreamPointer);
        let pointer: StoredStreamPointer =
            storage_serde::from_bytes(&row.data).expect("decode shared CDC pointer");
        assert_eq!(pointer.table_name(), &table);

        let image_stream = StreamRowKind::Item(pointer.history_address().physical_stream_name());
        let images = observe(store.stream_operations.read_forward(image_stream, None, 10)).await;
        images.assert_no_warning_or_error();
        let images = images
            .result
            .expect("read item image through replacement stream");
        assert_eq!(images.items.len(), 1);
        assert_eq!(images.items[0].data_type, StreamDataType::DynamoDbJson);
        let actual: AttributeMap =
            storage_serde::from_bytes(&images.items[0].data).expect("decode durable new image");
        let expected: AttributeMap = serde_json::from_value(item).expect("decode expected image");
        assert_eq!(actual, expected);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Negative rule: a rejected conditional write leaves both the item and its CDC
// history unchanged.
#[tokio::test]
async fn given_existing_item_when_conditional_put_fails_then_no_cdc_record_is_added() {
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
        })).expect("valid stream-enabled table");
        store.operations.create_table(create).await.expect("create stream-enabled table");

        let existing = json!({"pk": {"S": "customer-1"}, "status": {"S": "existing"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": existing
        })).expect("valid initial PutItem");
        store.operations.put_item(put).await.expect("write initial item");

        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.expect("read CDC before rejected write");
        assert_eq!(before.items.len(), 1);

        let rejected = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "status": {"S": "replacement"}},
            "ConditionExpression": "attribute_not_exists(pk)"
        })).expect("valid conditional PutItem");
        let failure = observe(store.operations.put_item(rejected)).await;
        failure.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            failure.result.expect_err("existing key rejects conditional put"),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );

        let after = observe(store.stream_operations.read_forward(system_stream, None, 10)).await;
        after.assert_no_warning_or_error();
        let after = after.result.expect("read CDC after rejected write");
        assert_eq!(after.items.len(), 1, "rejected write must not add a CDC record");
        assert_eq!(after.items[0].id, before.items[0].id);

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        })).expect("valid GetItem request");
        let read = store.operations.get_item(get).await.expect("read unchanged item");
        let item = storage_types::AttributeMap::try_from(read.item.expect("existing item"))
            .expect("decode item");
        assert_eq!(serde_json::to_value(item).expect("serialize item"), existing);
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule: a continuation returns each committed change once in stream
// order.
#[tokio::test]
async fn given_three_committed_items_when_cdc_is_paged_then_every_change_appears_once() {
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
        .expect("valid stream-enabled table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create stream-enabled table");

        for key in ["customer-1", "customer-2", "customer-3"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .expect("valid PutItem request");
            store.operations.put_item(put).await.expect("commit item");
        }

        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let mut cursor = None;
        let mut rows = Vec::new();
        let mut exhausted = false;
        for _ in 0..4 {
            let page = observe(
                store
                    .stream_operations
                    .read_forward(stream.clone(), cursor, 2),
            )
            .await;
            page.assert_no_warning_or_error();
            let page = page.result.expect("read CDC page");
            assert!(page.items.len() <= 2, "requested limit bounds every page");
            assert!(!page.items.is_empty() || !page.has_more);
            if let Some(previous) = cursor {
                assert!(page.items.iter().all(|row| row.id > previous));
            }
            cursor = page.last_evaluated_key;
            rows.extend(page.items);
            if !page.has_more {
                exhausted = true;
                break;
            }
            assert!(cursor.is_some(), "a continuing page has a cursor");
        }
        assert!(exhausted, "three changes fit in four bounded pages");
        assert_eq!(rows.len(), 3, "every committed change appears once");
        assert!(rows.windows(2).all(|pair| pair[0].id < pair[1].id));

        for row in &rows {
            assert_eq!(row.data_type, StreamDataType::StreamPointer);
            let pointer: StoredStreamPointer =
                storage_serde::from_bytes(&row.data).expect("decode CDC pointer");
            assert_eq!(pointer.table_name(), &table);
        }

        let exhausted = observe(store.stream_operations.read_forward(stream, cursor, 2)).await;
        exhausted.assert_no_warning_or_error();
        assert!(
            exhausted
                .result
                .expect("read exhausted CDC page")
                .items
                .is_empty()
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and boundary rules: a table stream contains only that table's
// committed changes.
#[tokio::test]
async fn given_two_stream_enabled_tables_when_writes_interleave_then_each_table_stream_pages_only_its_own_changes()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let first = unique_table_name();
        let second = unique_table_name();
        for table in [&first, &second] {
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
            .expect("valid stream-enabled table");
            store
                .operations
                .create_table(create)
                .await
                .expect("create table");
        }

        for (table, key) in [
            (&first, "first-1"),
            (&second, "second-1"),
            (&first, "first-2"),
        ] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .expect("valid PutItem request");
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.result.expect("commit item");
        }

        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let mut cursor = None;
        let mut system_rows = Vec::new();
        let mut exhausted = false;
        for _ in 0..4 {
            let page = observe(store.stream_operations.read_forward(
                system_stream.clone(),
                cursor,
                10,
            ))
            .await;
            page.assert_no_warning_or_error();
            let page = page.result.expect("read committed changes");
            cursor = page.last_evaluated_key;
            system_rows.extend(page.items);
            if !page.has_more {
                exhausted = true;
                break;
            }
            assert!(cursor.is_some(), "continuing system page has a cursor");
        }
        assert!(exhausted, "three changes fit in four bounded pages");
        let pointers: Vec<StoredStreamPointer> = system_rows
            .iter()
            .map(|row| storage_serde::from_bytes(&row.data).expect("decode committed pointer"))
            .collect();
        assert_eq!(
            pointers.len(),
            3,
            "committed pointer tables: {:?}",
            pointers
                .iter()
                .map(|pointer| pointer.table_name().to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(pointers[0].table_name(), &first);
        assert_eq!(pointers[1].table_name(), &second);
        assert_eq!(pointers[2].table_name(), &first);

        let table_stream = |table: &storage_types::TableName, pointer: &StoredStreamPointer| {
            StreamRowKind::Table(
                ItemHistoryAddress {
                    history_id: pointer.history_address().history_id,
                    stream_name: StreamName::table_stream(table),
                }
                .physical_stream_name(),
            )
        };
        let first_stream = table_stream(&first, &pointers[0]);
        let first_page = observe(store.stream_operations.read_forward(
            first_stream.clone(),
            None,
            1,
        ))
        .await;
        first_page.assert_no_warning_or_error();
        let first_page = first_page.result.expect("read first table page");
        assert_eq!(first_page.items.len(), 1);
        assert!(first_page.has_more);
        let first_cursor = first_page.last_evaluated_key.expect("first page cursor");

        let second_page = observe(store.stream_operations.read_forward(
            first_stream,
            Some(first_cursor),
            1,
        ))
        .await;
        second_page.assert_no_warning_or_error();
        let second_page = second_page.result.expect("read second table page");
        assert_eq!(second_page.items.len(), 1);
        assert!(!second_page.has_more);
        assert!(second_page.items[0].id > first_cursor);

        let second_stream = table_stream(&second, &pointers[1]);
        let second_table = observe(
            store
                .stream_operations
                .read_forward(second_stream, None, 10),
        )
        .await;
        second_table.assert_no_warning_or_error();
        let second_table = second_table.result.expect("read other table stream");
        assert_eq!(second_table.items.len(), 1);
        assert!(!second_table.has_more);

        for (row, table, expected_key) in [
            (&first_page.items[0], &first, "first-1"),
            (&second_page.items[0], &first, "first-2"),
            (&second_table.items[0], &second, "second-1"),
        ] {
            let pointer: StoredStreamPointer =
                storage_serde::from_bytes(&row.data).expect("decode table stream pointer");
            assert_eq!(pointer.table_name(), table);
            let image = observe(store.stream_operations.read_forward(
                StreamRowKind::Item(pointer.history_address().physical_stream_name()),
                None,
                10,
            ))
            .await;
            image.assert_no_warning_or_error();
            let image = image.result.expect("read committed image");
            assert_eq!(image.items.len(), 1);
            let actual: AttributeMap =
                storage_serde::from_bytes(&image.items[0].data).expect("decode committed image");
            let expected: AttributeMap = serde_json::from_value(json!({"pk": {"S": expected_key}}))
                .expect("decode expected image");
            assert_eq!(actual, expected);
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and format rules: update and delete changes retain the exact prior
// image.
#[tokio::test]
async fn given_one_item_when_updated_then_deleted_then_cdc_preserves_each_old_image() {
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
        .expect("valid stream-enabled table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create stream-enabled table");

        let original = json!({"pk": {"S": "customer-1"}, "status": {"S": "original"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": original
        }))
        .expect("valid PutItem request");
        store
            .operations
            .put_item(put)
            .await
            .expect("write original item");

        let update = UpdateItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}},
            "UpdateExpression": "SET #status = :next",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":next": {"S": "updated"}}
        }))
        .expect("valid UpdateItem request");
        let updated = observe(store.operations.update_item(update)).await;
        updated.assert_no_warning_or_error();
        updated.result.expect("update item");

        let delete = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        }))
        .expect("valid DeleteItem request");
        let deleted = observe(store.operations.delete_item(delete)).await;
        deleted.assert_no_warning_or_error();
        deleted.result.expect("delete item");

        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let mut cursor = None;
        let mut rows = Vec::new();
        let mut exhausted = false;
        for _ in 0..4 {
            let page = observe(
                store
                    .stream_operations
                    .read_forward(stream.clone(), cursor, 10),
            )
            .await;
            page.assert_no_warning_or_error();
            let page = page.result.expect("read item changes");
            cursor = page.last_evaluated_key;
            rows.extend(page.items);
            if !page.has_more {
                exhausted = true;
                break;
            }
            assert!(
                cursor.is_some(),
                "nonempty CDC page has a continuation cursor"
            );
        }
        assert!(exhausted, "CDC must be exhausted within four bounded pages");
        assert_eq!(
            rows.len(),
            3,
            "put, update, and delete each produce one change"
        );

        let pointers: Vec<StoredStreamPointer> = rows
            .iter()
            .map(|row| {
                assert_eq!(row.data_type, StreamDataType::StreamPointer);
                let pointer: StoredStreamPointer =
                    storage_serde::from_bytes(&row.data).expect("decode CDC pointer");
                assert_eq!(pointer.table_name(), &table);
                pointer
            })
            .collect();
        let expected_original: AttributeMap =
            serde_json::from_value(original).expect("decode original item");
        let expected_updated: AttributeMap = serde_json::from_value(json!({
            "pk": {"S": "customer-1"}, "status": {"S": "updated"}
        }))
        .expect("decode updated item");

        let created_images = pointers[0]
            .embedded_items()
            .expect("small put image is embedded");
        assert_eq!(created_images.len(), 1);
        assert_eq!(created_images[0].data_type, StreamDataType::DynamoDbJson);
        let created: AttributeMap =
            storage_serde::from_bytes(&created_images[0].data).expect("decode put image");
        assert_eq!(created, expected_original);

        let updated_images = pointers[1]
            .embedded_items()
            .expect("small update images are embedded");
        assert_eq!(updated_images.len(), 2);
        assert_eq!(updated_images[0].data_type, StreamDataType::DynamoDbJson);
        assert_eq!(updated_images[1].data_type, StreamDataType::DynamoDbJson);
        let new: AttributeMap =
            storage_serde::from_bytes(&updated_images[0].data).expect("decode update new image");
        let old: AttributeMap =
            storage_serde::from_bytes(&updated_images[1].data).expect("decode update old image");
        assert_eq!(new, expected_updated);
        assert_eq!(old, expected_original);

        let deleted_images = pointers[2]
            .embedded_items()
            .expect("small delete images are embedded");
        assert_eq!(deleted_images.len(), 2);
        assert_eq!(deleted_images[0].data_type, StreamDataType::DeleteMarker);
        assert_eq!(deleted_images[1].data_type, StreamDataType::DynamoDbJson);
        let old: AttributeMap =
            storage_serde::from_bytes(&deleted_images[1].data).expect("decode delete old image");
        assert_eq!(old, expected_updated);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary and format rules: a larger indexed image remains logical in the
// change stream.
#[tokio::test]
async fn given_large_indexed_item_when_put_then_cdc_image_has_exact_logical_attributes() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 1,
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .expect("valid stream-enabled indexed table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create indexed table");

        let item = json!({
            "pk": {"S": "customer-1"},
            "status": {"S": "active"},
            "payload": {"S": "x".repeat(120_000)}
        });
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": item,
            "Indexers": ["status"]
        }))
        .expect("valid indexed PutItem");
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.expect("write indexed item");

        let cdc = observe(store.stream_operations.read_forward(
            StreamRowKind::System(StreamName::system_table_stream()),
            None,
            10,
        ))
        .await;
        cdc.assert_no_warning_or_error();
        let cdc = cdc.result.expect("read CDC pointer");
        assert_eq!(cdc.items.len(), 1);
        assert_eq!(cdc.items[0].data_type, StreamDataType::StreamPointer);
        let pointer: StoredStreamPointer =
            storage_serde::from_bytes(&cdc.items[0].data).expect("decode CDC pointer");
        assert_eq!(pointer.table_name(), &table);

        let image = observe(store.stream_operations.read_forward(
            StreamRowKind::Item(pointer.history_address().physical_stream_name()),
            None,
            10,
        ))
        .await;
        image.assert_no_warning_or_error();
        let image = image.result.expect("read large item image");
        assert_eq!(image.items.len(), 1);
        assert_eq!(image.items[0].data_type, StreamDataType::DynamoDbJson);
        let actual: AttributeMap =
            storage_serde::from_bytes(&image.items[0].data).expect("decode logical CDC image");
        let expected: AttributeMap =
            serde_json::from_value(item).expect("decode expected logical item");
        assert_eq!(
            actual, expected,
            "CDC image excludes physical indexer metadata"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule: a recreated table owns new stream history even when its name
// and item key match.
#[tokio::test]
async fn given_deleted_table_when_recreated_with_same_key_then_cdc_keeps_histories_separate() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = || {
            CreateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }))
            .expect("valid stream-enabled table")
        };
        store
            .operations
            .create_table(create())
            .await
            .expect("create first table incarnation");

        let put = |status| {
            PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": "customer-1"}, "status": {"S": status}}
            }))
            .expect("valid PutItem request")
        };
        store
            .operations
            .put_item(put("before-delete"))
            .await
            .expect("write old incarnation");

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        deleted.result.expect("delete first table incarnation");
        let rejected = observe(store.operations.create_table(create())).await;
        rejected.assert_no_warning_or_error();
        assert!(
            rejected.result.is_err(),
            "deleted table name remains reserved for 30 seconds"
        );
        store.advance_time(std::time::Duration::from_secs(31));
        store
            .operations
            .create_table(create())
            .await
            .expect("recreate table");
        let written = observe(store.operations.put_item(put("after-recreate"))).await;
        written.assert_no_warning_or_error();
        written.result.expect("write new incarnation with same key");

        let cdc = observe(store.stream_operations.read_forward(
            StreamRowKind::System(StreamName::system_table_stream()),
            None,
            10,
        ))
        .await;
        cdc.assert_no_warning_or_error();
        let cdc = cdc.result.expect("read change stream across recreation");
        assert_eq!(cdc.items.len(), 2, "both committed changes remain visible");
        assert!(cdc.items[0].id < cdc.items[1].id);
        let pointers: Vec<StoredStreamPointer> = cdc
            .items
            .iter()
            .map(|row| {
                assert_eq!(row.data_type, StreamDataType::StreamPointer);
                let pointer: StoredStreamPointer =
                    storage_serde::from_bytes(&row.data).expect("decode CDC pointer");
                assert_eq!(pointer.table_name(), &table);
                pointer
            })
            .collect();
        assert_ne!(
            pointers[0].history_address().history_id,
            pointers[1].history_address().history_id,
            "new table incarnation must have a separate history identity"
        );

        for (pointer, expected_status) in pointers.iter().zip(["before-delete", "after-recreate"]) {
            let images = observe(store.stream_operations.read_forward(
                StreamRowKind::Item(pointer.history_address().physical_stream_name()),
                None,
                10,
            ))
            .await;
            images.assert_no_warning_or_error();
            let images = images.result.expect("read incarnation image");
            assert_eq!(images.items.len(), 1);
            assert_eq!(images.items[0].data_type, StreamDataType::DynamoDbJson);
            let actual: AttributeMap =
                storage_serde::from_bytes(&images.items[0].data).expect("decode incarnation image");
            let expected: AttributeMap = serde_json::from_value(json!({
                "pk": {"S": "customer-1"}, "status": {"S": expected_status}
            }))
            .expect("decode expected image");
            assert_eq!(actual, expected);
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
