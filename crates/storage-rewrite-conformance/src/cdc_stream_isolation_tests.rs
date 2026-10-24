//! Deleting one table must preserve a neighboring table's durable change
//! stream.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    AttributeMap, CreateTableRequest, DeleteTableRequest, ItemHistoryAddress, PutItemRequest,
    StreamName, TableName, storage_serde,
};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamRowKind};

use super::test_support::{TestStore, unique_table_name};

// Boundary rule: a deleted table's stream namespace cannot consume a live
// table's stream, even when their names differ at an SQL wildcard position.
#[tokio::test]
async fn given_neighboring_stream_table_names_when_one_is_deleted_then_other_stream_remains_writable()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let base = unique_table_name();
        let deleted_table = TableName::new(&format!("{base}_one"));
        let live_table = TableName::new(&format!("{base}Xone"));
        for table in [&deleted_table, &live_table] {
            let request = CreateTableRequest::try_from(json!({
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
            let created = observe(store.operations.create_table(request)).await;
            created.assert_no_warning_or_error();
            created.result.unwrap();
        }

        let target_put = PutItemRequest::try_from(json!({
            "TableName": deleted_table.as_ref(),
            "Item": {"pk": {"S": "target-before-delete"}}
        }))
        .unwrap();
        let target_written = observe(store.operations.put_item(target_put)).await;
        target_written.assert_no_warning_or_error();
        target_written.result.unwrap();

        let first_item = json!({"pk": {"S": "before-delete"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": live_table.as_ref(),
            "Item": first_item
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.unwrap();

        let system = StreamRowKind::System(StreamName::system_table_stream());
        let before = observe(store.stream_operations.read_forward(system, None, 10)).await;
        before.assert_no_warning_or_error();
        let before = before.result.unwrap();
        assert_eq!(before.items.len(), 2);
        let pointer: StoredStreamPointer =
            storage_serde::from_bytes(&before.items[1].data).unwrap();
        assert_eq!(pointer.table_name(), &live_table);
        let live_stream = StreamRowKind::Table(
            ItemHistoryAddress {
                history_id: pointer.history_address().history_id,
                stream_name: StreamName::table_stream(&live_table),
            }
            .physical_stream_name(),
        );

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: deleted_table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        deleted.one_span_with("dynamo_operation", &[("operation", "DeleteTable")]);
        deleted.result.unwrap();

        let second_item = json!({"pk": {"S": "after-delete"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": live_table.as_ref(),
            "Item": second_item
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        written.result.unwrap();

        let retained = observe(store.stream_operations.read_forward(live_stream, None, 10)).await;
        retained.assert_no_warning_or_error();
        let retained = retained.result.unwrap();
        assert_eq!(retained.items.len(), 2);
        assert!(!retained.has_more);
        for (row, expected) in retained.items.iter().zip([first_item, second_item]) {
            assert_eq!(row.data_type, StreamDataType::StreamPointer);
            let pointer: StoredStreamPointer = storage_serde::from_bytes(&row.data).unwrap();
            assert_eq!(pointer.table_name(), &live_table);
            let image = observe(store.stream_operations.read_forward(
                StreamRowKind::Item(pointer.history_address().physical_stream_name()),
                None,
                10,
            ))
            .await;
            image.assert_no_warning_or_error();
            let image = image.result.unwrap();
            assert_eq!(image.items.len(), 1);
            let actual: AttributeMap = storage_serde::from_bytes(&image.items[0].data).unwrap();
            let expected: AttributeMap = serde_json::from_value(expected).unwrap();
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
