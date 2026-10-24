//! Customer-visible BatchWriteItem rules retained from the legacy multi-table
//! batch tests.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Map, Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    BatchWriteItemRequest, CreateTableRequest, GetItemRequest, PutItemRequest, StreamName,
    storage_serde,
};
use stream_provider::{StoredStreamPointer, StreamRowKind};

use super::test_support::{TestStore, unique_table_name};

// Business and boundary rules: one batch may mutate two tables, and each
// accepted member produces a durable change without disturbing other items.
#[tokio::test]
async fn given_two_tables_and_an_unrelated_item_when_a_mixed_batch_writes_then_items_and_cdc_match()
{
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table_a = unique_table_name();
        let table_b = unique_table_name();
        for table in [&table_a, &table_b] {
            let create = CreateTableRequest::try_from(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
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

        let retained = json!({"pk": {"S": "keep"}, "sk": {"S": "1"}, "data": {"S": "original"}});
        let doomed = json!({"pk": {"S": "delete"}, "sk": {"S": "1"}, "data": {"S": "old"}});
        for (table, item) in [(&table_a, &retained), (&table_b, &doomed)] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            }))
            .expect("valid seed item");
            store.operations.put_item(put).await.expect("seed item");
        }
        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store
            .stream_operations
            .read_forward(system_stream.clone(), None, 10)
            .await
            .expect("read seed CDC");
        assert_eq!(before.items.len(), 2);

        let added = json!({"pk": {"S": "add"}, "sk": {"S": "1"}, "data": {"S": "new"}});
        let mut request_items = Map::new();
        request_items.insert(
            table_a.to_string(),
            json!([{"PutRequest": {"Item": added}}]),
        );
        request_items.insert(
            table_b.to_string(),
            json!([{
                "DeleteRequest": {"Key": {"pk": {"S": "delete"}, "sk": {"S": "1"}}}
            }]),
        );
        let request = BatchWriteItemRequest::try_from(json!({"RequestItems": request_items}))
            .expect("valid mixed batch");
        let written = observe(store.operations.batch_write_item(request)).await;
        written.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(written.result.expect("commit mixed batch"))
                .expect("serialize response"),
            json!({"UnprocessedItems": {}})
        );

        for (table, key, expected) in [
            (&table_a, "keep", retained),
            (&table_a, "add", added),
            (&table_b, "delete", Value::Null),
        ] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": key}, "sk": {"S": "1"}}
            }))
            .expect("valid readback key");
            let read = store
                .operations
                .get_item(get)
                .await
                .expect("read after batch");
            assert_eq!(
                serde_json::to_value(read).expect("serialize readback")["Item"],
                expected
            );
        }

        let mut cursor = None;
        let mut rows = Vec::new();
        let mut exhausted = false;
        for _ in 0..4 {
            let page = observe(store.stream_operations.read_forward(
                system_stream.clone(),
                cursor,
                10,
            ))
            .await;
            page.assert_no_warning_or_error();
            let page = page.result.expect("read committed CDC page");
            cursor = page.last_evaluated_key;
            rows.extend(page.items);
            if !page.has_more {
                exhausted = true;
                break;
            }
            assert!(cursor.is_some(), "continuing CDC page has a cursor");
        }
        assert!(exhausted, "four changes fit in four bounded pages");
        assert_eq!(rows.len(), 4, "two seed and two batch changes");
        assert!(rows.windows(2).all(|pair| pair[0].id < pair[1].id));
        let tables = rows[2..]
            .iter()
            .map(|row| {
                let pointer: StoredStreamPointer =
                    storage_serde::from_bytes(&row.data).expect("decode durable CDC pointer");
                pointer.table_name().clone()
            })
            .collect::<Vec<_>>();
        assert!(tables.contains(&table_a));
        assert!(tables.contains(&table_b));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
