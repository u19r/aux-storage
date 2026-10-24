//! Concurrent PutItem commits through the public replacement operation.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, GetItemRequest, PutItemRequest, StreamName};
use stream_provider::StreamRowKind;
use tokio::sync::Barrier;

use super::test_support::{TestStore, unique_table_name};

// Business rule from legacy KV write tests: same-key writers all commit, with
// no lost CDC effects.
#[tokio::test]
async fn given_eight_concurrent_puts_to_one_key_when_all_commit_then_eight_changes_are_visible() {
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
            .expect("create table");

        let barrier = Arc::new(Barrier::new(8));
        let mut writers = Vec::new();
        for writer in 0..8 {
            let operations = Arc::clone(&store.operations);
            let barrier = Arc::clone(&barrier);
            let table = table.clone();
            writers.push(tokio::spawn(async move {
                let request = PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "hot"}, "writer": {"N": writer.to_string()}}
                }))
                .expect("valid concurrent PutItem");
                barrier.wait().await;
                observe(operations.put_item(request)).await
            }));
        }
        for writer in writers {
            let observed = writer.await.expect("writer task completes");
            observed.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(observed.result.expect("concurrent PutItem commits"))
                    .expect("serialize PutItem response"),
                json!({}),
            );
        }

        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "hot"}}
        }))
        .expect("valid GetItem");
        let found = store
            .operations
            .get_item(read)
            .await
            .expect("read final item");
        let found = serde_json::to_value(found).expect("serialize final item");
        let winner = found["Item"]["writer"]["N"]
            .as_str()
            .expect("writer number");
        assert!((0..8).any(|writer| winner == writer.to_string()));

        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let mut cursor = None;
        let mut changes = Vec::new();
        let mut exhausted = false;
        for _ in 0..9 {
            let page = observe(
                store
                    .stream_operations
                    .read_forward(stream.clone(), cursor, 10),
            )
            .await;
            page.assert_no_warning_or_error();
            let page = page.result.expect("read committed CDC changes");
            assert!(
                !page.items.is_empty() || !page.has_more,
                "CDC page must advance"
            );
            cursor = page.last_evaluated_key;
            changes.extend(page.items);
            if !page.has_more {
                exhausted = true;
                break;
            }
        }
        assert!(exhausted, "CDC stream must have a finite end");
        assert_eq!(
            changes.len(),
            8,
            "one durable CDC pointer per committed write"
        );
        assert!(changes.windows(2).all(|pair| pair[0].id < pair[1].id));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
