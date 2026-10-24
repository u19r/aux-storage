//! Public stream append receipts and bounded reads on every replacement
//! backend.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use storage_observability_test_support::observation::observe;
use storage_types::UserStreamName;
use stream_provider::{StreamDataType, StreamPartitioningMode, StreamRowKind};

use super::test_support::TestStore;

// Business and boundary rules migrated from the old SQL and KV stream tests:
// append receipts identify committed rows, and continuation never repeats one.
#[tokio::test]
async fn given_user_stream_when_three_items_are_appended_then_pages_preserve_receipts_and_data() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let name = UserStreamName::new(&format!("stream_{}", uuid::Uuid::new_v4().simple()));
        let created = observe(store.stream_operations.create_stream(
            name.clone(),
            None,
            StreamPartitioningMode::Single,
        ))
        .await;
        created.assert_no_warning_or_error();
        let stream = StreamRowKind::User(created.result.unwrap());

        let mut receipts = Vec::new();
        for data in [b"first".as_slice(), b"second", b"third"] {
            let appended = observe(
                store
                    .stream_operations
                    .append_item(name.clone(), data, None),
            )
            .await;
            appended.assert_no_warning_or_error();
            receipts.push(appended.result.unwrap());
        }
        assert!(receipts.windows(2).all(|pair| pair[0] < pair[1]));

        // PostgreSQL's safe visibility frontier may lag an acknowledged append
        // while an older transaction elsewhere in the cluster is still open.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let page = store
                    .stream_operations
                    .read_forward(stream.clone(), None, 3)
                    .await
                    .unwrap();
                if page.items.len() == receipts.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let first = observe(
            store
                .stream_operations
                .read_forward(stream.clone(), None, 2),
        )
        .await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.items[0].id, receipts[0]);
        assert_eq!(first.items[0].data, b"first");
        assert_eq!(first.items[0].data_type, StreamDataType::Text);
        assert_eq!(first.items[1].id, receipts[1]);
        assert_eq!(first.items[1].data, b"second");
        assert!(first.has_more);
        assert_eq!(first.last_evaluated_key, Some(receipts[1]));

        let second = observe(store.stream_operations.read_forward(
            stream.clone(),
            first.last_evaluated_key,
            2,
        ))
        .await;
        second.assert_no_warning_or_error();
        let second = second.result.unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].id, receipts[2]);
        assert_eq!(second.items[0].data, b"third");
        assert_eq!(second.items[0].data_type, StreamDataType::Text);
        assert!(!second.has_more);
        assert_eq!(second.last_evaluated_key, Some(receipts[2]));

        let exhausted = observe(store.stream_operations.read_forward(
            stream.clone(),
            second.last_evaluated_key,
            2,
        ))
        .await;
        exhausted.assert_no_warning_or_error();
        assert!(exhausted.result.unwrap().items.is_empty());

        let newest = observe(
            store
                .stream_operations
                .read_backward(stream.clone(), None, 2),
        )
        .await;
        newest.assert_no_warning_or_error();
        let newest = newest.result.unwrap();
        assert_eq!(newest.items.len(), 2);
        assert_eq!(newest.items[0].id, receipts[2]);
        assert_eq!(newest.items[0].data, b"third");
        assert_eq!(newest.items[1].id, receipts[1]);
        assert_eq!(newest.items[1].data, b"second");
        assert!(newest.has_more);
        assert_eq!(newest.last_evaluated_key, Some(receipts[1]));

        let oldest = observe(store.stream_operations.read_backward(
            stream,
            newest.last_evaluated_key,
            2,
        ))
        .await;
        oldest.assert_no_warning_or_error();
        let oldest = oldest.result.unwrap();
        assert_eq!(oldest.items.len(), 1);
        assert_eq!(oldest.items[0].id, receipts[0]);
        assert_eq!(oldest.items[0].data, b"first");
        assert!(!oldest.has_more);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
