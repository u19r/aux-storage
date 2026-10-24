//! Stream catalog time through the replacement store's fixture clock.

use std::time::Duration;

use storage_observability_test_support::observation::observe;
use storage_types::UserStreamName;
use stream_provider::StreamPartitioningMode;

use super::test_support::TestStore;

#[tokio::test]
async fn given_advanced_store_time_when_stream_is_created_then_catalog_records_that_time() {
    let store = TestStore::new().await;
    let captured_at = store.advance_time(Duration::from_secs(86_400));
    let name = UserStreamName::new(&format!("clock_{}", uuid::Uuid::new_v4().simple()));
    let created = observe(store.stream_operations.create_stream(
        name.clone(),
        None,
        StreamPartitioningMode::Single,
    ))
    .await;
    created.assert_no_warning_or_error();
    let id = created.result.expect("create user stream");

    let found = observe(store.stream_operations.get_stream(name)).await;
    found.assert_no_warning_or_error();
    let stream = found
        .result
        .expect("read user stream")
        .expect("stream exists");
    assert_eq!(stream.internal_id, id);
    assert_eq!(stream.created_at, captured_at);
    store.finish().await;
}
