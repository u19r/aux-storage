use std::collections::HashMap;

use storage_common::GSI_UPDATE_JOB;
use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, StreamItemId, TableName, UserStreamName,
};
use stream_provider::CursorName;
#[cfg(test)]
use stream_provider::{CursorPosition, StreamProvider};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::{
    kv_support_tests::{TestProvider, create_test_provider as make_test_provider},
    sorted_kv_store::SortedKvStore as _,
};

async fn create_test_provider() -> TestProvider {
    let provider = make_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    provider
}

#[tokio::test]
async fn stream_initialization() {
    let _provider = create_test_provider().await;
    // If we get here without panicking, initialization worked
}

#[tokio::test]
async fn create_stream() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Stream should exist immediately after creation
    let stream = provider.get_stream(user_stream_name.clone()).await.unwrap();
    assert!(stream.is_some());
    let stream = stream.unwrap();
    assert_eq!(stream.name.as_str(), *user_stream_name);
    assert_eq!(stream.internal_id, stream_name);
    assert_eq!(stream.ttl_seconds, Some(3600.into()));

    // Add an item to the stream
    provider
        .append_item(stream_name.clone(), b"test data", None)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_duplicate_stream() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600u32.into()),
            Default::default(),
        )
        .await;
    let Ok(_stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Duplicate stream creation should now fail
    let result = provider
        .create_stream(user_stream_name.clone(), None, Default::default())
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn append_and_read_items() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Append items
    let item1_id = provider
        .append_item(stream_name.clone(), b"item1 data", None)
        .await
        .unwrap();
    let item2_id = provider
        .append_item(stream_name.clone(), b"item2 data", None)
        .await
        .unwrap();

    // Read forward
    let page = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, item1_id);
    assert_eq!(page.items[1].id, item2_id);
    assert_eq!(page.items[0].data, b"item1 data");
    assert_eq!(page.items[1].data, b"item2 data");
    assert!(!page.has_more);
    assert!(page.last_evaluated_key.is_some());
}

#[tokio::test]
async fn stream_key_suffix_matches_id() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let stream_name = provider
        .create_stream(
            user_stream_name.clone(),
            Some(60.into()),
            Default::default(),
        )
        .await
        .unwrap();

    let assigned = provider
        .append_item(stream_name.clone(), b"payload", None)
        .await
        .unwrap();

    let mut prefix: Vec<u8> = (&stream_name).into();
    prefix.push(b'/');
    let range = provider
        .kv_store
        .get_prefix(&prefix, true, Some(1), true)
        .await
        .unwrap();
    assert!(!range.items.is_empty());
    let (key, value) = &range.items[0];
    assert!(!value.is_empty());
    assert_eq!(
        &key[key.len() - assigned.as_bytes().len()..],
        assigned.as_bytes(),
    );
}

#[tokio::test]
async fn read_backward() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Append items
    let item1_id = provider
        .append_item(stream_name.clone(), b"item1 data", None)
        .await
        .unwrap();
    let item2_id = provider
        .append_item(stream_name.clone(), b"item2 data", None)
        .await
        .unwrap();

    // Read backward (should get newest first)
    let page = provider
        .read_backward(stream_name.clone(), None, 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, item2_id);
    assert_eq!(page.items[1].id, item1_id);
}

#[tokio::test]
async fn read_backward_with_page_token() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Append multiple items
    let item1_id = provider
        .append_item(stream_name.clone(), b"item1 data", None)
        .await
        .unwrap();
    let item2_id = provider
        .append_item(stream_name.clone(), b"item2 data", None)
        .await
        .unwrap();
    let item3_id = provider
        .append_item(stream_name.clone(), b"item3 data", None)
        .await
        .unwrap();

    // Read backward with page_token = item3_id (should exclude item3 and return
    // item2, item1)
    let page = provider
        .read_backward(stream_name.clone(), Some(item3_id), 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, item2_id); // Newest first
    assert_eq!(page.items[1].id, item1_id);

    // Read backward with page_token = item2_id (should exclude item3, item2 and
    // return item1)
    let page2 = provider
        .read_backward(stream_name.clone(), Some(item2_id), 10)
        .await
        .unwrap();

    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.items[0].id, item1_id);

    // Read backward with page_token = item1_id (should exclude all items)
    let page3 = provider
        .read_backward(stream_name.clone(), Some(item1_id), 10)
        .await
        .unwrap();

    assert_eq!(page3.items.len(), 0);
}

#[tokio::test]
async fn create_cursor_at_head() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    // Add some items
    provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();
    provider
        .append_item(stream_name.clone(), b"item2", None)
        .await
        .unwrap();

    // Create cursor at head
    let result = provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await;
    assert!(result.is_ok());

    // Verify cursor exists
    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .unwrap();
    assert!(cursor.is_some());
    let cursor = cursor.unwrap();
    assert_eq!(cursor.name.as_str(), *cursor_name);
    assert_eq!(cursor.stream_name, stream_name);
}

#[tokio::test]
async fn create_duplicate_cursor() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();

    let result = provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn read_from_cursor() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    // Add some items
    provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();
    provider
        .append_item(stream_name.clone(), b"item2", None)
        .await
        .unwrap();

    // Create cursor at tail (after the last item)
    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Tail,
        )
        .await
        .unwrap();

    // Add more items after cursor creation
    let item3_id = provider
        .append_item(stream_name.clone(), b"item3", None)
        .await
        .unwrap();
    let item4_id = provider
        .append_item(stream_name.clone(), b"item4", None)
        .await
        .unwrap();

    // Read from cursor (should get items after cursor position)
    let page = provider
        .read_from_cursor(stream_name.clone(), cursor_name.clone(), 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, item3_id);
    assert_eq!(page.items[1].id, item4_id);
    assert!(!page.has_more);

    // Advance cursor to the last read item
    provider
        .advance_cursor(stream_name.clone(), cursor_name.clone(), item4_id)
        .await
        .unwrap();

    // Verify cursor position was updated
    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.position, item4_id);
}

#[tokio::test]
async fn advance_cursor() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    // Add some items
    let _item1_id = provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();
    let item2_id = provider
        .append_item(stream_name.clone(), b"item2", None)
        .await
        .unwrap();
    let item3_id = provider
        .append_item(stream_name.clone(), b"item3", None)
        .await
        .unwrap();

    // Create cursor at head (pointing to item1)
    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();

    // Advance cursor to item2
    provider
        .advance_cursor(stream_name.clone(), cursor_name.clone(), item2_id)
        .await
        .unwrap();

    // Verify cursor position
    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.position, item2_id);

    // Reading from cursor should now start after item2 (get item3)
    let page = provider
        .read_from_cursor(stream_name.clone(), cursor_name.clone(), 10)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, item3_id);
}

#[tokio::test]
async fn advance_cursor_errors() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    let item_id = provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();

    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();

    // Advancing a missing cursor must fail.
    let result = provider
        .advance_cursor(
            stream_name.clone(),
            CursorName::new("DoesNotExist"),
            item_id,
        )
        .await;
    assert!(result.is_err());

    // Advancing to a missing item must fail.
    let missing_item_id = StreamItemId::from(Uuid::new_v4());
    let result = provider
        .advance_cursor(stream_name.clone(), cursor_name, missing_item_id)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn delete_cursor() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();

    // Delete cursor
    let result = provider
        .delete_cursor(stream_name.clone(), cursor_name.clone())
        .await;
    assert!(result.is_ok());

    // Verify cursor is gone
    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name)
        .await
        .unwrap();
    assert!(cursor.is_none());
}

#[tokio::test]
async fn delete_stream() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };
    let cursor_name = CursorName::new("consumer1");

    provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();

    provider
        .create_cursor(
            stream_name.clone(),
            cursor_name.clone(),
            CursorPosition::Head,
        )
        .await
        .unwrap();

    // Delete stream
    let result = provider.delete_stream(user_stream_name.clone()).await;
    assert!(result.is_ok(), "Failed to delete stream: {result:?}");

    // Verify stream and related data are gone
    let stream = provider.get_stream(user_stream_name.clone()).await.unwrap();
    assert!(stream.is_none());

    let cursor = provider
        .get_cursor(stream_name.clone(), cursor_name.clone())
        .await
        .unwrap();
    assert!(
        cursor.is_none(),
        "cursor not deleted with stream, {cursor:?}"
    );
}

#[tokio::test]
async fn pagination() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("stream-name");

    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(3600.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    for i in 0..10 {
        let data = format!("item{i}").into_bytes();
        let _id = provider
            .append_item(stream_name.clone(), &data, None)
            .await
            .unwrap();
    }

    // Read with small limit
    let page1 = provider
        .read_forward(stream_name.clone(), None, 3)
        .await
        .unwrap();

    assert_eq!(page1.items.len(), 3);
    assert!(page1.has_more);
    assert!(page1.last_evaluated_key.is_some());

    // Read next page
    let page2 = provider
        .read_forward(stream_name.clone(), page1.last_evaluated_key, 3)
        .await
        .unwrap();

    assert_eq!(page2.items.len(), 3);
    assert!(page2.has_more);

    // Verify we got different items
    assert_ne!(page1.items[0].id, page2.items[0].id);
}

#[tokio::test]
async fn ttl_cleanup_job_registration() {
    let _provider = create_test_provider().await;

    // The TTL cleanup job should be registered during initialization
    // We can verify this by checking that the job manager has the job
    // Since we don't have a public API to list jobs, we'll just ensure
    // initialization completes without error
}

#[tokio::test]
async fn ttl_cleanup_expired_items() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("ttl-stream");

    // Create a stream with TTL
    let ttl_seconds = 1u32; // 1 second TTL for testing
    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(ttl_seconds.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Add some items
    let _item1_id = provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();
    let _item2_id = provider
        .append_item(stream_name.clone(), b"item2", None)
        .await
        .unwrap();

    // Verify items exist
    let page = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);

    // Wait for TTL to expire
    sleep(Duration::from_secs(2)).await;

    // Run cleanup
    let cleaned_count = provider.cleanup_expired_items().await.unwrap();
    assert_eq!(cleaned_count, 2);

    // Verify items are gone
    let page = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 0);
}

#[tokio::test]
async fn ttl_cleanup_mixed_expiration() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("mixed-ttl-stream");

    // Create a stream with short TTL
    let ttl_seconds = 1u32;
    let result = provider
        .create_stream(
            user_stream_name.clone(),
            Some(ttl_seconds.into()),
            Default::default(),
        )
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Add items
    provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();
    provider
        .append_item(stream_name.clone(), b"item2", None)
        .await
        .unwrap();

    // Wait for TTL to expire on first two items
    sleep(Duration::from_secs(2)).await;

    // Add a fresh item
    provider
        .append_item(stream_name.clone(), b"fresh_item", None)
        .await
        .unwrap();

    // Run cleanup
    let cleaned_count = provider.cleanup_expired_items().await.unwrap();
    assert_eq!(cleaned_count, 2); // Only the first two should be cleaned

    // Verify only fresh item remains
    let page = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data, b"fresh_item");
}

#[tokio::test]
async fn gsi_key_creation() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("gsi-test-stream");

    // Create a stream
    let result = provider
        .create_stream(user_stream_name.clone(), None, Default::default())
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Add an item
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("test-pk".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("test-sk".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("gsi-test-pk".to_string()),
    );
    item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S("gsi-test-sk".to_string()),
    );

    provider
        .append_item(stream_name.clone(), b"test data", None)
        .await
        .unwrap();

    // Test GSI key creation (this would normally be done by the background job)
    // For now, just ensure the basic functionality works
}

#[tokio::test]
async fn gsi_update_job_execution() {
    let provider = create_test_provider().await;

    let table = TableName::new("GsiUpdateJobExecution");
    let request = CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    provider.create_table(&request).await.unwrap();
    provider
        .put_item(
            table,
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                ("sk".to_string(), AttributeValue::S("item".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("group".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Test that the registered job path processes queued GSI work.
    let result = provider.run_job(GSI_UPDATE_JOB).await;
    assert!(result.is_ok(), "GSI update processing failed: {result:?}");
}

#[tokio::test]
async fn ttl_cleanup_no_ttl_stream() {
    let provider = create_test_provider().await;
    let user_stream_name = UserStreamName::new("no-ttl-stream");

    // Create a stream without TTL
    let result = provider
        .create_stream(user_stream_name.clone(), None, Default::default())
        .await;
    let Ok(stream_name) = result else {
        panic!("failed to create stream: {result:?}");
    };

    // Add items
    provider
        .append_item(stream_name.clone(), b"item1", None)
        .await
        .unwrap();

    // Run cleanup
    let cleaned_count = provider.cleanup_expired_items().await.unwrap();
    assert_eq!(cleaned_count, 0); // No items should be cleaned

    // Verify item still exists
    let page = provider
        .read_forward(stream_name.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
}

#[tokio::test]
async fn initialize_stream_idempotent() {
    let provider = create_test_provider().await;
    // initialize_stream is called once inside create_test_provider
    provider.initialize_stream().await.unwrap();
    provider.initialize_stream().await.unwrap();
}

#[tokio::test]
async fn start_cleanup_task_idempotent() {
    let provider = create_test_provider().await;
    provider.start_cleanup_task(1).await.unwrap();
    provider.start_cleanup_task(1).await.unwrap();
    provider.start_cleanup_task(1).await.unwrap();
}

#[tokio::test]
async fn stop_cleanup_task_idempotent() {
    let provider = create_test_provider().await;
    // stopping when not running should be Ok
    provider.stop_cleanup_task().await.unwrap();
    // starting then stopping multiple times should be Ok
    provider.start_cleanup_task(1).await.unwrap();
    provider.stop_cleanup_task().await.unwrap();
    provider.stop_cleanup_task().await.unwrap();
}
