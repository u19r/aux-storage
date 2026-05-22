use storage_types::{StorageEnum, StreamItemId, UserStreamName};
use stream_provider::{CursorName, StreamDataType, StreamEnum};
#[cfg(test)]
use stream_provider::{CursorPosition, StreamProvider};
use uuid::Uuid;

use crate::{SQLiteStorageProvider, sql_statements};

async fn create_test_provider() -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider.initialize_stream().await.unwrap();
    provider
}

fn assert_stream_unsupported_contains(err: &stream_provider::StreamError, expected: &str) {
    let StreamEnum::StorageError(storage_error) = err.as_ref() else {
        panic!("expected storage error, got {err:?}");
    };
    let StorageEnum::Unsupported { message } = storage_error.as_ref() else {
        panic!("expected unsupported storage error, got {storage_error:?}");
    };
    assert!(
        message.contains(expected),
        "expected '{message}' to contain '{expected}'"
    );
}

#[tokio::test]
async fn stream_initialization() {
    let _provider = create_test_provider().await;
    // If we get here without panicking, initialization worked
}

#[tokio::test]
async fn stream_initialization_writes_item_versioned_format_metadata() {
    let provider = create_test_provider().await;

    let version = provider
        .connection
        .call_unwrap(|conn| {
            conn.query_row(
                "SELECT format_version FROM sys_stream_format_metadata WHERE format_key = \
                 'item_versioned_stream'",
                [],
                |row| row.get::<_, i64>(0),
            )
        })
        .await
        .unwrap();

    assert_eq!(version, 1);
}

#[tokio::test]
async fn stream_initialization_rejects_nonempty_stream_items_without_format_metadata() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider
        .connection
        .call_unwrap(|conn| {
            let (sql, params) = sql_statements::create_user_streams_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_items_table();
            conn.execute(sql, params)?;
            conn.execute(
                "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
                 VALUES ('old-stream', 'old-id', x'01', 1, 2)",
                [],
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old stream rows without format metadata should be rejected");

    assert_stream_unsupported_contains(&err, "in-place upgrade");
}

#[tokio::test]
async fn stream_initialization_rejects_missing_metadata_without_migration_or_payload() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider
        .connection
        .call_unwrap(|conn| {
            let (sql, params) = sql_statements::create_user_streams_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_items_table();
            conn.execute(sql, params)?;
            conn.execute(
                "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
                 VALUES ('old-stream', 'old-id', x'534543524554', 1, 2)",
                [],
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old stream rows without format metadata should be rejected");
    assert_stream_unsupported_contains(&err, "in-place upgrade");
    let err_text = format!("{err}");
    assert!(!err_text.contains("SECRET"));
    assert!(!err_text.contains("534543524554"));

    let (metadata_rows, stream_rows) = provider
        .connection
        .call_unwrap(|conn| {
            let metadata_rows = conn.query_row(
                "SELECT COUNT(*) FROM sys_stream_format_metadata",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let stream_rows = conn.query_row(
                "SELECT COUNT(*) FROM sys_stream_items WHERE stream_name = 'old-stream' AND \
                 item_id = 'old-id'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok::<_, rusqlite::Error>((metadata_rows, stream_rows))
        })
        .await
        .unwrap();
    assert_eq!(metadata_rows, 0);
    assert_eq!(stream_rows, 1);
}

#[tokio::test]
async fn stream_initialization_rejects_incompatible_format_metadata() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    provider
        .connection
        .call_unwrap(|conn| {
            let (sql, params) = sql_statements::create_user_streams_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_items_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_format_metadata_table();
            conn.execute(sql, params)?;
            conn.execute(
                "INSERT INTO sys_stream_format_metadata (format_key, format_version) VALUES \
                 ('item_versioned_stream', 999)",
                [],
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("incompatible format metadata should be rejected");

    assert_stream_unsupported_contains(&err, "unsupported stream format metadata version");
}

#[tokio::test]
async fn stream_initialization_rejects_old_format_pointer_payload_with_metadata() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    let old_pointer = serde_json::json!({
        "type": "pointer",
        "stream_name": "item-stream",
        "table_name": "OldPointerTable"
    });
    let old_pointer_bytes = storage_types::storage_serde::to_bytes(&old_pointer).unwrap();

    provider
        .connection
        .call_unwrap(move |conn| {
            let (sql, params) = sql_statements::create_user_streams_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_items_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_format_metadata_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::upsert_stream_format_version();
            conn.execute(sql, params)?;
            conn.execute(
                "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    "system",
                    "pointer-id",
                    old_pointer_bytes.as_slice(),
                    1_i64,
                    StreamDataType::StreamPointer as i32,
                ),
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old pointer payload should be rejected");

    assert_stream_unsupported_contains(&err, "old-format stream pointer payload");
}

#[tokio::test]
async fn stream_initialization_rejects_old_pointer_without_repair_or_payload() {
    let provider = SQLiteStorageProvider::new(":memory:").await.unwrap();
    let old_pointer = serde_json::json!({
        "type": "pointer",
        "stream_name": "secret-item-stream",
        "table_name": "SecretPointerTable"
    });
    let old_pointer_bytes = storage_types::storage_serde::to_bytes(&old_pointer).unwrap();

    provider
        .connection
        .call_unwrap(move |conn| {
            let (sql, params) = sql_statements::create_user_streams_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_items_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::create_stream_format_metadata_table();
            conn.execute(sql, params)?;
            let (sql, params) = sql_statements::upsert_stream_format_version();
            conn.execute(sql, params)?;
            conn.execute(
                "INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    "system",
                    "pointer-id",
                    old_pointer_bytes.as_slice(),
                    1_i64,
                    StreamDataType::StreamPointer as i32,
                ),
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .unwrap();

    let err = provider
        .initialize_stream()
        .await
        .expect_err("old pointer payload should be rejected");
    assert_stream_unsupported_contains(&err, "old-format stream pointer payload");
    let err_text = format!("{err}");
    assert!(!err_text.contains("SecretPointerTable"));
    assert!(!err_text.contains("secret-item-stream"));

    let stream_rows = provider
        .connection
        .call_unwrap(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sys_stream_items WHERE stream_name = 'system' AND item_id = \
                 'pointer-id'",
                [],
                |row| row.get::<_, i64>(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(stream_rows, 1);
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
    assert!(cursor.is_none());
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
