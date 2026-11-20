use storage_observability_test_support::observation::observe;
use storage_types::{StorageEnum, StorageError, StreamItemId, UserStreamName};
use stream_provider::{CursorName, CursorPosition, ReadDirection, StreamPartitioningMode};

use crate::DatabaseManager;

const CURSOR_CHARACTERS_ERROR: &str = "Cursor name can only contain alphanumeric characters, _, -";
const CURSOR_BOUNDARY_ERROR: &str = "Cursor name must start and end with alphanumeric characters";
const STREAM_BOUNDARY_ERROR: &str = "Stream name must start and end with alphanumeric characters";
const TTL_ERROR: &str = "TTL must be between 1 second and 1 year (31,536,000 seconds)";

fn user(name: &str) -> UserStreamName {
    UserStreamName::new(name)
}

fn cursor(name: &str) -> CursorName {
    CursorName::new(name)
}

#[track_caller]
fn assert_validation(error: &StorageError, expected: &str) {
    match error.as_ref() {
        StorageEnum::Validation { message, .. } => assert_eq!(message, expected),
        other => panic!("expected validation, got {other:?}"),
    }
}

#[track_caller]
fn assert_stream_not_found(error: &StorageError, expected_name: &str) {
    match error.as_ref() {
        StorageEnum::ResourceNotFound {
            resource_type,
            resource_id,
        } => {
            assert_eq!(*resource_type, "stream");
            assert_eq!(resource_id, expected_name);
        }
        other => panic!("expected stream not found, got {other:?}"),
    }
}

fn data_of(items: &[stream_provider::StreamItem]) -> Vec<&[u8]> {
    items.iter().map(|item| item.data.as_slice()).collect()
}

#[tokio::test]
async fn given_user_stream_when_full_lifecycle_runs_through_the_entry_then_each_call_answers_by_name()
 {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let orders = user("orders");
    let id = database
        .create_user_stream(orders.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    let found = database
        .get_user_stream(orders.clone())
        .await
        .unwrap()
        .expect("stream exists");
    assert_eq!(found.internal_id, id);

    let mut ids = Vec::new();
    for data in [b"one".as_slice(), b"two", b"three"] {
        ids.push(
            database
                .append_user_stream_item(orders.clone(), data.to_vec(), None)
                .await
                .unwrap(),
        );
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

    let forward = database
        .read_user_stream(orders.clone(), ReadDirection::Forward, None, None)
        .await
        .unwrap();
    assert_eq!(
        data_of(&forward.items),
        [b"one".as_slice(), b"two", b"three"]
    );
    assert!(!forward.has_more);
    let backward = database
        .read_user_stream(orders.clone(), ReadDirection::Backward, None, Some(2))
        .await
        .unwrap();
    assert_eq!(data_of(&backward.items), [b"three".as_slice(), b"two"]);
    assert!(backward.has_more);

    database
        .create_user_stream_cursor(orders.clone(), cursor("reader"), CursorPosition::Head)
        .await
        .unwrap();
    let page = database
        .read_user_stream_cursor(orders.clone(), cursor("reader"), Some(2))
        .await
        .unwrap();
    assert_eq!(data_of(&page.items), [b"one".as_slice(), b"two"]);
    let stored = database
        .get_user_stream_cursor(orders.clone(), cursor("reader"))
        .await
        .unwrap()
        .expect("cursor exists");
    assert_eq!(stored.position, ids[1]);
    database
        .advance_user_stream_cursor(orders.clone(), cursor("reader"), ids[2])
        .await
        .unwrap();
    let drained = database
        .read_user_stream_cursor(orders.clone(), cursor("reader"), Some(10))
        .await
        .unwrap();
    assert!(drained.items.is_empty());
    database
        .delete_user_stream_cursor(orders.clone(), cursor("reader"))
        .await
        .unwrap();
    assert!(
        database
            .get_user_stream_cursor(orders.clone(), cursor("reader"))
            .await
            .unwrap()
            .is_none()
    );
    database.delete_user_stream(orders.clone()).await.unwrap();
    assert!(database.get_user_stream(orders).await.unwrap().is_none());
}

#[tokio::test]
async fn given_invalid_stream_names_when_created_then_validation_rejects_before_any_write() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let too_long = "é".repeat(128);
    for (name, message) in [
        ("", "Stream name cannot be empty"),
        ("_a", STREAM_BOUNDARY_ERROR),
        ("a_", STREAM_BOUNDARY_ERROR),
        (
            "a!_",
            "Stream name can only contain alphanumeric characters, _, -, .",
        ),
        (
            too_long.as_str(),
            "Stream name must not exceed 255 characters",
        ),
    ] {
        let error = database
            .create_user_stream(user(name), None, StreamPartitioningMode::Single)
            .await
            .unwrap_err();
        assert_validation(&error, message);
        assert!(
            database
                .get_user_stream(user(name))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn given_ttl_out_of_bounds_when_created_then_validation_rejects_and_bounds_are_accepted() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    for ttl in [0, 31_536_001] {
        let error = database
            .create_user_stream(
                user("expiring"),
                Some(ttl.into()),
                StreamPartitioningMode::Single,
            )
            .await
            .unwrap_err();
        assert_validation(&error, TTL_ERROR);
    }
    database
        .create_user_stream(
            user("yearly"),
            Some(31_536_000.into()),
            StreamPartitioningMode::Single,
        )
        .await
        .unwrap();
    let stream = database
        .get_user_stream(user("yearly"))
        .await
        .unwrap()
        .expect("stream exists");
    assert_eq!(stream.ttl_seconds, Some(31_536_000.into()));
}

#[tokio::test]
async fn given_invalid_cursor_names_when_created_then_validation_rejects_even_on_a_missing_stream()
{
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    database
        .create_user_stream(user("events"), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    let too_long = format!("{}a", "é".repeat(32));
    for (name, message) in [
        ("", "Cursor name cannot be empty"),
        ("a.b", CURSOR_CHARACTERS_ERROR),
        ("-a", CURSOR_BOUNDARY_ERROR),
        ("a-", CURSOR_BOUNDARY_ERROR),
        (
            too_long.as_str(),
            "Cursor name must not exceed 64 characters",
        ),
    ] {
        let error = database
            .create_user_stream_cursor(user("events"), cursor(name), CursorPosition::Head)
            .await
            .unwrap_err();
        assert_validation(&error, message);
    }
    let error = database
        .create_user_stream_cursor(user("missing"), cursor("a.b"), CursorPosition::Head)
        .await
        .unwrap_err();
    assert_validation(&error, CURSOR_CHARACTERS_ERROR);
}

#[tokio::test]
async fn given_missing_stream_when_each_named_operation_runs_then_stream_not_found() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let missing = || user("missing");
    let reader = || cursor("reader");
    let errors = [
        database
            .append_user_stream_item(missing(), b"x".to_vec(), None)
            .await
            .unwrap_err(),
        database.delete_user_stream(missing()).await.unwrap_err(),
        database
            .read_user_stream(missing(), ReadDirection::Forward, None, None)
            .await
            .unwrap_err(),
        database
            .read_user_stream(missing(), ReadDirection::Backward, None, None)
            .await
            .unwrap_err(),
        database
            .create_user_stream_cursor(missing(), reader(), CursorPosition::Head)
            .await
            .unwrap_err(),
        database
            .get_user_stream_cursor(missing(), reader())
            .await
            .unwrap_err(),
        database
            .delete_user_stream_cursor(missing(), reader())
            .await
            .unwrap_err(),
        database
            .advance_user_stream_cursor(missing(), reader(), StreamItemId::from([255; 12]))
            .await
            .unwrap_err(),
        database
            .read_user_stream_cursor(missing(), reader(), None)
            .await
            .unwrap_err(),
    ];
    for error in &errors {
        assert_stream_not_found(error, "missing");
    }
    assert!(database.get_user_stream(missing()).await.unwrap().is_none());
}

#[tokio::test]
async fn given_limits_when_reading_then_default_is_one_hundred_and_out_of_range_is_rejected() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let paged = user("paged");
    database
        .create_user_stream(paged.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    for index in 0..101 {
        database
            .append_user_stream_item(paged.clone(), format!("item-{index}").into_bytes(), None)
            .await
            .unwrap();
    }
    let page = database
        .read_user_stream(paged.clone(), ReadDirection::Forward, None, None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 100);
    assert!(page.has_more);
    for limit in [0, 1001] {
        let error = database
            .read_user_stream(paged.clone(), ReadDirection::Forward, None, Some(limit))
            .await
            .unwrap_err();
        assert_validation(&error, "invalid limit");
    }
    database
        .create_user_stream_cursor(paged.clone(), cursor("reader"), CursorPosition::Head)
        .await
        .unwrap();
    let error = database
        .read_user_stream_cursor(paged.clone(), cursor("reader"), Some(0))
        .await
        .unwrap_err();
    assert_validation(&error, "invalid limit");
    let page = database
        .read_user_stream_cursor(paged, cursor("reader"), None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 100);
}

#[tokio::test]
async fn given_missing_or_recreated_stream_when_reading_then_current_incarnation_is_read() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let name = user("é-stream");
    let error = database
        .append_user_stream_item(name.clone(), b"payload".to_vec(), None)
        .await
        .unwrap_err();
    assert_stream_not_found(&error, "é-stream");
    let old = database
        .create_user_stream(name.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    database
        .append_user_stream_item(name.clone(), b"old payload".to_vec(), None)
        .await
        .unwrap();
    database
        .create_user_stream_cursor(name.clone(), cursor("reader"), CursorPosition::Head)
        .await
        .unwrap();
    database.delete_user_stream(name.clone()).await.unwrap();
    let new = database
        .create_user_stream(name.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    assert_ne!(old, new);
    let page = database
        .read_user_stream(name.clone(), ReadDirection::Forward, None, None)
        .await
        .unwrap();
    assert!(page.items.is_empty());
    // The cursor belonged to the old incarnation; the name resolves to the new
    // one on the operation's own attempt.
    assert!(
        database
            .get_user_stream_cursor(name, cursor("reader"))
            .await
            .unwrap()
            .is_none()
    );
    database
        .create_user_stream(user("a!b"), None, StreamPartitioningMode::Single)
        .await
        .unwrap_err();
    assert!(
        database
            .get_user_stream(user("a!b"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn given_key_ordered_request_on_sqlite_when_created_then_single_partition_reads_back() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let name = user("partitioned");
    database
        .create_user_stream(name.clone(), None, StreamPartitioningMode::KeyOrdered)
        .await
        .unwrap();
    let stream = database
        .get_user_stream(name.clone())
        .await
        .unwrap()
        .expect("stream exists");
    assert_eq!(stream.partitioning_mode, StreamPartitioningMode::Single);
    let id = database
        .append_user_stream_item(
            name.clone(),
            b"payload".to_vec(),
            Some("customer-1".to_owned()),
        )
        .await
        .unwrap();
    let page = database
        .read_user_stream(name, ReadDirection::Forward, None, Some(10))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, id);
    assert_eq!(page.items[0].data, b"payload");
}

#[tokio::test]
async fn given_payload_out_of_bounds_when_appended_then_validation_rejects() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let payloads = user("payloads");
    database
        .create_user_stream(payloads.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    let error = database
        .append_user_stream_item(payloads.clone(), Vec::new(), None)
        .await
        .unwrap_err();
    assert_validation(&error, "item data cannot be empty");
    let error = database
        .append_user_stream_item(payloads.clone(), vec![0; 1_048_577], None)
        .await
        .unwrap_err();
    assert_validation(&error, "item data too large");
    database
        .append_user_stream_item(payloads, vec![0; 1_048_576], None)
        .await
        .unwrap();
}

#[tokio::test]
async fn given_private_stream_and_cursor_when_items_are_consumed_then_traces_omit_identifiers_and_payload()
 {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let observed = observe(async {
        let name = user("private-stream-log-marker");
        database
            .create_user_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .unwrap();
        let id = database
            .append_user_stream_item(name.clone(), b"private-stream-body-marker".to_vec(), None)
            .await
            .unwrap();
        database
            .create_user_stream_cursor(
                name.clone(),
                cursor("private-cursor-log-marker"),
                CursorPosition::Head,
            )
            .await
            .unwrap();
        let page = database
            .read_user_stream_cursor(name, cursor("private-cursor-log-marker"), Some(10))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        id
    })
    .await;
    let rendered = format!("{:?}{:?}", observed.events, observed.spans);
    for marker in [
        "private-stream-log-marker",
        "private-stream-body-marker",
        "private-cursor-log-marker",
    ] {
        assert!(!rendered.contains(marker), "trace leaked {marker}");
    }
    assert!(!rendered.contains(&observed.result.to_string()));
}
