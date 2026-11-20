//! A user stream addressed by name resolves its catalog row on the
//! operation's own native attempt: one attempt, and against the same
//! operation by internal id one catalog lookup fewer on KV.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use storage_types::UserStreamName;
use stream_provider::{
    CursorName, CursorPosition, StreamAddress, StreamEnum, StreamPartitioningMode,
};

use super::{native_census_test_support::NativeCensus, test_support::TestStore};

#[tokio::test]
async fn given_user_stream_when_addressed_by_name_then_one_attempt_resolves_the_catalog() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let backend = TestStore::selected_backend();
        let kv = matches!(backend.as_str(), "rocksdb" | "foundationdb");
        let name = UserStreamName::new(&format!("census_{}", uuid::Uuid::new_v4().simple()));
        let stream = store
            .stream_operations
            .create_stream(name.clone(), None, StreamPartitioningMode::Single)
            .await
            .unwrap();
        let mut receipts = Vec::new();
        for data in [b"one".as_slice(), b"two", b"three", b"four"] {
            receipts.push(
                store
                    .stream_operations
                    .append_item(name.clone(), data, None)
                    .await
                    .unwrap(),
            );
        }
        store
            .wait_until_user_item_is_readable(*receipts.last().unwrap())
            .await;
        for cursor in ["by_id", "by_name"] {
            store
                .stream_operations
                .create_cursor(
                    stream.clone().into(),
                    CursorName::new(cursor),
                    CursorPosition::Head,
                )
                .await
                .unwrap();
        }

        // Page read: read-only attempt.
        census.reset();
        let by_id = store
            .stream_operations
            .read_forward(stream.clone().into(), None, 2)
            .await
            .unwrap();
        let id_read = census.snapshot();
        census.reset();
        let by_name = store
            .stream_operations
            .read_forward(StreamAddress::from(name.clone()), None, 2)
            .await
            .unwrap();
        let name_read = census.snapshot();
        let ids = |page: &stream_provider::StreamPage| {
            page.items.iter().map(|item| item.id).collect::<Vec<_>>()
        };
        assert_eq!(ids(&by_name), ids(&by_id));
        assert_eq!(ids(&by_name), receipts[..2].to_vec());
        assert_eq!(
            (id_read.begins, name_read.begins),
            (1, 1),
            "{id_read} / {name_read}"
        );

        // Cursor page read: the attempt commits the cursor advance.
        census.reset();
        let id_page = store
            .stream_operations
            .read_from_cursor(stream.clone().into(), CursorName::new("by_id"), 1)
            .await
            .unwrap();
        let id_cursor = census.snapshot();
        census.reset();
        let name_page = store
            .stream_operations
            .read_from_cursor(name.clone().into(), CursorName::new("by_name"), 1)
            .await
            .unwrap();
        let name_cursor = census.snapshot();
        assert_eq!(id_page.items[0].id, receipts[0]);
        assert_eq!(name_page.items[0].id, receipts[0]);
        assert_eq!(
            (id_cursor.begins, name_cursor.begins),
            (1, 1),
            "{id_cursor} / {name_cursor}"
        );
        assert_eq!(
            (id_cursor.commits, name_cursor.commits),
            (1, 1),
            "{id_cursor} / {name_cursor}"
        );

        if kv {
            assert_eq!(name_read.keys + 1, id_read.keys, "{id_read} / {name_read}");
            assert_eq!(
                name_cursor.keys + 1,
                id_cursor.keys,
                "{id_cursor} / {name_cursor}"
            );
        } else {
            assert_eq!(name_read.waves, id_read.waves, "{id_read} / {name_read}");
            assert_eq!(
                name_read.statements, id_read.statements,
                "{id_read} / {name_read}"
            );
            assert_eq!(
                name_cursor.waves, id_cursor.waves,
                "{id_cursor} / {name_cursor}"
            );
            assert_eq!(
                name_cursor.statements, id_cursor.statements,
                "{id_cursor} / {name_cursor}"
            );
        }
        if backend == "foundationdb" {
            assert_eq!(
                name_read.waves + 1,
                id_read.waves,
                "{id_read} / {name_read}"
            );
            assert_eq!(
                name_cursor.waves + 1,
                id_cursor.waves,
                "{id_cursor} / {name_cursor}"
            );
            assert_eq!(
                name_cursor.conflicts.read_keys + 1,
                id_cursor.conflicts.read_keys,
                "{id_cursor} / {name_cursor}"
            );
        }

        // A missing name: one attempt, stream not found by the user name.
        census.reset();
        let missing = format!("census_missing_{}", uuid::Uuid::new_v4().simple());
        let error = store
            .stream_operations
            .read_forward(UserStreamName::new(&missing).into(), None, 2)
            .await
            .unwrap_err();
        assert_eq!(census.snapshot().begins, 1);
        match error.base_error() {
            Some(StreamEnum::ResourceNotFound {
                resource_type,
                resource_id,
            }) => {
                assert_eq!(*resource_type, "stream");
                assert_eq!(resource_id, &missing);
            }
            other => panic!("expected stream not found, got {other:?}"),
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
