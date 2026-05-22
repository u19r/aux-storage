use storage_types::AttributeValue;

use crate::{
    helpers::deserialize_item_from_bytes,
    kv_support_tests::{TestStore, cleanup_store, create_test_store},
    sorted_kv_store::{
        DirectWriteOperation, OldNewItems, RangeResult, SortedKvStore, TransactWriteOperation,
        TransactWriteOutput,
    },
};

async fn run_get_range_test<S>(store: S)
where S: SortedKvStore {
    // Insert test data
    store.clone().put(b"key1", b"value1", None).await.unwrap();
    store.clone().put(b"key2", b"value2", None).await.unwrap();
    store.clone().put(b"key3", b"value3", None).await.unwrap();
    store.clone().put(b"key4", b"value4", None).await.unwrap();
    store.clone().put(b"key5", b"value5", None).await.unwrap();
    store
        .clone()
        .put(b"otherkey", b"othervalue", None)
        .await
        .unwrap();

    let mut item1 = std::collections::HashMap::new();
    item1.insert("id".to_string(), AttributeValue::S("key1".to_string()));
    let item1_bytes = storage_types::storage_serde::to_bytes(&item1).unwrap();
    store
        .clone()
        .put(b"table/1/key1", &item1_bytes, None)
        .await
        .unwrap();

    let mut item2 = std::collections::HashMap::new();
    item2.insert("id".to_string(), AttributeValue::S("key2".to_string()));
    let item2_bytes = storage_types::storage_serde::to_bytes(&item2).unwrap();
    store
        .clone()
        .put(b"table/1/key2", &item2_bytes, None)
        .await
        .unwrap();

    let mut item3 = std::collections::HashMap::new();
    item3.insert("id".to_string(), AttributeValue::S("key3".to_string()));
    let item3_bytes = storage_types::storage_serde::to_bytes(&item3).unwrap();
    store
        .clone()
        .put(b"table/1/key3", &item3_bytes, None)
        .await
        .unwrap();

    let result = store
        .clone()
        .get_range(
            b"table/1/key1",
            b"table/1/key4",
            Some(2),
            None::<storage_types::ItemKey>,
            true,
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);

    if let Ok(val) = deserialize_item_from_bytes(&result.items[0].1) {
        assert_eq!(
            val.get("id")
                .map(|v| if let AttributeValue::S(s) = v {
                    s
                } else {
                    panic!("Expected id to be a string")
                })
                .unwrap(),
            "key1"
        );
    } else {
        panic!("Expected first item to have id='key1'");
    }

    let result_no_limit = store
        .clone()
        .get_range(
            b"table/1/key1",
            b"table/1/key4",
            None,
            None::<storage_types::ItemKey>,
            true,
        )
        .await
        .unwrap();
    assert_eq!(result_no_limit.items.len(), 3);

    store
        .delete_prefix(Vec::new())
        .await
        .expect("cleanup delete_prefix");
}

#[tokio::test]
async fn get_range() {
    let store: TestStore = create_test_store();
    run_get_range_test(store.clone()).await;
    cleanup_store(&store).await;
}

#[test]
fn range_result_discards_storage_keys_when_callers_only_need_values() {
    let result = RangeResult {
        items: vec![
            (
                b"key-1".to_vec().into_boxed_slice(),
                b"value-1".to_vec().into_boxed_slice(),
            ),
            (
                b"key-2".to_vec().into_boxed_slice(),
                b"value-2".to_vec().into_boxed_slice(),
            ),
        ],
        has_more: true,
    };

    let values = result.into_values_result();

    assert_eq!(
        values.values,
        vec![b"value-1".to_vec(), b"value-2".to_vec()]
    );
    assert!(values.has_more);
}

#[test]
fn direct_write_operations_map_to_transaction_operations_without_conditions() {
    let cases = vec![
        (
            DirectWriteOperation::Put {
                key: b"put-key".to_vec(),
                value: b"value".to_vec(),
            },
            "put-key",
        ),
        (
            DirectWriteOperation::Delete {
                key: b"delete-key".to_vec(),
            },
            "delete-key",
        ),
        (
            DirectWriteOperation::DeleteRange {
                start: b"range-start".to_vec(),
                exclusive_end: b"range-end".to_vec(),
            },
            "range-start",
        ),
    ];

    for (direct, expected_key) in cases {
        let mapped = TransactWriteOperation::from(direct);
        match mapped {
            TransactWriteOperation::Put { key, condition, .. }
            | TransactWriteOperation::Delete { key, condition } => {
                assert_eq!(key, expected_key.as_bytes());
                assert!(condition.is_none());
            }
            _ => panic!("unexpected mapped operation"),
        }
    }
}

#[test]
fn direct_check_value_preserves_expected_value_for_transaction_write() {
    let mapped = TransactWriteOperation::from(DirectWriteOperation::CheckValue {
        key: b"message-key".to_vec(),
        expected_value: Some(b"stored-value".to_vec()),
    });

    assert!(
        matches!(
            mapped,
            TransactWriteOperation::CheckValue {
                key,
                expected_value: Some(value)
            } if key == b"message-key" && value == b"stored-value"
        ),
        "check value should preserve key and expected value"
    );
}

#[test]
fn transact_write_output_starts_with_no_placeholder_versions() {
    let items: Vec<OldNewItems> = vec![(None, None)];

    let output = TransactWriteOutput::new(items);

    assert_eq!(output.items, vec![(None, None)]);
    assert!(output.placeholder_versions.is_empty());
}
