use std::time::Duration;

use foundationdb::tuple::{Bytes, Element, Subspace, pack};
use storage_provider::{
    ReadSequenceExecution, ReadSequenceFlatResult, ReadSequenceMappedEntry,
    ReadSequenceMappedRangePage, ReadSequenceMappedRangeRequest, StorageProvider,
};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchWriteItemRequest, BillingMode,
    CreateGlobalSecondaryIndex, GetItemRequest, GlobalSecondaryIndex, IndexKey, IndexName, ItemKey,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, PutRequest,
    QueryRequest, ReadSequenceConsistency, ReadSequenceFromInput, ReadSequenceInputCardinality,
    ReadSequenceNode, ReadSequenceNodeInput, ReadSequenceNodeOperation, ReadSequenceOnMissing,
    ReadSequenceRequest, ReadSequenceSelector, TableKey, TableName, WireItem, WriteRequest,
    context::WrappedError, plan_read_sequence, read_sequence_string_template,
};

use crate::{
    FoundationDbConfig, FoundationDbKvStore,
    backends::fdb::mapped_range::{is_mapper_bad_index, validate_request},
    keyspace::{table_identity::TableIdentity, table_keys},
    partition_family::PartitionFamilyKvStore,
    sorted_kv_store::SortedKvStore,
    storage_ops::encode_wire_item_storage_bytes,
};

#[test]
fn complete_empty_secondary_ranges_are_valid() {
    let page = ReadSequenceMappedRangePage {
        entries: vec![ReadSequenceMappedEntry {
            parent_key: b"p".to_vec(),
            parent_value: Vec::new(),
            begin: b"a".to_vec(),
            end: b"b".to_vec(),
            key_values: Vec::new(),
        }],
        more: false,
    };
    assert!(!page.more);
    assert!(page.entries[0].key_values.is_empty());
}

fn mapped_range_request() -> foundationdb::RangeOption<'static> {
    foundationdb::RangeOption {
        begin: foundationdb::KeySelector::first_greater_or_equal(vec![1]),
        end: foundationdb::KeySelector::first_greater_or_equal(vec![2]),
        ..foundationdb::RangeOption::default()
    }
}

#[tokio::test]
#[ignore = "requires a live FoundationDB 7.4 cluster"]
async fn given_nil_and_short_value_when_mapping_value_slot_then_746_behavior_is_explicit() {
    let Some(cluster_file_path) = live_cluster_file() else {
        return;
    };
    let store = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: None,
        subspace_prefix: None,
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    })
    .expect("connect FoundationDB 7.4 fixture");
    store
        .check_reachable(Duration::from_secs(3))
        .await
        .expect("reachable FoundationDB fixture");
    let namespace = format!("indexer-v-slot-{}", uuid::Uuid::now_v7());
    let parent = Subspace::all().subspace(&(namespace.as_str(), "parent"));
    let present_key = parent.pack(&("present",));
    let nil_key = parent.pack(&("nil",));
    let present_value = pack(&vec![
        Element::Bytes(Bytes::from(vec![0x10])),
        Element::Bytes(Bytes::from(b"{}".to_vec())),
        Element::Bytes(Bytes::from(b"customer-1".to_vec())),
    ]);
    let nil_value = pack(&vec![
        Element::Bytes(Bytes::from(vec![0x10])),
        Element::Bytes(Bytes::from(b"{}".to_vec())),
        Element::Nil,
    ]);
    let child_key = pack(&vec![
        Element::String(namespace.as_str().into()),
        Element::String("child".into()),
        Element::String("S".into()),
        Element::Bytes(Bytes::from(b"customer-1".to_vec())),
        Element::String("row".into()),
    ]);
    let write = store.create_transaction().expect("write transaction");
    write.set(&present_key, &present_value);
    write.set(&nil_key, &nil_value);
    write.set(&child_key, b"child");
    write.commit().await.expect("write mapped fixture");

    let mapper = pack(&(namespace.as_str(), "child", "S", "{V[2]}", "{...}"));
    let read = store.create_transaction().expect("mapped transaction");
    store
        .configure_read_transaction(&read, Some("indexer-v-slot-proof"), true)
        .expect("configure mapped transaction");
    let mapped = read
        .get_mapped_range(&exact_range(&present_key), &mapper, 1, false)
        .await
        .expect("map present value slot");
    assert_eq!(mapped.len(), 1);
    let present = &mapped[0];
    assert_eq!(present.key_values().len(), 1);
    assert_eq!(present.key_values()[0].key(), child_key);

    let nil = read
        .get_mapped_range(&exact_range(&nil_key), &mapper, 2, false)
        .await
        .expect("Tuple Nil maps to an empty secondary range on 7.4.6");
    assert_eq!(nil.len(), 1);
    assert!(nil[0].key_values().is_empty());

    let out_of_range_mapper = pack(&(namespace.as_str(), "child", "S", "{V[3]}", "{...}"));
    let error = match read
        .get_mapped_range(&exact_range(&present_key), &out_of_range_mapper, 3, false)
        .await
    {
        Ok(_) => panic!("out-of-range value slot must fail on 7.4.6"),
        Err(error) => error,
    };
    assert_eq!(
        (error.code(), error.message()),
        (
            2030,
            "The index in K[] or V[] is not a valid number or out of range",
        ),
        "unexpected mapped-range error: {error:?}"
    );
    assert_eq!(
        read.get(&present_key, true)
            .await
            .expect("transaction remains usable after mapper error")
            .as_deref(),
        Some(present_value.as_slice())
    );

    let provider_parent_key = pack(&(namespace.as_str(), "provider-parent", "short"));
    store
        .put(&provider_parent_key, &present_value, None)
        .await
        .expect("write provider-prefixed parent");
    let mut end = provider_parent_key.clone();
    end.push(0);
    let provider_result = store
        .read_sequence_mapped_range(ReadSequenceMappedRangeRequest {
            begin: provider_parent_key.clone(),
            end,
            mapper: Some(out_of_range_mapper),
            exclusive_start: None,
            reverse: false,
            target_bytes: 4 * 1024 * 1024,
        })
        .await
        .expect("provider classifies mapper result");
    assert!(
        provider_result.is_none(),
        "a short tuple must restart through the ordinary DAG"
    );
    store
        .delete(&provider_parent_key)
        .await
        .expect("clean provider-prefixed parent");

    let cleanup = store.create_transaction().expect("cleanup transaction");
    cleanup.clear_subspace_range(&Subspace::all().subspace(&(namespace.as_str(),)));
    cleanup.commit().await.expect("clean mapped fixture");
}

fn exact_range(key: &[u8]) -> foundationdb::RangeOption<'static> {
    let mut end = key.to_vec();
    end.push(0);
    foundationdb::RangeOption {
        begin: foundationdb::KeySelector::first_greater_or_equal(key.to_vec()),
        end: foundationdb::KeySelector::first_greater_or_equal(end),
        ..Default::default()
    }
}

fn live_cluster_file() -> Option<std::path::PathBuf> {
    std::env::var_os("FDB_CLUSTER_FILE")
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_file())
        .or_else(|| {
            [
                "/usr/local/etc/foundationdb/fdb.cluster",
                "/opt/homebrew/etc/foundationdb/fdb.cluster",
                "/etc/foundationdb/fdb.cluster",
            ]
            .into_iter()
            .map(std::path::PathBuf::from)
            .find(|path| path.is_file())
        })
}

#[test]
fn request_validation_rejects_empty_bounds() {
    let empty = foundationdb::RangeOption::default();
    assert!(validate_request(&empty).is_err());
}

#[test]
fn request_validation_accepts_the_canonical_range() {
    assert!(validate_request(&mapped_range_request()).is_ok());
}

#[test]
fn mapper_bad_index_is_the_only_native_layout_miss() {
    assert!(is_mapper_bad_index(&foundationdb::FdbError::from_code(
        2030
    )));
    assert!(!is_mapper_bad_index(&foundationdb::FdbError::from_code(
        2108
    )));
    assert!(!is_mapper_bad_index(&foundationdb::FdbError::from_code(
        1007
    )));
}

#[tokio::test]
async fn projected_gsi_range_does_not_read_base_items() {
    let Some(cluster_file_path) = [
        "/usr/local/etc/foundationdb/fdb.cluster",
        "/opt/homebrew/etc/foundationdb/fdb.cluster",
        "/etc/foundationdb/fdb.cluster",
    ]
    .into_iter()
    .map(std::path::PathBuf::from)
    .find(|path| path.is_file()) else {
        return;
    };
    let tenant = format!("mapped-test-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant.clone()),
        subspace_prefix: Some(
            format!("mapped-projected-gsi-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }

    let table_name = TableName::new("mapped-test");
    let index_name = IndexName::new("status");
    let mut table = TableIdentity::user_indexes_for_table(
        crate::keyspace::compact::TableStorageId::new(1),
        &table_name,
        Some(&[GlobalSecondaryIndex {
            index_name: index_name.clone(),
            key_schema: vec![],
            projection: Projection {
                projection_type: None,
                non_key_attributes: None,
            },
        }]),
    );
    table.tenant_keyspace = tenant;

    let mut keys = Vec::new();
    for index in 0..10 {
        let pk = format!("item-{index:02}");
        let key = ItemKey::Index(IndexKey {
            table_name: table_name.clone(),
            index_id: index_name.clone(),
            hash_key: AttributeValue::S("open".to_string()),
            range_key: None,
            table_key: TableKey::new(table_name.clone(), AttributeValue::S(pk.to_string()), None),
        });
        let gsi_key = table_keys::item_key(&table, &key).expect("gsi key");
        let item = WireItem::dynamo_json(
            serde_json::to_vec(&serde_json::json!({
                "pk": {"S": pk},
                "status": {"S": "open"},
                "payload": {"S": "x".repeat(2048)}
            }))
            .expect("item json"),
        );
        let value = encode_wire_item_storage_bytes(
            crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
            &item,
            None,
            storage_types::MaxIndexers::ZERO,
        )
        .expect("item value");
        store.put(&gsi_key, &value, None).await.expect("gsi write");
        keys.push(gsi_key);
    }

    let prefix = ItemKey::IndexPrefix(storage_types::IndexKeyPrefix::new(
        table_name.clone(),
        index_name,
        AttributeValue::S("open".to_string()),
        None,
    ));
    let page = store
        .read_sequence_mapped_range(ReadSequenceMappedRangeRequest {
            begin: table_keys::item_key_prefix(&table, &prefix).expect("range begin"),
            end: table_keys::item_key_prefix_end(&table, &prefix).expect("range end"),
            mapper: None,
            exclusive_start: None,
            reverse: false,
            target_bytes: 4 * 1024 * 1024,
        })
        .await
        .unwrap_or_else(|error| panic!("mapped range: {error:?}"))
        .expect("supported mapped range");
    page.validate_complete(false).expect("complete mapped page");
    assert_eq!(page.entries.len(), 10);
    assert!(page.entries.iter().all(|entry| entry.key_values.is_empty()));

    for gsi_key in keys {
        store.delete(&gsi_key).await.expect("gsi cleanup");
    }
}

#[tokio::test]
async fn provider_mapped_read_sequence_executes_composite_string_template() {
    let Some(cluster_file_path) = mapped_test_cluster_file() else {
        return;
    };
    let tenant = format!("mapped-sequence-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant),
        subspace_prefix: Some(
            format!("mapped-sequence-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: true,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }
    let provider =
        crate::SortedKvDbStorageProvider::new(store).with_immediate_gsi_consistency(true);
    let table_name_value = format!("mapped-sequence-{}", uuid::Uuid::now_v7());
    let table_name = TableName::new(&table_name_value);
    let index_name = IndexName::new("status-index");
    let create = storage_types::CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: index_name.clone(),
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
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .create_table(&create)
        .await
        .expect("create mapped table");
    for sub_id in ["a", "b"] {
        let pk = format!("entity#account-1#sub_model#{sub_id}#v1");
        provider
            .put_item(
                table_name.clone(),
                std::collections::HashMap::from([
                    ("pk".to_string(), AttributeValue::S(pk)),
                    ("gsi_pk".to_string(), AttributeValue::S("open".to_string())),
                    (
                        "entity_id".to_string(),
                        AttributeValue::S("account-1".to_string()),
                    ),
                    ("sub_id".to_string(), AttributeValue::S(sub_id.to_string())),
                    (
                        "payload".to_string(),
                        AttributeValue::S(format!("payload-{sub_id}")),
                    ),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("write mapped item: {:?}", error.to_enum()));
    }

    let mut query = QueryRequest::new(table_name.clone(), "gsi_pk = :gsi_pk".to_string())
        .with_index_name(Some(index_name));
    query.expression_attribute_values = Some(std::collections::HashMap::from([(
        ":gsi_pk".to_string(),
        AttributeValue::S("open".to_string()),
    )]));
    let child = GetItemRequest::new(
        table_name.clone(),
        [(
            "pk".to_string(),
            read_sequence_string_template("entity#{entity_id}#sub_model#{sub_id}#v1"),
        )]
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>(),
    );
    let ordinary_query = query.clone();
    let query_node = ReadSequenceNode {
        name: "query".to_string(),
        operation: ReadSequenceNodeOperation::Query(query),
        inputs: None,
        iterate: None,
        after: None,
    };
    let get_node = ReadSequenceNode {
        name: "get".to_string(),
        operation: ReadSequenceNodeOperation::Get(child),
        inputs: Some(
            [
                (
                    "entity_id".to_string(),
                    ReadSequenceNodeInput {
                        from: ReadSequenceFromInput {
                            node: "query".to_string(),
                            select: ReadSequenceSelector("$.Query.Items[0].entity_id".to_string()),
                        },
                        mapped_key_source: None,
                        cardinality: ReadSequenceInputCardinality::One,
                        on_missing: ReadSequenceOnMissing::Error,
                    },
                ),
                (
                    "sub_id".to_string(),
                    ReadSequenceNodeInput {
                        from: ReadSequenceFromInput {
                            node: "query".to_string(),
                            select: ReadSequenceSelector("$.Query.Items[*].sub_id".to_string()),
                        },
                        mapped_key_source: None,
                        cardinality: ReadSequenceInputCardinality::Many,
                        on_missing: ReadSequenceOnMissing::Skip,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        ),
        iterate: Some("sub_id".to_string()),
        after: None,
    };
    let independent_node = ReadSequenceNode {
        name: "independent".to_string(),
        operation: ReadSequenceNodeOperation::Get(GetItemRequest::new(
            table_name.clone(),
            [(
                "pk".to_string(),
                AttributeValue::S("entity#account-1#sub_model#a#v1".to_string()),
            )]
            .into_iter()
            .collect::<storage_types::KeyAttributes>(),
        )),
        inputs: None,
        iterate: None,
        after: None,
    };
    let request = ReadSequenceRequest {
        // Deliberately present the dependent Get before its Query parent;
        // graph planning and mapped selection must remain reorder-independent.
        nodes: vec![get_node, query_node, independent_node],
        ..Default::default()
    };
    let plan = plan_read_sequence(&request).expect("mapped plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("provider did not select mapped lowering");
    };
    assert_eq!(executed.rows.len(), 4);
    assert!(matches!(
        executed.rows.iter().find(|row| row.node.index() == 1),
        Some(row) if matches!(
            &row.result,
            storage_provider::ReadSequenceFlatResult::Query { items, .. } if items.len() == 2
        )
    ));
    assert!(
        executed
            .rows
            .iter()
            .filter(|row| row.node.index() == 0)
            .all(|row| matches!(
                &row.result,
                storage_provider::ReadSequenceFlatResult::Get { item: Some(item) }
                    if item.get("payload").is_some()
            ))
    );
    let independent = executed
        .rows
        .iter()
        .find(|row| row.node.index() == 2)
        .expect("independent root row");
    assert!(independent.input_refs.is_empty());
    assert!(matches!(
        &independent.result,
        storage_provider::ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("pk") == Some(&AttributeValue::S(
                "entity#account-1#sub_model#a#v1".to_string()
            ))
    ));

    let ordinary_query_items = provider
        .query_table(&storage_types::QueryTableRequest {
            table_name: table_name.clone(),
            index_name: ordinary_query.index_name.clone(),
            key_condition_expression: ordinary_query.key_condition_expression.clone(),
            expression_attribute_names: ordinary_query.expression_attribute_names.clone(),
            expression_attribute_values: ordinary_query.expression_attribute_values.clone(),
            projection_expression: ordinary_query.projection_expression.clone(),
            limit: ordinary_query.limit,
            exclusive_start_key: None,
            scan_index_forward: ordinary_query.scan_index_forward,
            consistent_read: ordinary_query.consistent_read.unwrap_or(false),
        })
        .await
        .expect("ordinary mapped query");
    let ordinary_query_items = ordinary_query_items.0;
    let mapped_query_items = match &executed
        .rows
        .iter()
        .find(|row| row.node.index() == 1)
        .expect("mapped query row")
        .result
    {
        storage_provider::ReadSequenceFlatResult::Query { items, .. } => items
            .iter()
            .map(storage_types::AttributeMap::to_hashmap)
            .collect::<Vec<_>>(),
        _ => panic!("mapped query row shape"),
    };
    assert_eq!(
        mapped_query_items, ordinary_query_items,
        "mapped parent values must match the ordinary GSI query"
    );
    for sub_id in ["a", "b"] {
        let pk = format!("entity#account-1#sub_model#{sub_id}#v1");
        let ordinary_item = provider
            .get_item(
                table_name.clone(),
                [("pk".to_string(), AttributeValue::S(pk.clone()))].into(),
                false,
            )
            .await
            .expect("ordinary mapped child get")
            .expect("ordinary mapped child item")
            .into_attribute_map()
            .expect("ordinary mapped child decode");
        let mapped_item = executed
            .rows
            .iter()
            .filter(|row| row.node.index() == 0)
            .find_map(|row| match &row.result {
                storage_provider::ReadSequenceFlatResult::Get { item: Some(item) }
                    if item.get("pk") == Some(&AttributeValue::S(pk.clone())) =>
                {
                    Some(item.to_hashmap())
                }
                _ => None,
            })
            .expect("mapped child item");
        assert_eq!(mapped_item, ordinary_item);
    }
    provider
        .delete_table(&table_name)
        .await
        .expect("cleanup mapped table");
}

#[tokio::test]
async fn provider_mapped_read_sequence_executes_static_child_sort_key() {
    let Some(cluster_file_path) = mapped_test_cluster_file() else {
        return;
    };
    let _metrics_guard = crate::backends::fdb::foundationdb_operation_metrics_test_guard();
    let tenant = format!("mapped-static-child-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant),
        subspace_prefix: Some(
            format!("mapped-static-child-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }
    let provider = crate::SortedKvDbStorageProvider::new(store);
    let table_name = TableName::new(&format!("mapped-static-child-{}", uuid::Uuid::now_v7()));
    let create = storage_types::CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
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
    );
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .create_table(&create)
        .await
        .expect("create mapped table");

    let phone = "+441234567890";
    provider
        .put_item(
            table_name.clone(),
            std::collections::HashMap::from([
                ("pk".to_string(), AttributeValue::S(format!("UPI#{phone}"))),
                ("sk".to_string(), AttributeValue::S("U#user-1".to_string())),
                (
                    "user_id".to_string(),
                    AttributeValue::S("user-1".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write phone lookup");
    provider
        .put_item(
            table_name.clone(),
            std::collections::HashMap::from([
                ("pk".to_string(), AttributeValue::S("U#user-1".to_string())),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
                (
                    "user_id".to_string(),
                    AttributeValue::S("user-1".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("mapped-user".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write user item");

    let mut query = QueryRequest::new(table_name.clone(), "pk = :pk".to_string());
    query.expression_attribute_values = Some(std::collections::HashMap::from([(
        ":pk".to_string(),
        AttributeValue::S(format!("UPI#{phone}")),
    )]));
    let ordinary_query = query.clone();
    let mut child = ReadSequenceNode::new(
        "user",
        ReadSequenceNodeOperation::Get(GetItemRequest::new(
            table_name.clone(),
            [
                (
                    "pk".to_string(),
                    storage_types::read_sequence_input_marker("user_pk"),
                ),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
            ]
            .into_iter()
            .collect::<storage_types::KeyAttributes>(),
        )),
    );
    child.iterate = Some("user_pk".to_string());
    child.inputs_mut().insert(
        "user_pk".to_string(),
        ReadSequenceNodeInput {
            from: ReadSequenceFromInput {
                node: "phone_lookup".to_string(),
                select: ReadSequenceSelector("$.Query.Items[*].sk".to_string()),
            },
            mapped_key_source: None,
            cardinality: ReadSequenceInputCardinality::Many,
            on_missing: ReadSequenceOnMissing::Skip,
        },
    );
    let request = ReadSequenceRequest::new(vec![
        ReadSequenceNode::new("phone_lookup", ReadSequenceNodeOperation::Query(query)),
        child,
    ]);
    let plan = plan_read_sequence(&request).expect("static child plan");
    crate::backends::fdb::foundationdb_operation_metrics_reset();
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("static child mapped execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("provider did not select static child mapped lowering");
    };
    let query_row = executed
        .rows
        .iter()
        .find(|row| row.node.index() == 0)
        .expect("phone query row");
    assert!(matches!(
        &query_row.result,
        storage_provider::ReadSequenceFlatResult::Query { items, .. }
            if items.len() == 1
    ));
    let child_row = executed
        .rows
        .iter()
        .find(|row| row.node.index() == 1)
        .expect("user child row");
    assert!(matches!(
        &child_row.result,
        storage_provider::ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("pk") == Some(&AttributeValue::S("U#user-1".to_string()))
                && item.get("sk") == Some(&AttributeValue::S("META".to_string()))
                && item.get("payload") == Some(&AttributeValue::S("mapped-user".to_string()))
    ));
    let metrics = crate::backends::fdb::foundationdb_operation_metrics_snapshot();
    assert_eq!(
        fdb_operation_metric(&metrics, "read_context", "range_read"),
        1,
        "mapped phone lookup must use one FoundationDB range operation\n{metrics}"
    );

    crate::backends::fdb::foundationdb_operation_metrics_reset();
    let ordinary_query_result = provider
        .query_table(&storage_types::QueryTableRequest {
            table_name: ordinary_query.table_name.clone(),
            index_name: ordinary_query.index_name.clone(),
            key_condition_expression: ordinary_query.key_condition_expression.clone(),
            expression_attribute_names: ordinary_query.expression_attribute_names.clone(),
            expression_attribute_values: ordinary_query.expression_attribute_values.clone(),
            projection_expression: ordinary_query.projection_expression.clone(),
            limit: ordinary_query.limit,
            exclusive_start_key: None,
            scan_index_forward: ordinary_query.scan_index_forward,
            consistent_read: ordinary_query.consistent_read.unwrap_or(false),
        })
        .await
        .expect("ordinary phone query");
    assert_eq!(ordinary_query_result.0.len(), 1);
    provider
        .get_item(
            table_name.clone(),
            [
                ("pk".to_string(), AttributeValue::S("U#user-1".to_string())),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
            ]
            .into_iter()
            .collect(),
            false,
        )
        .await
        .expect("ordinary user get")
        .expect("ordinary user item");
    let ordinary_metrics = crate::backends::fdb::foundationdb_operation_metrics_snapshot();
    assert!(
        fdb_operation_metric(&ordinary_metrics, "range", "range_read") >= 1,
        "serial phone lookup must issue a query range operation\n{ordinary_metrics}"
    );
    assert!(
        fdb_operation_metric(&ordinary_metrics, "get", "snapshot_point_read") >= 1,
        "serial phone lookup must issue a point read\n{ordinary_metrics}"
    );

    provider
        .delete_table(&table_name)
        .await
        .expect("cleanup static child table");
}

#[tokio::test]
async fn provider_mapped_read_sequence_executes_point_get_to_partition_query() {
    let Some(cluster_file_path) = mapped_test_cluster_file() else {
        return;
    };
    let _metrics_guard = crate::backends::fdb::foundationdb_operation_metrics_test_guard();
    let tenant = format!("mapped-get-query-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant),
        subspace_prefix: Some(
            format!("mapped-get-query-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    let suffix = uuid::Uuid::now_v7();
    let source = TableName::new(&format!("mapped-get-source-{suffix}"));
    let target = TableName::new(&format!("mapped-get-target-{suffix}"));
    create_composite_table(&provider, &source, "pk", "sk").await;
    create_composite_table(&provider, &target, "account", "event").await;
    provider
        .put_item(
            source.clone(),
            std::collections::HashMap::from([
                ("pk".to_string(), AttributeValue::S("account-1".to_string())),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
                ("name".to_string(), AttributeValue::S("Acme".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("write parent");
    for event in ["a", "b"] {
        provider
            .put_item(
                target.clone(),
                std::collections::HashMap::from([
                    (
                        "account".to_string(),
                        AttributeValue::S("account-1".to_string()),
                    ),
                    ("event".to_string(), AttributeValue::S(event.to_string())),
                    (
                        "payload".to_string(),
                        AttributeValue::S(format!("payload-{event}")),
                    ),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("write child");
    }

    let request: ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "account", "Operation": {"Get": {
                "TableName": source, "Key": {
                    "pk": {"S": "account-1"}, "sk": {"S": "META"}
                }
            }}},
            {"Name": "events", "Operation": {"Query": {
                "TableName": target, "KeyConditionExpression": "account = :account",
                "ExpressionAttributeValues": {":account": {"FromInput": "account"}}
            }}, "Inputs": {
                "account": {"From": {"Node": "account", "Select": "$.Get.Item.pk"},
                    "Cardinality": "ONE", "OnMissing": "ERROR"}
            }}
        ]
    }))
    .expect("get/query request");
    let plan = plan_read_sequence(&request).expect("get/query plan");

    crate::backends::fdb::foundationdb_operation_metrics_reset();
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped get/query execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("provider did not select get/query mapped lowering: {execution:?}");
    };
    assert!(matches!(
        executed.rows.iter().find(|row| row.node.index() == 0),
        Some(row) if matches!(
            &row.result,
            ReadSequenceFlatResult::Get { item: Some(item) }
                if item.get("pk") == Some(&AttributeValue::S("account-1".to_string()))
        )
    ));
    assert!(matches!(
        executed.rows.iter().find(|row| row.node.index() == 1),
        Some(row) if matches!(
            &row.result,
            ReadSequenceFlatResult::Query { items, count, scanned_count, .. }
                if items.len() == 2 && *count == 2 && *scanned_count == 2
        )
    ));
    let mapped_metrics = crate::backends::fdb::foundationdb_operation_metrics_snapshot();
    assert_eq!(
        fdb_operation_metric(&mapped_metrics, "read_context", "range_read"),
        1,
        "mapped get/query must use one FoundationDB range operation\n{mapped_metrics}"
    );

    crate::backends::fdb::foundationdb_operation_metrics_reset();
    provider
        .get_item(
            source.clone(),
            [
                ("pk".to_string(), AttributeValue::S("account-1".to_string())),
                ("sk".to_string(), AttributeValue::S("META".to_string())),
            ]
            .into_iter()
            .collect(),
            false,
        )
        .await
        .expect("serial parent get")
        .expect("serial parent item");
    provider
        .query_table(&storage_types::QueryTableRequest {
            table_name: target.clone(),
            index_name: None,
            key_condition_expression: "account = :account".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(std::collections::HashMap::from([(
                ":account".to_string(),
                AttributeValue::S("account-1".to_string()),
            )])),
            projection_expression: None,
            limit: None,
            exclusive_start_key: None,
            scan_index_forward: None,
            consistent_read: false,
        })
        .await
        .expect("serial child query");
    let serial_metrics = crate::backends::fdb::foundationdb_operation_metrics_snapshot();
    assert!(
        fdb_operation_metric(&serial_metrics, "get", "snapshot_point_read") >= 1,
        "serial get/query must issue a point read\n{serial_metrics}"
    );
    assert!(
        fdb_operation_metric(&serial_metrics, "range", "range_read") >= 1,
        "serial get/query must issue a range read\n{serial_metrics}"
    );

    provider.delete_table(&source).await.expect("delete source");
    provider.delete_table(&target).await.expect("delete target");
}

#[tokio::test]
async fn provider_maps_indexed_range_key_from_composite_gsi_rows() {
    let Some(cluster_file_path) = mapped_test_cluster_file() else {
        return;
    };
    let tenant = format!("mapped-indexed-gsi-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant),
        subspace_prefix: Some(
            format!("mapped-indexed-gsi-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: true,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }
    let provider =
        crate::SortedKvDbStorageProvider::new(store).with_immediate_gsi_consistency(true);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    let table = TableName::new(&format!("mapped-indexed-gsi-{}", uuid::Uuid::now_v7()));
    let index = IndexName::new("status");
    let mut create = storage_types::CreateTableRequest::new(
        table.clone(),
        ["pk", "sk", "gsi_pk", "gsi_sk"]
            .into_iter()
            .map(|name| AttributeDefinition {
                attribute_name: name.to_string(),
                attribute_type: KeyAttributeType::S,
            })
            .collect(),
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
        index_name: index,
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    create.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    provider.create_table(&create).await.expect("create table");

    let mut writes = Vec::with_capacity(4);
    for suffix in ["a", "b"] {
        writes.push(WriteRequest {
            put_request: Some(PutRequest {
                item: std::collections::HashMap::from([
                    ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                    (
                        "sk".to_string(),
                        AttributeValue::S(format!("parent-{suffix}")),
                    ),
                    (
                        "related_sk".to_string(),
                        AttributeValue::S(format!("child-{suffix}")),
                    ),
                    ("gsi_pk".to_string(), AttributeValue::S("open".to_string())),
                    ("gsi_sk".to_string(), AttributeValue::S(suffix.to_string())),
                ]),
                indexers: Some(vec!["related_sk".to_string()]),
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        });
        writes.push(WriteRequest {
            put_request: Some(PutRequest {
                item: std::collections::HashMap::from([
                    ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                    (
                        "sk".to_string(),
                        AttributeValue::S(format!("child-{suffix}")),
                    ),
                    (
                        "payload".to_string(),
                        AttributeValue::S(format!("payload-{suffix}")),
                    ),
                    (
                        "related_sk".to_string(),
                        AttributeValue::S(format!("child-{suffix}")),
                    ),
                ]),
                indexers: Some(vec!["related_sk".to_string()]),
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        });
    }
    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: std::collections::HashMap::from([(table.clone(), writes)]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            false,
        )
        .await
        .expect("batch write indexed fixture");

    let request: ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": table, "IndexName": "status",
                "KeyConditionExpression": "gsi_pk = :open",
                "ExpressionAttributeValues": {":open": {"S": "open"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": table,
                "Key": {"pk": {"FromInput": "pk"}, "sk": {"FromInput": "sk"}}
            }}, "Inputs": {
                "pk": {"From": {"Node": "parents", "Select": "$.Query.Items[0].pk"}, "Cardinality": "ONE", "OnMissing": "ERROR"},
                "sk": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].related_sk"},
                    "MappedKeySource": {"AttributeName": "related_sk", "Indexer": 0},
                    "Cardinality": "MANY", "OnMissing": "SKIP"
                }
            }, "Iterate": "sk"}
        ]
    }))
    .expect("indexed mapped request");
    let plan = plan_read_sequence(&request).expect("indexed mapped plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("indexed mapped execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("provider did not execute indexed mapped lowering: {execution:?}");
    };
    let payloads = executed
        .rows
        .iter()
        .filter_map(|row| match &row.result {
            ReadSequenceFlatResult::Get { item: Some(item) } => item.get("payload"),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(payloads.len(), 2);
    provider.delete_table(&table).await.expect("delete table");
}

#[tokio::test]
async fn provider_maps_reverse_composite_base_range_directly_to_another_table() {
    let Some(cluster_file_path) = mapped_test_cluster_file() else {
        return;
    };
    let tenant = format!("mapped-cross-table-{}", uuid::Uuid::now_v7()).into_bytes();
    let Ok(store) = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().into_owned()),
        tenant_name: Some(tenant),
        subspace_prefix: Some(
            format!("mapped-cross-table-prefix-{}", uuid::Uuid::now_v7()).into_bytes(),
        ),
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    }) else {
        return;
    };
    if store.check_reachable(Duration::from_secs(3)).await.is_err() {
        return;
    }
    let provider = crate::SortedKvDbStorageProvider::new(store);
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    let suffix = uuid::Uuid::now_v7();
    let source = TableName::new(&format!("mapped-source-{suffix}"));
    let target = TableName::new(&format!("mapped-target-{suffix}"));
    create_composite_table(&provider, &source, "pk", "sk").await;
    create_composite_table(&provider, &target, "account", "event").await;
    write_cross_table_fixture(&provider, &source, &target).await;

    let plan = plan_read_sequence(&cross_table_request(&source, &target)).expect("mapped plan");
    let execution = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped execution");
    let ReadSequenceExecution::Executed(executed) = execution else {
        panic!("provider did not select mapped lowering");
    };
    assert_cross_table_rows(&executed.rows);
    provider.delete_table(&source).await.expect("delete source");
    provider.delete_table(&target).await.expect("delete target");
}

fn mapped_test_cluster_file() -> Option<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("TEST_FDB_CLUSTER_FILE")
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_file())
    {
        return Some(path);
    }
    [
        "/usr/local/etc/foundationdb/fdb.cluster",
        "/opt/homebrew/etc/foundationdb/fdb.cluster",
        "/etc/foundationdb/fdb.cluster",
    ]
    .into_iter()
    .map(std::path::PathBuf::from)
    .find(|path| path.is_file())
}

fn fdb_operation_metric(metrics: &str, path: &str, operation: &str) -> u64 {
    let needle = format!("path=\"{path}\",operation=\"{operation}\"");
    metrics
        .lines()
        .find(|line| line.contains(&needle))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0)
}

async fn create_composite_table(
    provider: &crate::SortedKvDbStorageProvider<FoundationDbKvStore>,
    table: &TableName,
    hash: &str,
    range: &str,
) {
    let request = storage_types::CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: hash.to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: range.to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: hash.to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: range.to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    );
    provider.create_table(&request).await.expect("create table");
}

async fn write_cross_table_fixture(
    provider: &crate::SortedKvDbStorageProvider<FoundationDbKvStore>,
    source: &TableName,
    target: &TableName,
) {
    for event in ["a", "b", "c"] {
        provider
            .put_item(
                source.clone(),
                std::collections::HashMap::from([
                    ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                    ("sk".to_string(), AttributeValue::S(event.to_string())),
                    ("enabled".to_string(), AttributeValue::BOOL(event != "a")),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("write source");
        provider
            .put_item(
                target.clone(),
                std::collections::HashMap::from([
                    (
                        "account".to_string(),
                        AttributeValue::S("tenant".to_string()),
                    ),
                    ("event".to_string(), AttributeValue::S(event.to_string())),
                    (
                        "payload".to_string(),
                        AttributeValue::S(format!("payload-{event}")),
                    ),
                    (
                        "hidden".to_string(),
                        AttributeValue::S("hidden".to_string()),
                    ),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("write target");
    }
}

fn cross_table_request(source: &TableName, target: &TableName) -> ReadSequenceRequest {
    serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": source, "KeyConditionExpression": "pk = :pk",
                "FilterExpression": "enabled = :enabled",
                "ProjectionExpression": "pk, sk",
                "ExpressionAttributeValues": {
                    ":pk": {"S": "tenant"}, ":enabled": {"BOOL": true}
                },
                "ScanIndexForward": false,
                "ExclusiveStartKey": {"pk": {"S": "tenant"}, "sk": {"S": "c"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": target,
                "Key": {"account": {"FromInput": "pk"}, "event": {"FromInput": "sk"}},
                "ProjectionExpression": "account, event, payload"
            }}, "Inputs": {
                "pk": {"From": {"Node": "parents", "Select": "$.Query.Items[0].pk"}, "Cardinality": "ONE", "OnMissing": "ERROR"},
                "sk": {"From": {"Node": "parents", "Select": "$.Query.Items[*].sk"}, "Cardinality": "MANY", "OnMissing": "SKIP"}
            }, "Iterate": "sk"}
        ]
    }))
    .expect("cross-table request")
}

fn assert_cross_table_rows(rows: &[storage_provider::ReadSequenceFlatRow]) {
    let query = rows
        .iter()
        .find(|row| row.node.index() == 0)
        .expect("query row");
    assert!(matches!(
        &query.result,
        ReadSequenceFlatResult::Query { items, count: 1, scanned_count: 2, .. }
            if items.len() == 1
                && items[0].get("sk") == Some(&AttributeValue::S("b".to_string()))
                && items[0].get("enabled").is_none()
    ));
    let child = rows
        .iter()
        .find(|row| row.node.index() == 1)
        .expect("child row");
    assert!(matches!(
        &child.result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("payload-b".to_string()))
                && item.get("hidden").is_none()
    ));
}
