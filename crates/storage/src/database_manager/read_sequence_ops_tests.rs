use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, GetItemRequest,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, ReadSequenceConsistency,
    ReadSequenceInvocationPayload, ReadSequenceNode, ReadSequenceNodeOperation,
    ReadSequenceRequest, StorageEnum, TableName, context::WrappedError,
};

use super::DatabaseManager;
use crate::{AdmissionClass, PutItemInput};

async fn create_database() -> DatabaseManager {
    let db = DatabaseManager::new_for_test().await.expect("database");
    seed_database(&db).await;
    db
}

async fn seed_database(db: &DatabaseManager) {
    db.create_table(&CreateTableRequest::new(
        TableName::new("read_sequence_manager"),
        vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("table");
    db.put_item(
        PutItemInput::builder()
            .table_name(TableName::new("read_sequence_manager"))
            .item(std::collections::HashMap::from([
                ("pk".into(), AttributeValue::S("present".into())),
                ("value".into(), AttributeValue::S("authoritative".into())),
            ]))
            .build(),
    )
    .await
    .expect("row");
}

fn build_request() -> ReadSequenceRequest {
    let nodes = [("first", "present"), ("second", "absent")]
        .into_iter()
        .map(|(name, key)| {
            ReadSequenceNode::new(
                name,
                ReadSequenceNodeOperation::Get(Box::new(GetItemRequest::new(
                    TableName::new("read_sequence_manager"),
                    KeyAttributes::from([("pk".into(), AttributeValue::S(key.into()))]),
                ))),
            )
        })
        .collect();
    let mut request = ReadSequenceRequest::new(nodes);
    request.nodes[1].after = Some(vec!["first".into()]);
    request.read_consistency = ReadSequenceConsistency::Transactional;
    request
}

#[tokio::test]
async fn given_transactional_graph_when_manager_reads_then_order_and_absence_are_preserved() {
    let db = create_database().await;
    let before = db.default_admission_controller().snapshot();
    let response = db.read_sequence(build_request()).await.expect("graph");
    assert_eq!(
        response.read_consistency,
        ReadSequenceConsistency::Transactional
    );
    assert!(!response.partial);
    assert!(response.next_sequence_token.is_none());
    assert_eq!(response.nodes.len(), 2);
    assert_eq!(response.nodes[0].name, "first");
    assert_eq!(response.nodes[1].name, "second");
    let ReadSequenceInvocationPayload::Get(first) = &response.nodes[0].invocations[0].result else {
        panic!("get result");
    };
    let row = first
        .item
        .as_ref()
        .expect("present item")
        .to_attribute_map()
        .expect("decode");
    assert_eq!(
        row.get("value"),
        Some(&AttributeValue::S("authoritative".into()))
    );
    assert_eq!(response.nodes[1].invocations[0].result.item_count(), 0);
    let after = db.default_admission_controller().snapshot();
    assert_eq!(after.in_flight, before.in_flight);
    assert_eq!(after.queue_depth, before.queue_depth);
}

#[tokio::test]
async fn given_full_admission_when_manager_reads_graph_then_storage_pressure_is_not_bypassed() {
    let db = create_database().await;
    let controller = db.default_admission_controller();
    let permits = (0..controller.snapshot().desired_limit)
        .map(|_| {
            controller
                .try_acquire(AdmissionClass::RangeRead)
                .expect("permit")
        })
        .collect::<Vec<_>>();
    let error = db
        .read_sequence(build_request())
        .await
        .expect_err("saturated");
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ServiceUnavailable { .. }
    ));
    drop(permits);
    assert!(db.read_sequence(build_request()).await.is_ok());
}

#[tokio::test]
async fn given_invalid_graph_bound_when_manager_reads_then_validation_and_recovery_are_preserved() {
    let db = create_database().await;
    let mut request = build_request();
    request.max_sequence_steps = Some(1);
    let error = db
        .read_sequence(request)
        .await
        .expect_err("two nodes exceed bound");
    assert!(matches!(error.to_enum(), StorageEnum::Validation { .. }));
    assert!(db.read_sequence(build_request()).await.is_ok());
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires an owned FoundationDB topology"]
async fn given_native_storage_when_public_read_apis_run_then_transaction_and_sequence_return_same_authority()
 {
    let scope = crate::FoundationDbTestScope::try_new(
        std::env::var("FDB_CLUSTER_FILE").expect("owned FDB_CLUSTER_FILE"),
    )
    .await
    .expect("isolated native scope");
    scope
        .run(async {
            let db = crate::FoundationDbTestScope::open_database(scope.settings().clone())
                .await
                .expect("native database");
            seed_database(&db).await;
            let graph = db
                .read_sequence(build_request())
                .await
                .expect("native read graph");
            let atomic = db
                .transact_get_items(storage_types::TransactGetItemsRequest {
                    transact_items: ["present", "absent"]
                        .into_iter()
                        .map(|key| storage_types::TransactGetItem {
                            get: storage_types::TransactGetRequest {
                                table_name: TableName::new("read_sequence_manager"),
                                key: KeyAttributes::from([(
                                    "pk".into(),
                                    AttributeValue::S(key.into()),
                                )]),
                                projection_expression: None,
                                expression_attribute_names: None,
                            },
                        })
                        .collect(),
                    return_consumed_capacity: None,
                })
                .await
                .expect("native atomic read");
            assert_eq!(atomic.responses.len(), 2);
            for (node, item) in graph.nodes.iter().zip(atomic.responses) {
                let ReadSequenceInvocationPayload::Get(read) = &node.invocations[0].result else {
                    panic!("Get result")
                };
                let decode = |item: Option<storage_types::WireItem>| {
                    item.map(|row| row.to_attribute_map().expect("decode"))
                };
                assert_eq!(decode(read.item.clone()), decode(item.item));
            }
            assert_eq!(db.default_admission_controller().snapshot().in_flight, 0);
        })
        .await
        .expect("isolated native cleanup");
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires an owned FoundationDB topology"]
async fn given_concurrent_native_pair_writes_when_public_snapshots_read_then_no_torn_generation_is_returned()
 {
    let scope = crate::FoundationDbTestScope::try_new(
        std::env::var("FDB_CLUSTER_FILE").expect("owned cluster"),
    )
    .await
    .expect("native scope");
    scope
        .run(async {
            let reader = crate::FoundationDbTestScope::open_database(scope.settings().clone())
                .await
                .expect("reader");
            let writer = crate::FoundationDbTestScope::open_database(scope.settings().clone())
                .await
                .expect("writer");
            seed_database(&reader).await;
            writer
                .transact_write_items(build_pair_write(0))
                .await
                .expect("initial pair");
            let progress = std::sync::atomic::AtomicUsize::new(0);
            let stop = std::sync::atomic::AtomicBool::new(false);
            let writes = async {
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let generation = progress.load(std::sync::atomic::Ordering::Acquire) + 1;
                    writer
                        .transact_write_items(build_pair_write(generation))
                        .await
                        .expect("atomic pair");
                    progress.store(generation, std::sync::atomic::Ordering::Release);
                }
            };
            let reads = async {
                for _ in 0..64 {
                    prove_pair_snapshot(&reader).await;
                }
                stop.store(true, std::sync::atomic::Ordering::Release);
            };
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                tokio::join!(writes, reads);
            })
            .await
            .expect("bounded concurrent native proof");
            assert!(
                progress.load(std::sync::atomic::Ordering::Acquire) >= 2,
                "concurrent writes made progress"
            );
        })
        .await
        .expect("native namespace cleanup");
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
fn build_pair_write(generation: usize) -> storage_types::TransactWriteItemsRequest {
    let items = ["left", "right"].map(|key| {
        serde_json::json!({"Put": {
            "TableName": "read_sequence_manager",
            "Item": {"pk": {"S": key}, "generation": {"N": generation.to_string()}}
        }})
    });
    serde_json::from_value(serde_json::json!({"TransactItems": items})).expect("pair transaction")
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
async fn prove_pair_snapshot(db: &DatabaseManager) {
    let request = storage_types::TransactGetItemsRequest {
        transact_items: ["left", "right"]
            .into_iter()
            .map(|key| storage_types::TransactGetItem {
                get: storage_types::TransactGetRequest {
                    table_name: TableName::new("read_sequence_manager"),
                    key: KeyAttributes::from([("pk".into(), AttributeValue::S(key.into()))]),
                    projection_expression: None,
                    expression_attribute_names: None,
                },
            })
            .collect(),
        return_consumed_capacity: None,
    };
    let nodes = request
        .transact_items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            ReadSequenceNode::new(
                format!("node{index}"),
                ReadSequenceNodeOperation::Get(Box::new(GetItemRequest::new(
                    item.get.table_name.clone(),
                    item.get.key.clone(),
                ))),
            )
        })
        .collect();
    let mut graph = ReadSequenceRequest::new(nodes);
    graph.read_consistency = ReadSequenceConsistency::Transactional;
    graph.nodes[1].after = Some(vec!["node0".into()]);
    let atomic = db
        .transact_get_items(request)
        .await
        .expect("atomic snapshot");
    assert_equal_generations(atomic.responses.into_iter().map(|row| row.item));
    let response = db
        .read_sequence(graph)
        .await
        .expect("dependent sequence snapshot");
    assert_equal_generations(response.nodes.into_iter().map(|node| {
        let ReadSequenceInvocationPayload::Get(row) = node
            .invocations
            .into_iter()
            .next()
            .expect("invocation")
            .result
        else {
            panic!("get result");
        };
        row.item
    }));
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
fn assert_equal_generations(rows: impl Iterator<Item = Option<storage_types::WireItem>>) {
    let generations = rows
        .map(|row| {
            row.expect("pair member")
                .to_attribute_map()
                .expect("decode")
                .remove("generation")
                .expect("generation")
        })
        .collect::<Vec<_>>();
    assert_eq!(generations.len(), 2);
    assert_eq!(
        generations[0], generations[1],
        "one response cannot mix committed generations"
    );
}
