use std::{collections::HashMap, future::Future, sync::Mutex};

use async_trait::async_trait;
use storage_common::{TtlConfigRecord, TtlIndexWrite};
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, MaxIndexers, Projection, ProjectionType,
    PutItemRequest, StoredTableInfo, StreamRetentionDuration, StreamSpecification, TableName,
    TableStatus, TimeToLiveStatus, TimestampMillis, WireItem,
};

use super::{
    models::{
        TableDeleteInput, TablePutInput, TableUpdateInput, TableWriteBatchInput, TableWriteError,
        WriteReceipts,
    },
    table_finish::PreparedTableBatch,
    table_writes::TableWriteRepository,
};
use crate::{PhysicalWrite, TableStreamContext, TtlWriteContext};

struct TestTableStore {
    staged_ordinals: Mutex<Vec<usize>>,
}

#[async_trait]
impl TableWriteRepository for TestTableStore {
    async fn execute_table_batch(
        &self,
        input: super::models::TableWriteBatchInput,
    ) -> storage_types::StorageResult<WriteReceipts> {
        let batch = self.prepare_table_batch(input)?;
        self.validate_prepared_batch(&batch, &super::models::ValidationSnapshot {})?;
        let mut ordinals = self.staged_ordinals.lock().unwrap();
        let mut receipts = Vec::new();
        for put in batch.puts {
            ordinals.push(put.ordinal);
            let finished = put
                .finish_with_old(None)
                .map_err(storage_types::StorageError::from)?;
            receipts.push(super::models::TableWriteReceipt {
                ordinal: finished.ordinal,
                old_item: None,
                new_item: None,
                condition_failed: None,
            });
        }
        for update in batch.updates {
            ordinals.push(update.ordinal);
            let finished = update
                .finish_with_old(None)
                .map_err(storage_types::StorageError::from)?;
            receipts.push(super::models::TableWriteReceipt {
                ordinal: finished.ordinal,
                old_item: None,
                new_item: None,
                condition_failed: None,
            });
        }
        for delete in batch.deletes {
            ordinals.push(delete.ordinal);
            let finished = delete
                .finish_with_old(None)
                .map_err(storage_types::StorageError::from)?;
            receipts.push(super::models::TableWriteReceipt {
                ordinal: finished.ordinal,
                old_item: None,
                new_item: None,
                condition_failed: None,
            });
        }
        for check in batch.checks {
            check
                .check_with_old(None)
                .map_err(storage_types::StorageError::from)?;
        }
        Ok(WriteReceipts { receipts })
    }
}

fn table_name() -> TableName {
    TableName::new("harmonised-orders")
}

fn key_schema() -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: "pk".to_owned(),
        key_type: KeyType::Hash,
    }]
}

fn bare_table() -> StoredTableInfo {
    StoredTableInfo {
        table_name: table_name(),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: key_schema(),
        max_indexers: MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration: StreamRetentionDuration::Forever,
        deletion_protection_enabled: false,
    }
}

fn armed_table() -> StoredTableInfo {
    let mut table = bare_table();
    table.max_indexers = MaxIndexers::try_new(4).unwrap();
    table.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
        index_name: IndexName::new("gsi1"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    }]);
    table.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: None,
    });
    table
}

fn ttl_config() -> TtlConfigRecord {
    TtlConfigRecord::try_new(
        "expires_at".to_owned(),
        &table_name(),
        TimeToLiveStatus::Enabled,
    )
    .unwrap()
}

fn stream_context() -> TableStreamContext {
    TableStreamContext {
        item_revision: None,
        stream_item_id: None,
        target_item_stream_version: None,
        created_at: None,
        immediate_gsi_consistency: true,
        gsi_lifecycle: None,
    }
}

fn ttl_context() -> TtlWriteContext {
    TtlWriteContext::direct(table_name()).unwrap()
}

fn put_input(
    table: &StoredTableInfo,
    item: HashMap<String, AttributeValue>,
    ttl_config: Option<TtlConfigRecord>,
) -> TablePutInput {
    TablePutInput {
        write: PhysicalWrite::try_new(
            0,
            table_name(),
            PutItemRequest::new(
                table_name(),
                storage_types::WireItem::try_from(&(item)).unwrap(),
            ),
            ttl_context(),
        )
        .unwrap(),
        table: std::sync::Arc::new(table.clone()),
        stream: stream_context(),
        ttl_config,
        item_stream_ttl_hours: None,
    }
}

fn item(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("data".to_owned(), AttributeValue::S("value".to_owned())),
    ])
}

fn wire_item(pk: &str) -> WireItem {
    WireItem::try_from(&item(pk)).unwrap()
}

fn key_attributes(pk: &str) -> KeyAttributes {
    let mut key = KeyAttributes::default();
    key.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    key
}

#[test]
fn given_bare_table_when_prepare_put_then_no_old_item_needed() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let prepared = store
        .prepare_table_put(put_input(&table, item("pk-1"), None))
        .unwrap();
    assert!(!prepared.observation.requests_old_item());
    assert_eq!(
        prepared.table_key.hash_key,
        AttributeValue::S("pk-1".to_owned())
    );
}

#[test]
fn given_direct_put_when_building_observation_contract_then_image_is_not_requested() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let prepared = store
        .prepare_table_put(put_input(&table, item("pk-1"), None))
        .unwrap();
    let batch = PreparedTableBatch {
        puts: vec![prepared],
        ..PreparedTableBatch::default()
    };
    let requirement = batch.observation_requirements().pop().unwrap();
    assert!(!requirement.fields.requests_old_item());
    assert!(requirement.fields.requests_durable_metadata());
}

#[test]
fn given_conditional_update_when_building_observation_contract_then_image_is_required() {
    use storage_types::UpdateItemRequest;

    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let request = UpdateItemRequest::builder()
        .table_name(table_name())
        .key(key_attributes("pk-1"))
        .condition_expression(Some("attribute_exists(pk)".to_owned()))
        .build();
    let prepared = store
        .prepare_table_update(TableUpdateInput {
            write: PhysicalWrite::try_new(0, table_name(), request, ttl_context()).unwrap(),
            table: std::sync::Arc::new(table),
            context: crate::TableUpdateContext {
                stream: stream_context(),
                index: 0,
                ttl_config: None,
                ttl_context: ttl_context(),
                item_stream_ttl_hours: None,
            },
        })
        .unwrap();
    let batch = PreparedTableBatch {
        updates: vec![prepared],
        ..PreparedTableBatch::default()
    };
    let requirement = batch.observation_requirements().pop().unwrap();
    assert!(requirement.fields.requests_old_item());
}

#[test]
fn given_update_without_expression_when_building_observation_contract_then_image_is_required() {
    use storage_types::UpdateItemRequest;

    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let request = UpdateItemRequest::builder()
        .table_name(table_name())
        .key(key_attributes("pk-1"))
        .build();
    let prepared = store
        .prepare_table_update(TableUpdateInput {
            write: PhysicalWrite::try_new(0, table_name(), request, ttl_context()).unwrap(),
            table: std::sync::Arc::new(table),
            context: crate::TableUpdateContext {
                stream: stream_context(),
                index: 0,
                ttl_config: None,
                ttl_context: ttl_context(),
                item_stream_ttl_hours: None,
            },
        })
        .unwrap();
    assert!(prepared.operations.is_empty());
    assert!(prepared.observation.requests_old_item());

    let batch = PreparedTableBatch {
        updates: vec![prepared],
        ..PreparedTableBatch::default()
    };
    let requirement = batch.observation_requirements().pop().unwrap();
    assert!(requirement.fields.requests_old_item());
}

#[test]
fn given_armed_table_when_prepare_put_then_old_image_is_requested() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = armed_table();
    let prepared = store
        .prepare_table_put(put_input(&table, item("pk-1"), Some(ttl_config())))
        .unwrap();
    assert!(prepared.observation.requests_old_item());
}

#[test]
fn given_update_without_expression_when_prepare_then_condition_only_update() {
    use storage_types::UpdateItemRequest;

    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let request = UpdateItemRequest::builder()
        .table_name(table_name())
        .key(key_attributes("pk-1"))
        .condition_expression(Some("attribute_exists(pk)".to_owned()))
        .build();
    let ttl = ttl_context();
    let prepared = store
        .prepare_table_update(TableUpdateInput {
            write: PhysicalWrite::try_new(0, table_name(), request, ttl_context()).unwrap(),
            table: std::sync::Arc::new(table.clone()),
            context: crate::TableUpdateContext {
                stream: stream_context(),
                index: 0,
                ttl_config: None,
                ttl_context: ttl.clone(),
                item_stream_ttl_hours: None,
            },
        })
        .unwrap();
    assert!(prepared.operations.is_empty());
    assert!(prepared.observation.requests_old_item());
}

#[test]
fn given_duplicate_keys_when_validate_batch_then_error() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let batch = TableWriteBatchInput::try_new(
        vec![
            put_input(&table, item("same"), None),
            put_input(&table, item("same"), None),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let prepared = store.prepare_table_batch(batch).unwrap();
    let snapshot = super::models::ValidationSnapshot {};
    let error = store
        .validate_prepared_batch(&prepared, &snapshot)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("multiple operations on one item")
    );
}

fn numeric_table() -> StoredTableInfo {
    let mut table = bare_table();
    table.table_name = TableName::new("harmonised-numbers");
    table.key_schema = vec![KeySchemaElement {
        attribute_name: "pk".to_owned(),
        key_type: KeyType::Hash,
    }];
    table.attribute_definitions = vec![AttributeDefinition {
        attribute_name: "pk".to_owned(),
        attribute_type: KeyAttributeType::N,
    }];
    table
}

fn numeric_item(pk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_owned(), AttributeValue::N(pk.to_owned()))])
}

#[test]
fn given_number_equivalent_keys_when_validate_batch_then_duplicate_error() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = numeric_table();
    let batch = TableWriteBatchInput::try_new(
        vec![
            put_input(&table, numeric_item("1E3"), None),
            put_input(&table, numeric_item("1000"), None),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let prepared = store.prepare_table_batch(batch).unwrap();
    let snapshot = super::models::ValidationSnapshot {};
    store
        .validate_prepared_batch(&prepared, &snapshot)
        .unwrap_err();
}

#[test]
fn given_condition_true_when_finish_put_then_finished() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let mut input = put_input(&table, item("pk-1"), None);
    input.write = PhysicalWrite::try_new(
        0,
        table_name(),
        PutItemRequest::builder()
            .table_name(table_name())
            .item(storage_types::WireItem::try_from(&item("pk-1")).unwrap())
            .condition_expression(Some("attribute_exists(pk)".to_owned()))
            .build(),
        ttl_context(),
    )
    .unwrap();
    let prepared = store.prepare_table_put(input).unwrap();
    assert!(prepared.observation.requests_old_item());
    let finished = prepared.finish_with_old(Some(wire_item("pk-1"))).unwrap();
    assert!(finished.old_item.is_some());
}

#[test]
fn given_condition_false_when_finish_put_then_old_image_follows_return_mode() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let mut input = put_input(&table, item("pk-1"), None);
    input.write = PhysicalWrite::try_new(
        0,
        table_name(),
        PutItemRequest::builder()
            .table_name(table_name())
            .item(storage_types::WireItem::try_from(&item("pk-1")).unwrap())
            .condition_expression(Some("attribute_not_exists(pk)".to_owned()))
            .build(),
        ttl_context(),
    )
    .unwrap();
    let mut prepared = store.prepare_table_put(input).unwrap();
    match prepared.clone().finish_with_old(Some(wire_item("pk-1"))) {
        Err(TableWriteError::ConditionFailed {
            ordinal, old_item, ..
        }) => {
            assert_eq!(ordinal, 0);
            assert!(old_item.is_none());
        }
        Err(TableWriteError::Failed(_)) => {
            panic!("expected condition failure, not storage failure")
        }
        Ok(_) => panic!("expected condition failure"),
    }
    prepared.return_old_on_condition_failure = true;
    match prepared.finish_with_old(Some(wire_item("pk-1"))) {
        Err(TableWriteError::ConditionFailed { old_item, .. }) => {
            assert!(old_item.is_some());
        }
        _ => panic!("expected condition failure with old image"),
    }
}

fn prepared_check(condition_expression: &str) -> super::table_finish::PreparedTableCheck {
    use super::{conditions::UncheckedCondition, table_finish::PreparedTableCheck};

    PreparedTableCheck {
        ordinal: 0,
        table_name: table_name(),
        key: key_attributes("requested"),
        key_json: "requested".to_owned(),
        table_key: storage_types::TableKey::new(
            table_name(),
            AttributeValue::S("requested".to_owned()),
            None,
        ),
        key_fingerprint: "requested".to_owned(),
        condition: UncheckedCondition::parse(Some(condition_expression.to_owned()), None, None)
            .unwrap()
            .unwrap(),
        return_old_on_condition_failure: false,
    }
}

#[test]
fn given_legacy_item_without_key_when_finish_check_then_key_condition_sees_addressed_key() {
    let legacy = WireItem::try_from(&HashMap::from([(
        "data".to_owned(),
        AttributeValue::S("value".to_owned()),
    )]))
    .unwrap();

    prepared_check("attribute_exists(pk)")
        .check_with_old(Some(legacy))
        .unwrap();
}

#[test]
fn given_mismatched_key_when_finish_check_then_true_non_key_condition_cannot_pass() {
    let stored = WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), AttributeValue::S("different".to_owned())),
        ("data".to_owned(), AttributeValue::S("present".to_owned())),
    ]))
    .unwrap();

    let error = prepared_check("attribute_exists(data)")
        .check_with_old(Some(stored))
        .unwrap_err();

    assert!(matches!(error, TableWriteError::Failed(_)));
}

#[test]
fn given_legacy_item_without_keys_when_finish_put_then_key_merge_passes() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let mut input = put_input(&table, item("pk-1"), None);
    input.write = PhysicalWrite::try_new(
        0,
        table_name(),
        PutItemRequest::builder()
            .table_name(table_name())
            .item(storage_types::WireItem::try_from(&item("pk-1")).unwrap())
            .condition_expression(Some("attribute_exists(pk)".to_owned()))
            .build(),
        ttl_context(),
    )
    .unwrap();
    let prepared = store.prepare_table_put(input).unwrap();
    let legacy = HashMap::from([("data".to_owned(), AttributeValue::S("value".to_owned()))]);
    let legacy_wire = WireItem::try_from(&legacy).unwrap();
    let finished = prepared.finish_with_old(Some(legacy_wire)).unwrap();
    let attributes = finished.old_item.unwrap().into_attribute_map().unwrap();
    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("pk-1".to_owned()))
    );
}

#[test]
fn given_update_without_old_when_finish_then_key_seeded_image() {
    use storage_types::UpdateItemRequest;

    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let request = UpdateItemRequest::builder()
        .table_name(table_name())
        .key(key_attributes("pk-1"))
        .update_expression("SET data = :v")
        .expression_attribute_values(Some(storage_types::AttributeMap::from_iter([(
            ":v".to_owned(),
            AttributeValue::S("updated".to_owned()),
        )])))
        .build();
    let ttl = ttl_context();
    let prepared = store
        .prepare_table_update(TableUpdateInput {
            write: PhysicalWrite::try_new(0, table_name(), request, ttl_context()).unwrap(),
            table: std::sync::Arc::new(table.clone()),
            context: crate::TableUpdateContext {
                stream: stream_context(),
                index: 0,
                ttl_config: None,
                ttl_context: ttl.clone(),
                item_stream_ttl_hours: None,
            },
        })
        .unwrap();
    let finished = prepared.finish_with_old(None).unwrap();
    let attributes = finished.new_item.into_attribute_map().unwrap();
    assert_eq!(
        attributes.get("data"),
        Some(&AttributeValue::S("updated".to_owned()))
    );
    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("pk-1".to_owned()))
    );
}

#[test]
fn given_batch_when_execute_then_receipts_follow_ordinals() {
    use storage_types::DeleteItemRequest;

    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let mut first = put_input(&table, item("pk-1"), None);
    first.write = PhysicalWrite::try_new(
        7,
        table_name(),
        PutItemRequest::new(
            table_name(),
            storage_types::WireItem::try_from(&(item("pk-1"))).unwrap(),
        ),
        ttl_context(),
    )
    .unwrap();
    let batch = TableWriteBatchInput::try_new(
        vec![first],
        Vec::new(),
        vec![TableDeleteInput {
            write: PhysicalWrite::try_new(
                3,
                table_name(),
                DeleteItemRequest {
                    table_name: table_name(),
                    key: key_attributes("pk-2"),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    expected: None,
                    conditional_operator: None,
                    return_values: None,
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                },
                ttl_context(),
            )
            .unwrap(),
            table: std::sync::Arc::new(table.clone()),
            stream: stream_context(),
            ttl_config: None,
            item_stream_ttl_hours: None,
        }],
        Vec::new(),
    )
    .unwrap();
    let receipts = tokio_test_block_on(store.execute_table_batch(batch)).unwrap();
    assert_eq!(receipts.receipts.len(), 2);
    assert!(receipts.receipts.iter().any(|receipt| receipt.ordinal == 7));
    assert!(receipts.receipts.iter().any(|receipt| receipt.ordinal == 3));
}

#[test]
fn given_ttl_enabled_and_old_present_when_finish_delete_then_ttl_delete_planned() {
    let store = TestTableStore {
        staged_ordinals: Mutex::new(Vec::new()),
    };
    let table = bare_table();
    let old = HashMap::from([
        ("pk".to_owned(), AttributeValue::S("pk-1".to_owned())),
        (
            "expires_at".to_owned(),
            AttributeValue::N("1700000001".to_owned()),
        ),
    ]);
    let input = TableDeleteInput {
        write: PhysicalWrite::try_new(
            0,
            table_name(),
            storage_types::DeleteItemRequest {
                table_name: table_name(),
                key: key_attributes("pk-1"),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                expected: None,
                conditional_operator: None,
                return_values: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            },
            ttl_context(),
        )
        .unwrap(),
        table: std::sync::Arc::new(table.clone()),
        stream: stream_context(),
        ttl_config: Some(ttl_config()),
        item_stream_ttl_hours: None,
    };
    let prepared = store.prepare_table_delete(input).unwrap();
    assert!(prepared.observation.requests_old_item());
    let old_wire = WireItem::try_from(&old).unwrap();
    let finished = prepared.finish_with_old(Some(old_wire)).unwrap();
    let writes: Vec<_> = finished.ttl_writes.into_iter().collect();
    assert!(!writes.is_empty());
    assert!(matches!(writes[0], TtlIndexWrite::Delete(_)));
}

fn tokio_test_block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
