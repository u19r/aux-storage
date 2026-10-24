use std::{collections::BTreeSet, hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use storage_types::{AttributeMap, AttributeValue, ReadSequenceNode, ReadSequenceNodeId};

use super::{
    ReadSequenceSqlDecodedRow, ReadSequenceSqlDialect, ReadSequenceSqlEnvelopeRow,
    ReadSequenceSqlIdentifier, ReadSequenceSqlIr, ReadSequenceSqlIrNode, ReadSequenceSqlKeyType,
    ReadSequenceSqlMetadata, ReadSequenceSqlNodeMetadata, ReadSequenceSqlPredicate,
    ReadSequenceSqlRowKind, ReadSequenceSqlShape, build_read_sequence_sql_ir,
    emit_postgresql_read_sequence_sql, emit_sqlite_read_sequence_sql,
    materialize_read_sequence_sql_mapped, read_sequence_sql_mapped_source,
};

const ITERATIONS: usize = 32;

#[test]
fn given_bounded_sql_key_metadata_when_validated_then_linear_scan_avoids_set_allocations() {
    let (_, _, mut metadata) = emitter_fixture();
    let node = metadata.nodes.values_mut().next().expect("emitter node");
    node.key_columns = (0..4)
        .map(|index| ReadSequenceSqlIdentifier::new(format!("key_{index}")))
        .collect::<Result<_, _>>()
        .expect("key identifiers");
    node.key_attribute_names = (0..4).map(|index| format!("key_{index}")).collect();
    node.key_types = vec![ReadSequenceSqlKeyType::String; 4];
    node.order_columns = node.key_columns.clone();
    node.predicates = node
        .key_columns
        .iter()
        .map(|column| ReadSequenceSqlPredicate {
            column: column.clone(),
            operator: super::ReadSequenceSqlOperator::Equal,
            value: AttributeValue::S("value".into()),
        })
        .collect();

    let baseline = measure_metadata_validation_baseline(node);
    let current = measure_metadata_validation_current(node);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(current.allocation_count < baseline.allocation_count);
    assert!(current.allocated_bytes < baseline.allocated_bytes);
}

fn measure_metadata_validation_baseline(
    metadata: &ReadSequenceSqlNodeMetadata,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "sql_key_metadata_btree_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..1024 {
        let allowed_columns = metadata
            .key_columns
            .iter()
            .chain(metadata.order_columns.iter())
            .map(|column| column.as_str())
            .collect::<BTreeSet<_>>();
        let valid = metadata
            .predicates
            .iter()
            .all(|predicate| allowed_columns.contains(predicate.column.as_str()));
        black_box(valid);
    }
    guard.finish()
}

fn measure_metadata_validation_current(
    metadata: &ReadSequenceSqlNodeMetadata,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "sql_key_metadata_linear_current",
        file!(),
        line!(),
        Some("optimized"),
    );
    for _ in 0..1024 {
        black_box(super::validate_sql_key_metadata(metadata).is_ok());
    }
    guard.finish()
}

#[test]
fn order_by_writer_avoids_temporary_fragment_allocations() {
    let columns = (0..24)
        .map(|index| ReadSequenceSqlIdentifier::new(format!("sort_{index}")))
        .collect::<Result<Vec<_>, _>>()
        .expect("order columns");
    let expected = legacy_order_by("items", &columns, &[]);
    let actual = super::emitter::order_by("items", &columns, &[]);
    assert_eq!(actual, expected);

    let baseline = measure_order_by(legacy_order_by, &columns);
    let current = measure_order_by(super::emitter::order_by, &columns);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected direct order-by writer to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected direct order-by writer to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );
}

#[test]
fn decoder_key_values_move_into_the_owned_attribute_map() {
    let (_, mut ir, _) = emitter_fixture();
    ir.nodes[0].metadata.key_attribute_names =
        (0..24).map(|index| format!("key_{index}")).collect();
    let node = &ir.nodes[0];
    let key_values = (0..24)
        .map(|index| AttributeValue::S(format!("value-{index}-{}", "x".repeat(64))))
        .collect::<Vec<_>>();

    let mut expected = AttributeMap::new();
    legacy_append_sql_key_values(&mut expected, key_values.clone(), node).expect("legacy keys");
    let mut actual = AttributeMap::new();
    super::decoder::append_sql_key_values(&mut actual, key_values.clone(), node)
        .expect("current keys");
    assert_eq!(actual, expected);

    let baseline = measure_decoder_key_append(legacy_append_sql_key_values, &key_values, node);
    let current =
        measure_decoder_key_append(super::decoder::append_sql_key_values, &key_values, node);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected key-value moves to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected key-value moves to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );
}

type KeyAppend = fn(
    &mut AttributeMap,
    Vec<AttributeValue>,
    &ReadSequenceSqlIrNode,
) -> Result<(), super::ReadSequenceSqlCompileError>;

fn measure_decoder_key_append(
    append: KeyAppend,
    key_values: &[AttributeValue],
    node: &ReadSequenceSqlIrNode,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "decoder_key_values_move_vs_clone",
        file!(),
        line!(),
        None,
    );
    for _ in 0..256 {
        let mut item = AttributeMap::with_capacity(key_values.len());
        append(&mut item, key_values.to_vec(), node).expect("keys");
        black_box(item);
    }
    guard.finish()
}

fn legacy_append_sql_key_values(
    item: &mut AttributeMap,
    key_values: Vec<AttributeValue>,
    metadata: &ReadSequenceSqlIrNode,
) -> Result<(), super::ReadSequenceSqlCompileError> {
    for (attribute_name, value) in metadata.metadata.key_attribute_names.iter().zip(key_values) {
        if item
            .get(attribute_name)
            .is_some_and(|stored| stored != &value)
        {
            return Err(super::ReadSequenceSqlCompileError::MalformedResult);
        }
        item.insert(attribute_name.clone(), value.clone());
    }
    Ok(())
}

type OrderByWriter = fn(&str, &[ReadSequenceSqlIdentifier], &[ReadSequenceSqlIdentifier]) -> String;

fn measure_order_by(
    writer: OrderByWriter,
    columns: &[ReadSequenceSqlIdentifier],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "read_sequence_order_by_writer",
        file!(),
        line!(),
        None,
    );
    for _ in 0..256 {
        black_box(writer("items", columns, &[]));
    }
    guard.finish()
}

fn legacy_order_by(
    relation: &str,
    columns: &[ReadSequenceSqlIdentifier],
    fallback_columns: &[ReadSequenceSqlIdentifier],
) -> String {
    let columns = if columns.is_empty() {
        fallback_columns
    } else {
        columns
    };
    columns
        .iter()
        .map(|column| format!("{relation}.\"{}\" ASC", column.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[test]
#[ignore = "performance evidence; run explicitly with --ignored --nocapture"]
fn given_sql_read_shapes_when_measured_then_emit_allocation_and_runtime_evidence() {
    let (plan, ir, metadata) = emitter_fixture();
    measure_emitter("emitter_get", &plan, &ir, ReadSequenceSqlDialect::Sqlite);
    measure_emitter(
        "emitter_get_postgres",
        &plan,
        &ir,
        ReadSequenceSqlDialect::PostgreSql,
    );
    let (batch_plan, batch_ir) = batch_emitter_fixture(100);
    measure_emitter(
        "emitter_batch_100",
        &batch_plan,
        &batch_ir,
        ReadSequenceSqlDialect::Sqlite,
    );

    for rows in [1, 25, 100, 500] {
        let envelope = envelope_fixture(rows, &metadata);
        measure_decoder("decoder_all_attributes", &plan, &ir, &envelope);

        let mut projected_ir = ir.clone();
        projected_ir.nodes[0].metadata.projected_attributes =
            Some(vec!["pk".into(), "sk".into(), "attribute_0".into()]);
        measure_decoder(
            "decoder_projected_attributes",
            &plan,
            &projected_ir,
            &envelope,
        );
    }

    let (mapped_plan, mapped_ir) = mapped_fixture();
    measure_emitter(
        "emitter_mapped_2",
        &mapped_plan,
        &mapped_ir,
        ReadSequenceSqlDialect::Sqlite,
    );
    for rows in [1, 25, 100, 500] {
        let decoded = mapped_rows(rows, &mapped_ir);
        measure_materialization(
            "materialize_mapped_response",
            &mapped_plan,
            &mapped_ir,
            &decoded,
        );
    }
}

fn measure_emitter(
    label: &'static str,
    plan: &storage_types::ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    dialect: ReadSequenceSqlDialect,
) {
    for _ in 0..4 {
        let statement = emit(plan, ir, dialect);
        black_box(statement.sql.len() ^ statement.parameters.len());
    }
    let guard = AllocationGuard::start(module_path!(), label, file!(), line!(), None);
    let started = Instant::now();
    let mut checksum = 0usize;
    for _ in 0..ITERATIONS {
        let statement = emit(plan, ir, dialect);
        checksum ^= statement.sql.len() ^ statement.parameters.len();
        black_box(&statement.cache_key);
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    println!(
        "{{\"label\":\"{label}\",\"iterations\":{ITERATIONS},\"ns_per_op\":{},\"\
         allocations_per_op\":{},\"bytes_per_op\":{},\"checksum\":{checksum}}}",
        elapsed.as_nanos() as f64 / ITERATIONS as f64,
        report.allocation_count as f64 / ITERATIONS as f64,
        report.allocated_bytes as f64 / ITERATIONS as f64,
    );
}

fn measure_decoder(
    label: &'static str,
    plan: &storage_types::ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    envelope: &[ReadSequenceSqlEnvelopeRow],
) {
    for _ in 0..4 {
        let rows = envelope.to_vec();
        let decoded = super::decode_read_sequence_sql_rows(plan, ir, rows).expect("decode");
        assert!(!decoded.is_empty());
        black_box(decoded);
    }
    let batches = (0..ITERATIONS)
        .map(|_| envelope.to_vec())
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start(module_path!(), label, file!(), line!(), None);
    let started = Instant::now();
    let mut checksum = 0usize;
    for rows in batches {
        let decoded = super::decode_read_sequence_sql_rows(plan, ir, rows).expect("decode");
        checksum ^= decoded.len();
        black_box(decoded);
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    println!(
        "{{\"label\":\"{label}\",\"rows\":{},\"iterations\":{ITERATIONS},\"ns_per_row\":{},\"\
         allocations_per_row\":{},\"bytes_per_row\":{},\"checksum\":{checksum}}}",
        envelope.len(),
        elapsed.as_nanos() as f64 / (ITERATIONS * envelope.len()) as f64,
        report.allocation_count as f64 / (ITERATIONS * envelope.len()) as f64,
        report.allocated_bytes as f64 / (ITERATIONS * envelope.len()) as f64,
    );
}

fn measure_materialization(
    label: &'static str,
    plan: &storage_types::ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    rows: &[ReadSequenceSqlDecodedRow],
) {
    for _ in 0..4 {
        let materialized =
            materialize_read_sequence_sql_mapped(plan, ir, rows.to_vec()).expect("materialize");
        assert!(!materialized.is_empty());
        black_box(materialized);
    }
    let batches = (0..ITERATIONS).map(|_| rows.to_vec()).collect::<Vec<_>>();
    let guard = AllocationGuard::start(module_path!(), label, file!(), line!(), None);
    let started = Instant::now();
    let mut checksum = 0usize;
    for rows in batches {
        let materialized =
            materialize_read_sequence_sql_mapped(plan, ir, rows).expect("materialize");
        checksum ^= materialized.len();
        black_box(materialized);
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    println!(
        "{{\"label\":\"{label}\",\"rows\":{},\"iterations\":{ITERATIONS},\"ns_per_row\":{},\"\
         allocations_per_row\":{},\"bytes_per_row\":{},\"checksum\":{checksum}}}",
        rows.len(),
        elapsed.as_nanos() as f64 / (ITERATIONS * rows.len()) as f64,
        report.allocation_count as f64 / (ITERATIONS * rows.len()) as f64,
        report.allocated_bytes as f64 / (ITERATIONS * rows.len()) as f64,
    );
}

fn emit(
    plan: &storage_types::ReadSequencePlan,
    ir: &ReadSequenceSqlIr,
    dialect: ReadSequenceSqlDialect,
) -> super::ReadSequenceSqlStatement {
    match dialect {
        ReadSequenceSqlDialect::PostgreSql => {
            emit_postgresql_read_sequence_sql(plan, ir, 1_048_576, 65_535).expect("emit PostgreSQL")
        }
        ReadSequenceSqlDialect::Sqlite => {
            emit_sqlite_read_sequence_sql(plan, ir, 1_048_576, 65_535).expect("emit SQLite")
        }
    }
}

fn emitter_fixture() -> (
    storage_types::ReadSequencePlan,
    ReadSequenceSqlIr,
    ReadSequenceSqlMetadata,
) {
    let mut plan =
        storage_types::plan_read_sequence(&storage_types::ReadSequenceRequest::new(vec![
            ReadSequenceNode::new(
                "root",
                storage_types::ReadSequenceNodeOperation::Get(Box::new(
                    storage_types::GetItemRequest {
                        table_name: storage_types::TableName::new("items"),
                        key: [("pk".into(), AttributeValue::S("tenant".into()))]
                            .into_iter()
                            .collect(),
                        attributes_to_get: None,
                        consistent_read: None,
                        projection_expression: None,
                        expression_attribute_names: None,
                        return_consumed_capacity: None,
                    },
                )),
            ),
        ]))
        .expect("plan");
    plan.nodes[0].operation =
        storage_types::ReadSequenceNodeOperation::Query(Box::new(storage_types::QueryRequest {
            table_name: storage_types::TableName::new("items"),
            index_name: None,
            key_condition_expression: "pk = :pk".into(),
            attributes_to_get: None,
            conditional_operator: None,
            filter_expression: None,
            query_filter: None,
            projection_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(
                [(String::from(":pk"), AttributeValue::S("tenant".into()))]
                    .into_iter()
                    .collect(),
            ),
            limit: Some(500),
            exclusive_start_key: None,
            return_consumed_capacity: None,
            consistent_read: None,
            scan_index_forward: Some(true),
            select: None,
        }));
    let relation = ReadSequenceSqlIdentifier::new("items").expect("relation");
    let column = ReadSequenceSqlIdentifier::new("pk").expect("column");
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "sql-perf-schema".into(),
        max_parameters: 65_535,
        max_sql_bytes: 1_048_576,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation,
                shape: ReadSequenceSqlShape::Query,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: vec![column.clone()],
                predicates: vec![super::ReadSequenceSqlPredicate {
                    column,
                    operator: super::ReadSequenceSqlOperator::Equal,
                    value: AttributeValue::S("tenant".into()),
                }],
                batch_keys: Vec::new(),
                limit: None,
                max_indexers: storage_types::MaxIndexers::try_new(2).expect("capacity"),
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).expect("IR");
    (plan, ir, metadata)
}

fn envelope_fixture(
    rows: usize,
    metadata: &ReadSequenceSqlMetadata,
) -> Vec<ReadSequenceSqlEnvelopeRow> {
    let item_json = serde_json::to_vec(&serde_json::json!({
        "attribute_0": {"S": "value-0"},
        "attribute_1": {"S": "value-1"},
        "attribute_2": {"S": "value-2"},
        "attribute_3": {"S": "value-3"},
        "attribute_4": {"S": "value-4"},
        "attribute_5": {"S": "value-5"},
        "attribute_6": {"S": "value-6"},
        "attribute_7": {"S": "value-7"},
        "attribute_8": {"S": "value-8"},
        "attribute_9": {"S": "x".repeat(1200)},
    }))
    .expect("item JSON");
    assert_eq!(
        metadata.nodes[&ReadSequenceNodeId::from_index(0)]
            .key_columns
            .len(),
        1
    );
    (0..rows)
        .map(|item_ordinal| ReadSequenceSqlEnvelopeRow {
            node_ordinal: 0,
            invocation_ordinal: 0,
            row_kind: ReadSequenceSqlRowKind::Item,
            item_ordinal: u32::try_from(item_ordinal).expect("ordinal"),
            key_values: vec![AttributeValue::S("tenant".into())],
            item_json: Some(item_json.clone()),
            indexer_values: vec![None, None],
        })
        .collect()
}

fn batch_emitter_fixture(key_count: usize) -> (storage_types::ReadSequencePlan, ReadSequenceSqlIr) {
    let (mut plan, _, mut metadata) = emitter_fixture();
    plan.nodes[0].operation = storage_types::ReadSequenceNodeOperation::BatchGet(Box::new(
        storage_types::BatchGetItemRequest {
            request_items: [(
                storage_types::TableName::new("items"),
                storage_types::KeysAndAttributes {
                    keys: Default::default(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: None,
                },
            )]
            .into_iter()
            .collect(),
            return_consumed_capacity: None,
        },
    ));
    let node_metadata = metadata
        .nodes
        .get_mut(&ReadSequenceNodeId::from_index(0))
        .expect("metadata");
    node_metadata.shape = ReadSequenceSqlShape::BatchGet;
    node_metadata.predicates.clear();
    node_metadata.batch_keys = (0..key_count)
        .map(|index| vec![AttributeValue::S(format!("tenant-{index}"))])
        .collect();
    let ir = build_read_sequence_sql_ir(&plan, &metadata).expect("batch IR");
    (plan, ir)
}

fn mapped_fixture() -> (storage_types::ReadSequencePlan, ReadSequenceSqlIr) {
    let request = serde_json::from_value::<storage_types::ReadSequenceRequest>(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "tenant"}}
            }}},
            {"Name": "child", "Operation": {"Get": {
                "TableName": "customers",
                "Key": {
                    "pk": {"S": "customer"},
                    "sk": {"FromInput": "customer"}
                }
            }}, "Inputs": {"customer": {
                "From": {"Node": "parents", "Select": "$.Query.Items[*].customer_id"},
                "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
                "Cardinality": "MANY",
                "OnMissing": "SKIP"
            }}, "Iterate": "customer"}
        ]
    }))
    .expect("mapped request");
    let plan = storage_types::plan_read_sequence(&request).expect("mapped plan");
    let (parent, child, source) = read_sequence_sql_mapped_source(&plan).expect("mapped source");
    let metadata = |mapped_source| ReadSequenceSqlNodeMetadata {
        relation: ReadSequenceSqlIdentifier::new("items").expect("relation"),
        shape: ReadSequenceSqlShape::Get,
        key_attribute_names: vec!["pk".into(), "sk".into()],
        key_columns: vec![
            ReadSequenceSqlIdentifier::new("pk").expect("pk"),
            ReadSequenceSqlIdentifier::new("sk").expect("sk"),
        ],
        key_types: vec![ReadSequenceSqlKeyType::String; 2],
        order_columns: vec![],
        predicates: vec![],
        batch_keys: vec![],
        limit: None,
        max_indexers: storage_types::MaxIndexers::try_new(1).expect("capacity"),
        projected_attributes: None,
        exclude_tombstones: false,
        mapped_source,
    };
    let ir = ReadSequenceSqlIr {
        schema_digest: "mapped-perf-schema".into(),
        nodes: vec![
            ReadSequenceSqlIrNode {
                node: parent,
                metadata: ReadSequenceSqlNodeMetadata {
                    shape: ReadSequenceSqlShape::Query,
                    ..metadata(None)
                },
            },
            ReadSequenceSqlIrNode {
                node: child,
                metadata: metadata(Some(source)),
            },
        ],
        parameter_count: 0,
    };
    (plan, ir)
}

fn mapped_rows(rows: usize, ir: &ReadSequenceSqlIr) -> Vec<ReadSequenceSqlDecodedRow> {
    let parent = ir.nodes[0].node;
    let child = ir.nodes[1].node;
    let child_item = |value: &str| {
        [
            ("pk".into(), AttributeValue::S("customer".into())),
            ("sk".into(), AttributeValue::S(value.into())),
            ("attribute_0".into(), AttributeValue::S("child".into())),
        ]
        .into_iter()
        .collect::<AttributeMap>()
    };
    let mut result = Vec::with_capacity(rows * 2);
    for item_ordinal in 0..rows {
        let value = format!("customer-{}", item_ordinal % 25);
        result.push(ReadSequenceSqlDecodedRow {
            node: parent,
            invocation_ordinal: 0,
            item_ordinal: u32::try_from(item_ordinal).expect("ordinal"),
            item: [
                ("pk".into(), AttributeValue::S("tenant".into())),
                (
                    "sk".into(),
                    AttributeValue::S(format!("order-{item_ordinal}")),
                ),
                ("customer_id".into(), AttributeValue::S(value.clone())),
            ]
            .into_iter()
            .collect(),
            indexers: vec!["customer_id".into()],
            mapped_indexer_value: Some(value.clone()),
        });
        result.push(ReadSequenceSqlDecodedRow {
            node: child,
            invocation_ordinal: u32::try_from(item_ordinal).expect("ordinal"),
            item_ordinal: 0,
            item: child_item(&value),
            indexers: vec![],
            mapped_indexer_value: None,
        });
    }
    result
}
