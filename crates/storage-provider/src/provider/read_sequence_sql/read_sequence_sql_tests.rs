use std::collections::HashMap;

use storage_types::{
    AttributeValue, GetItemRequest, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeOperation, ReadSequencePlan, ReadSequenceRequest, TableName,
};

use crate::provider::{
    ReadSequenceSqlCompileError, ReadSequenceSqlEnvelopeRow, ReadSequenceSqlIdentifier,
    ReadSequenceSqlKeyType, ReadSequenceSqlMetadata, ReadSequenceSqlNodeMetadata,
    ReadSequenceSqlOperator, ReadSequenceSqlPredicate, ReadSequenceSqlRowKind,
    ReadSequenceSqlShape, build_read_sequence_sql_ir, decode_read_sequence_sql_rows,
    emit_postgresql_read_sequence_sql, emit_sqlite_read_sequence_sql,
    read_sequence_sql_mapped_source,
};

fn root_plan() -> ReadSequencePlan {
    storage_types::plan_read_sequence(&ReadSequenceRequest::new(vec![ReadSequenceNode::new(
        "root",
        ReadSequenceNodeOperation::Get(GetItemRequest {
            table_name: TableName::new("items"),
            key: [("pk".into(), AttributeValue::S("x".into()))]
                .into_iter()
                .collect(),
            attributes_to_get: None,
            consistent_read: None,
            projection_expression: None,
            expression_attribute_names: None,
            return_consumed_capacity: None,
        }),
    )]))
    .expect("plan")
}

fn mapped_plan(cardinality: &str) -> ReadSequencePlan {
    let (select, iterate) = if cardinality == "MANY" {
        ("$.Query.Items[*].customer_id", Some("customer"))
    } else {
        ("$.Query.Items[0].customer_id", None)
    };
    let mut child = serde_json::json!({
        "Name": "child",
        "Operation": {"Get": {
            "TableName": "customers",
            "Key": {
                "pk": {"S": "customer"},
                "sk": {"FromInput": "customer"}
            }
        }},
        "Inputs": {"customer": {
            "From": {"Node": "parents", "Select": select},
            "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
            "Cardinality": cardinality,
            "OnMissing": if cardinality == "MANY" { "SKIP" } else { "ERROR" }
        }}
    });
    if let Some(iterate) = iterate {
        child["Iterate"] = serde_json::Value::String(iterate.to_string());
    }
    let request = serde_json::from_value::<ReadSequenceRequest>(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "tenant"}}
            }}},
            child
        ]
    }))
    .expect("mapped request");
    storage_types::plan_read_sequence(&request).expect("mapped plan")
}

#[test]
fn mapped_sql_source_accepts_one_and_many_with_hash_and_range_targets() {
    for (cardinality, iterates) in [("ONE", false), ("MANY", true)] {
        let plan = mapped_plan(cardinality);
        let (_, _, source) = read_sequence_sql_mapped_source(&plan).expect("mapped SQL source");
        assert_eq!(source.iterates, iterates);
        assert_eq!(source.keys.len(), 2);
    }
}

#[test]
fn identifier_validation_rejects_sql_injection() {
    assert!(ReadSequenceSqlIdentifier::new("table_surface-20260809").is_ok());
    assert_eq!(
        ReadSequenceSqlIdentifier::new("items; DROP TABLE users").unwrap_err(),
        ReadSequenceSqlCompileError::UnsafeIdentifier
    );
}

#[test]
fn emitters_bind_values_and_keep_order_stable() {
    let plan = root_plan();
    let relation = ReadSequenceSqlIdentifier::new("items").unwrap();
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "schema-v1".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation,
                shape: ReadSequenceSqlShape::Get,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: vec![column.clone()],
                predicates: vec![ReadSequenceSqlPredicate {
                    column,
                    operator: ReadSequenceSqlOperator::Equal,
                    value: AttributeValue::S("x'); DROP TABLE items;--".into()),
                }],
                batch_keys: Vec::new(),
                limit: Some(1),
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let statement = emit_postgresql_read_sequence_sql(
        &plan,
        &ir,
        metadata.max_sql_bytes,
        metadata.max_parameters,
    )
    .unwrap();
    assert!(statement.sql.contains("$1"));
    assert!(!statement.sql.contains("DROP TABLE"));
    assert_eq!(statement.parameters.len(), 2);
    let sqlite =
        emit_sqlite_read_sequence_sql(&plan, &ir, metadata.max_sql_bytes, metadata.max_parameters)
            .unwrap();
    assert!(sqlite.sql.contains("?1"));
    assert!(!sqlite.sql.contains("$1"));
    assert_eq!(sqlite.parameters, statement.parameters);
}

#[test]
fn numeric_keys_use_explicit_casts_and_prefix_predicates_are_rejected() {
    let mut plan = root_plan();
    plan.nodes[0].operation = ReadSequenceNodeOperation::Get(GetItemRequest {
        table_name: TableName::new("items"),
        key: [("pk".into(), AttributeValue::N("001.50".into()))]
            .into_iter()
            .collect(),
        attributes_to_get: None,
        consistent_read: None,
        projection_expression: None,
        expression_attribute_names: None,
        return_consumed_capacity: None,
    });
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "numeric-schema".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation: ReadSequenceSqlIdentifier::new("items").unwrap(),
                shape: ReadSequenceSqlShape::Get,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::Number],
                order_columns: vec![column.clone()],
                predicates: vec![ReadSequenceSqlPredicate {
                    column: column.clone(),
                    operator: ReadSequenceSqlOperator::Equal,
                    value: AttributeValue::N("001.50".into()),
                }],
                batch_keys: Vec::new(),
                limit: None,
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let postgres = emit_postgresql_read_sequence_sql(&plan, &ir, 32_768, 8).unwrap();
    assert!(postgres.sql.contains("CAST($1 AS TEXT)::NUMERIC"));
    assert!(postgres.sql.contains("CAST(\"items\".\"pk\" AS TEXT)"));
    let sqlite = emit_sqlite_read_sequence_sql(&plan, &ir, 32_768, 8).unwrap();
    assert!(sqlite.sql.contains("CAST(?1 AS NUMERIC)"));

    let mut prefix_metadata = metadata;
    prefix_metadata
        .nodes
        .get_mut(&ReadSequenceNodeId::from_index(0))
        .unwrap()
        .predicates[0]
        .operator = ReadSequenceSqlOperator::Prefix;
    let prefix_ir = build_read_sequence_sql_ir(&plan, &prefix_metadata).unwrap();
    assert_eq!(
        emit_sqlite_read_sequence_sql(&plan, &prefix_ir, 32_768, 8).unwrap_err(),
        ReadSequenceSqlCompileError::InvalidKeyMetadata
    );
}

#[test]
fn string_prefix_predicates_escape_sql_like_metacharacters() {
    let plan = root_plan();
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "prefix-schema".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation: ReadSequenceSqlIdentifier::new("items").unwrap(),
                shape: ReadSequenceSqlShape::Get,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: vec![column.clone()],
                predicates: vec![ReadSequenceSqlPredicate {
                    column,
                    operator: ReadSequenceSqlOperator::Prefix,
                    value: AttributeValue::S("a%_\\b".into()),
                }],
                batch_keys: Vec::new(),
                limit: None,
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let statement = emit_sqlite_read_sequence_sql(&plan, &ir, 32_768, 8).unwrap();
    assert!(statement.sql.contains("ESCAPE '\\'"));
    assert_eq!(
        statement.parameters,
        vec![AttributeValue::S("a\\%\\_\\\\b".into())]
    );
}

#[test]
fn batch_limit_is_ranked_per_input_and_limits_are_checked_before_emission() {
    let mut plan = root_plan();
    plan.nodes[0].operation =
        ReadSequenceNodeOperation::BatchGet(storage_types::BatchGetItemRequest {
            request_items: [(
                TableName::new("items"),
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
        });
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "batch-schema".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation: ReadSequenceSqlIdentifier::new("items").unwrap(),
                shape: ReadSequenceSqlShape::BatchGet,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: vec![column],
                predicates: Vec::new(),
                batch_keys: vec![
                    vec![AttributeValue::S("a".into())],
                    vec![AttributeValue::S("b".into())],
                ],
                limit: Some(2),
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let statement = emit_sqlite_read_sequence_sql(&plan, &ir, 32_768, 8).unwrap();
    assert!(
        statement
            .sql
            .contains("ranked.item_ordinal < CAST(?3 AS INTEGER)")
    );
    assert!(
        statement
            .sql
            .contains("ORDER BY ranked.invocation_ordinal, ranked.item_ordinal")
    );

    let mut too_small = metadata;
    too_small.max_parameters = 2;
    assert_eq!(
        build_read_sequence_sql_ir(&plan, &too_small).unwrap_err(),
        ReadSequenceSqlCompileError::ParameterLimit
    );
    let mut too_large = ir;
    too_large.parameter_count = 9;
    assert_eq!(
        emit_sqlite_read_sequence_sql(&plan, &too_large, 32_768, 8).unwrap_err(),
        ReadSequenceSqlCompileError::ParameterLimit
    );
}

#[test]
fn duplicate_batch_keys_preserve_input_ordinals_and_unsafe_predicates_fall_back_before_sql() {
    let mut plan = root_plan();
    plan.nodes[0].operation =
        ReadSequenceNodeOperation::BatchGet(storage_types::BatchGetItemRequest {
            request_items: [(
                TableName::new("items"),
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
        });
    let relation = ReadSequenceSqlIdentifier::new("items").unwrap();
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "schema-v1".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation,
                shape: ReadSequenceSqlShape::BatchGet,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: vec![column.clone()],
                predicates: Vec::new(),
                batch_keys: vec![vec![AttributeValue::S("x".into())]; 2],
                limit: None,
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let duplicate_ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let duplicate_sql = emit_postgresql_read_sequence_sql(
        &plan,
        &duplicate_ir,
        metadata.max_sql_bytes,
        metadata.max_parameters,
    )
    .unwrap();
    assert!(duplicate_sql.sql.contains("(0, $1), (1, $2)"));
    assert_eq!(duplicate_sql.parameters.len(), 2);
    let mut safe = metadata.clone();
    safe.nodes
        .get_mut(&ReadSequenceNodeId::from_index(0))
        .unwrap()
        .batch_keys = vec![vec![AttributeValue::S("x".into())]];
    safe.nodes
        .get_mut(&ReadSequenceNodeId::from_index(0))
        .unwrap()
        .predicates = vec![ReadSequenceSqlPredicate {
        column: ReadSequenceSqlIdentifier::new("attributes_blob").unwrap(),
        operator: ReadSequenceSqlOperator::Equal,
        value: AttributeValue::S("x".into()),
    }];
    assert_eq!(
        build_read_sequence_sql_ir(&plan, &safe).unwrap_err(),
        ReadSequenceSqlCompileError::InvalidKeyMetadata
    );
}

#[test]
fn query_lowering_keeps_cursor_predicates_and_fetches_one_extra_row() {
    let mut plan = root_plan();
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("group".into()));
    plan.nodes[0].operation = ReadSequenceNodeOperation::Query(storage_types::QueryRequest {
        table_name: TableName::new("items"),
        index_name: None,
        key_condition_expression: "pk = :pk".into(),
        attributes_to_get: None,
        conditional_operator: None,
        filter_expression: None,
        query_filter: None,
        projection_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(values),
        limit: Some(2),
        exclusive_start_key: None,
        return_consumed_capacity: None,
        consistent_read: None,
        scan_index_forward: Some(true),
        select: None,
    });
    let relation = ReadSequenceSqlIdentifier::new("items").unwrap();
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let sort = ReadSequenceSqlIdentifier::new("sk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "query-schema".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation,
                shape: ReadSequenceSqlShape::Query,
                key_attribute_names: vec!["pk".into(), "sk".into()],
                key_columns: vec![column.clone(), sort.clone()],
                key_types: vec![ReadSequenceSqlKeyType::String; 2],
                order_columns: vec![column, sort.clone()],
                predicates: vec![
                    ReadSequenceSqlPredicate {
                        column: ReadSequenceSqlIdentifier::new("pk").unwrap(),
                        operator: ReadSequenceSqlOperator::Equal,
                        value: AttributeValue::S("group".into()),
                    },
                    ReadSequenceSqlPredicate {
                        column: sort,
                        operator: ReadSequenceSqlOperator::GreaterThan,
                        value: AttributeValue::S("a".into()),
                    },
                ],
                batch_keys: Vec::new(),
                limit: Some(2),
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let statement = emit_postgresql_read_sequence_sql(&plan, &ir, 32_768, 8).unwrap();
    assert!(statement.sql.contains("\"items\".\"sk\" > $2"));
    assert!(statement.sql.contains("LIMIT CAST($3 AS INTEGER)"));
    assert_eq!(statement.parameters.len(), 3);
}

#[test]
fn decoder_rejects_bad_ordinals_and_decodes_typed_keys() {
    let plan = root_plan();
    let relation = ReadSequenceSqlIdentifier::new("items").unwrap();
    let column = ReadSequenceSqlIdentifier::new("pk").unwrap();
    let metadata = ReadSequenceSqlMetadata {
        schema_digest: "schema-v1".into(),
        max_parameters: 8,
        max_sql_bytes: 32_768,
        nodes: [(
            ReadSequenceNodeId::from_index(0),
            ReadSequenceSqlNodeMetadata {
                relation,
                shape: ReadSequenceSqlShape::Get,
                key_attribute_names: vec!["pk".into()],
                key_columns: vec![column],
                key_types: vec![ReadSequenceSqlKeyType::String],
                order_columns: Vec::new(),
                predicates: Vec::new(),
                batch_keys: Vec::new(),
                limit: None,
                max_indexers: storage_types::MaxIndexers::ZERO,
                projected_attributes: None,
                exclude_tombstones: false,
                mapped_source: None,
            },
        )]
        .into_iter()
        .collect(),
    };
    let ir = build_read_sequence_sql_ir(&plan, &metadata).unwrap();
    let rows = vec![ReadSequenceSqlEnvelopeRow {
        node_ordinal: 0,
        invocation_ordinal: 0,
        row_kind: ReadSequenceSqlRowKind::Item,
        item_ordinal: 0,
        key_values: vec![AttributeValue::S("pk".into())],
        item_json: Some(br#"{"value":{"S":"ok"}}"#.to_vec()),
        indexer_values: Vec::new(),
    }];
    let decoded = decode_read_sequence_sql_rows(&plan, &ir, rows.clone()).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded[0].item.get("pk"),
        Some(&AttributeValue::S("pk".into()))
    );
    assert!(decoded[0].item.contains_key("value"));

    let mut wrong_key_type = rows.clone();
    wrong_key_type[0].key_values = vec![AttributeValue::N("1".into())];
    assert_eq!(
        decode_read_sequence_sql_rows(&plan, &ir, wrong_key_type).unwrap_err(),
        ReadSequenceSqlCompileError::MalformedResult
    );

    let mut malformed = rows;
    malformed[0].item_ordinal = 1;
    assert_eq!(
        decode_read_sequence_sql_rows(&plan, &ir, malformed).unwrap_err(),
        ReadSequenceSqlCompileError::MalformedResult
    );
}
