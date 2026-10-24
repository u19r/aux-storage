use std::sync::Arc;

use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, MaxIndexers, Projection, ProjectionType, QueryTableRequest,
    ScanTableRequest, StorageEnum, StoredTableInfo, StreamRetentionDuration, TableName,
    TableStatus, TimestampMillis, WireItem, context::WrappedError as _,
};

use super::{RangeReadPage, RangeReadPlan, RangeReadiness};

fn table() -> Arc<StoredTableInfo> {
    Arc::new(StoredTableInfo {
        table_name: TableName::new("items"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        max_indexers: MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    })
}

fn keys_only_gsi_table() -> Arc<StoredTableInfo> {
    let mut table = (*table()).clone();
    table.attribute_definitions.push(AttributeDefinition {
        attribute_name: "gsi_pk".to_string(),
        attribute_type: KeyAttributeType::S,
    });
    table.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
        index_name: IndexName::new("gsi"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::KeysOnly),
            non_key_attributes: None,
        },
    }]);
    Arc::new(table)
}

#[test]
fn given_empty_source_when_finishing_page_then_progress_is_empty() {
    let result = RangeReadPage::<WireItem>::default().finish().unwrap();
    assert!(result.items.is_empty());
    assert_eq!(result.progress.evaluated_count, 0);
    assert_eq!(result.progress.evaluated_bytes, 0);
    assert!(result.progress.last_evaluated_key.is_none());
}

#[test]
fn query_bind_validates_schema_before_native_execution() {
    let request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: None,
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_query(request).unwrap();
    plan.bind(table(), RangeReadiness::BaseTable).unwrap();
}

#[test]
fn query_bind_rejects_unprojected_gsi_attributes() {
    let request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: Some(IndexName::new("gsi")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: Some("payload".to_string()),
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_query(request).unwrap();
    let error = plan
        .bind(keys_only_gsi_table(), RangeReadiness::GsiPublished)
        .unwrap_err();
    assert!(error.to_string().contains("does not project [payload]"));
}

#[test]
fn query_bind_rejects_missing_index_before_native_readiness() {
    let request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: Some(IndexName::new("missing")),
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: None,
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_query(request).unwrap();
    let error = plan.bind(table(), RangeReadiness::GsiPending).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not have the specified index: missing")
    );
}

#[test]
fn gsi_page_requires_native_readiness_evidence() {
    let request = ScanTableRequest {
        table_name: TableName::new("items"),
        index_name: Some(IndexName::new("gsi")),
        limit: Some(1),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_scan(request).unwrap();
    let bound = plan
        .bind(keys_only_gsi_table(), RangeReadiness::GsiPending)
        .unwrap();
    let error = bound.page(25, 100).unwrap_err();
    assert!(error.to_string().contains("Requested index is not ready"));
    let bound = bound.mark_gsi_published().unwrap();
    bound.page(25, 100).unwrap();
}

#[test]
fn gsi_scan_uses_its_published_projection_without_base_hydration() {
    let request = ScanTableRequest {
        table_name: TableName::new("items"),
        index_name: Some(IndexName::new("gsi")),
        limit: Some(2),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_scan(request).unwrap();
    let bound = plan
        .bind(keys_only_gsi_table(), RangeReadiness::GsiPublished)
        .unwrap();
    assert_eq!(
        bound.index.unwrap().projection.projection_type,
        Some(ProjectionType::KeysOnly)
    );
}

#[test]
fn bound_page_owns_cursor_limit_and_direction() {
    let request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: None,
        limit: Some(3),
        exclusive_start_key: None,
        scan_index_forward: Some(false),
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_query(request).unwrap();
    let bound = plan.bind(table(), RangeReadiness::BaseTable).unwrap();
    let page = bound.page(25, 100).unwrap();
    assert_eq!(page.budget.max_items, 3);
    assert!(!page.forward);
    assert!(page.start.is_none());
    assert!(page.condition.is_some());
}

#[test]
fn given_invalid_cursor_when_querying_then_key_validation_is_returned() {
    let mut request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: None,
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };
    request.exclusive_start_key = Some("invalid-token".to_string());
    let plan = RangeReadPlan::try_from_query(request).unwrap();
    let bound = plan.bind(table(), RangeReadiness::BaseTable).unwrap();
    let error = bound.page(25, 100).unwrap_err();
    assert!(matches!(error.as_ref(), StorageEnum::KeyValidation(_)));
}

#[test]
fn given_invalid_cursor_when_scanning_then_key_validation_is_returned() {
    let request = ScanTableRequest {
        table_name: TableName::new("items"),
        index_name: None,
        limit: Some(1),
        exclusive_start_key: Some("invalid-token".to_string()),
        consistent_read: false,
    };
    let plan = RangeReadPlan::try_from_scan(request).unwrap();
    let bound = plan.bind(table(), RangeReadiness::BaseTable).unwrap();
    let error = bound.page(25, 100).unwrap_err();
    assert!(matches!(error.as_ref(), StorageEnum::KeyValidation(_)));
}

#[test]
fn query_consistency_rule_is_owned_by_the_shared_range_plan() {
    let request = QueryTableRequest {
        table_name: TableName::new("items"),
        index_name: Some(IndexName::new("gsi")),
        key_condition_expression: "gsi_pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(storage_types::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("a".to_string()),
        )])),
        projection_expression: None,
        limit: Some(1),
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: true,
    };
    let error = RangeReadPlan::try_from_query(request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Consistent reads are not supported")
    );
}

#[test]
fn page_rejects_a_return_count_above_evaluated_count() {
    let error = RangeReadPage {
        items: vec![WireItem::dynamo_json(Vec::new())],
        progress: super::RangeReadProgress::default(),
        continuation: None,
    }
    .finish()
    .unwrap_err();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("evaluated")
    ));
}

#[test]
fn given_source_page_when_decoding_then_progress_and_cursor_are_preserved() {
    let wire = WireItem::try_from(&std::collections::HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("source".to_string()),
    )]))
    .unwrap();
    let key = wire.item_key(&table(), &None).unwrap().unwrap();
    let bytes = u64::from(wire.logical_size().unwrap());
    let token = key.next_page_token().unwrap();
    let page = RangeReadPage {
        items: vec![wire],
        progress: super::RangeReadProgress {
            evaluated_count: 1,
            evaluated_bytes: bytes,
            last_evaluated_key: Some(key),
        },
        continuation: Some(token.clone()),
    }
    .finish()
    .unwrap();
    let decoded = page.try_map(WireItem::to_attribute_map).unwrap();
    assert_eq!(
        decoded.items[0].get("pk").unwrap().clone(),
        AttributeValue::S("source".to_string())
    );
    assert_eq!(decoded.progress.evaluated_count, 1);
    assert_eq!(decoded.progress.evaluated_bytes, bytes);
    assert_eq!(
        decoded
            .progress
            .last_evaluated_key
            .unwrap()
            .next_page_token()
            .unwrap(),
        token
    );
    assert_eq!(decoded.continuation, Some(token));
}

#[test]
fn given_inconsistent_source_progress_when_finishing_then_page_is_rejected() {
    for (count, bytes, key) in [(0, 1, None), (1, 0, None), (1, 1, None)] {
        let page = RangeReadPage {
            items: vec![(); count as usize],
            progress: super::RangeReadProgress {
                evaluated_count: count,
                evaluated_bytes: bytes,
                last_evaluated_key: key,
            },
            continuation: None,
        };
        assert!(matches!(
            page.finish().unwrap_err().to_enum(),
            StorageEnum::InternalServerError { .. }
        ));
    }
}
