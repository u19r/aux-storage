use std::sync::Arc;

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, MaxIndexers, QueryRequest, ScanRequest, StorageError,
    StoredTableInfo, StreamRetentionDuration, TableName, TableStatus, TimestampMillis,
};

use super::dynamo_range_read::*;
use crate::{RangeReadItem, RangeReadPage, RangeReadProgress, RangeReadiness};

const FILTER: &str = "attribute_exists(a) AND (b = :b OR contains(c, :c))";
const ATTEMPT_ITERATIONS: usize = 64;

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
        stream_enabled_at: None,
        table_stream_duration: StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    })
}

fn string_value(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_string())
}

fn query(filter: Option<&str>, index: Option<&str>) -> ValidatedDynamoRangeRequest {
    let mut request = QueryRequest::new(TableName::new("items"), "pk = :pk".to_string());
    request.expression_attribute_values = Some(AttributeMap::from_iter([
        (":pk".to_string(), string_value("a")),
        (":b".to_string(), string_value("b")),
        (":c".to_string(), string_value("c")),
    ]));
    request.filter_expression = filter.map(str::to_owned);
    request.index_name = index.map(IndexName::new);
    ValidatedDynamoRangeRequest::compile(DynamoRangeRequest::Query(request))
}

/// Allocations of 64 attempt steps (prepare, bind, page) on this thread only.
fn attempt_allocations(
    request: &ValidatedDynamoRangeRequest,
    label: &'static str,
) -> alloc_counter::AllocationReport<'static> {
    let table = table();
    let guard = AllocationGuard::start(
        module_path!(),
        "range_attempt_steps",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ATTEMPT_ITERATIONS {
        let plan = request.0.prepare_range(&table, &request.1).unwrap();
        let bound = plan
            .bind(Arc::clone(&table), RangeReadiness::BaseTable)
            .unwrap();
        let page = bound.page(1000).unwrap();
        std::hint::black_box(&page);
    }
    guard.finish()
}

#[test]
fn given_query_when_two_attempts_bind_then_key_condition_is_shared() {
    let request = query(None, None);
    let table_1 = table();
    let plan_1 = request.0.prepare_range(&table_1, &request.1).unwrap();
    let bound_1 = plan_1.bind(table_1, RangeReadiness::BaseTable).unwrap();
    let page_1 = bound_1.page(1000).unwrap();
    let table_2 = table();
    let plan_2 = request.0.prepare_range(&table_2, &request.1).unwrap();
    let bound_2 = plan_2.bind(table_2, RangeReadiness::BaseTable).unwrap();
    let page_2 = bound_2.page(1000).unwrap();
    assert!(std::ptr::eq(
        page_1.condition.unwrap(),
        page_2.condition.unwrap()
    ));
}

#[test]
fn given_filter_when_attempt_steps_run_then_allocations_equal_unfiltered() {
    let plain = query(None, None);
    let filtered = query(Some(FILTER), None);
    let plain_report = attempt_allocations(&plain, "plain");
    let filtered_report = attempt_allocations(&filtered, "filtered");
    assert_eq!(
        plain_report.allocation_count, filtered_report.allocation_count,
        "plain {} vs filtered {}",
        plain_report.allocation_count, filtered_report.allocation_count
    );
}

#[test]
fn given_stored_filter_error_when_prepare_query_then_filter_error() {
    let mut request = query(Some(FILTER), None);
    request.1.filter = Err(StorageError::validation("filter-parse-marker"));
    let message = request
        .0
        .prepare_range(&table(), &request.1)
        .unwrap_err()
        .to_string();
    assert!(message.contains("filter-parse-marker"), "{message}");
}

#[test]
fn given_stored_key_error_and_missing_index_when_bind_then_index_error_first() {
    let mut request = query(None, Some("missing"));
    request.1.key_condition = Some(Err(StorageError::validation("key-parse-marker")));
    let plan = request.0.prepare_range(&table(), &request.1).unwrap();
    let message = plan
        .bind(table(), RangeReadiness::GsiPending)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("does not have the specified index: missing"),
        "{message}"
    );
    assert!(!message.contains("key-parse-marker"), "{message}");
}

#[test]
fn given_stored_key_error_when_bind_then_key_error() {
    let mut request = query(None, None);
    request.1.key_condition = Some(Err(StorageError::validation("key-parse-marker")));
    let plan = request.0.prepare_range(&table(), &request.1).unwrap();
    let message = plan
        .bind(table(), RangeReadiness::BaseTable)
        .unwrap_err()
        .to_string();
    assert!(message.contains("key-parse-marker"), "{message}");
}

#[test]
fn given_scan_stored_filter_error_when_prepare_then_ok_and_finish_then_filter_error() {
    let mut scan = ScanRequest::new(TableName::new("items"));
    scan.filter_expression = Some("a = :b".to_owned());
    scan.expression_attribute_values = Some(AttributeMap::from_iter([(
        ":b".to_string(),
        string_value("b"),
    )]));
    let mut request = ValidatedDynamoRangeRequest::compile(DynamoRangeRequest::Scan(scan));
    request.1.filter = Err(StorageError::validation("filter-parse-marker"));
    assert!(request.0.prepare_range(&table(), &request.1).is_ok());
    let page = RangeReadPage::<RangeReadItem> {
        items: Vec::new(),
        progress: RangeReadProgress::default(),
        continuation: None,
    };
    let message = request
        .0
        .finish_source(&table(), &request.1, page, false)
        .unwrap_err()
        .to_string();
    assert!(message.contains("filter-parse-marker"), "{message}");
}

fn range_item(pk: &str, json: &str) -> RangeReadItem {
    RangeReadItem {
        key: storage_types::ItemKey::table_key(TableName::new("items"), string_value(pk), None),
        item: storage_types::WireItem::dynamo_json(json.as_bytes().to_vec()),
    }
}

fn finish_scan(
    filter: Option<&str>,
    projection: Option<&str>,
    include_cursors: bool,
) -> DynamoRangeOutput {
    let mut scan = ScanRequest::new(TableName::new("items"));
    scan.filter_expression = filter.map(str::to_owned);
    scan.projection_expression = projection.map(str::to_owned);
    if filter.is_some() {
        scan.expression_attribute_values = Some(AttributeMap::from_iter([(
            ":n".to_string(),
            AttributeValue::N("1".to_string()),
        )]));
    }
    let request = ValidatedDynamoRangeRequest::compile(DynamoRangeRequest::Scan(scan));
    let page = RangeReadPage::<RangeReadItem> {
        items: vec![
            range_item(
                "a",
                r#"{"pk":{"S":"a"},"x":{"N":"2"},"y":{"S":"ya"},"z":{"S":"za"}}"#,
            ),
            range_item(
                "b",
                r#"{"pk":{"S":"b"},"x":{"N":"1"},"y":{"S":"yb"},"z":{"S":"zb"}}"#,
            ),
            range_item(
                "c",
                r#"{"pk":{"S":"c"},"x":{"N":"3"},"y":{"S":"yc"},"z":{"S":"zc"}}"#,
            ),
        ],
        progress: RangeReadProgress::default(),
        continuation: None,
    };
    request
        .0
        .finish_source(&table(), &request.1, page, include_cursors)
        .expect("finish page")
}

fn output_items(output: &DynamoRangeOutput) -> Vec<AttributeMap> {
    output
        .items
        .as_ref()
        .expect("items")
        .iter()
        .map(|item| AttributeMap::try_from(&item.item).expect("decode"))
        .collect()
}

fn cursor_names(output: &DynamoRangeOutput) -> Vec<String> {
    output
        .item_cursors
        .as_ref()
        .expect("cursors")
        .iter()
        .map(|key| match key.get("pk") {
            Some(AttributeValue::S(value)) => value.clone(),
            other => panic!("unexpected cursor {other:?}"),
        })
        .collect()
}

#[test]
fn finish_page_filter_and_projection_match_full_decode() {
    let output = finish_scan(Some("x > :n"), Some("y"), true);
    assert_eq!(
        output_items(&output),
        vec![
            AttributeMap::from_iter([("y".to_string(), string_value("ya"))]),
            AttributeMap::from_iter([("y".to_string(), string_value("yc"))]),
        ]
    );
    assert_eq!(output.count, 2);
    assert_eq!(cursor_names(&output), ["a", "c"]);

    let cursor_only = finish_scan(None, None, true);
    assert_eq!(output_items(&cursor_only).len(), 3);
    assert_eq!(
        output_items(&cursor_only)[1].get("z"),
        Some(&string_value("zb"))
    );
    assert_eq!(cursor_names(&cursor_only), ["a", "b", "c"]);
}
