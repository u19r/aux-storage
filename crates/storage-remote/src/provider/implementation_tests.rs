use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloc_counter::AllocationGuard;
use http::{HeaderMap, HeaderValue};
use http_request::Url;
use storage_common::serializable_payload_bytes;
use storage_provider::{MAX_RANGE_READ_EVALUATED_BYTES, RangeReadBudget, RangeReadPage};
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, GlobalSecondaryIndex, IndexName,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, KeysAndAttributes, MaxIndexers,
    Projection, ProjectionType, STORAGE_LEADER_HINT_HEADER, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, TableStatus, TimestampMillis, WireItem,
    context::WrappedError as _,
};

use crate::{
    constants::MAX_RETRY_AFTER_SECS,
    provider::{
        implementation::{
            leader_hint_header, parse_retry_after, preserve_retry_after,
            strip_batch_get_projections,
        },
        wire_item_helper::ScanQueryWireParsed,
    },
};

const BILLING_BYTE_COUNT_ITERATIONS: usize = 256;
const BATCH_GET_PROJECTION_ITERATIONS: usize = 64;
const HTTP_ATTEMPT_SETUP_ITERATIONS: usize = 128;

fn range_table_info_with_keys_only_gsi() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("remote-range-table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        max_indexers: MaxIndexers::default(),
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("by_gsi"),
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
                projection_type: Some(ProjectionType::KeysOnly),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: Default::default(),
        default_item_stream_duration: Default::default(),
        deletion_protection_enabled: false,
    }
}

fn range_wire_item(payload: Option<&str>) -> WireItem {
    let mut item = serde_json::Map::from_iter([
        ("pk".to_string(), serde_json::json!({"S": "tenant#1"})),
        ("sk".to_string(), serde_json::json!({"S": "item#1"})),
        ("gsi_pk".to_string(), serde_json::json!({"S": "group#1"})),
        ("gsi_sk".to_string(), serde_json::json!({"S": "item#1"})),
    ]);
    if let Some(payload) = payload {
        item.insert("payload".to_string(), serde_json::json!({"S": payload}));
    }
    WireItem::dynamo_json(serde_json::to_vec(&item).expect("range wire item"))
}

fn source_page(
    items: Vec<WireItem>,
    table_info: &StoredTableInfo,
    index_name: &Option<IndexName>,
    max_items: u32,
) -> StorageResult<RangeReadPage> {
    let count =
        u32::try_from(items.len()).expect("source page test item count fits its wire count type");
    ScanQueryWireParsed {
        items,
        count,
        scanned_count: count,
        last_evaluated_key: None,
    }
    .into_page(
        Some(table_info),
        index_name,
        RangeReadBudget::try_new(max_items, MAX_RANGE_READ_EVALUATED_BYTES)?,
    )
}

#[test]
fn given_keys_only_gsi_source_when_page_is_built_then_cursor_uses_source_keys() {
    let table = range_table_info_with_keys_only_gsi();
    let index_name = Some(IndexName::new("by_gsi"));
    let page = source_page(vec![range_wire_item(None)], &table, &index_name, 1)
        .expect("keys-only source page");

    assert_eq!(page.progress.evaluated_count, 1);
    assert!(page.progress.last_evaluated_key.is_some());
    assert!(
        page.items[0]
            .attribute_value("payload")
            .expect("payload lookup")
            .is_none()
    );
}

#[test]
fn given_source_page_over_item_budget_when_page_is_built_then_rejects_response() {
    let table = range_table_info_with_keys_only_gsi();
    let index_name = Some(IndexName::new("by_gsi"));
    let error = source_page(
        vec![range_wire_item(None), range_wire_item(Some("second"))],
        &table,
        &index_name,
        1,
    )
    .expect_err("upstream response over item budget");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("bounded read budget")
    ));
}

#[test]
fn given_large_source_image_when_page_is_built_then_logical_bytes_are_counted() {
    let table = range_table_info_with_keys_only_gsi();
    let index_name = Some(IndexName::new("by_gsi"));
    let payload = "x".repeat(32_000);
    let page = source_page(
        vec![range_wire_item(Some(payload.as_str()))],
        &table,
        &index_name,
        1,
    )
    .expect("large source page");

    assert!(page.progress.evaluated_bytes > payload.len() as u64);
    assert!(
        page.items[0]
            .attribute_value("payload")
            .expect("payload lookup")
            .is_some()
    );
}

#[test]
fn given_source_page_over_byte_budget_when_page_is_built_then_rejects_response() {
    let table = range_table_info_with_keys_only_gsi();
    let index_name = Some(IndexName::new("by_gsi"));
    let payload = "x".repeat(350_000);
    let error = source_page(
        vec![
            range_wire_item(Some(payload.as_str())),
            range_wire_item(Some(payload.as_str())),
            range_wire_item(Some(payload.as_str())),
        ],
        &table,
        &index_name,
        3,
    )
    .expect_err("upstream response over logical byte budget");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("bounded read budget")
    ));
}

fn measure_billing_byte_count_baseline(
    item: &HashMap<String, storage_types::AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "billing_byte_count_baseline",
        file!(),
        line!(),
        None,
    );
    for _ in 0..BILLING_BYTE_COUNT_ITERATIONS {
        let bytes = serde_json::to_vec(item).expect("billing payload serializes");
        std::hint::black_box(bytes.len());
    }
    guard.finish()
}

fn measure_billing_byte_count_candidate(
    item: &HashMap<String, storage_types::AttributeValue>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "billing_byte_count_candidate",
        file!(),
        line!(),
        None,
    );
    for _ in 0..BILLING_BYTE_COUNT_ITERATIONS {
        std::hint::black_box(serializable_payload_bytes(item));
    }
    guard.finish()
}

#[test]
fn given_serializable_payload_when_billing_bytes_are_counted_then_no_output_buffer_is_allocated() {
    let item = HashMap::from([(
        "payload".to_string(),
        storage_types::AttributeValue::S("x".repeat(16_384)),
    )]);
    let expected = serde_json::to_vec(&item).expect("billing payload serializes");
    assert_eq!(serializable_payload_bytes(&item), expected.len() as u64);

    let baseline = measure_billing_byte_count_baseline(&item);
    let candidate = measure_billing_byte_count_candidate(&item);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(
        candidate.allocated_bytes < baseline.allocated_bytes,
        "expected counting writer to allocate fewer bytes, baseline={} candidate={}",
        baseline.allocated_bytes,
        candidate.allocated_bytes
    );
}

fn batch_get_projection_fixture() -> BatchGetItemRequest {
    let keys = (0..100)
        .map(|index| {
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("partition-{index}")),
                ),
                ("sk".to_string(), AttributeValue::S(format!("sort-{index}"))),
            ])
            .into()
        })
        .collect::<Vec<KeyAttributes>>();
    let attributes_to_get = (0..10).map(|index| format!("attribute_{index}"));
    let expression_attribute_names = (0..10)
        .map(|index| (format!("#a{index}"), format!("attribute_{index}")))
        .collect();

    BatchGetItemRequest {
        request_items: HashMap::from([(
            TableName::new("items"),
            KeysAndAttributes {
                keys,
                attributes_to_get: Some(attributes_to_get.collect()),
                projection_expression: Some(
                    (0..10)
                        .map(|index| format!("#a{index}"))
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                expression_attribute_names: Some(expression_attribute_names),
                consistent_read: Some(true),
            },
        )])
        .into(),
        return_consumed_capacity: Some(storage_types::ReturnConsumedCapacity::Total),
    }
}

fn clone_and_strip_batch_get_request(request: &BatchGetItemRequest) -> BatchGetItemRequest {
    let mut remote_request = request.clone();
    for keys_and_attributes in remote_request.request_items.values_mut() {
        keys_and_attributes.attributes_to_get = None;
        keys_and_attributes.projection_expression = None;
        keys_and_attributes.expression_attribute_names = None;
    }
    remote_request
}

fn measure_batch_get_projection_clone_baseline(
    fixture: &BatchGetItemRequest,
) -> alloc_counter::AllocationReport<'static> {
    let inputs = (0..BATCH_GET_PROJECTION_ITERATIONS)
        .map(|_| fixture.clone())
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start(
        module_path!(),
        "batch_get_projection_clone_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for input in inputs {
        std::hint::black_box(clone_and_strip_batch_get_request(&input));
    }
    guard.finish()
}

fn measure_batch_get_projection_sidecar_candidate(
    fixture: &BatchGetItemRequest,
) -> alloc_counter::AllocationReport<'static> {
    let inputs = (0..BATCH_GET_PROJECTION_ITERATIONS)
        .map(|_| fixture.clone())
        .collect::<Vec<_>>();
    let guard = AllocationGuard::start(
        module_path!(),
        "batch_get_projection_sidecar_candidate",
        file!(),
        line!(),
        Some("candidate"),
    );
    for mut input in inputs {
        let projections = strip_batch_get_projections(&mut input);
        std::hint::black_box((input, projections));
    }
    guard.finish()
}

#[test]
fn given_batch_get_projection_metadata_when_preparing_remote_request_then_sidecar_preserves_wire_and_reduces_clone_allocations()
 {
    let fixture = batch_get_projection_fixture();
    let baseline = clone_and_strip_batch_get_request(&fixture);
    let mut candidate = fixture.clone();
    let projections = strip_batch_get_projections(&mut candidate);
    assert_eq!(
        serde_json::to_vec(&candidate).expect("candidate serializes"),
        serde_json::to_vec(&baseline).expect("baseline serializes")
    );

    let table_name = TableName::new("items");
    let mut restored = candidate
        .request_items
        .remove(&table_name)
        .expect("candidate table");
    let projection = projections.into_iter().next().expect("one projection").1;
    projection.restore(&mut restored);
    assert_eq!(
        serde_json::to_vec(&restored).expect("restored serializes"),
        serde_json::to_vec(
            fixture
                .request_items
                .get(&table_name)
                .expect("fixture table")
        )
        .expect("fixture serializes")
    );

    let baseline_report = measure_batch_get_projection_clone_baseline(&fixture);
    let candidate_report = measure_batch_get_projection_sidecar_candidate(&fixture);
    let _ = alloc_counter::emit_report(&baseline_report);
    let _ = alloc_counter::emit_report(&candidate_report);
    assert!(
        candidate_report.allocated_bytes < baseline_report.allocated_bytes,
        "expected projection sidecar to allocate fewer bytes, baseline={} candidate={}",
        baseline_report.allocated_bytes,
        candidate_report.allocated_bytes
    );
}

fn attempt_base_headers(target: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_str(target).expect("fixture target header"),
    );
    headers
}

fn response_headers_fixture() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        STORAGE_LEADER_HINT_HEADER,
        HeaderValue::from_static("http://leader.example/"),
    );
    headers.insert("retry-after", HeaderValue::from_static("7"));
    for index in 0..8 {
        headers.insert(
            http::header::HeaderName::from_bytes(format!("x-fixture-{index}").as_bytes())
                .expect("fixture header name"),
            HeaderValue::from_static("fixture-value"),
        );
    }
    headers
}

fn measure_legacy_http_attempt_setup(
    endpoint: &str,
    target: &str,
    response: &HeaderMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "http_attempt_setup_legacy",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..HTTP_ATTEMPT_SETUP_ITERATIONS {
        let request_url = Url::parse(endpoint).expect("fixture endpoint URL");
        let visited_urls = HashSet::from([request_url.as_str().to_owned()]);
        let base_headers = attempt_base_headers(target);
        let first_headers = base_headers.clone();
        let response_headers = response.clone();
        let leader_hint = leader_hint_header(&response_headers);
        let retry_after = parse_retry_after(&response_headers);
        std::hint::black_box((
            request_url,
            visited_urls,
            first_headers,
            response_headers,
            leader_hint,
            retry_after,
        ));
    }
    guard.finish()
}

fn measure_current_http_attempt_setup(
    endpoint: &Url,
    target: &str,
    response: &HeaderMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "http_attempt_setup_current",
        file!(),
        line!(),
        Some("candidate"),
    );
    for _ in 0..HTTP_ATTEMPT_SETUP_ITERATIONS {
        let request_url = endpoint.clone();
        let first_headers = attempt_base_headers(target);
        let visited_urls: Option<HashSet<String>> = None;
        let leader_hint = leader_hint_header(response);
        let retry_after = parse_retry_after(response);
        std::hint::black_box((
            request_url,
            first_headers,
            visited_urls,
            leader_hint,
            retry_after,
        ));
    }
    guard.finish()
}

#[test]
fn given_common_http_attempt_setup_when_redirects_are_disabled_then_legacy_clones_are_removed() {
    let endpoint = "http://127.0.0.1:8080/";
    let endpoint_url = Url::parse(endpoint).expect("fixture endpoint URL");
    let response = response_headers_fixture();
    let baseline =
        measure_legacy_http_attempt_setup(endpoint, "DynamoDB_20120810.GetItem", &response);
    let candidate =
        measure_current_http_attempt_setup(&endpoint_url, "DynamoDB_20120810.GetItem", &response);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(
        candidate.allocated_bytes < baseline.allocated_bytes,
        "expected common HTTP setup to allocate fewer bytes, baseline={} candidate={}",
        baseline.allocated_bytes,
        candidate.allocated_bytes
    );
}

#[test]
fn given_service_unavailable_when_retry_after_is_parsed_then_hint_reaches_returned_error() {
    let error = preserve_retry_after(
        StorageError::service_unavailable(1),
        Some(Duration::from_secs(7)),
    );

    assert!(matches!(
        error.to_enum(),
        StorageEnum::ServiceUnavailable {
            retry_after_seconds: 7,
            ..
        }
    ));
}

#[test]
fn given_non_service_error_when_retry_after_is_parsed_then_error_class_is_preserved() {
    let error = preserve_retry_after(
        StorageError::from(StorageEnum::Throttled {
            message: "slow down".to_string(),
        }),
        Some(Duration::from_secs(7)),
    );

    assert!(matches!(error.to_enum(), StorageEnum::Throttled { .. }));
}

#[test]
fn given_delta_seconds_retry_after_when_parsed_then_duration_is_preserved() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("7"));

    assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(7)));
}

#[test]
fn given_absurd_retry_after_when_parsed_then_duration_is_clamped() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("999999999"));

    assert_eq!(
        parse_retry_after(&headers),
        Some(Duration::from_secs(MAX_RETRY_AFTER_SECS))
    );
}
