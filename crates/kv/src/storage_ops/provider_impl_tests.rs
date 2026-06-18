use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_common::ttl::TtlConfigRecord;
use storage_condition::parse_condition_expression;
use storage_types::{
    AttributeDefinition, AttributeValue, DeleteRequest, EncodePutRequest, EncodeWriteRequest,
    IndexName, ItemKey, KeyAttributeType, KeySchemaElement, KeyType, StoredTableInfo, TableName,
    TableStatus, TimeToLiveStatus, TimestampMillis, WireItem,
};

use crate::{
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    sorted_kv_store::TransactWriteOperation,
    storage_provider::{
        TransactConditionBindingCacheEntry, cached_transact_condition_binding,
        encode_requests_to_write_requests, normalized_attribute_map_for_write,
        normalized_wire_item_for_write, project_wire_item_table_key_and_ttl,
        ttl_index_direct_operations_for_wire_items, ttl_tracking_enabled,
        wire_item_key_token_from_item_key,
    },
};

fn table_identity() -> TableIdentity {
    TableIdentity::new(TableStorageId::new(1), TableName::new("jobs"), Vec::new())
}

fn table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("jobs"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "ttl".to_string(),
                attribute_type: KeyAttributeType::N,
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
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn ttl_config(status: TimeToLiveStatus) -> TtlConfigRecord {
    TtlConfigRecord::new("ttl".to_string(), &IndexName::new("ttl-index"), status)
}

fn wire_item(pk: &str, sk: &str, ttl: Option<&str>) -> WireItem {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::N(sk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(
                "large value that should not be needed for key projection".to_string(),
            ),
        ),
    ]);
    if let Some(ttl) = ttl {
        item.insert("ttl".to_string(), AttributeValue::N(ttl.to_string()));
    }
    WireItem::from_attribute_map(&item).expect("wire item")
}

#[test]
fn normalized_attribute_map_for_write_borrows_when_numbers_are_already_plain_tests() {
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("job#1".to_string())),
        ("sk".to_string(), AttributeValue::N("12.3".to_string())),
    ]);

    assert!(matches!(
        normalized_attribute_map_for_write(&item),
        std::borrow::Cow::Borrowed(_)
    ));
}

#[test]
fn normalized_attribute_map_for_write_expands_scientific_numbers_tests() {
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("job#1".to_string())),
        ("sk".to_string(), AttributeValue::N("1E2".to_string())),
    ]);

    let normalized = normalized_attribute_map_for_write(&item);
    assert!(matches!(normalized, std::borrow::Cow::Owned(_)));
    assert_eq!(
        normalized.get("sk"),
        Some(&AttributeValue::N("100".to_string()))
    );
}

#[test]
fn normalized_wire_item_for_write_expands_scientific_numbers_tests() {
    let item = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("job#1".to_string())),
        ("sk".to_string(), AttributeValue::N("1E-2".to_string())),
    ]))
    .expect("wire item");

    let normalized = normalized_wire_item_for_write(&item).expect("normalize wire item");
    assert!(matches!(normalized, std::borrow::Cow::Owned(_)));
    assert_eq!(
        normalized
            .to_attribute_map()
            .expect("normalized item map")
            .get("sk"),
        Some(&AttributeValue::N("0.01".to_string()))
    );
}

const TRANSACT_CONDITION_CACHE_ITERATIONS: usize = 512;
const TRANSACT_CONDITION_CACHE_WIDTH: usize = 25;
type TransactConditionInput = (
    String,
    Option<HashMap<String, String>>,
    Option<HashMap<String, AttributeValue>>,
);

#[test]
fn transact_condition_binding_cache_reuses_repeated_condition_parse_tests() {
    let baseline = measure_uncached_transact_condition_binding();
    let cached = measure_cached_transact_condition_binding();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&cached);

    assert!(
        cached.allocation_count < baseline.allocation_count,
        "expected cached transaction condition binding to allocate less often, baseline={} \
         cached={}",
        baseline.allocation_count,
        cached.allocation_count
    );
    assert!(
        cached.allocated_bytes < baseline.allocated_bytes,
        "expected cached transaction condition binding to allocate fewer bytes, baseline={} \
         cached={}",
        baseline.allocated_bytes,
        cached.allocated_bytes
    );
}

fn measure_uncached_transact_condition_binding() -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "transact_condition_binding_uncached",
        file!(),
        line!(),
        Some("uncached"),
    );
    for _ in 0..TRANSACT_CONDITION_CACHE_ITERATIONS {
        for (condition, names, values) in repeated_transaction_condition_inputs() {
            let parsed =
                parse_condition_expression(condition.as_str(), names.as_ref(), values.as_ref())
                    .expect("parse condition");
            std::hint::black_box(parsed);
        }
    }
    guard.finish()
}

fn measure_cached_transact_condition_binding() -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "transact_condition_binding_cached",
        file!(),
        line!(),
        Some("cached"),
    );
    for _ in 0..TRANSACT_CONDITION_CACHE_ITERATIONS {
        let mut cache = Vec::<TransactConditionBindingCacheEntry>::new();
        for (condition, names, values) in repeated_transaction_condition_inputs() {
            let parsed =
                cached_transact_condition_binding(&mut cache, Some(condition), names, values)
                    .expect("bind condition");
            std::hint::black_box(parsed);
        }
    }
    guard.finish()
}

fn repeated_transaction_condition_inputs() -> Vec<TransactConditionInput> {
    let names = HashMap::from([
        ("#status".to_string(), "status".to_string()),
        ("#sk".to_string(), "sk".to_string()),
        ("#tags".to_string(), "tags".to_string()),
        ("#metrics".to_string(), "metrics".to_string()),
        ("#count".to_string(), "count".to_string()),
    ]);
    let values = HashMap::from([
        (":open".to_string(), AttributeValue::S("open".to_string())),
        (
            ":pending".to_string(),
            AttributeValue::S("pending".to_string()),
        ),
        (
            ":sk_prefix".to_string(),
            AttributeValue::S("tenant#".to_string()),
        ),
        (
            ":required_tag".to_string(),
            AttributeValue::S("required".to_string()),
        ),
        (
            ":number_type".to_string(),
            AttributeValue::S("N".to_string()),
        ),
    ]);
    (0..TRANSACT_CONDITION_CACHE_WIDTH)
        .map(|_| {
            (
                "#status IN (:open, :pending) AND begins_with(#sk, :sk_prefix) AND \
                 contains(#tags, :required_tag) AND attribute_type(#metrics.#count, :number_type)"
                    .to_string(),
                Some(names.clone()),
                Some(values.clone()),
            )
        })
        .collect()
}

#[test]
fn ttl_tracking_only_runs_while_ttl_is_enabled_or_enabling() {
    assert!(!ttl_tracking_enabled(None));
    assert!(ttl_tracking_enabled(Some(&ttl_config(
        TimeToLiveStatus::Enabled
    ))));
    assert!(ttl_tracking_enabled(Some(&ttl_config(
        TimeToLiveStatus::Enabling
    ))));
    assert!(!ttl_tracking_enabled(Some(&ttl_config(
        TimeToLiveStatus::Disabling
    ))));
    assert!(!ttl_tracking_enabled(Some(&ttl_config(
        TimeToLiveStatus::Disabled
    ))));
}

#[test]
fn write_path_projects_table_key_and_ttl_without_decoding_the_full_item_map() {
    let item = wire_item("JOB#1", "42", Some("1700000500"));

    let (item_key, ttl) = project_wire_item_table_key_and_ttl(&item, &table_info(), Some("ttl"))
        .expect("project key and ttl");

    assert_eq!(ttl, Some(1_700_000_500));
    assert_eq!(item_key.hash_key(), &AttributeValue::S("JOB#1".to_string()));
    assert_eq!(
        item_key.range_key(),
        Some(&AttributeValue::N("42".to_string()))
    );
}

#[test]
fn write_path_ignores_non_numeric_ttl_values_when_projecting_the_ttl_index_key() {
    let item = wire_item("JOB#1", "42", Some("not-a-number"));

    let (item_key, ttl) = project_wire_item_table_key_and_ttl(&item, &table_info(), Some("ttl"))
        .expect("project key and ttl");

    assert_eq!(ttl, None);
    assert_eq!(item_key.hash_key(), &AttributeValue::S("JOB#1".to_string()));
}

#[test]
fn write_path_rejects_items_missing_any_part_of_the_table_key() {
    let item = WireItem::from_attribute_map(&HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("JOB#1".to_string()),
    )]))
    .expect("wire item");

    let error = project_wire_item_table_key_and_ttl(&item, &table_info(), Some("ttl"))
        .expect_err("missing range key fails");

    assert!(format!("{error}").contains("Invalid or missing key"));
}

#[test]
fn wire_item_key_tokens_round_trip_through_the_item_key_parser() {
    let item_key = ItemKey::table_key(
        TableName::new("jobs"),
        AttributeValue::S("JOB#1".to_string()),
        Some(AttributeValue::N("42".to_string())),
    );

    let token = wire_item_key_token_from_item_key(&item_key).expect("key token");
    let decoded =
        ItemKey::item_key_from_next_page_token(&token, &table_info(), &None).expect("decode token");
    let decoded = decoded.expect("decoded key");

    assert_eq!(decoded.hash_key(), item_key.hash_key());
    assert_eq!(decoded.range_key(), item_key.range_key());
}

#[test]
fn ttl_index_direct_operations_are_empty_when_ttl_tracking_is_not_active() {
    let operations = ttl_index_direct_operations_for_wire_items(
        &TableName::new("jobs"),
        &table_identity(),
        &table_info(),
        Some(&ttl_config(TimeToLiveStatus::Disabled)),
        Some(&wire_item("JOB#1", "42", Some("1700000500"))),
        Some(&wire_item("JOB#1", "42", Some("1700000600"))),
        None,
        None,
    )
    .expect("ttl operations");

    assert!(operations.is_empty());
}

#[test]
fn ttl_index_direct_operations_skip_writes_when_the_expiration_bucket_is_unchanged() {
    let old_item = wire_item("JOB#1", "42", Some("1700000500"));
    let new_item = wire_item("JOB#1", "42", Some("1700000500"));
    let (new_item_key, ttl) =
        project_wire_item_table_key_and_ttl(&new_item, &table_info(), Some("ttl"))
            .expect("project key");
    let token = wire_item_key_token_from_item_key(&new_item_key).expect("key token");

    let operations = ttl_index_direct_operations_for_wire_items(
        &TableName::new("jobs"),
        &table_identity(),
        &table_info(),
        Some(&ttl_config(TimeToLiveStatus::Enabled)),
        Some(&old_item),
        Some(&new_item),
        Some(&token),
        ttl,
    )
    .expect("ttl operations");

    assert!(operations.is_empty());
}

#[test]
fn ttl_index_direct_operations_delete_old_bucket_and_put_new_bucket_when_ttl_changes() {
    let old_item = wire_item("JOB#1", "42", Some("1700000500"));
    let new_item = wire_item("JOB#1", "42", Some("1700000600"));
    let (new_item_key, ttl) =
        project_wire_item_table_key_and_ttl(&new_item, &table_info(), Some("ttl"))
            .expect("project key");
    let token = wire_item_key_token_from_item_key(&new_item_key).expect("key token");

    let operations = ttl_index_direct_operations_for_wire_items(
        &TableName::new("jobs"),
        &table_identity(),
        &table_info(),
        Some(&ttl_config(TimeToLiveStatus::Enabled)),
        Some(&old_item),
        Some(&new_item),
        Some(&token),
        ttl,
    )
    .expect("ttl operations");

    assert_eq!(operations.len(), 2);
    assert!(matches!(
        operations[0],
        TransactWriteOperation::Delete { .. }
    ));
    assert!(matches!(operations[1], TransactWriteOperation::Put { .. }));
}

#[test]
fn encoded_batch_writes_preserve_put_and_delete_semantics_for_legacy_batch_apis() {
    let put_item = wire_item("JOB#1", "42", Some("1700000500"));
    let delete_key = HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("JOB#deleted".to_string()),
    )]);

    let converted = encode_requests_to_write_requests(&[
        EncodeWriteRequest {
            put_request: Some(EncodePutRequest {
                item: put_item,
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        },
        EncodeWriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest {
                key: delete_key.clone().into(),
                aux_item_stream_ttl_hours: None,
            }),
        },
    ])
    .expect("encoded write requests convert");

    assert_eq!(converted.len(), 2);
    assert!(converted[0].put_request.is_some());
    assert_eq!(
        converted[1]
            .delete_request
            .as_ref()
            .expect("delete request")
            .key,
        delete_key.into()
    );
}

#[test]
fn encoded_batch_writes_reject_requests_that_are_not_exactly_one_operation() {
    let error = encode_requests_to_write_requests(&[EncodeWriteRequest {
        put_request: None,
        delete_request: None,
    }])
    .expect_err("empty write request fails");

    assert!(format!("{error}").contains("Each WriteRequest must contain exactly one"));
}
