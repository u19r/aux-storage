use std::{
    collections::{HashMap, HashSet},
    hint::black_box,
    time::Instant,
};

use alloc_counter::AllocationGuard;
use serde::Serialize;
use serde_json::json;

use crate::{
    AllOld, AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest,
    DescribeTableResponse, GetItemResponse, GetStreamRecordsRequest, GetStreamRecordsResponse,
    KeyAttributeType, KeySchemaElement, KeyType, MaxIndexers, PutItemResponse, StreamRecord,
    StreamSpecification, StreamViewType, SystemStreamPosition, SystemStreamScope, TableName,
    TableStreamFrontier, TimestampMillis, WireItem, WireItemKeyAttributes, latest_stream_metadata,
    prepare_table_metadata_at,
};

#[derive(Clone, Serialize)]
struct MapGetItemResponse {
    #[serde(rename = "Item")]
    item: crate::AttributeMap,
}

const GET_ITEM_JSON: &[u8] = br#"{"pk":{"S":"tenant#001"},"sk":{"S":"item#001"},"payload":{"S":"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},"counter":{"N":"42"},"active":{"BOOL":true}}"#;

#[test]
fn given_wire_item_when_get_item_response_round_trips_then_dynamo_json_is_preserved() {
    let response = GetItemResponse {
        item: Some(WireItem::dynamo_json(GET_ITEM_JSON.to_vec())),
    };
    let bytes = serde_json::to_vec(&response).unwrap();
    let expected: serde_json::Value = serde_json::from_slice(GET_ITEM_JSON).unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(actual["Item"], expected);

    let decoded: GetItemResponse = serde_json::from_slice(&bytes).unwrap();
    let WireItem::DynamoJson { data } = decoded.item.unwrap() else {
        panic!("GetItem must retain Dynamo JSON wire data");
    };
    assert_eq!(data, GET_ITEM_JSON);
}

#[test]
fn given_local_split_item_when_get_item_response_serializes_then_keys_override_blob() {
    let primary = WireItemKeyAttributes::new(
        "pk".to_owned(),
        AttributeValue::S("key".to_owned()),
        None,
        None,
    );
    let response = GetItemResponse {
        item: Some(WireItem::local_split(
            primary,
            None,
            Some(br#"{"pk":{"S":"stale"},"payload":{"N":"42"}}"#.to_vec()),
        )),
    };
    let value = serde_json::to_value(response).unwrap();
    assert_eq!(value["Item"]["pk"]["S"], "key");
    assert_eq!(value["Item"]["payload"]["N"], "42");
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_get_item_image_when_response_clones_and_serializes_then_report_allocations_and_time() {
    const ITERATIONS: usize = 20_000;
    let map = MapGetItemResponse {
        item: serde_json::from_slice(GET_ITEM_JSON).unwrap(),
    };
    let wire = GetItemResponse {
        item: Some(WireItem::dynamo_json(GET_ITEM_JSON.to_vec())),
    };
    for label in ["map_clone", "wire_clone", "map_serialize", "wire_serialize"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "given_get_item_image_when_response_clones_and_serializes_then_report_allocations_and_time",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut total = 0_usize;
        for _ in 0..ITERATIONS {
            total += match label {
                "map_clone" => black_box(map.clone()).item.len(),
                "wire_clone" => black_box(wire.clone()).item.unwrap().payload_len(),
                "map_serialize" => serde_json::to_vec(black_box(&map)).unwrap().len(),
                _ => serde_json::to_vec(black_box(&wire)).unwrap().len(),
            };
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);
        println!(
            "get_item_response label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

fn legacy_validate_key_schema(request: &CreateTableRequest) -> Result<(), crate::StorageError> {
    if request.key_schema.is_empty() || request.key_schema.len() > 2 {
        return Err(crate::StorageError::validation("key_schema"));
    }
    let mut seen = HashSet::new();
    for key_schema in &request.key_schema {
        if !seen.insert(&key_schema.attribute_name) {
            return Err(crate::StorageError::validation("duplicate"));
        }
    }
    let definitions: HashMap<_, _> = request
        .attribute_definitions
        .iter()
        .map(|definition| (&definition.attribute_name, definition))
        .collect();
    for key_schema in &request.key_schema {
        if !definitions.contains_key(&key_schema.attribute_name) {
            return Err(crate::StorageError::validation("missing"));
        }
    }
    Ok(())
}

fn table_schema_fixture() -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new("orders"),
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

fn measure_key_schema_allocations(
    label: &'static str,
    validate: fn(&CreateTableRequest) -> Result<(), crate::StorageError>,
) -> alloc_counter::AllocationReport<'static> {
    let request = table_schema_fixture();
    let guard = AllocationGuard::start(
        module_path!(),
        "create_table_key_schema_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let result = validate(&request);
        assert!(result.is_ok());
    }
    guard.finish()
}

#[test]
fn enabled_table_stream_metadata_uses_creation_time_and_table_identity() {
    let (arn, label) = latest_stream_metadata(
        &TableName::new("orders"),
        TimestampMillis::from_timestamp(1_700_000_000_123),
        Some(&StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
    );

    assert_eq!(label.as_deref(), Some("2023-11-14T22:13:20.123"));
    assert_eq!(
        arn.as_deref(),
        Some("arn:aws:dynamodb:us-east-1:123456789012:table/orders/stream/2023-11-14T22:13:20.123")
    );
}

#[test]
fn disabled_or_missing_table_stream_metadata_is_absent() {
    assert_eq!(
        latest_stream_metadata(
            &TableName::new("orders"),
            TimestampMillis::from_timestamp(1),
            None
        ),
        (None, None)
    );
    assert_eq!(
        latest_stream_metadata(
            &TableName::new("orders"),
            TimestampMillis::from_timestamp(1),
            Some(&StreamSpecification {
                stream_enabled: false,
                stream_view_type: None,
            }),
        ),
        (None, None)
    );
}

#[test]
fn describe_table_response_deserializes_fractional_creation_datetime_tests() {
    let payload = json!({
        "Table": {
            "TableName": "sys",
            "TableStatus": "ACTIVE",
            "CreationDateTime": 1_771_945_767.554,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "TableSizeBytes": 0,
            "ItemCount": 0,
            "MaxIndexers": 0,
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/sys"
        }
    });

    let decoded: DescribeTableResponse = serde_json::from_value(payload).unwrap();
    let created_at = TimestampMillis::from(decoded.table.created_at);
    assert_eq!(created_at.timestamp_millis(), 1_771_945_767_554);
}

#[test]
fn standard_dynamodb_table_description_defaults_absent_max_indexers() {
    let payload = json!({
        "Table": {
            "TableName": "standard",
            "TableStatus": "ACTIVE",
            "CreationDateTime": 1_771_945_767.554,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "TableSizeBytes": 0,
            "ItemCount": 0,
            "TableArn": "arn:aws:dynamodb:us-west-2:123456789012:table/standard"
        }
    });

    let decoded: DescribeTableResponse = serde_json::from_value(payload).unwrap();
    assert_eq!(decoded.table.max_indexers, MaxIndexers::ZERO);
}

#[test]
fn standard_create_table_request_omits_zero_max_indexers() {
    let request = CreateTableRequest::new(
        TableName::new("standard"),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    );

    let encoded = serde_json::to_value(request).unwrap();
    assert!(encoded.get("MaxIndexers").is_none());
}

#[test]
fn given_one_key_table_when_validating_schema_then_no_temporary_maps_are_allocated() {
    let request = table_schema_fixture();
    assert!(request.validate_key_schema().is_ok());
    let legacy = measure_key_schema_allocations("hash_set_and_map", legacy_validate_key_schema);
    let candidate = measure_key_schema_allocations(
        "bounded_slice_and_iterator",
        CreateTableRequest::validate_key_schema,
    );
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyStreamResponseWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<TableName>,
    records: Vec<StreamRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_frontier: Option<TableStreamFrontier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<crate::SystemStreamCheckpointToken>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyStreamRequestWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<TableName>,
    system_stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<crate::SystemStreamCheckpointToken>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

fn stream_request_fixture() -> GetStreamRecordsRequest {
    GetStreamRecordsRequest::table(
        TableName::new("orders"),
        Some("cursor-63".to_owned()),
        Some(100),
    )
}

fn legacy_stream_request_bytes(request: &GetStreamRecordsRequest) -> Vec<u8> {
    request.validate().unwrap();
    serde_json::to_vec(&LegacyStreamRequestWire {
        table_name: request.table_name().cloned(),
        system_stream: request.is_system_stream(),
        last_evaluated_key: request.last_evaluated_key().map(str::to_owned),
        source_scope: request.source_scope().cloned(),
        checkpoint: request.checkpoint().cloned(),
        limit: request.limit(),
    })
    .unwrap()
}

fn direct_stream_request_bytes(request: &GetStreamRecordsRequest) -> Vec<u8> {
    serde_json::to_vec(request).unwrap()
}

fn stream_response_fixture() -> GetStreamRecordsResponse {
    let records = (0..64)
        .map(|index| StreamRecord {
            cursor: Some(format!("cursor-{index}")),
            source_table_name: Some(TableName::new("orders")),
            keys: crate::KeyAttributes::default(),
            sequence_number: format!("sequence-{index}"),
            old_image: None,
            new_image: None,
            system_position: None,
        })
        .collect();
    GetStreamRecordsResponse::table(
        TableName::new("orders"),
        records,
        None,
        TableStreamFrontier {
            table_history_id: 1u32.into(),
            oldest: None,
            head: None,
        },
    )
}

fn legacy_stream_response_bytes(response: &GetStreamRecordsResponse) -> Vec<u8> {
    response.validate().unwrap();
    serde_json::to_vec(&LegacyStreamResponseWire {
        table_name: response.table_name().cloned(),
        records: response.records().to_vec(),
        last_evaluated_key: response.last_evaluated_key().map(str::to_owned),
        table_frontier: response.table_frontier().cloned(),
        source_scope: response.source_scope().cloned(),
        checkpoint: response.checkpoint().cloned(),
    })
    .unwrap()
}

fn direct_stream_response_bytes(response: &GetStreamRecordsResponse) -> Vec<u8> {
    serde_json::to_vec(response).unwrap()
}

fn measure_stream_response_serialization(
    label: &'static str,
    response: &GetStreamRecordsResponse,
    serialize: fn(&GetStreamRecordsResponse) -> Vec<u8>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "stream_response_serialization_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..32 {
        std::hint::black_box(serialize(response));
    }
    guard.finish()
}

fn measure_stream_request_serialization(
    label: &'static str,
    request: &GetStreamRecordsRequest,
    serialize: fn(&GetStreamRecordsRequest) -> Vec<u8>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "stream_request_serialization_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        std::hint::black_box(serialize(request));
    }
    guard.finish()
}

#[test]
fn given_stream_response_when_serializing_then_records_are_not_cloned() {
    let response = stream_response_fixture();
    let legacy = legacy_stream_response_bytes(&response);
    let direct = direct_stream_response_bytes(&response);
    assert_eq!(direct, legacy);

    let legacy = measure_stream_response_serialization(
        "owned_wire_clones",
        &response,
        legacy_stream_response_bytes,
    );
    let direct = measure_stream_response_serialization(
        "borrowed_wire_view",
        &response,
        direct_stream_response_bytes,
    );
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&direct);
    assert!(direct.allocation_count < legacy.allocation_count);
    assert!(direct.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn given_stream_request_when_serializing_then_fields_are_not_cloned() {
    let request = stream_request_fixture();
    let legacy = legacy_stream_request_bytes(&request);
    let direct = direct_stream_request_bytes(&request);
    assert_eq!(direct, legacy);

    let legacy = measure_stream_request_serialization(
        "owned_wire_clones",
        &request,
        legacy_stream_request_bytes,
    );
    let direct = measure_stream_request_serialization(
        "borrowed_wire_view",
        &request,
        direct_stream_request_bytes,
    );
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&direct);
    assert!(direct.allocation_count < legacy.allocation_count);
    assert!(direct.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn system_stream_response_uses_scope_checkpoint_and_record_position_fields() {
    let scope = SystemStreamScope::try_new("catalog", 3).unwrap();
    let position = SystemStreamPosition::try_new("shard-a", "lineage-a", "offset-a").unwrap();
    let record = StreamRecord {
        cursor: None,
        source_table_name: Some(TableName::new("orders")),
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: Some(position),
    };
    let checkpoint =
        crate::SystemStreamCheckpointToken::try_from(format!("v1.{}", "42".repeat(16))).unwrap();

    let response =
        GetStreamRecordsResponse::system_stream(scope, vec![record], Some(checkpoint)).unwrap();
    let encoded = serde_json::to_value(&response).unwrap();

    assert_eq!(
        encoded["SourceScope"],
        json!({
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 3,
        })
    );
    assert!(encoded["Checkpoint"].is_string());
    assert_eq!(encoded["Records"][0]["SourceTableName"], json!("orders"));
    assert_eq!(
        encoded["Records"][0]["SystemPosition"],
        json!({
            "ShardId": "shard-a",
            "LineageId": "lineage-a",
            "Offset": "offset-a",
        })
    );
    assert!(encoded.get("LastEvaluatedKey").is_none());

    let decoded = serde_json::from_value::<GetStreamRecordsResponse>(encoded).unwrap();
    assert_eq!(
        decoded.records()[0]
            .source_table_name
            .as_ref()
            .map(TableName::as_ref),
        Some("orders")
    );
}

#[test]
fn prepare_table_metadata_at_encodes_catalog_columns() {
    let request = table_schema_fixture();
    let created_at = TimestampMillis::now();
    let metadata = prepare_table_metadata_at(&request, created_at).unwrap();

    assert_eq!(*metadata.created_at, *created_at);
    assert!(metadata.attribute_definitions_json.contains("\"pk\""));
    assert!(metadata.key_schema_json.contains("\"pk\""));
    assert_eq!(metadata.max_indexers, MaxIndexers::ZERO);
    assert!(metadata.global_secondary_indexes_json.is_none());
    assert!(metadata.stream_specification_json.is_none());
    assert!(!metadata.deletion_protection_enabled);
    assert_eq!(
        metadata.table_stream_duration,
        crate::StreamRetentionDuration::default()
    );
    assert_eq!(
        metadata.default_item_stream_duration,
        metadata.table_stream_duration
    );
}

#[test]
fn given_old_item_when_all_old_requested_then_attributes_surface() {
    let old_item = HashMap::from([("pk".to_string(), AttributeValue::S("tenant".to_string()))]);

    let response = PutItemResponse::for_return_values(Some(old_item.into()), Some(AllOld::AllOld));

    assert!(response.attributes.is_some());
}

#[test]
fn given_old_item_when_no_return_values_then_attributes_stay_absent() {
    let old_item = HashMap::from([("pk".to_string(), AttributeValue::S("tenant".to_string()))]);

    let response = PutItemResponse::for_return_values(Some(old_item.into()), None);

    assert!(response.attributes.is_none());
}

#[test]
fn system_stream_response_preserves_each_logical_source_table_in_a_multi_table_page() {
    let records = [("orders", "shard-orders"), ("customers", "shard-customers")]
        .into_iter()
        .enumerate()
        .map(|(index, (table_name, shard_id))| StreamRecord {
            cursor: None,
            source_table_name: Some(TableName::new(table_name)),
            keys: crate::KeyAttributes::default(),
            sequence_number: format!("sequence-{index}"),
            old_image: None,
            new_image: None,
            system_position: Some(
                SystemStreamPosition::try_new(
                    shard_id,
                    format!("lineage-{index}"),
                    format!("offset-{index}"),
                )
                .unwrap(),
            ),
        })
        .collect();
    let response = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).unwrap(),
        records,
        None,
    )
    .unwrap();

    let encoded = serde_json::to_value(&response).unwrap();
    assert_eq!(
        encoded["Records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| record["SourceTableName"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["orders", "customers"]
    );
    let decoded = serde_json::from_value::<GetStreamRecordsResponse>(encoded).unwrap();
    assert_eq!(
        decoded
            .records()
            .iter()
            .map(|record| record
                .source_table_name
                .as_ref()
                .map(TableName::as_ref)
                .unwrap())
            .collect::<Vec<_>>(),
        ["orders", "customers"]
    );
}

#[test]
fn system_stream_response_rejects_records_without_logical_position() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: Some(TableName::new("physical-table")),
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: None,
    };
    let error = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).unwrap(),
        vec![record],
        None,
    )
    .unwrap_err();
    assert_eq!(
        error,
        crate::SystemStreamContractError::MissingRecordPosition
    );
}

#[test]
fn system_stream_response_rejects_records_without_logical_source_table() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: Some(
            SystemStreamPosition::try_new("shard-a", "lineage-a", "offset-a").unwrap(),
        ),
    };
    let error = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).unwrap(),
        vec![record],
        None,
    )
    .unwrap_err();
    assert_eq!(
        error,
        crate::SystemStreamContractError::MissingSourceTableName
    );
}

#[test]
fn system_stream_response_rejects_non_logical_source_table_name() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: Some(TableName::new("arn:aws:dynamodb:table/orders")),
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: Some(
            SystemStreamPosition::try_new("shard-a", "lineage-a", "offset-a").unwrap(),
        ),
    };
    let error = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).unwrap(),
        vec![record],
        None,
    )
    .unwrap_err();
    assert_eq!(
        error,
        crate::SystemStreamContractError::InvalidSourceTableName
    );
}

#[test]
fn system_stream_response_deserialization_rejects_invalid_logical_source_table_name() {
    let error = serde_json::from_value::<GetStreamRecordsResponse>(json!({
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 3,
        },
        "Records": [{
            "SourceTableName": "arn:aws:dynamodb:table/orders",
            "Keys": {},
            "SequenceNumber": "opaque-sequence",
            "SystemPosition": {
                "ShardId": "shard-a",
                "LineageId": "lineage-a",
                "Offset": "offset-a",
            },
        }]
    }))
    .unwrap_err();

    assert!(error.to_string().contains("source table name is invalid"));
}

#[test]
fn system_stream_response_rejects_physical_pointer_without_position() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: None,
    };
    let error = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).unwrap(),
        vec![record],
        None,
    )
    .unwrap_err();
    assert_eq!(
        error,
        crate::SystemStreamContractError::MissingRecordPosition
    );
}

#[test]
fn ordinary_stream_response_rejects_logical_system_position() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: Some(
            SystemStreamPosition::try_new("shard-a", "lineage-a", "offset-a").unwrap(),
        ),
    };
    let response = GetStreamRecordsResponse::table(
        TableName::new("ordinary"),
        vec![record],
        None,
        TableStreamFrontier {
            table_history_id: 1u32.into(),
            oldest: None,
            head: None,
        },
    );

    assert_eq!(
        response.validate(),
        Err(crate::SystemStreamContractError::UnexpectedSystemStreamFields)
    );
    assert!(
        serde_json::to_value(&response).is_err(),
        "serialization must revalidate provider-built responses"
    );
}

#[test]
fn given_table_response_without_frontier_when_decoded_then_it_is_rejected() {
    let error = serde_json::from_value::<GetStreamRecordsResponse>(json!({
        "TableName": "ordinary",
        "Records": []
    }))
    .expect_err("ordinary stream must report the observed head");
    assert!(error.to_string().contains("observed frontier"));
}

#[test]
fn given_retained_table_range_when_checkpoint_is_checked_then_gap_and_recreation_fail_closed() {
    let before: crate::StreamItemId = "000000000000000000000000".parse().unwrap();
    let oldest: crate::StreamItemId = "000000000000000000000001".parse().unwrap();
    let head: crate::StreamItemId = "000000000000000000000003".parse().unwrap();
    let after: crate::StreamItemId = "000000000000000000000004".parse().unwrap();
    let history_id = 1u32.into();
    let frontier = TableStreamFrontier {
        table_history_id: history_id,
        oldest: Some(oldest),
        head: Some(head),
    };
    assert!(frontier.can_resume_from(history_id, oldest));
    assert!(frontier.can_resume_from(history_id, head));
    assert!(!frontier.can_resume_from(history_id, before));
    assert!(!frontier.can_resume_from(history_id, after));
    assert!(!frontier.can_resume_from(2u32.into(), head));
    let response =
        GetStreamRecordsResponse::table(TableName::new("ordinary"), vec![], None, frontier);
    assert!(serde_json::to_value(response).is_ok());
}

#[test]
fn given_inverted_or_partial_table_range_when_serialized_then_it_is_rejected() {
    let oldest: crate::StreamItemId = "000000000000000000000003".parse().unwrap();
    let head: crate::StreamItemId = "000000000000000000000001".parse().unwrap();
    for (oldest, head) in [(Some(oldest), Some(head)), (Some(oldest), None)] {
        let response = GetStreamRecordsResponse::table(
            TableName::new("ordinary"),
            vec![],
            None,
            TableStreamFrontier {
                table_history_id: 1u32.into(),
                oldest,
                head,
            },
        );
        assert_eq!(
            response.validate(),
            Err(crate::SystemStreamContractError::InvalidTableFrontier)
        );
    }
}

#[test]
fn given_table_frontier_on_system_response_when_decoded_then_it_is_rejected() {
    let response = GetStreamRecordsResponse::system_stream(
        SystemStreamScope::try_new("catalog", 3).expect("scope"),
        Vec::new(),
        None,
    )
    .expect("system response");
    let mut encoded = serde_json::to_value(response).expect("wire response");
    encoded["TableFrontier"] = serde_json::to_value(TableStreamFrontier {
        table_history_id: 1u32.into(),
        oldest: None,
        head: None,
    })
    .expect("frontier");
    let error = serde_json::from_value::<GetStreamRecordsResponse>(encoded)
        .expect_err("system stream cannot carry a table frontier");
    assert!(
        error
            .to_string()
            .contains("mixed legacy and logical fields")
    );
}

#[test]
fn stream_response_deserialization_rejects_mixed_ordinary_and_logical_fields() {
    let error = serde_json::from_value::<GetStreamRecordsResponse>(json!({
        "TableName": "ordinary",
        "Records": [],
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 3,
        }
    }))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("mixed legacy and logical fields")
    );
}

#[test]
fn stream_response_deserialization_rejects_legacy_only_logical_response() {
    let error = serde_json::from_value::<GetStreamRecordsResponse>(json!({
        "Records": [],
        "LastEvaluatedKey": "legacy-cursor"
    }))
    .unwrap_err();

    assert!(error.to_string().contains("not supported for SystemStream"));
}

#[test]
fn ordinary_stream_response_preserves_reviewed_source_table_name_wire_field() {
    let record = StreamRecord {
        cursor: None,
        source_table_name: Some(TableName::new("orders.v2-prod")),
        keys: crate::KeyAttributes::default(),
        sequence_number: "opaque-sequence".to_string(),
        old_image: None,
        new_image: None,
        system_position: None,
    };
    let response = GetStreamRecordsResponse::table(
        TableName::new("orders.v2-prod"),
        vec![record],
        Some("cursor-1".to_string()),
        TableStreamFrontier {
            table_history_id: 1u32.into(),
            oldest: None,
            head: None,
        },
    );

    let encoded = serde_json::to_value(&response).unwrap();
    assert_eq!(encoded["TableName"], json!("orders.v2-prod"));
    assert_eq!(
        encoded["Records"][0]["SourceTableName"],
        json!("orders.v2-prod")
    );
    assert!(encoded["Records"][0].get("SystemPosition").is_none());

    let decoded = serde_json::from_value::<GetStreamRecordsResponse>(encoded).unwrap();
    assert_eq!(
        decoded.records()[0]
            .source_table_name
            .as_ref()
            .map(TableName::as_ref),
        Some("orders.v2-prod")
    );
}

#[test]
fn system_stream_response_accepts_valid_logical_source_table_name_wire() {
    let decoded = serde_json::from_value::<GetStreamRecordsResponse>(json!({
        "SourceScope": {
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 3,
        },
        "Records": [{
            "SourceTableName": "orders.v2-prod",
            "Keys": {},
            "SequenceNumber": "opaque-sequence",
            "SystemPosition": {
                "ShardId": "shard-a",
                "LineageId": "lineage-a",
                "Offset": "offset-a",
            },
        }]
    }))
    .unwrap();

    assert_eq!(
        decoded.records()[0]
            .source_table_name
            .as_ref()
            .map(TableName::as_ref),
        Some("orders.v2-prod")
    );
}

#[test]
fn given_absent_optional_wire_fields_when_decoding_responses_then_items_are_none() {
    let get: crate::GetItemResponse = serde_json::from_str("{}").expect("missing item");
    let member: crate::ItemResponse = serde_json::from_str("{}").expect("missing member item");
    let batch: crate::BatchGetItemResponse = serde_json::from_str("{}").expect("missing tables");
    let query: crate::QueryResponse =
        serde_json::from_str(r#"{"Count":0,"ScannedCount":0}"#).expect("missing query items");
    let scan: crate::ScanResponse =
        serde_json::from_str(r#"{"Count":0,"ScannedCount":0}"#).expect("missing scan items");
    assert!(get.item.is_none());
    assert!(member.item.is_none());
    assert!(batch.responses.is_none());
    assert!(query.items.is_none());
    assert!(scan.items.is_none());
    assert_eq!(
        serde_json::to_value(get).expect("empty response"),
        serde_json::json!({})
    );
    assert!(serde_json::from_str::<crate::GetItemResponse>(r#"{"Item":[]}"#).is_err());
}
