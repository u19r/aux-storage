use std::collections::HashMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use storage_derive::{WireItemDecode, WireItemEncode, WireProjectionDecode};

use crate::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageEnum, StoredTableInfo, TableName,
    TableStatus, TimestampMillis, TryIntoWireItem, WireItem, decode_wire_field,
};

fn sample_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("jobs"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        max_indexers: crate::MaxIndexers::ZERO,
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
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi1"),
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
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        stream_enabled_at: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, WireItemEncode, WireItemDecode)]
struct TimestampWireFixture {
    pk: String,
    sk: String,
    created_at: crate::TimestampMillis,
    expires_at: crate::TimestampMillis,
    #[serde(rename = "validAt")]
    valid_at: crate::TimestampMillis,
    consumed_at: Option<crate::TimestampMillis>,
    ttl: crate::TimestampSeconds,
}

#[derive(Debug, Clone, Deserialize, WireProjectionDecode)]
struct DefaultedBoolProjection {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    optional_active: Option<bool>,
}

fn sample_timestamp_wire_fixture() -> TimestampWireFixture {
    TimestampWireFixture {
        pk: "TS#1".to_string(),
        sk: "META".to_string(),
        created_at: crate::TimestampMillis::from_timestamp(1_700_010_123_456),
        expires_at: crate::TimestampMillis::from_timestamp(1_700_010_223_456),
        valid_at: crate::TimestampMillis::from_timestamp(1_700_010_323_456),
        consumed_at: Some(crate::TimestampMillis::from_timestamp(1_700_010_423_456)),
        ttl: crate::TimestampSeconds::from_timestamp(1_700_010_500),
    }
}

#[test]
fn last_evaluated_key_projection_uses_index_and_table_keys_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::try_from(&HashMap::from([
        (
            "gsi_pk".to_string(),
            AttributeValue::S("STATE#ready".to_string()),
        ),
        (
            "gsi_sk".to_string(),
            AttributeValue::N("1700000000000".to_string()),
        ),
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
    ]))
    .unwrap();

    let lek = wire_item
        .last_evaluated_key(&table_info, &Some(IndexName::new("gsi1")))
        .unwrap()
        .unwrap();
    let decoded_key = crate::ItemKey::item_key_from_next_page_token(
        &lek,
        &table_info,
        &Some(IndexName::new("gsi1")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        decoded_key.hash_key(),
        &AttributeValue::S("STATE#ready".to_string())
    );
}

#[test]
fn wire_attribute_decode_rejects_boolean_values_outside_the_persisted_contract_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error = decode_wire_field::<bool>(&wire_item, Some("yes"), "enabled").unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message == "invalid enabled field: expected boolean")
    );
}

#[test]
fn wire_attribute_decode_accepts_rfc3339_seconds_and_milliseconds_for_datetime_fields_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let rfc3339 =
        decode_wire_field::<Timestamp>(&wire_item, Some("2026-05-14T12:34:56Z"), "scheduled_at")
            .unwrap();
    let seconds =
        decode_wire_field::<Timestamp>(&wire_item, Some("1700000000"), "scheduled_at").unwrap();
    let millis =
        decode_wire_field::<Timestamp>(&wire_item, Some("1700000000000"), "scheduled_at").unwrap();

    assert_eq!(rfc3339.as_second(), 1_778_762_096);
    assert_eq!(seconds.as_second(), 1_700_000_000);
    assert_eq!(millis.as_millisecond(), 1_700_000_000_000);
}

#[test]
fn wire_attribute_decode_rejects_datetime_values_that_are_not_timestamps_or_rfc3339_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error =
        decode_wire_field::<Timestamp>(&wire_item, Some("tomorrow"), "scheduled_at").unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("invalid scheduled_at field datetime format"))
    );
}

#[test]
fn projection_decode_bool_defaults_only_absent_fields_tests() {
    let missing = WireItem::try_from(&HashMap::new()).unwrap();
    let decoded: DefaultedBoolProjection = missing.try_decode().unwrap();
    assert!(!decoded.active);
    assert_eq!(decoded.optional_active, None);

    let false_value = WireItem::try_from(&HashMap::from([(
        "active".to_string(),
        AttributeValue::BOOL(false),
    )]))
    .unwrap();
    let decoded: DefaultedBoolProjection = false_value.try_decode().unwrap();
    assert!(!decoded.active);

    let explicit_null = WireItem::try_from(&HashMap::from([(
        "optional_active".to_string(),
        AttributeValue::NULL(true),
    )]))
    .unwrap();
    let decoded: DefaultedBoolProjection = explicit_null.try_decode().unwrap();
    assert_eq!(decoded.optional_active, None);
    assert!(
        explicit_null
            .try_decode::<DefaultedBoolProjection>()
            .is_ok()
    );
}

#[test]
fn timestamp_aliases_keep_old_and_new_field_names_read_compatible_tests() {
    let item = HashMap::from([
        (
            "c_at".to_string(),
            AttributeValue::N("1700010123456".to_string()),
        ),
        (
            "updated_at".to_string(),
            AttributeValue::N("1700010223456".to_string()),
        ),
        (
            "e_at".to_string(),
            AttributeValue::N("1700010323456".to_string()),
        ),
    ]);
    let wire_item = WireItem::try_from(&item).unwrap();

    assert_eq!(
        wire_item.number_attribute_i64("created_at").unwrap(),
        Some(1_700_010_123_456)
    );
    assert_eq!(
        wire_item.attribute_value("u_at").unwrap(),
        Some(AttributeValue::N("1700010223456".to_string()))
    );
    assert_eq!(
        wire_item.scalar_attributes(&["expires_at"]).unwrap()[0].as_deref(),
        Some("1700010323456")
    );
}

#[test]
fn scalar_projection_aliases_preserve_canonical_precedence_and_duplicate_order_tests() {
    let wire_item = WireItem::dynamo_json(
        br#"{
            "created_at": {"N": "100"},
            "c_at": {"N": "200"},
            "updated_at": {"N": "300"},
            "expires_at": {"M": {}},
            "e_at": {"N": "600"}
        }"#
        .to_vec(),
    );

    let projected = wire_item
        .scalar_attributes(&["created_at", "created_at", "updated_at", "expires_at"])
        .unwrap();

    assert_eq!(projected[0].as_deref(), Some("100"));
    assert_eq!(projected[1].as_deref(), Some("200"));
    assert_eq!(projected[2].as_deref(), Some("300"));
    assert_eq!(projected[3].as_deref(), Some("600"));
}

#[test]
fn wire_item_decode_reads_numeric_timestamp_storage_tests() {
    let fixture = sample_timestamp_wire_fixture();
    let mut map = HashMap::new();
    map.insert("pk".to_string(), AttributeValue::S(fixture.pk.clone()));
    map.insert("sk".to_string(), AttributeValue::S(fixture.sk.clone()));
    map.insert(
        "c_at".to_string(),
        AttributeValue::N((*fixture.created_at).to_string()),
    );
    map.insert(
        "e_at".to_string(),
        AttributeValue::N((*fixture.expires_at).to_string()),
    );
    map.insert(
        "validAt".to_string(),
        AttributeValue::N((*fixture.valid_at).to_string()),
    );
    map.insert(
        "consumed_at".to_string(),
        AttributeValue::N(fixture.consumed_at.unwrap().timestamp_millis().to_string()),
    );
    map.insert(
        "ttl".to_string(),
        AttributeValue::N(fixture.ttl.as_seconds().to_string()),
    );

    let wire = WireItem::try_from(&map).unwrap();
    let decoded = wire.try_decode::<TimestampWireFixture>().unwrap();

    assert_eq!(decoded, fixture);
}

#[test]
fn wire_item_decode_rejects_fractional_ttl_seconds_tests() {
    let fixture = sample_timestamp_wire_fixture();
    let mut map = HashMap::new();
    map.insert("pk".to_string(), AttributeValue::S(fixture.pk.clone()));
    map.insert("sk".to_string(), AttributeValue::S(fixture.sk.clone()));
    map.insert(
        "created_at".to_string(),
        AttributeValue::N((*fixture.created_at).to_string()),
    );
    map.insert(
        "expires_at".to_string(),
        AttributeValue::N((*fixture.expires_at).to_string()),
    );
    map.insert(
        "validAt".to_string(),
        AttributeValue::N((*fixture.valid_at).to_string()),
    );
    map.insert(
        "consumed_at".to_string(),
        AttributeValue::N(fixture.consumed_at.unwrap().timestamp_millis().to_string()),
    );
    map.insert(
        "ttl".to_string(),
        AttributeValue::N(format!("{}.5", fixture.ttl.as_seconds())),
    );

    let wire = WireItem::try_from(&map).unwrap();
    wire.try_decode::<TimestampWireFixture>().unwrap_err();
}

#[test]
fn ttl_index_token_includes_hash_and_range_keys_when_the_table_has_a_range_key_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N("1700000500".to_string()),
        ),
    ]))
    .unwrap();

    let (ttl, token) = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .unwrap()
        .unwrap();
    let decoded_key =
        crate::ItemKey::item_key_from_next_page_token(&token, &table_info, &None).unwrap();
    let decoded_key = decoded_key.unwrap();

    assert_eq!(ttl, 1_700_000_500);
    assert_eq!(
        decoded_key.hash_key(),
        &AttributeValue::S("JOB#1".to_string())
    );
    assert_eq!(
        decoded_key.range_key(),
        Some(&AttributeValue::S("LOCK".to_string()))
    );
}

#[test]
fn last_evaluated_key_is_absent_until_all_required_table_key_parts_are_projected_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::try_from(&HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("JOB#1".to_string()),
    )]))
    .unwrap();

    let token = wire_item.last_evaluated_key(&table_info, &None).unwrap();

    assert!(token.is_none());
}

#[derive(Debug, PartialEq, Serialize, Deserialize, WireItemEncode, WireProjectionDecode)]
struct ScalarWireFields {
    id: String,
    active: bool,
    lower: i64,
    upper: u64,
}

struct JsonScalarFields<'a>(&'a ScalarWireFields);

impl Serialize for JsonScalarFields<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        macro_rules! field {
            ($name:ident) => {
                map.serialize_entry(
                    stringify!($name),
                    &crate::encode_wire_attribute(&self.0.$name, stringify!($name))
                        .map_err(serde::ser::Error::custom)?,
                )?;
            };
        }
        field!(active);
        field!(id);
        field!(lower);
        field!(upper);
        map.end()
    }
}

fn encode_json_scalar_fields(fixture: &ScalarWireFields) -> Vec<u8> {
    // Match the previous derive's output-buffer construction, not to_vec's
    // capacity hint.
    let mut data = Vec::new();
    serde_json::to_writer(&mut data, &JsonScalarFields(fixture)).unwrap();
    data
}

#[test]
fn given_scalar_entity_when_encoding_wire_item_then_bytes_match_without_json_value_allocations() {
    let fixture = ScalarWireFields {
        id: "quotes\" slash\\ snowman☃".into(),
        active: true,
        lower: i64::MIN,
        upper: u64::MAX,
    };
    let baseline_bytes = encode_json_scalar_fields(&fixture);
    let direct = fixture.try_into_wire_item().unwrap();
    let WireItem::DynamoJson { data } = &direct;
    assert_eq!(*data, baseline_bytes);
    assert_eq!(direct.try_decode::<ScalarWireFields>().unwrap(), fixture);

    let baseline_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "scalar_wire_encoding",
        file!(),
        line!(),
        Some("json_value"),
    );
    for _ in 0..256 {
        std::hint::black_box(encode_json_scalar_fields(&fixture));
    }
    let baseline = baseline_guard.finish();
    let direct_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "scalar_wire_encoding",
        file!(),
        line!(),
        Some("borrowed_wire"),
    );
    for _ in 0..256 {
        std::hint::black_box(fixture.try_into_wire_item().unwrap());
    }
    let candidate = direct_guard.finish();
    alloc_counter::emit_report(&baseline).unwrap();
    alloc_counter::emit_report(&candidate).unwrap();
    eprintln!("scalar baseline={baseline:?} candidate={candidate:?}");
    assert!(candidate.allocation_count < baseline.allocation_count);
    assert!(candidate.allocated_bytes < baseline.allocated_bytes);
}
