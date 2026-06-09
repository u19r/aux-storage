use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use storage_derive::{WireItemDecode, WireItemEncode, WireProjectionDecode};

use crate::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageEnum, StorageError, StoredEntity,
    StoredTableInfo, TableName, TableStatus, TimestampMillis, TryFromWireItem, TryIntoWireItem,
    ValidatedEntity, WireItem, WireItemKeyAttributes, decode_wire_field, decode_wire_field_json,
    decode_wire_serde_string,
};

fn sample_table_info() -> StoredTableInfo {
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
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
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
struct ValidatedFixtureProjection {
    id: String,
    active: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ValidatedFixture {
    id: String,
    active: bool,
}

impl ValidatedEntity for ValidatedFixture {
    type ValidationError = &'static str;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.id.trim().is_empty() {
            return Err("fixture_id_required");
        }
        Ok(())
    }
}

impl StoredEntity for ValidatedFixture {
    fn storage_type_name() -> &'static str {
        "validated fixture"
    }

    fn try_from_stored_item(item: &WireItem) -> crate::StorageResult<Self> {
        Self::decode_projection::<ValidatedFixtureProjection, _>(item, |projection| {
            Self {
                id: projection.id,
                active: projection.active,
            }
            .into_validated()
        })
    }
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
fn number_projection_reads_only_target_field_from_dynamo_wire_tests() {
    let item = HashMap::from([
        (
            "lease_until_ms".to_string(),
            AttributeValue::N("1729".to_string()),
        ),
        (
            "nested".to_string(),
            AttributeValue::M(HashMap::from([(
                "x".to_string(),
                AttributeValue::L(vec![AttributeValue::S("y".to_string())]),
            )])),
        ),
    ]);
    let payload = serde_json::to_vec(&item).expect("serialize test item");
    let wire_item = WireItem::dynamo_json(payload);

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms")
        .expect("project lease_until_ms");
    assert_eq!(projected, Some(1729));

    let missing = decode_wire_field::<Option<i64>>(&wire_item, None, "does_not_exist")
        .expect("project missing field");
    assert_eq!(missing, None);
}

#[test]
fn number_projection_reads_from_local_split_blob_tests() {
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("JOB#a".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("LOCK".to_string())),
    );
    let blob = serde_json::to_vec(&HashMap::from([(
        "lease_until_ms".to_string(),
        AttributeValue::N("4242".to_string()),
    )]))
    .expect("serialize local split blob");
    let wire_item = WireItem::local_split(primary, None, Some(blob));

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms")
        .expect("project from local split blob");
    assert_eq!(projected, Some(4242));
}

#[test]
fn number_projection_reads_from_local_split_key_tests() {
    let primary = WireItemKeyAttributes::new(
        "lease_until_ms".to_string(),
        AttributeValue::N("9".to_string()),
        None,
        None,
    );
    let wire_item = WireItem::local_split(primary, None, None);

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms")
        .expect("project from local split key");
    assert_eq!(projected, Some(9));
}

#[test]
fn last_evaluated_key_projection_uses_local_split_keys_tests() {
    let table_info = sample_table_info();
    let primary = WireItemKeyAttributes::new(
        "gsi_pk".to_string(),
        AttributeValue::S("STATE#ready".to_string()),
        Some("gsi_sk".to_string()),
        Some(AttributeValue::N("1700000000000".to_string())),
    );
    let secondary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("JOB#1".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("LOCK".to_string())),
    );
    let wire_item = WireItem::local_split(primary, Some(secondary), None);

    let lek = wire_item
        .last_evaluated_key(&table_info, &Some(IndexName::new("gsi1")))
        .expect("create lek from local split")
        .expect("lek should exist");
    let decoded_key = crate::ItemKey::item_key_from_next_page_token(
        &lek,
        &table_info,
        &Some(IndexName::new("gsi1")),
    )
    .expect("decode lek")
    .expect("decoded key exists");
    assert_eq!(
        decoded_key.hash_key(),
        &AttributeValue::S("STATE#ready".to_string())
    );
}

#[test]
fn last_evaluated_key_projection_reads_only_requested_dynamo_fields_tests() {
    let table_info = sample_table_info();
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("JOB#2".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([(
                "huge".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::S("alpha".to_string()),
                    AttributeValue::S("beta".to_string()),
                ]),
            )])),
        ),
    ]);
    let payload = serde_json::to_vec(&item).expect("serialize item");
    let wire_item = WireItem::dynamo_json(payload);

    let lek = wire_item
        .last_evaluated_key(&table_info, &None)
        .expect("create table lek")
        .expect("lek should exist");
    let decoded_key =
        crate::ItemKey::item_key_from_next_page_token(&lek, &table_info, &None).expect("decode");
    let decoded_key = decoded_key.expect("decoded key exists");
    assert_eq!(
        decoded_key.hash_key(),
        &AttributeValue::S("JOB#2".to_string())
    );
    assert_eq!(
        decoded_key.range_key(),
        Some(&AttributeValue::S("LOCK".to_string()))
    );
}

#[test]
fn string_projection_reads_only_target_field_from_dynamo_wire_tests() {
    let item = HashMap::from([
        (
            "rate_limit_key".to_string(),
            AttributeValue::S("premium".to_string()),
        ),
        ("attempts".to_string(), AttributeValue::N("4".to_string())),
    ]);
    let payload = serde_json::to_vec(&item).expect("serialize test item");
    let wire_item = WireItem::dynamo_json(payload);

    let projected = decode_wire_field::<Option<String>>(&wire_item, None, "rate_limit_key")
        .expect("project rate_limit_key");
    assert_eq!(projected.as_deref(), Some("premium"));

    let missing = decode_wire_field::<Option<String>>(&wire_item, None, "does_not_exist")
        .expect("project missing field");
    assert_eq!(missing, None);
}

#[test]
fn bool_projection_reads_only_target_field_from_dynamo_wire_tests() {
    let item = HashMap::from([
        ("active".to_string(), AttributeValue::BOOL(true)),
        ("attempts".to_string(), AttributeValue::N("4".to_string())),
    ]);
    let payload = serde_json::to_vec(&item).expect("serialize test item");
    let wire_item = WireItem::dynamo_json(payload);

    let projected =
        decode_wire_field::<Option<bool>>(&wire_item, None, "active").expect("project active");
    assert_eq!(projected, Some(true));

    let missing = decode_wire_field::<Option<bool>>(&wire_item, None, "does_not_exist")
        .expect("project missing field");
    assert_eq!(missing, None);
}

#[test]
fn wire_attribute_decode_accepts_numeric_boolean_storage_used_by_legacy_records_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let enabled =
        decode_wire_field::<bool>(&wire_item, Some("1"), "enabled").expect("decode true flag");
    let disabled =
        decode_wire_field::<bool>(&wire_item, Some("0"), "enabled").expect("decode false flag");

    assert!(enabled);
    assert!(!disabled);
}

#[test]
fn wire_attribute_decode_rejects_boolean_values_outside_the_persisted_contract_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error = decode_wire_field::<bool>(&wire_item, Some("yes"), "enabled")
        .expect_err("invalid bool fails");

    assert!(matches!(error,
        StorageError::Base(StorageEnum::InternalServerError { ref message })
            if message.contains("invalid enabled field: yes")));
}

#[test]
fn wire_attribute_decode_accepts_rfc3339_seconds_and_milliseconds_for_datetime_fields_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let rfc3339 = decode_wire_field::<DateTime<Utc>>(
        &wire_item,
        Some("2026-05-14T12:34:56Z"),
        "scheduled_at",
    )
    .expect("decode rfc3339 time");
    let seconds =
        decode_wire_field::<DateTime<Utc>>(&wire_item, Some("1700000000"), "scheduled_at")
            .expect("decode epoch seconds");
    let millis =
        decode_wire_field::<DateTime<Utc>>(&wire_item, Some("1700000000000"), "scheduled_at")
            .expect("decode epoch millis");

    assert_eq!(rfc3339.timestamp(), 1_778_762_096);
    assert_eq!(seconds.timestamp(), 1_700_000_000);
    assert_eq!(millis.timestamp_millis(), 1_700_000_000_000);
}

#[test]
fn wire_attribute_decode_rejects_datetime_values_that_are_not_timestamps_or_rfc3339_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error = decode_wire_field::<DateTime<Utc>>(&wire_item, Some("tomorrow"), "scheduled_at")
        .expect_err("invalid datetime fails");

    assert!(matches!(error,
        StorageError::Base(StorageEnum::InternalServerError { ref message })
            if message.contains("invalid scheduled_at field datetime format")));
}

#[test]
fn wire_json_field_decode_accepts_plain_scalar_strings_when_callers_store_legacy_raw_values_tests()
{
    let wire_item = WireItem::dynamo_json(Vec::new());

    let decoded = decode_wire_field_json::<String>(&wire_item, Some("plain-value"), "status")
        .expect("decode legacy raw scalar");

    assert_eq!(decoded, "plain-value");
}

#[test]
fn wire_json_field_decode_rejects_missing_required_non_nullable_fields_tests() {
    let wire_item = WireItem::from_attribute_map(&HashMap::new()).expect("empty wire item");

    let error = decode_wire_field_json::<String>(&wire_item, None, "status")
        .expect_err("missing required json field fails");

    assert!(matches!(error,
        StorageError::Base(StorageEnum::InternalServerError { ref message })
            if message.contains("missing required field status")));
}

#[test]
fn wire_serde_string_decode_reports_the_business_field_name_for_invalid_json_scalar_tests() {
    let error = decode_wire_serde_string::<u32>("not-a-number", "attempts")
        .expect_err("invalid scalar should fail");

    assert!(matches!(error,
        StorageError::Base(StorageEnum::InternalServerError { ref message })
            if message.contains("invalid attempts field")));
}

#[test]
fn stored_entity_decode_runs_validation_tests() {
    let wire_item = WireItem::from_attribute_map(&HashMap::from([
        (
            "id".to_string(),
            AttributeValue::S("fixture_123".to_string()),
        ),
        ("active".to_string(), AttributeValue::BOOL(true)),
    ]))
    .expect("encode fixture");

    let decoded: ValidatedFixture = wire_item.try_decode().expect("decode fixture");
    assert_eq!(
        decoded,
        ValidatedFixture {
            id: "fixture_123".to_string(),
            active: true,
        }
    );
}

#[test]
fn stored_entity_decode_rejects_invalid_projection_tests() {
    let wire_item = WireItem::from_attribute_map(&HashMap::from([
        ("id".to_string(), AttributeValue::S("   ".to_string())),
        ("active".to_string(), AttributeValue::BOOL(false)),
    ]))
    .expect("encode fixture");

    let err = wire_item
        .try_decode::<ValidatedFixture>()
        .expect_err("blank id should fail validation");
    assert!(
        format!("{err:?}").contains("invalid persisted validated fixture: fixture_id_required")
    );
}

#[test]
fn bool_projection_reads_from_local_split_blob_tests() {
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("JOB#a".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("LOCK".to_string())),
    );
    let blob = serde_json::to_vec(&HashMap::from([(
        "active".to_string(),
        AttributeValue::BOOL(false),
    )]))
    .expect("serialize local split blob");
    let wire_item = WireItem::local_split(primary, None, Some(blob));

    let projected = decode_wire_field::<Option<bool>>(&wire_item, None, "active")
        .expect("project from local split blob");
    assert_eq!(projected, Some(false));
}

#[test]
fn local_split_empty_non_key_blob_is_treated_as_no_projected_attributes_tests() {
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("JOB#a".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("LOCK".to_string())),
    );
    let wire_item = WireItem::local_split(primary, None, Some(b"{}".to_vec()));

    let active = wire_item
        .bool_attribute("active")
        .expect("empty blob is ignored");

    assert_eq!(active, None);
}

#[test]
fn wire_item_payload_len_counts_each_local_split_component_tests() {
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("JOB#a".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("LOCK".to_string())),
    );
    let secondary = WireItemKeyAttributes::new(
        "gsi_pk".to_string(),
        AttributeValue::S("READY".to_string()),
        None,
        None,
    );
    let blob = serde_json::to_vec(&HashMap::from([(
        "active".to_string(),
        AttributeValue::BOOL(true),
    )]))
    .expect("serialize local split blob");
    let expected_len = "pk".len()
        + "JOB#a".len()
        + "sk".len()
        + "LOCK".len()
        + "gsi_pk".len()
        + "READY".len()
        + blob.len();

    let wire_item = WireItem::local_split(primary, Some(secondary), Some(blob));

    assert_eq!(wire_item.payload_len(), expected_len);
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
    let wire_item = WireItem::from_attribute_map(&item).expect("wire item");

    assert_eq!(
        wire_item
            .number_attribute_i64("created_at")
            .expect("read created alias"),
        Some(1_700_010_123_456)
    );
    assert_eq!(
        wire_item
            .attribute_value("u_at")
            .expect("read updated alias"),
        Some(AttributeValue::N("1700010223456".to_string()))
    );
    assert_eq!(
        wire_item
            .scalar_attributes(&["expires_at"])
            .expect("read expires alias")[0]
            .as_deref(),
        Some("1700010323456")
    );
}

#[test]
fn wire_item_encode_uses_numeric_timestamp_storage_tests() {
    let fixture = sample_timestamp_wire_fixture();
    let wire = fixture
        .try_into_wire_item()
        .expect("encode timestamp fixture");
    let map = wire
        .to_attribute_map()
        .expect("decode encoded wire payload");

    assert!(matches!(
        map.get("c_at"),
        Some(AttributeValue::N(value))
            if value == &(*fixture.created_at).to_string()
    ));
    assert!(matches!(
        map.get("e_at"),
        Some(AttributeValue::N(value))
            if value == &(*fixture.expires_at).to_string()
    ));
    assert!(matches!(
        map.get("validAt"),
        Some(AttributeValue::N(value))
            if value == &(*fixture.valid_at).to_string()
    ));
    assert!(matches!(
        map.get("consumed_at"),
        Some(AttributeValue::N(value))
            if value
                == &fixture
                    .consumed_at
                    .expect("consumed_at set in fixture")
                    .timestamp_millis()
                    .to_string()
    ));
    assert!(matches!(
        map.get("ttl"),
        Some(AttributeValue::N(value)) if value == &fixture.ttl.as_seconds().to_string()
    ));
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
        AttributeValue::N(
            fixture
                .consumed_at
                .expect("consumed_at set in fixture")
                .timestamp_millis()
                .to_string(),
        ),
    );
    map.insert(
        "ttl".to_string(),
        AttributeValue::N(fixture.ttl.as_seconds().to_string()),
    );

    let wire = WireItem::from_attribute_map(&map).expect("encode attribute map to wire");
    let decoded = wire
        .try_decode::<TimestampWireFixture>()
        .expect("decode timestamp wire fixture");

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
        AttributeValue::N(
            fixture
                .consumed_at
                .expect("consumed_at set in fixture")
                .timestamp_millis()
                .to_string(),
        ),
    );
    map.insert(
        "ttl".to_string(),
        AttributeValue::N(format!("{}.5", fixture.ttl.as_seconds())),
    );

    let wire = WireItem::from_attribute_map(&map).expect("encode attribute map to wire");
    wire.try_decode::<TimestampWireFixture>()
        .expect_err("fractional ttl must fail decode");
}

#[test]
fn ttl_index_token_is_not_written_when_the_item_has_no_parseable_ttl_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::S("not-a-number".to_string()),
        ),
    ]))
    .expect("wire item");

    let token = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .expect("ttl token projection");

    assert!(token.is_none());
}

#[test]
fn ttl_index_token_requires_the_table_key_when_ttl_is_present_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::from_attribute_map(&HashMap::from([(
        "ttl".to_string(),
        AttributeValue::N("1700000500".to_string()),
    )]))
    .expect("wire item");

    let error = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .expect_err("ttl without table key fails");

    assert!(matches!(error,
        StorageError::Base(StorageEnum::InternalServerError { ref message })
            if message.contains("ttl index token missing hash key attribute")));
}

#[test]
fn ttl_index_token_includes_hash_and_range_keys_when_the_table_has_a_range_key_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N("1700000500".to_string()),
        ),
    ]))
    .expect("wire item");

    let (ttl, token) = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .expect("ttl token projection")
        .expect("ttl token exists");
    let decoded_key =
        crate::ItemKey::item_key_from_next_page_token(&token, &table_info, &None).expect("decode");
    let decoded_key = decoded_key.expect("decoded key exists");

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
    let wire_item = WireItem::from_attribute_map(&HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("JOB#1".to_string()),
    )]))
    .expect("wire item");

    let token = wire_item
        .last_evaluated_key(&table_info, &None)
        .expect("last evaluated key projection");

    assert!(token.is_none());
}

#[derive(Debug, PartialEq, Eq)]
struct ApiKeyViewModel {
    full_key: String,
    user_id: String,
}

impl TryFromWireItem for ApiKeyViewModel {
    fn try_from_wire_item(item: &WireItem) -> crate::StorageResult<Self> {
        let values = item.scalar_attributes(&["full_key", "user_id"])?;
        let full_key = values
            .first()
            .and_then(|value| value.as_ref())
            .map(|value| value.to_string())
            .ok_or_else(|| crate::StorageError::internal(&"missing full_key"))?;
        let user_id = values
            .get(1)
            .and_then(|value| value.as_ref())
            .map(|value| value.to_string())
            .ok_or_else(|| crate::StorageError::internal(&"missing user_id"))?;
        Ok(Self { full_key, user_id })
    }
}

#[test]
fn view_model_decode_reads_selected_fields_from_wire_tests() {
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("BOOTSTRAP".to_string())),
        ("sk".to_string(), AttributeValue::S("PLATFORM".to_string())),
        (
            "full_key".to_string(),
            AttributeValue::S("aux_test_123".to_string()),
        ),
        (
            "user_id".to_string(),
            AttributeValue::S("user_1".to_string()),
        ),
        (
            "ignored".to_string(),
            AttributeValue::M(HashMap::from([(
                "nested".to_string(),
                AttributeValue::L(vec![AttributeValue::S("large".to_string())]),
            )])),
        ),
    ]);
    let payload = serde_json::to_vec(&item).expect("serialize test item");
    let wire_item = WireItem::dynamo_json(payload);

    let decoded = wire_item
        .try_decode::<ApiKeyViewModel>()
        .expect("decode view model");
    assert_eq!(
        decoded,
        ApiKeyViewModel {
            full_key: "aux_test_123".to_string(),
            user_id: "user_1".to_string(),
        }
    );
}
