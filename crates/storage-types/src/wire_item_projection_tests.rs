use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use storage_derive::{WireItemDecode, WireItemEncode, WireProjectionDecode};

use crate::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageEnum, StoredEntity,
    StoredTableInfo, TableName, TableStatus, TimestampMillis, TryFromWireItem, TryIntoWireItem,
    ValidatedEntity, WireItem, WireItemKeyAttributes, decode_wire_field, decode_wire_field_json,
    decode_wire_serde_string,
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
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn given_binary_and_numeric_source_keys_when_extracting_progress_then_cursor_round_trips() {
    let mut table = sample_table_info();
    table.attribute_definitions[0].attribute_type = KeyAttributeType::B;
    table.attribute_definitions[1].attribute_type = KeyAttributeType::N;
    let item = WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), AttributeValue::B("AAEC".to_owned())),
        ("sk".to_owned(), AttributeValue::N("1E2".to_owned())),
        ("payload".to_owned(), AttributeValue::S("source".to_owned())),
    ]))
    .unwrap();
    let key = item.item_key(&table, &None).unwrap().unwrap();
    let token = key.next_page_token().unwrap();
    assert_eq!(
        item.last_evaluated_key(&table, &None).unwrap(),
        Some(token.clone())
    );
    let decoded = crate::ItemKey::item_key_from_next_page_token(&token, &table, &None)
        .unwrap()
        .unwrap();
    assert_eq!(decoded.hash_key(), key.hash_key());
    assert_eq!(decoded.range_key(), key.range_key());
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

#[derive(Debug, Clone, Deserialize, WireProjectionDecode)]
struct DefaultedFixtureProjection {
    id: String,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, WireProjectionDecode)]
struct DefaultedBoolProjection {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    optional_active: Option<bool>,
}

fn default_projection_aliases() -> Vec<String> {
    vec!["fallback".to_string()]
}

#[derive(Debug, Clone, Deserialize, WireProjectionDecode)]
struct DefaultedAttributeOrderProjection {
    #[serde(
        default = "default_projection_aliases",
        skip_serializing_if = "Vec::is_empty"
    )]
    labels: Vec<String>,
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        default,
        alias = "legacy_aliases"
    )]
    aliases: Vec<String>,
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
    let payload = serde_json::to_vec(&item).unwrap();
    let wire_item = WireItem::dynamo_json(payload);

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms").unwrap();
    assert_eq!(projected, Some(1729));

    let missing = decode_wire_field::<Option<i64>>(&wire_item, None, "does_not_exist").unwrap();
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
    .unwrap();
    let wire_item = WireItem::local_split(primary, None, Some(blob));

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms").unwrap();
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

    let projected = decode_wire_field::<Option<i64>>(&wire_item, None, "lease_until_ms").unwrap();
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
fn last_evaluated_key_supports_gsi_and_table_reusing_range_attribute_tests() {
    let mut table_info = sample_table_info();
    table_info
        .global_secondary_indexes
        .as_mut()
        .unwrap()
        .first_mut()
        .unwrap()
        .key_schema[1]
        .attribute_name = "sk".to_string();
    let wire_item = WireItem::try_from(&HashMap::from([
        (
            "gsi_pk".to_string(),
            AttributeValue::S("STATE#ready".to_string()),
        ),
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
    ]))
    .unwrap();

    let token = wire_item
        .last_evaluated_key(&table_info, &Some(IndexName::new("gsi1")))
        .unwrap()
        .unwrap();
    let decoded = crate::ItemKey::item_key_from_next_page_token(
        &token,
        &table_info,
        &Some(IndexName::new("gsi1")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        decoded.hash_key(),
        &AttributeValue::S("STATE#ready".to_string())
    );
    assert_eq!(
        decoded.range_key(),
        Some(&AttributeValue::S("LOCK".to_string()))
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
    let payload = serde_json::to_vec(&item).unwrap();
    let wire_item = WireItem::dynamo_json(payload);

    let lek = wire_item
        .last_evaluated_key(&table_info, &None)
        .unwrap()
        .unwrap();
    let decoded_key =
        crate::ItemKey::item_key_from_next_page_token(&lek, &table_info, &None).unwrap();
    let decoded_key = decoded_key.unwrap();
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
    let payload = serde_json::to_vec(&item).unwrap();
    let wire_item = WireItem::dynamo_json(payload);

    let projected =
        decode_wire_field::<Option<String>>(&wire_item, None, "rate_limit_key").unwrap();
    assert_eq!(projected.as_deref(), Some("premium"));

    let missing = decode_wire_field::<Option<String>>(&wire_item, None, "does_not_exist").unwrap();
    assert_eq!(missing, None);
}

#[test]
fn bool_projection_reads_only_target_field_from_dynamo_wire_tests() {
    let item = HashMap::from([
        ("active".to_string(), AttributeValue::BOOL(true)),
        ("attempts".to_string(), AttributeValue::N("4".to_string())),
    ]);
    let payload = serde_json::to_vec(&item).unwrap();
    let wire_item = WireItem::dynamo_json(payload);

    let projected = decode_wire_field::<Option<bool>>(&wire_item, None, "active").unwrap();
    assert_eq!(projected, Some(true));

    let missing = decode_wire_field::<Option<bool>>(&wire_item, None, "does_not_exist").unwrap();
    assert_eq!(missing, None);
}

#[test]
fn wire_attribute_decode_accepts_numeric_boolean_storage_used_by_legacy_records_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let enabled = decode_wire_field::<bool>(&wire_item, Some("1"), "enabled").unwrap();
    let disabled = decode_wire_field::<bool>(&wire_item, Some("0"), "enabled").unwrap();

    assert!(enabled);
    assert!(!disabled);
}

#[test]
fn wire_attribute_decode_rejects_boolean_values_outside_the_persisted_contract_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error = decode_wire_field::<bool>(&wire_item, Some("yes"), "enabled").unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("invalid enabled field: yes"))
    );
}

#[test]
fn wire_attribute_decode_accepts_rfc3339_seconds_and_milliseconds_for_datetime_fields_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let rfc3339 = decode_wire_field::<DateTime<Utc>>(
        &wire_item,
        Some("2026-05-14T12:34:56Z"),
        "scheduled_at",
    )
    .unwrap();
    let seconds =
        decode_wire_field::<DateTime<Utc>>(&wire_item, Some("1700000000"), "scheduled_at").unwrap();
    let millis =
        decode_wire_field::<DateTime<Utc>>(&wire_item, Some("1700000000000"), "scheduled_at")
            .unwrap();

    assert_eq!(rfc3339.timestamp(), 1_778_762_096);
    assert_eq!(seconds.timestamp(), 1_700_000_000);
    assert_eq!(millis.timestamp_millis(), 1_700_000_000_000);
}

#[test]
fn wire_attribute_decode_rejects_datetime_values_that_are_not_timestamps_or_rfc3339_tests() {
    let wire_item = WireItem::dynamo_json(Vec::new());

    let error = decode_wire_field::<DateTime<Utc>>(&wire_item, Some("tomorrow"), "scheduled_at")
        .unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("invalid scheduled_at field datetime format"))
    );
}

#[test]
fn wire_json_field_decode_accepts_plain_scalar_strings_when_callers_store_legacy_raw_values_tests()
{
    let wire_item = WireItem::dynamo_json(Vec::new());

    let decoded =
        decode_wire_field_json::<String>(&wire_item, Some("plain-value"), "status").unwrap();

    assert_eq!(decoded, "plain-value");
}

#[test]
fn wire_json_field_decode_rejects_missing_required_non_nullable_fields_tests() {
    let wire_item = WireItem::try_from(&HashMap::new()).unwrap();

    let error = decode_wire_field_json::<String>(&wire_item, None, "status").unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("missing required field status"))
    );
}

#[test]
fn wire_serde_string_decode_reports_the_business_field_name_for_invalid_json_scalar_tests() {
    let error = decode_wire_serde_string::<u32>("not-a-number", "attempts").unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("invalid attempts field"))
    );
}

#[test]
fn stored_entity_decode_runs_validation_tests() {
    let wire_item = WireItem::try_from(&HashMap::from([
        (
            "id".to_string(),
            AttributeValue::S("fixture_123".to_string()),
        ),
        ("active".to_string(), AttributeValue::BOOL(true)),
    ]))
    .unwrap();

    let decoded: ValidatedFixture = wire_item.try_decode().unwrap();
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
    let wire_item = WireItem::try_from(&HashMap::from([
        ("id".to_string(), AttributeValue::S("   ".to_string())),
        ("active".to_string(), AttributeValue::BOOL(false)),
    ]))
    .unwrap();

    let err = wire_item.try_decode::<ValidatedFixture>().unwrap_err();
    assert!(
        format!("{err:?}").contains("invalid persisted validated fixture: fixture_id_required")
    );
}

#[test]
fn projection_decode_honors_serde_default_when_wire_field_is_absent_tests() {
    let item = WireItem::try_from(&HashMap::from([(
        "id".to_string(),
        AttributeValue::S("fixture-1".to_string()),
    )]))
    .unwrap();

    let decoded: DefaultedFixtureProjection = item.try_decode().unwrap();

    assert_eq!(decoded.id, "fixture-1");
    assert!(decoded.labels.is_empty());
}

#[test]
fn projection_decode_parses_combined_serde_default_attributes_tests() {
    let item = WireItem::try_from(&HashMap::from([(
        "aliases".to_string(),
        AttributeValue::SS(vec!["persisted".to_string()]),
    )]))
    .unwrap();
    let decoded: DefaultedAttributeOrderProjection = item.try_decode().unwrap();

    assert_eq!(decoded.labels, ["fallback".to_string()]);
    assert_eq!(decoded.aliases, ["persisted".to_string()]);

    let missing = WireItem::try_from(&HashMap::new()).unwrap();
    let decoded: DefaultedAttributeOrderProjection = missing.try_decode().unwrap();
    assert_eq!(decoded.labels, ["fallback".to_string()]);
    assert!(decoded.aliases.is_empty());
    let legacy = WireItem::try_from(&HashMap::from([(
        "legacy_aliases".to_string(),
        AttributeValue::SS(vec!["historical".to_string()]),
    )]))
    .unwrap();
    let decoded: DefaultedAttributeOrderProjection = legacy.try_decode().unwrap();
    assert_eq!(decoded.aliases, ["historical".to_string()]);
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
fn projection_decode_bool_rejects_present_non_boolean_values_tests() {
    for wire_item in [
        WireItem::try_from(&HashMap::from([(
            "active".to_string(),
            AttributeValue::S("false".to_string()),
        )]))
        .unwrap(),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".to_string(),
                AttributeValue::S("BOOL#1".to_string()),
                Some("sk".to_string()),
                Some(AttributeValue::S("META".to_string())),
            ),
            None,
            Some(
                serde_json::to_vec(&HashMap::from([(
                    "active".to_string(),
                    AttributeValue::N("0".to_string()),
                )]))
                .unwrap(),
            ),
        ),
    ] {
        assert!(wire_item.try_decode::<DefaultedBoolProjection>().is_err());
    }
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
    .unwrap();
    let wire_item = WireItem::local_split(primary, None, Some(blob));

    let projected = decode_wire_field::<Option<bool>>(&wire_item, None, "active").unwrap();
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

    let active = wire_item.bool_attribute("active").unwrap();

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
    .unwrap();
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
fn malformed_number_attribute_does_not_decode_as_an_absent_number() {
    let wire_item = WireItem::try_from(&HashMap::from([(
        "ttl".to_owned(),
        AttributeValue::N("not-a-number".to_owned()),
    )]))
    .unwrap();

    assert!(wire_item.number_attribute_i64("ttl").is_err());
}

#[test]
fn malformed_split_key_number_does_not_decode_as_an_absent_number() {
    let wire_item = WireItem::local_split(
        WireItemKeyAttributes::new(
            "ttl".to_owned(),
            AttributeValue::N("not-a-number".to_owned()),
            None,
            None,
        ),
        None,
        None,
    );

    assert!(wire_item.number_attribute_i64("ttl").is_err());
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
fn scalar_projection_aliases_preserve_malformed_attribute_errors_tests() {
    let wire_item = WireItem::dynamo_json(br#"{"created_at":{"N":7},"c_at":{"N":"200"}}"#.to_vec());

    let error = wire_item.scalar_attributes(&["created_at"]).unwrap_err();
    assert!(error.to_string().contains("invalid type"));
}

#[test]
fn scalar_projection_aliases_preserve_reverse_requests_and_alias_errors_tests() {
    let alias_only = WireItem::dynamo_json(br#"{"c_at":{"N":"200"}}"#.to_vec());
    let projected = alias_only
        .scalar_attributes(&["created_at", "c_at"])
        .unwrap();
    assert_eq!(projected[0].as_deref(), Some("200"));
    assert_eq!(projected[1].as_deref(), Some("200"));

    let malformed_alias = WireItem::dynamo_json(br#"{"c_at":{"N":7}}"#.to_vec());
    let error = malformed_alias
        .scalar_attributes(&["created_at"])
        .unwrap_err();
    assert!(error.to_string().contains("invalid type"));
}

#[test]
fn wire_item_encode_uses_numeric_timestamp_storage_tests() {
    let fixture = sample_timestamp_wire_fixture();
    let wire = fixture.try_into_wire_item().unwrap();
    let map = wire.to_attribute_map().unwrap();

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
                    .unwrap()
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
fn ttl_index_token_is_not_written_when_the_item_has_no_parseable_ttl_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("JOB#1".to_string())),
        ("sk".to_string(), AttributeValue::S("LOCK".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::S("not-a-number".to_string()),
        ),
    ]))
    .unwrap();

    let token = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .unwrap();

    assert!(token.is_none());
}

#[test]
fn ttl_index_token_requires_the_table_key_when_ttl_is_present_tests() {
    let table_info = sample_table_info();
    let wire_item = WireItem::try_from(&HashMap::from([(
        "ttl".to_string(),
        AttributeValue::N("1700000500".to_string()),
    )]))
    .unwrap();

    let error = wire_item
        .ttl_value_and_table_key_token(&table_info, "ttl")
        .unwrap_err();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::InternalServerError { message })
            if message.contains("ttl index token missing hash key attribute"))
    );
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
    let payload = serde_json::to_vec(&item).unwrap();
    let wire_item = WireItem::dynamo_json(payload);

    let decoded = wire_item.try_decode::<ApiKeyViewModel>().unwrap();
    assert_eq!(
        decoded,
        ApiKeyViewModel {
            full_key: "aux_test_123".to_string(),
            user_id: "user_1".to_string(),
        }
    );
}

#[derive(Debug, PartialEq, Serialize, Deserialize, WireItemEncode, WireProjectionDecode)]
struct OmittedWireFields {
    id: String,
    #[serde(default, skip_serializing_if = "is_false")]
    active: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[test]
fn given_serde_omission_predicates_when_encoding_wire_item_then_absent_and_present_fields_round_trip()
 {
    for fixture in [
        OmittedWireFields {
            id: "omitted".into(),
            active: false,
            labels: vec![],
            count: None,
        },
        OmittedWireFields {
            id: "present".into(),
            active: true,
            labels: vec!["label".into()],
            count: Some(u64::MAX),
        },
    ] {
        let item = fixture.try_into_wire_item().expect("encoded item");
        assert_eq!(
            item.attribute_value("active").unwrap().is_some(),
            fixture.active
        );
        assert_eq!(
            item.attribute_value("labels").unwrap().is_some(),
            !fixture.labels.is_empty()
        );
        assert_eq!(
            item.attribute_value("count").unwrap().is_some(),
            fixture.count.is_some()
        );
        assert_eq!(item.try_decode::<OmittedWireFields>().unwrap(), fixture);
    }
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
    let WireItem::DynamoJson { data } = &direct else {
        panic!("wire encoding");
    };
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

#[test]
fn given_canonical_and_legacy_timestamps_when_reading_wire_fields_then_alias_lookup_and_exact_selection_remain_distinct()
 {
    let canonical = AttributeValue::N("7".into());
    for legacy in [None, Some(AttributeValue::N("9".into()))] {
        let mut attributes = HashMap::from([("u_at".to_string(), canonical.clone())]);
        if let Some(value) = &legacy {
            attributes.insert("updated_at".to_string(), value.clone());
        }
        let bytes = serde_json::to_vec(&attributes).unwrap();
        let primary =
            WireItemKeyAttributes::new("pk".into(), AttributeValue::S("row".into()), None, None);
        for item in [
            WireItem::dynamo_json(bytes.clone()),
            WireItem::local_split(primary, None, Some(bytes)),
        ] {
            assert_eq!(
                item.attribute_value("updated_at").unwrap(),
                legacy.clone().or_else(|| Some(canonical.clone()))
            );
            assert_eq!(
                item.select_attribute_values(&["u_at", "updated_at"])
                    .unwrap(),
                vec![Some(canonical.clone()), legacy.clone()]
            );
            assert_eq!(
                item.select_attribute_values(&["updated_at", "u_at"])
                    .unwrap(),
                vec![legacy.clone(), Some(canonical.clone())]
            );
        }
    }
}
