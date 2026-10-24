use serde::Serialize;
use storage_derive::{WireEntityDecode, WireItemDecode, WireProjectionDecode};

use crate::{AttributeMap, AttributeValue, TryFromWireItem, WireItem};

macro_rules! alias_fixture {
    ($name:ident, $decoder:path) => {
        #[derive(Debug, Serialize, $decoder)]
        struct $name {
            #[serde(rename = "_v", alias = "version", default = "initial_version")]
            version: u64,
            #[serde(default, alias = "legacy_enabled")]
            enabled: bool,
            #[serde(alias = "message")]
            label: String,
            #[serde(default, alias = "legacy_items")]
            items: Vec<String>,
        }
    };
}
alias_fixture!(ItemAliases, WireItemDecode);
alias_fixture!(ProjectionAliases, WireProjectionDecode);
alias_fixture!(EntityAliases, WireEntityDecode);

impl TryFromWireItem for EntityAliases {
    fn try_from_wire_item(item: &WireItem) -> crate::StorageResult<Self> {
        Self::try_from_wire_unvalidated(item)
    }
}

fn initial_version() -> u64 {
    11
}

fn legacy_item() -> AttributeMap {
    AttributeMap::from_iter([
        ("version".into(), AttributeValue::N("7".into())),
        ("legacy_enabled".into(), AttributeValue::BOOL(true)),
        ("message".into(), AttributeValue::S("historical".into())),
        (
            "legacy_items".into(),
            AttributeValue::L(vec![AttributeValue::S("persisted".into())]),
        ),
    ])
}

fn verify_alias_contract<T: TryFromWireItem + Serialize>() {
    let mut attributes = legacy_item();
    let decoded = WireItem::try_from(&attributes)
        .unwrap()
        .try_decode::<T>()
        .unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::json!({
            "_v":7,"enabled":true,"label":"historical","items":["persisted"]
        })
    );
    attributes.insert("_v", AttributeValue::N("7".into()));
    assert!(
        WireItem::try_from(&attributes)
            .unwrap()
            .try_decode::<T>()
            .is_err()
    );
    attributes.remove("_v");
    attributes.insert("legacy_enabled", AttributeValue::S("invalid".into()));
    assert!(
        WireItem::try_from(&attributes)
            .unwrap()
            .try_decode::<T>()
            .is_err()
    );
    attributes.remove("legacy_enabled");
    attributes.insert("version", AttributeValue::S("invalid".into()));
    assert!(
        WireItem::try_from(&attributes)
            .unwrap()
            .try_decode::<T>()
            .is_err()
    );
    attributes.remove("version");
    attributes.remove("legacy_items");
    let decoded = WireItem::try_from(&attributes)
        .unwrap()
        .try_decode::<T>()
        .unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::json!({
            "_v":11,"enabled":false,"label":"historical","items":[]
        })
    );
    attributes.remove("message");
    assert!(
        WireItem::try_from(&attributes)
            .unwrap()
            .try_decode::<T>()
            .is_err()
    );
}

#[test]
fn given_legacy_item_fields_when_wire_decoding_then_values_survive_and_defaults_cannot_hide_corruption()
 {
    verify_alias_contract::<ItemAliases>();
}
#[test]
fn given_legacy_projection_fields_when_wire_decoding_then_values_survive_and_defaults_cannot_hide_corruption()
 {
    verify_alias_contract::<ProjectionAliases>();
}
#[test]
fn given_legacy_entity_fields_when_wire_decoding_then_values_survive_and_defaults_cannot_hide_corruption()
 {
    verify_alias_contract::<EntityAliases>();
}

macro_rules! timestamp_alias_fixture {
    ($name:ident, $decoder:path) => {
        #[derive(Debug, Serialize, $decoder)]
        struct $name {
            #[serde(rename = "c_at", alias = "created_at", default)]
            created_at: u64,
        }
    };
}
timestamp_alias_fixture!(ItemTimestampAlias, WireItemDecode);
timestamp_alias_fixture!(ProjectionTimestampAlias, WireProjectionDecode);
timestamp_alias_fixture!(EntityTimestampAlias, WireEntityDecode);

impl TryFromWireItem for EntityTimestampAlias {
    fn try_from_wire_item(item: &WireItem) -> crate::StorageResult<Self> {
        Self::try_from_wire_unvalidated(item)
    }
}

fn verify_timestamp_alias<T: TryFromWireItem + Serialize>() {
    for name in ["c_at", "created_at"] {
        let mut attributes =
            AttributeMap::from_iter([(name.to_string(), AttributeValue::N("7".into()))]);
        let decoded = WireItem::try_from(&attributes)
            .unwrap()
            .try_decode::<T>()
            .unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::json!({"c_at": 7})
        );
        attributes.insert(
            if name == "c_at" { "created_at" } else { "c_at" },
            AttributeValue::N("7".into()),
        );
        assert!(
            WireItem::try_from(&attributes)
                .unwrap()
                .try_decode::<T>()
                .is_err()
        );
    }
}

#[test]
fn given_builtin_timestamp_alias_when_wire_decoding_then_one_representation_is_accepted_and_duplicates_are_rejected()
 {
    verify_timestamp_alias::<ItemTimestampAlias>();
    verify_timestamp_alias::<ProjectionTimestampAlias>();
    verify_timestamp_alias::<EntityTimestampAlias>();
}
