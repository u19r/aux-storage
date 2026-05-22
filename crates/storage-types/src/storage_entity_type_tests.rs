use std::str::FromStr;

use crate::StorageEntityType;

#[test]
fn storage_entity_type_accepts_uppercase_codes_for_storage_layouts() {
    let entity_type =
        StorageEntityType::new("ORDER_ITEM_2").expect("uppercase entity type should parse");

    assert_eq!(entity_type.as_str(), "ORDER_ITEM_2");
    assert_eq!(entity_type.as_static_str(), "ORDER_ITEM_2");
    assert_eq!(entity_type.as_db_code(), "ORDER_ITEM_2");
    assert_eq!(entity_type.to_string(), "ORDER_ITEM_2");
    assert_eq!(String::from(entity_type), "ORDER_ITEM_2");
}

#[test]
fn storage_entity_type_rejects_codes_that_cannot_be_persisted_safely() {
    for invalid in ["", "order", "Order", "_ORDER", "ORDER-ITEM", "ORDER ITEM"] {
        assert!(
            StorageEntityType::new(invalid).is_none(),
            "{invalid:?} must not be accepted as a storage entity type"
        );
        assert!(
            StorageEntityType::parse_db(invalid).is_none(),
            "{invalid:?} must not parse from database storage"
        );
        assert!(
            StorageEntityType::from_str(invalid).is_err(),
            "{invalid:?} must not parse from strings"
        );
    }
}

#[test]
fn storage_entity_type_serializes_as_its_database_code() {
    let entity_type = StorageEntityType::parse("CUSTOMER").expect("entity type should parse");

    let json = serde_json::to_string(&entity_type).expect("entity type should serialize");
    let parsed: StorageEntityType =
        serde_json::from_str(&json).expect("entity type should deserialize");

    assert_eq!(json, "\"CUSTOMER\"");
    assert_eq!(parsed, entity_type);
}

#[test]
fn storage_entity_type_deserialization_rejects_invalid_codes() {
    let error = serde_json::from_str::<StorageEntityType>("\"customer\"")
        .expect_err("lowercase entity type must fail");

    assert!(
        error.to_string().contains("invalid storage entity type"),
        "unexpected error: {error}"
    );
}
