use storage_derive::WireEntityDecode;
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BillingMode, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, NoopValidatedEntity, StorageResult, StoredEntity,
    TableName, WireItem, single_table_entity::SingleTableEntity,
};

use crate::{DatabaseManager, PutItemInput, QueryTableInput};

#[derive(Debug, serde::Deserialize, WireEntityDecode)]
struct QueryEntity {
    id: String,
    #[serde(alias = "old_note")]
    note: Option<String>,
}

impl NoopValidatedEntity for QueryEntity {}

impl SingleTableEntity for QueryEntity {
    const STORAGE_ENTITY_TYPE: &'static str = "query-fixture";

    fn pk(&self) -> String {
        Self::STORAGE_ENTITY_TYPE.into()
    }

    fn sk(&self) -> String {
        self.id.clone()
    }
}

impl StoredEntity for QueryEntity {
    fn try_from_stored_item(item: &WireItem) -> StorageResult<Self> {
        Self::validate_loaded(Self::try_from_wire_unvalidated(item)?)
    }
}

async fn create_fixture() -> (DatabaseManager, TableName) {
    let database = DatabaseManager::new_for_test().await.unwrap();
    let table = TableName::new("stored-query-identity");
    let schema = [("pk", KeyType::Hash), ("sk", KeyType::Range)];
    database
        .create_table(&CreateTableRequest::new(
            table.clone(),
            schema
                .iter()
                .map(|(name, _)| AttributeDefinition {
                    attribute_name: (*name).into(),
                    attribute_type: KeyAttributeType::S,
                })
                .collect(),
            schema
                .iter()
                .map(|(name, key_type)| KeySchemaElement {
                    attribute_name: (*name).into(),
                    key_type: key_type.clone(),
                })
                .collect(),
            BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();
    (database, table)
}

fn query_input(table: &TableName) -> QueryTableInput {
    QueryTableInput::builder()
        .table_name(table.clone())
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(AttributeMap::from_iter([(
            ":pk".into(),
            AttributeValue::S("query-fixture".into()),
        )]))
        .consistent_read(true)
        .build()
}

#[tokio::test]
async fn given_stored_entity_query_when_identity_is_corrupt_then_no_entity_is_admitted() {
    let (database, table) = create_fixture().await;
    for (entity_type, legacy_type, id, is_valid) in [
        (Some("query-fixture"), None, "item", true),
        (None, Some("query-fixture"), "item", true),
        (Some("foreign-entity"), None, "item", false),
        (None, None, "item", false),
        (Some("query-fixture"), None, "foreign-id", false),
        (Some("query-fixture"), Some("foreign-entity"), "item", false),
        (Some("query-fixture"), Some("query-fixture"), "item", false),
    ] {
        let mut item = AttributeMap::from_iter([
            ("pk".into(), AttributeValue::S("query-fixture".into())),
            ("sk".into(), AttributeValue::S("item".into())),
            ("id".into(), AttributeValue::S(id.into())),
        ]);
        if let Some(entity_type) = entity_type {
            item.insert("et", AttributeValue::S(entity_type.into()));
        }
        if let Some(legacy_type) = legacy_type {
            item.insert("entity_type", AttributeValue::S(legacy_type.into()));
        }
        database
            .put_item(
                PutItemInput::builder()
                    .table_name(table.clone())
                    .item(item)
                    .build(),
            )
            .await
            .unwrap();
        let result = database
            .query_table_stored_entity::<QueryEntity>(query_input(&table))
            .await;
        if is_valid {
            assert_eq!(result.unwrap().items[0].id, "item");
        } else {
            assert!(
                result.is_err(),
                "identity {entity_type:?}/{legacy_type:?}/{id} must be rejected"
            );
        }
    }
}

#[tokio::test]
async fn given_json_shaped_string_attributes_when_stored_rows_are_read_then_both_derived_decoders_preserve_exact_strings()
 {
    #[derive(serde::Deserialize, storage_derive::WireProjectionDecode)]
    struct QueryProjection {
        id: String,
        #[serde(alias = "old_note")]
        note: Option<String>,
    }

    let (database, table) = create_fixture().await;
    for (index, value) in [
        "\"opaque-1\"",
        "null",
        "true",
        "42",
        "\"\"",
        "{\"nested\":true}",
    ]
    .into_iter()
    .enumerate()
    {
        let note_field = if index % 2 == 0 { "old_note" } else { "note" };
        let item = AttributeMap::from_iter([
            ("pk".into(), AttributeValue::S("query-fixture".into())),
            ("sk".into(), AttributeValue::S(value.into())),
            ("et".into(), AttributeValue::S("query-fixture".into())),
            ("id".into(), AttributeValue::S(value.into())),
            (note_field.into(), AttributeValue::S(value.into())),
        ]);
        database
            .put_item(
                PutItemInput::builder()
                    .table_name(table.clone())
                    .item(item)
                    .build(),
            )
            .await
            .unwrap();
    }
    let rows = database
        .query_table(query_input(&table))
        .await
        .unwrap()
        .items;
    for row in rows {
        let expected = row.attribute_value("sk").unwrap().unwrap();
        let AttributeValue::S(expected) = expected else {
            panic!("string sort key")
        };
        let projected: QueryProjection = row.try_decode().unwrap();
        let entity = QueryEntity::try_from_stored_item(&row).unwrap();
        assert_eq!(projected.id, expected);
        assert_eq!(entity.id, expected);
        assert_eq!(projected.note.as_deref(), Some(expected.as_str()));
        assert_eq!(entity.note.as_deref(), Some(expected.as_str()));
    }
}
