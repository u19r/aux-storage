use std::collections::HashMap;

use storage_provider::{ReadSequenceExecution, ReadSequenceFlatResult, StorageProvider};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, ProjectionType, ReadSequenceConsistency, ReadSequenceRequest,
    TableName, plan_read_sequence,
};

use super::{TursoStorageProvider, reset_turso_statement_counters, turso_statement_counters};
use crate::sql_test_support::{
    mapped_gsi_parent_item, mapped_gsi_read_sequence_request,
    mapped_gsi_table_request_with_projection,
};

static STATEMENT_COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn given_hash_range_child_when_turso_maps_indexer_then_one_statement_returns_present_and_nil()
{
    let _counter_guard = STATEMENT_COUNTER_LOCK.lock().await;
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("provider");
    provider.initialize_storage().await.expect("initialize");
    let parents = TableName::new("turso_mapped_parents");
    let children = TableName::new("turso_mapped_children");
    let mut parent_table = table_request(parents.clone(), true);
    parent_table.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    provider
        .create_table(&parent_table)
        .await
        .expect("create parent table");
    provider
        .create_table(&table_request(children.clone(), true))
        .await
        .expect("create child table");

    for (sort, customer) in [("a", Some("customer-1")), ("b", None)] {
        let mut item = HashMap::from([
            ("pk".to_string(), AttributeValue::S("group".to_string())),
            ("sk".to_string(), AttributeValue::S(sort.to_string())),
        ]);
        if let Some(customer) = customer {
            item.insert(
                "customer_id".to_string(),
                AttributeValue::S(customer.to_string()),
            );
        }
        let mut put = storage_types::PutItemRequest::new(parents.clone(), item);
        put.indexers = Some(vec!["customer_id".to_string()]);
        provider.put_item_request(put).await.expect("put parent");
    }
    provider
        .put_item(
            children.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("group".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("customer-1".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("related".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put child");

    let request: ReadSequenceRequest = serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": parents, "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "group"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": children, "Key": {
                    "pk": {"S": "group"},
                    "sk": {"FromInput": "customer"}
                }
            }}, "Inputs": {
                "customer": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].customer_id"},
                    "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
                    "Cardinality": "MANY", "OnMissing": "SKIP"
                }
            }, "Iterate": "customer"}
        ]
    }))
    .expect("request");
    let plan = plan_read_sequence(&request).expect("plan");
    provider
        .get_table_info(&parents)
        .await
        .expect("cache parent");
    provider
        .get_table_info(&children)
        .await
        .expect("cache child");
    reset_turso_statement_counters();

    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped execution")
    else {
        panic!("mapped Turso plan should compile");
    };

    assert_eq!(turso_statement_counters(), (1, 0));
    assert_eq!(executed.rows.len(), 3);
    assert!(matches!(
        &executed.rows[0].result,
        ReadSequenceFlatResult::Query { items, count: 2, .. } if items.len() == 2
    ));
    assert!(matches!(
        &executed.rows[1].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("related".to_string()))
    ));
    assert!(matches!(
        &executed.rows[2].result,
        ReadSequenceFlatResult::Get { item: None }
    ));
}

#[tokio::test]
async fn given_keys_only_gsi_when_mapping_indexer_then_turso_uses_projected_row() {
    let _counter_guard = STATEMENT_COUNTER_LOCK.lock().await;
    let provider = TursoStorageProvider::new(":memory:")
        .await
        .expect("provider")
        .with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.expect("initialize");
    let parents = TableName::new("turso_mapped_gsi_parents");
    let children = TableName::new("turso_mapped_gsi_children");
    provider
        .create_table(&mapped_gsi_table_request_with_projection(
            parents.clone(),
            ProjectionType::KeysOnly,
        ))
        .await
        .expect("create GSI parent table");
    provider
        .create_table(&table_request(children.clone(), true))
        .await
        .expect("create child table");
    for (sort, customer) in [("a", Some("customer-1")), ("b", None)] {
        let mut put = storage_types::PutItemRequest::new(
            parents.clone(),
            mapped_gsi_parent_item(sort, customer),
        );
        put.indexers = Some(vec!["customer_id".to_string()]);
        provider.put_item_request(put).await.expect("put parent");
    }
    provider
        .put_item(
            children.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("group".to_string())),
                (
                    "sk".to_string(),
                    AttributeValue::S("customer-1".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("related".to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("put child");
    let plan = plan_read_sequence(&mapped_gsi_read_sequence_request(&parents, &children))
        .expect("mapped GSI plan");
    provider
        .get_table_info(&parents)
        .await
        .expect("cache parent");
    provider
        .get_table_info(&children)
        .await
        .expect("cache child");
    reset_turso_statement_counters();

    let ReadSequenceExecution::Executed(executed) = provider
        .execute_read_sequence_plan(&plan, ReadSequenceConsistency::Eventual, None)
        .await
        .expect("mapped GSI execution")
    else {
        panic!("mapped GSI shape should compile");
    };
    assert_eq!(turso_statement_counters(), (1, 0));
    assert_eq!(executed.rows.len(), 3);
    assert!(matches!(
        &executed.rows[0].result,
        ReadSequenceFlatResult::Query { items, .. }
            if items.iter().all(|item| !item.contains_key("customer_id"))
    ));
    assert!(matches!(
        &executed.rows[1].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("payload") == Some(&AttributeValue::S("related".to_string()))
    ));
    assert!(matches!(
        &executed.rows[2].result,
        ReadSequenceFlatResult::Get { item: None }
    ));
}

fn table_request(table_name: TableName, range: bool) -> CreateTableRequest {
    let mut definitions = vec![AttributeDefinition {
        attribute_name: "pk".to_string(),
        attribute_type: KeyAttributeType::S,
    }];
    let mut schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];
    if range {
        definitions.push(AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        });
        schema.push(KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        });
    }
    CreateTableRequest::new(table_name, definitions, schema, BillingMode::PayPerRequest)
}
