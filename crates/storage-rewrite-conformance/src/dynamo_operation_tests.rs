use std::collections::HashMap;

use serde_json::json;
use storage_types::{AttributeValue, GetItemRequest, PutItemRequest};

use crate::test_support::TestStore;

/// Business and format rules: the public response contains the full stored
/// item.
#[tokio::test]
async fn given_replacement_store_when_put_and_get_run_then_dynamo_item_response_is_exact() {
    // Arrange
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;

    // Act
    store
        .operations
        .put_item(PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(&HashMap::from([
                ("pk".to_owned(), AttributeValue::S("one".to_owned())),
                ("value".to_owned(), AttributeValue::S("stored".to_owned())),
            ]))
            .unwrap(),
        ))
        .await
        .expect("put item through shared Dynamo operation");
    let response = store
        .operations
        .get_item(GetItemRequest::new(
            table,
            HashMap::from([("pk".to_owned(), AttributeValue::S("one".to_owned()))]),
        ))
        .await
        .expect("get item through shared Dynamo operation");

    // Assert
    assert_eq!(
        serde_json::to_value(response).expect("serialize Dynamo response"),
        json!({"Item": {"pk": {"S": "one"}, "value": {"S": "stored"}}}),
    );
    store.finish().await;
}
