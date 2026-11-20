use std::collections::HashMap;

use serde_json::json;
use storage::{DatabaseManager, PutItemInput};
use storage_types::{AttributeMap, AttributeValue, CreateTableRequest, TableName};

use crate::{
    routes::routes_tests::{
        create_string_key_table, default_conformance_backends, handle_scan, seed_attributes,
        seed_item, seed_nested_projection_item,
    },
    types::Response,
};

fn decode_scan_items(
    items: Option<Vec<storage_types::WireItem>>,
) -> Option<Vec<storage_types::AttributeMap>> {
    items.map(|items| {
        items
            .into_iter()
            .map(|item| item.into_attribute_map().expect("scan item should decode"))
            .collect()
    })
}

async fn create_test_table(db: &DatabaseManager, table_name: &str) {
    create_string_key_table(db, table_name, "id", None).await;
}

async fn create_test_table_hash_range(db: &DatabaseManager, table_name: &str) {
    create_string_key_table(db, table_name, "pk", Some("sk")).await;
}

async fn put_test_item(db: &DatabaseManager, table_name: &str, id: &str, name: &str, age: u32) {
    seed_attributes(
        db,
        table_name,
        vec![
            ("id", AttributeValue::S(id.to_string())),
            ("name", AttributeValue::S(name.to_string())),
            ("age", AttributeValue::N(age.to_string())),
        ],
    )
    .await;
}

async fn put_test_item_hash_range(db: &DatabaseManager, table_name: &str, pk: &str, sk: &str) {
    seed_attributes(
        db,
        table_name,
        vec![
            ("pk", AttributeValue::S(pk.to_string())),
            ("sk", AttributeValue::S(sk.to_string())),
        ],
    )
    .await;
}

#[tokio::test]
async fn scan_caps_source_page_by_logical_bytes_and_resume_token_continues() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "ScanBytePageTable").await;
        let data = "\\".repeat(300 * 1024);
        for id in ["item#001", "item#002", "item#003", "item#004"] {
            let mut item = HashMap::new();
            item.insert("id".to_string(), AttributeValue::S(id.to_string()));
            item.insert("data".to_string(), AttributeValue::S(data.clone()));
            seed_item(
                &db,
                PutItemInput::builder()
                    .table_name(TableName::new("ScanBytePageTable"))
                    .item(item)
                    .build(),
            )
            .await
            .expect("put large scan item");
        }

        let first_payload = json!({
            "TableName": "ScanBytePageTable"
        });
        let first = handle_scan(db.clone(), first_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} first scan page: {err:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        let first_items = decode_scan_items(first.items).expect("scan should return items");
        assert!(
            serde_json::to_vec(&first_items).unwrap().len() > 1_024 * 1_024,
            "escaped response should exceed the removed encoded cutoff: {}",
            backend.name
        );
        assert_eq!(first_items.len(), 3, "{}", backend.name);
        assert_eq!(first.count, 3, "{}", backend.name);
        assert_eq!(first.scanned_count, 3, "{}", backend.name);
        let mut seen_ids = scan_item_ids(&first_items);
        let resume = first
            .last_evaluated_key
            .expect("logical-budgeted scan should return resume token");

        let second_payload = json!({
            "TableName": "ScanBytePageTable",
            "ExclusiveStartKey": resume
        });
        let second = handle_scan(db, second_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} second scan page: {err:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected scan response", backend.name);
        };
        let second_items =
            decode_scan_items(second.items).expect("scan should return remaining item");
        assert_eq!(second_items.len(), 1, "{}", backend.name);
        assert_eq!(second.count, 1, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        seen_ids.extend(scan_item_ids(&second_items));
        seen_ids.sort();
        assert_eq!(
            seen_ids,
            vec![
                "item#001".to_string(),
                "item#002".to_string(),
                "item#003".to_string(),
                "item#004".to_string()
            ],
            "{}",
            backend.name
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_projected_logical_page_uses_unprojected_source_keys() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "ProjectedScanBytePageTable").await;
        let data = "x".repeat(300 * 1024);
        for id in ["item#001", "item#002", "item#003", "item#004"] {
            let item = HashMap::from([
                ("id".to_string(), AttributeValue::S(id.to_string())),
                ("data".to_string(), AttributeValue::S(data.clone())),
            ]);
            seed_item(
                &db,
                PutItemInput::builder()
                    .table_name(TableName::new("ProjectedScanBytePageTable"))
                    .item(item)
                    .build(),
            )
            .await
            .expect("put projected scan item");
        }

        let first = handle_scan(
            db.clone(),
            json!({
                "TableName": "ProjectedScanBytePageTable",
                "ProjectionExpression": "#data",
                "ExpressionAttributeNames": {"#data": "data"}
            })
            .try_into()
            .expect("projected scan request"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} projected scan: {error:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        let first_items = decode_scan_items(first.items).expect("projected scan items");
        assert_eq!(first_items.len(), 3, "{}", backend.name);
        assert!(
            first_items
                .iter()
                .all(|item| item.len() == 1 && item.contains_key("data"))
        );
        let resume = first.last_evaluated_key.expect("source frontier");

        let second = handle_scan(
            db,
            json!({
                "TableName": "ProjectedScanBytePageTable",
                "ProjectionExpression": "#data",
                "ExpressionAttributeNames": {"#data": "data"},
                "ExclusiveStartKey": resume
            })
            .try_into()
            .expect("continued projected scan"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} continued projected scan: {error:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected continued scan response", backend.name);
        };
        assert_eq!(
            decode_scan_items(second.items)
                .expect("last projected item")
                .len(),
            1
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_with_malformed_exclusive_start_key_returns_dynamodb_error() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "MalformedScanStartKeyTable").await;
        put_test_item_hash_range(&db, "MalformedScanStartKeyTable", "p", "s001").await;

        let payload = json!({
            "TableName": "MalformedScanStartKeyTable",
            "ExclusiveStartKey": {
                "pk": {"S": "p"}
            }
        });

        let result = handle_scan(db, payload.try_into().unwrap()).await;
        let error = result.expect_err("malformed scan start key should fail");
        assert_eq!(error.status_code, 400, "{}", backend.name);
        assert_eq!(
            error.error_type, "com.amazon.coral.validate#ValidationException",
            "{}",
            backend.name
        );
        assert_eq!(
            error.message,
            "The provided starting key is invalid: The provided key element does not match the \
             schema",
            "{}",
            backend.name
        );
    }
}

#[tokio::test]
async fn scan_with_limit_returns_compatible_hash_range_last_evaluated_key() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table_hash_range(db.as_ref(), "LimitedHashRangeScanTable").await;

        for sk in ["001", "002", "003"] {
            put_test_item_hash_range(&db, "LimitedHashRangeScanTable", "tenant#1", sk).await;
        }

        let payload = json!({
            "TableName": "LimitedHashRangeScanTable",
            "Limit": 2
        });

        let first = handle_scan(db.clone(), payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} first limited scan: {err:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        let first_items = decode_scan_items(first.items).expect("scan should return items");
        assert_eq!(first_items.len(), 2, "{}", backend.name);
        let last_item = &first_items[1];
        let last_evaluated_key = first
            .last_evaluated_key
            .expect("limited scan should return a DynamoDB key map");
        assert_eq!(
            last_evaluated_key.get("pk"),
            last_item.get("pk"),
            "{}",
            backend.name
        );
        assert_eq!(
            last_evaluated_key.get("sk"),
            last_item.get("sk"),
            "{}",
            backend.name
        );

        let resume_payload = json!({
            "TableName": "LimitedHashRangeScanTable",
            "ExclusiveStartKey": last_evaluated_key
        });
        let second = handle_scan(db, resume_payload.try_into().unwrap())
            .await
            .unwrap_or_else(|err| panic!("{} second limited scan: {err:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected scan response", backend.name);
        };
        assert_eq!(
            decode_scan_items(second.items)
                .expect("scan should resume")
                .len(),
            1,
            "{}",
            backend.name
        );
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

fn scan_item_ids(items: &[storage_types::AttributeMap]) -> Vec<String> {
    items
        .iter()
        .map(|item| {
            let Some(AttributeValue::S(id)) = item.get("id") else {
                panic!("scan item should contain string id: {item:?}");
            };
            id.clone()
        })
        .collect()
}

#[tokio::test]
async fn scan_empty_filtered_page_keeps_public_continuation() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "EmptyFilteredScanTable").await;
        put_test_item(db.as_ref(), "EmptyFilteredScanTable", "item-1", "Alice", 20).await;
        put_test_item(db.as_ref(), "EmptyFilteredScanTable", "item-2", "Bob", 30).await;

        let first = handle_scan(
            db.clone(),
            json!({
                "TableName": "EmptyFilteredScanTable",
                "FilterExpression": "age > :min_age",
                "ExpressionAttributeValues": {":min_age": {"N": "100"}},
                "Limit": 1
            })
            .try_into()
            .expect("empty-filter scan request"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} first filtered scan: {error:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        assert!(first.items.as_ref().is_some_and(Vec::is_empty));
        assert_eq!(first.count, 0, "{}", backend.name);
        assert_eq!(first.scanned_count, 1, "{}", backend.name);
        let resume = first
            .last_evaluated_key
            .expect("filtered empty page must preserve its continuation");

        let second = handle_scan(
            db,
            json!({
                "TableName": "EmptyFilteredScanTable",
                "FilterExpression": "age > :min_age",
                "ExpressionAttributeValues": {":min_age": {"N": "100"}},
                "Limit": 1,
                "ExclusiveStartKey": resume
            })
            .try_into()
            .expect("continued empty-filter scan request"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} second filtered scan: {error:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected continued scan response", backend.name);
        };
        assert!(second.items.as_ref().is_some_and(Vec::is_empty));
        assert_eq!(second.count, 0, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        assert!(second.last_evaluated_key.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_count_filter_and_limit_keep_evaluated_frontier() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "CountFilteredScanTable").await;
        for (id, age) in [("item-1", 20), ("item-2", 30), ("item-3", 40)] {
            put_test_item(db.as_ref(), "CountFilteredScanTable", id, "name", age).await;
        }

        let first = handle_scan(
            db.clone(),
            json!({
                "TableName": "CountFilteredScanTable",
                "FilterExpression": "age > :minimum",
                "ExpressionAttributeValues": {":minimum": {"N": "25"}},
                "Select": "COUNT",
                "Limit": 1
            })
            .try_into()
            .expect("first count scan"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} count scan: {error:?}", backend.name));
        let Response::Scan(first) = first else {
            panic!("{} expected scan response", backend.name);
        };
        assert_eq!(first.count, 0, "{}", backend.name);
        assert_eq!(first.scanned_count, 1, "{}", backend.name);
        assert!(first.items.is_none(), "{}", backend.name);
        let resume = first.last_evaluated_key.expect("native evaluated frontier");

        let second = handle_scan(
            db,
            json!({
                "TableName": "CountFilteredScanTable",
                "FilterExpression": "age > :minimum",
                "ExpressionAttributeValues": {":minimum": {"N": "25"}},
                "Select": "COUNT",
                "Limit": 1,
                "ExclusiveStartKey": resume
            })
            .try_into()
            .expect("continued count scan"),
        )
        .await
        .unwrap_or_else(|error| panic!("{} continued count scan: {error:?}", backend.name));
        let Response::Scan(second) = second else {
            panic!("{} expected scan response", backend.name);
        };
        assert_eq!(second.count, 1, "{}", backend.name);
        assert_eq!(second.scanned_count, 1, "{}", backend.name);
        assert!(second.items.is_none(), "{}", backend.name);
    }
}

#[tokio::test]
async fn scan_filter_expression_uses_full_condition_expression_engine() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "FullFilterTable").await;
        seed_nested_projection_item(db.as_ref(), "FullFilterTable", "id").await;

        let payload = json!({
            "TableName": "FullFilterTable",
            "FilterExpression": "contains(tags, :tag) AND profile.age >= :minAge",
            "ExpressionAttributeValues": {
                ":tag": {"S": "red"},
                ":minAge": {"N": "30"}
            }
        });

        let result = handle_scan(db, payload.try_into().unwrap()).await.unwrap();
        let Response::Scan(response) = result else {
            panic!("Expected Scan response");
        };

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn scan_filter_expression_supports_size_function() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "FullFilterSizeTable").await;
        seed_nested_projection_item(db.as_ref(), "FullFilterSizeTable", "id").await;

        let payload = json!({
            "TableName": "FullFilterSizeTable",
            "FilterExpression": "size(tags) = :tagCount",
            "ExpressionAttributeValues": {
                ":tagCount": {"N": "2"}
            }
        });

        let result = handle_scan(db, payload.try_into().unwrap()).await.unwrap();
        let Response::Scan(response) = result else {
            panic!("Expected Scan response");
        };

        assert_eq!(response.count, 1);
        assert_eq!(response.scanned_count, 1);
    }
}

#[tokio::test]
async fn scan_projection_reconstructs_nested_maps_and_lists_like_dynamodb() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "NestedProjectionTable").await;
        seed_nested_projection_item(db.as_ref(), "NestedProjectionTable", "id").await;

        let payload = json!({
            "TableName": "NestedProjectionTable",
            "ProjectionExpression": "id, profile.#n, tags[0], nested.#list[0].v",
            "ExpressionAttributeNames": {
                "#n": "name",
                "#list": "list"
            }
        });

        let result = handle_scan(db, payload.try_into().unwrap()).await.unwrap();
        let Response::Scan(response) = result else {
            panic!("Expected Scan response");
        };
        let items = decode_scan_items(response.items).expect("projected items");
        assert_eq!(items.len(), 1);
        let item = &items[0];

        assert_eq!(
            item.get("profile"),
            Some(&AttributeValue::M(AttributeMap::from(HashMap::from([(
                "name".to_string(),
                AttributeValue::S("Ada".to_string()),
            )]))))
        );
        assert_eq!(
            item.get("tags"),
            Some(&AttributeValue::L(vec![AttributeValue::S(
                "red".to_string()
            )]))
        );
        assert_eq!(
            item.get("nested"),
            Some(&AttributeValue::M(AttributeMap::from(HashMap::from([(
                "list".to_string(),
                AttributeValue::L(vec![AttributeValue::M(AttributeMap::from(HashMap::from(
                    [("v".to_string(), AttributeValue::S("first".to_string()),)]
                )))]),
            )]))))
        );
    }
}

#[tokio::test]
async fn scan_consumed_capacity_none() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "TestTable").await;

        // Add test items
        put_test_item(db.as_ref(), "TestTable", "1", "Alice", 25).await;
        put_test_item(db.as_ref(), "TestTable", "2", "Bob", 30).await;

        let payload = json!({
            "TableName": "TestTable"
        });

        let request: storage_types::ScanRequest = payload.try_into().unwrap();
        let result = handle_scan(db.clone(), request).await.unwrap();

        if let Response::Scan(response) = result {
            assert_eq!(response.consumed_capacity, None);
        } else {
            panic!("Expected Scan response");
        }
    }
}

#[tokio::test]
async fn scan_consumed_capacity_total() {
    for backend in default_conformance_backends() {
        let db = backend.create_db().await;
        create_test_table(db.as_ref(), "TestTable").await;

        // Add test items
        put_test_item(db.as_ref(), "TestTable", "1", "Alice", 25).await;
        put_test_item(db.as_ref(), "TestTable", "2", "Bob", 30).await;

        let payload = json!({
            "TableName": "TestTable",
            "ReturnConsumedCapacity": "TOTAL"
        });

        let request: storage_types::ScanRequest = payload.try_into().unwrap();
        let result = handle_scan(db.clone(), request).await.unwrap();

        if let Response::Scan(response) = result {
            assert!(response.consumed_capacity.is_some());
            let consumed_capacity = response.consumed_capacity.unwrap();

            assert_eq!(consumed_capacity.table_name.to_string(), "TestTable");
            assert_eq!(consumed_capacity.capacity_units, 0.0);
        } else {
            panic!("Expected Scan response");
        }
    }
}

#[tokio::test]
async fn given_partial_gsi_projection_when_scan_projects_missing_attributes_then_request_is_rejected()
 {
    for backend in default_conformance_backends() {
        for projection in [
            json!({"ProjectionType": "KEYS_ONLY"}),
            json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": ["included"]}),
        ] {
            let db = backend.create_db().await;
            let table = "ScanProjectionBoundary";
            let create: CreateTableRequest = serde_json::from_value(json!({
                "TableName": table,
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "gpk", "AttributeType": "S"}
                ],
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{
                    "IndexName": "ByGroup",
                    "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                    "Projection": projection
                }]
            }))
            .expect("create request");
            db.create_table(&create).await.expect("create GSI table");
            for selection in [
                json!({"ProjectionExpression": "#missing", "ExpressionAttributeNames": {"#missing": "private_note"}}),
                json!({"AttributesToGet": ["private_note"]}),
            ] {
                let mut request = json!({"TableName": table, "IndexName": "ByGroup"});
                request
                    .as_object_mut()
                    .unwrap()
                    .extend(selection.as_object().unwrap().clone());
                let error = handle_scan(db.clone(), request.try_into().expect("scan request"))
                    .await
                    .expect_err("GSI scan must reject attributes outside its stored projection");
                assert_eq!(error.status_code, 400, "{}", backend.name);
                assert!(
                    error.message.contains("does not project [private_note]"),
                    "{error:?}"
                );
            }
            let error = handle_scan(
                db.clone(),
                json!({"TableName": table, "IndexName": "ByGroup", "Select": "ALL_ATTRIBUTES"})
                    .try_into()
                    .expect("scan request"),
            )
            .await
            .expect_err("Select ALL_ATTRIBUTES needs an ALL projection");
            assert_eq!(error.status_code, 400, "{}", backend.name);
            assert_eq!(
                error.message,
                "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not \
                 supported for global secondary index ByGroup because its projection type is not \
                 ALL"
            );
            // AWS (probe 2026-11-13) evaluates a Scan filter against the
            // projected index item: an unprojected attribute is missing.
            for filter in [
                json!({"FilterExpression": "#missing = :wanted", "ExpressionAttributeNames": {"#missing": "private_note"}, "ExpressionAttributeValues": {":wanted": {"S": "hidden"}}}),
                json!({"FilterExpression": "attribute_exists(gpk) OR attribute_exists(#missing.child[0])", "ExpressionAttributeNames": {"#missing": "private_note"}}),
            ] {
                let mut request = json!({"TableName": table, "IndexName": "ByGroup"});
                request
                    .as_object_mut()
                    .unwrap()
                    .extend(filter.as_object().unwrap().clone());
                let response = handle_scan(db.clone(), request.try_into().expect("scan request"))
                    .await
                    .expect("GSI scan accepts a filter on an unprojected attribute");
                assert!(matches!(response, Response::Scan(_)), "{}", backend.name);
            }
            let allowed = json!({
                "TableName": table, "IndexName": "ByGroup", "ProjectionExpression": "id, gpk", "FilterExpression": "attribute_exists(gpk)"
            });
            let response = handle_scan(db, allowed.try_into().expect("allowed scan"))
                .await
                .expect("stored keys remain selectable");
            assert!(matches!(response, Response::Scan(_)));
        }
    }
}
