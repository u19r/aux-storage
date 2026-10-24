//! Parent-to-child ReadSequence mapping through replacement providers.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, KeyAttributeType, PutItemRequest, ReadSequenceRequest};

use super::test_support::{TestStore, unique_table_name};

// Business rule from the legacy FoundationDB mapped-range test: a child Get
// combines a mapped partition key with its fixed sort key.
#[tokio::test]
async fn given_lookup_row_when_read_sequence_gets_static_sort_child_then_only_meta_item_returns() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store
            .create_hash_range_table(table.clone(), KeyAttributeType::S)
            .await;
        let lookup = json!({
            "pk": {"S": "UPI#+441234567890"},
            "sk": {"S": "U#user-1"}
        });
        let user = json!({
            "pk": {"S": "U#user-1"},
            "sk": {"S": "META"},
            "payload": {"S": "mapped-user"}
        });
        let decoy = json!({
            "pk": {"S": "U#user-1"},
            "sk": {"S": "OTHER"},
            "payload": {"S": "wrong-user"}
        });
        for item in [&lookup, &user, &decoy] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let request = ReadSequenceRequest::try_from(json!({
            "Nodes": [
                {"Name": "lookup", "Operation": {"Query": {
                    "TableName": table.as_ref(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "UPI#+441234567890"}}
                }}},
                {"Name": "user", "Operation": {"Get": {
                    "TableName": table.as_ref(),
                    "Key": {
                        "pk": {"FromInput": "user_pk"},
                        "sk": {"S": "META"}
                    }
                }}, "Inputs": {"user_pk": {
                    "From": {"Node": "lookup", "Select": "$.Query.Items[*].sk"},
                    "Cardinality": "MANY",
                    "OnMissing": "SKIP"
                }}, "Iterate": "user_pk"}
            ],
            "Outputs": ["lookup", "user"]
        }))
        .unwrap();
        let observed = observe(store.operations.read_sequence(request)).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with("dynamo_operation", &[("operation", "ReadSequence")]);
        assert_eq!(span.fields.len(), 1);
        assert_eq!(
            serde_json::to_value(observed.result.unwrap()).unwrap(),
            json!({
                "ReadConsistency": "EVENTUAL",
                "Partial": false,
                "Nodes": [
                    {"Name": "lookup", "Invocations": [{
                        "Ordinal": 0,
                        "InputRefs": {},
                        "Result": {"Query": {"Items": [lookup], "Count": 1, "ScannedCount": 1}}
                    }]},
                    {"Name": "user", "Invocations": [{
                        "Ordinal": 0,
                        "InputRefs": {"user_pk": {
                            "Node": "lookup", "InvocationOrdinal": 0, "ItemOrdinal": 0
                        }},
                        "Result": {"Get": {"Item": user}}
                    }]}
                ]
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule from the legacy FoundationDB mapped-range test: a dependent
// Get composes its key from two parent fields even when declared first.
#[tokio::test]
async fn given_parent_fields_when_read_sequence_builds_child_key_then_template_and_graph_order_match()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let source = store.create_standard_table().await;
        let target = store.create_standard_table().await;
        let parent = json!({
            "pk": {"S": "account-1"},
            "entity_id": {"S": "account-1"},
            "sub_id": {"S": "a"}
        });
        let child = json!({
            "pk": {"S": "entity#account-1#sub_model#a#v1"},
            "payload": {"S": "related"}
        });
        for (table, item) in [(&source, &parent), (&target, &child)] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": item
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let request = ReadSequenceRequest::try_from(json!({
            "Nodes": [
                {"Name": "child", "Operation": {"Get": {
                    "TableName": target.as_ref(),
                    "Key": {"pk": {"StringTemplate": "entity#{entity_id}#sub_model#{sub_id}#v1"}}
                }}, "Inputs": {
                    "entity_id": {
                        "From": {"Node": "parent", "Select": "$.Query.Items[0].entity_id"},
                        "Cardinality": "ONE",
                        "OnMissing": "ERROR"
                    },
                    "sub_id": {
                        "From": {"Node": "parent", "Select": "$.Query.Items[*].sub_id"},
                        "Cardinality": "MANY",
                        "OnMissing": "SKIP"
                    }
                }, "Iterate": "sub_id"},
                {"Name": "parent", "Operation": {"Query": {
                    "TableName": source.as_ref(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "account-1"}}
                }}}
            ],
            "Outputs": ["parent", "child"]
        }))
        .unwrap();
        let observed = observe(store.operations.read_sequence(request)).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with(
            "dynamo_operation",
            &[("operation", "ReadSequence"), ],
        );
        assert_eq!(span.fields.len(), 1);
        assert_eq!(serde_json::to_value(observed.result.unwrap()).unwrap(), json!({
            "ReadConsistency": "EVENTUAL",
            "Partial": false,
            "Nodes": [
                {"Name": "child", "Invocations": [{
                    "Ordinal": 0,
                    "InputRefs": {
                        "entity_id": {"Node": "parent", "InvocationOrdinal": 0, "ItemOrdinal": 0},
                        "sub_id": {"Node": "parent", "InvocationOrdinal": 0, "ItemOrdinal": 0}
                    },
                    "Result": {"Get": {"Item": child}}
                }]},
                {"Name": "parent", "Invocations": [{
                    "Ordinal": 0,
                    "InputRefs": {},
                    "Result": {"Query": {"Items": [parent], "Count": 1, "ScannedCount": 1}}
                }]}
            ]
        }));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and format rules from the ReadSequence Cucumber matrix: mapped
// indexers preserve parent values, missing values, and ordinal compatibility.
#[tokio::test]
async fn given_indexed_parents_when_read_sequence_maps_child_keys_then_each_child_result_matches() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let parents = unique_table_name();
        let children = unique_table_name();
        let parent_table = CreateTableRequest::try_from(json!({
            "TableName": parents.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 2
        }))
        .unwrap();
        store.operations.create_table(parent_table).await.unwrap();
        let child_table = CreateTableRequest::try_from(json!({
            "TableName": children.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap();
        store.operations.create_table(child_table).await.unwrap();
        let mut table_names = [parents.as_ref(), children.as_ref()];
        table_names.sort_unstable();
        let _table_names = table_names.join(",");

        let child = json!({"id": {"S": "child#1"}, "value": {"S": "related"}});
        let put = PutItemRequest::try_from(json!({
            "TableName": children.as_ref(), "Item": child
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        for (parent_key, related_id, indexers, ordinal, expected_child) in [
            (
                "present",
                Some("child#1"),
                json!(["related_id"]),
                0,
                Some("child#1"),
            ),
            (
                "missing-child",
                Some("absent"),
                json!(["related_id"]),
                0,
                None,
            ),
            ("missing-value", None, json!(["related_id"]), 0, None),
            (
                "one-based",
                Some("child#1"),
                json!(["related_id"]),
                1,
                Some("child#1"),
            ),
            (
                "fallback",
                Some("child#1"),
                json!(["other"]),
                0,
                Some("child#1"),
            ),
        ] {
            let mut item = json!({"pk": {"S": parent_key}});
            if let Some(related_id) = related_id {
                item["related_id"] = json!({"S": related_id});
            }
            if parent_key == "fallback" {
                item["other"] = json!({"S": "unrelated"});
            }
            let put = PutItemRequest::try_from(json!({
                "TableName": parents.as_ref(), "Item": item, "Indexers": indexers
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();

            let request = ReadSequenceRequest::try_from(json!({
                "ReadConsistency": "EVENTUAL",
                "Nodes": [
                    {"Name": "parents", "Operation": {"Query": {
                        "TableName": parents.as_ref(),
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {":pk": {"S": parent_key}}
                    }}},
                    {"Name": "children", "Operation": {"Get": {
                        "TableName": children.as_ref(),
                        "Key": {"id": {"FromInput": "related"}}
                    }}, "Inputs": {"related": {
                        "From": {"Node": "parents", "Select": "$.Query.Items[*].related_id"},
                        "MappedKeySource": {"AttributeName": "related_id", "Indexer": ordinal},
                        "Cardinality": "MANY",
                        "OnMissing": "SKIP"
                    }}, "Iterate": "related"}
                ],
                "Outputs": ["parents", "children"]
            }))
            .unwrap();
            let observed = observe(store.operations.read_sequence(request)).await;
            observed.assert_no_warning_or_error();
            let span = observed.one_span_with("dynamo_operation", &[("operation", "ReadSequence")]);
            assert_eq!(span.fields.len(), 1);
            let response = serde_json::to_value(observed.result.unwrap()).unwrap();
            assert_eq!(response["Partial"], false, "{parent_key}");
            assert_eq!(response["Nodes"][0]["Name"], "parents", "{parent_key}");
            assert_eq!(
                response["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Items"],
                json!([item]),
                "{parent_key}"
            );
            assert_eq!(response["Nodes"][1]["Name"], "children", "{parent_key}");
            let invocations = response["Nodes"][1]["Invocations"].as_array().unwrap();
            assert_eq!(
                invocations.len(),
                usize::from(related_id.is_some()),
                "{parent_key}"
            );
            if related_id.is_some() {
                let expected = expected_child.map_or_else(
                    || json!({}),
                    |id| {
                        json!({
                            "Item": {"id": {"S": id}, "value": {"S": "related"}}
                        })
                    },
                );
                assert_eq!(invocations[0]["Result"]["Get"], expected, "{parent_key}");
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule from legacy SQLite mapped reads: duplicate parent values bind
// one child invocation to the first matching parent row.
#[tokio::test]
async fn given_duplicate_parent_keys_when_read_sequence_maps_child_then_one_child_keeps_first_reference()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let parents = unique_table_name();
        let children = unique_table_name();
        let parent_table = CreateTableRequest::try_from(json!({
            "TableName": parents.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "MaxIndexers": 1
        }))
        .unwrap();
        store.operations.create_table(parent_table).await.unwrap();
        store
            .create_hash_range_table(children.clone(), KeyAttributeType::S)
            .await;

        for sort in ["a", "b"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": parents.as_ref(),
                "Item": {
                    "pk": {"S": "group"},
                    "sk": {"S": sort},
                    "customer_id": {"S": "customer-1"}
                },
                "Indexers": ["customer_id"]
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let child = json!({
            "pk": {"S": "group"},
            "sk": {"S": "customer-1"},
            "payload": {"S": "related"}
        });
        let put = PutItemRequest::try_from(json!({
            "TableName": children.as_ref(), "Item": child
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        let request = ReadSequenceRequest::try_from(json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [
                {"Name": "parents", "Operation": {"Query": {
                    "TableName": parents.as_ref(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "group"}}
                }}},
                {"Name": "children", "Operation": {"Get": {
                    "TableName": children.as_ref(),
                    "Key": {
                        "pk": {"S": "group"},
                        "sk": {"FromInput": "customer"}
                    }
                }}, "Inputs": {"customer": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].customer_id"},
                    "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
                    "Cardinality": "MANY",
                    "OnMissing": "SKIP"
                }}, "Iterate": "customer"}
            ],
            "Outputs": ["parents", "children"]
        }))
        .unwrap();
        let observed = observe(store.operations.read_sequence(request)).await;
        observed.assert_no_warning_or_error();
        let body = serde_json::to_value(observed.result.unwrap()).unwrap();
        assert_eq!(body["Partial"], false);
        assert_eq!(
            body["Nodes"][0]["Invocations"][0]["Result"]["Query"]["Count"],
            2
        );
        let children = body["Nodes"][1]["Invocations"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["Result"]["Get"], json!({"Item": child}));
        assert_eq!(
            children[0]["InputRefs"]["customer"],
            json!({
                "Node": "parents", "InvocationOrdinal": 0, "ItemOrdinal": 0
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
