//! ReadSequence root ordering and dependent pagination through replacement
//! providers.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{AttributeMap, AttributeValue, PutItemRequest, ReadSequenceRequest};

use crate::test_support::{TestStore, unique_table_name};

// Business rule from legacy SQLite and Postgres ReadSequence tests: independent
// roots retain response order and a missing Get does not remove its invocation.
#[tokio::test]
async fn given_independent_get_and_batch_roots_when_read_sequence_runs_then_order_and_missing_results_are_preserved()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        for key in ["a", "b"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let request = ReadSequenceRequest::try_from(json!({
            "ReadConsistency": "EVENTUAL",
            "Nodes": [
                {"Name": "first", "Operation": {"Get": {
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "a"}}
                }}},
                {"Name": "missing", "Operation": {"Get": {
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "absent"}}
                }}},
                {"Name": "batch", "Operation": {"BatchGet": {
                    "RequestItems": {(table.as_ref()): {"Keys": [
                        {"pk": {"S": "b"}},
                        {"pk": {"S": "absent"}}
                    ]}}
                }}}
            ],
            "Outputs": ["first", "missing", "batch"]
        }))
        .unwrap();
        let observed = observe(store.operations.read_sequence(request)).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with("dynamo_operation", &[("operation", "ReadSequence")]);
        assert_eq!(span.fields.len(), 1);
        let response = observed.result.unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "ReadConsistency": "EVENTUAL",
                "Partial": false,
                "Nodes": [
                    {"Name": "first", "Invocations": [{
                        "Ordinal": 0,
                        "InputRefs": {},
                        "Result": {"Get": {"Item": {"pk": {"S": "a"}}}}
                    }]},
                    {"Name": "missing", "Invocations": [{
                        "Ordinal": 0,
                        "InputRefs": {},
                        "Result": {"Get": {}}
                    }]},
                    {"Name": "batch", "Invocations": [{
                        "Ordinal": 0,
                        "InputRefs": {},
                        "Result": {"BatchGet": {"Responses": {(table.as_ref()): [
                            {"pk": {"S": "b"}}
                        ]}}}
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

#[tokio::test]
async fn given_paged_parent_when_read_sequence_resumes_then_each_page_runs_its_children() {
    let store = TestStore::new().await;
    let parents = unique_table_name();
    let children = unique_table_name();
    store
        .create_hash_range_table(parents.clone(), storage_types::KeyAttributeType::S)
        .await;
    store.create_hash_table(children.clone()).await;

    for (sort, child) in [("a", "child-a"), ("b", "child-b")] {
        store
            .operations
            .put_item(PutItemRequest::new(
                parents.clone(),
                storage_types::WireItem::try_from(&AttributeMap::from_iter([
                    ("pk".to_owned(), AttributeValue::S("group".to_owned())),
                    ("sk".to_owned(), AttributeValue::S(sort.to_owned())),
                    ("related_id".to_owned(), AttributeValue::S(child.to_owned())),
                ]))
                .unwrap(),
            ))
            .await
            .unwrap();
        store
            .operations
            .put_item(PutItemRequest::new(
                children.clone(),
                storage_types::WireItem::try_from(&AttributeMap::from_iter([(
                    "pk".to_owned(),
                    AttributeValue::S(child.to_owned()),
                )]))
                .unwrap(),
            ))
            .await
            .unwrap();
    }

    let request = json!({
        "Nodes": [
            {
                "Name": "parents",
                "Operation": {"Query": {
                    "TableName": parents.to_string(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "group"}},
                    "Limit": 1
                }}
            },
            {
                "Name": "children",
                "Operation": {"Get": {
                    "TableName": children.to_string(),
                    "Key": {"pk": {"FromInput": "related"}}
                }},
                "Inputs": {"related": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].related_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "SKIP"
                }},
                "Iterate": "related"
            },
            {
                "Name": "independent",
                "Operation": {"Query": {
                    "TableName": parents.to_string(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "group"}},
                    "Limit": 1
                }}
            }
        ],
        "Outputs": ["parents", "children", "independent"]
    });
    let mut token: Option<String> = None;
    let mut child_invocations = 0;
    let mut independent_items = 0;
    for page_index in 0..5 {
        let mut page = request.clone();
        if let Some(previous) = token.take() {
            page["NextSequenceToken"] = json!(previous);
        }
        let parsed = ReadSequenceRequest::try_from(page).unwrap();
        let response = store
            .operations
            .read_sequence(parsed)
            .await
            .unwrap_or_else(|error| panic!("read sequence page {page_index}: {error}"));
        let body = serde_json::to_value(&response).unwrap();
        child_invocations += body["Nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|node| node["Name"] == "children")
            .map(|node| node["Invocations"].as_array().unwrap().len())
            .sum::<usize>();
        independent_items += body["Nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|node| node["Name"] == "independent")
            .flat_map(|node| node["Invocations"].as_array().unwrap())
            .map(|invocation| invocation["Result"]["Query"]["Count"].as_u64().unwrap() as usize)
            .sum::<usize>();
        token = response.next_sequence_token;
        if token.is_none() {
            break;
        }
    }
    assert!(token.is_none(), "read sequence did not finish");
    assert_eq!(child_invocations, 2, "each parent row must bind one child");
    assert_eq!(
        independent_items, 2,
        "independent query must resume after the parent"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_nested_queries_when_child_paginates_then_parent_page_replays() {
    let store = TestStore::new().await;
    let parents = unique_table_name();
    let children = unique_table_name();
    let grandchildren = unique_table_name();
    store
        .create_hash_range_table(parents.clone(), storage_types::KeyAttributeType::S)
        .await;
    store
        .create_hash_range_table(children.clone(), storage_types::KeyAttributeType::S)
        .await;
    store.create_hash_table(grandchildren.clone()).await;

    for (sort, child) in [("a", "child-a"), ("b", "child-b")] {
        store
            .operations
            .put_item(PutItemRequest::new(
                parents.clone(),
                storage_types::WireItem::try_from(&AttributeMap::from_iter([
                    ("pk".to_owned(), AttributeValue::S("group".to_owned())),
                    ("sk".to_owned(), AttributeValue::S(sort.to_owned())),
                    ("related_id".to_owned(), AttributeValue::S(child.to_owned())),
                ]))
                .unwrap(),
            ))
            .await
            .unwrap();
        for child_sort in ["1", "2"] {
            let grandchild = format!("{child}-{child_sort}");
            store
                .operations
                .put_item(PutItemRequest::new(
                    children.clone(),
                    storage_types::WireItem::try_from(&AttributeMap::from_iter([
                        ("pk".to_owned(), AttributeValue::S(child.to_owned())),
                        ("sk".to_owned(), AttributeValue::S(child_sort.to_owned())),
                        ("grand_id".to_owned(), AttributeValue::S(grandchild.clone())),
                    ]))
                    .unwrap(),
                ))
                .await
                .unwrap();
            store
                .operations
                .put_item(PutItemRequest::new(
                    grandchildren.clone(),
                    storage_types::WireItem::try_from(&AttributeMap::from_iter([(
                        "pk".to_owned(),
                        AttributeValue::S(grandchild),
                    )]))
                    .unwrap(),
                ))
                .await
                .unwrap();
        }
    }

    let request = json!({
        "Nodes": [
            {
                "Name": "parents",
                "Operation": {"Query": {
                    "TableName": parents.to_string(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "group"}},
                    "Limit": 1
                }}
            },
            {
                "Name": "children",
                "Operation": {"Query": {
                    "TableName": children.to_string(),
                    "KeyConditionExpression": "pk = :child",
                    "ExpressionAttributeValues": {":child": {"FromInput": "related"}},
                    "Limit": 1
                }},
                "Inputs": {"related": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].related_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "SKIP"
                }},
                "Iterate": "related"
            },
            {
                "Name": "grandchildren",
                "Operation": {"Get": {
                    "TableName": grandchildren.to_string(),
                    "Key": {"pk": {"FromInput": "grand"}}
                }},
                "Inputs": {"grand": {
                    "From": {"Node": "children", "Select": "$.Query.Items[*].grand_id"},
                    "Cardinality": "MANY",
                    "OnMissing": "SKIP"
                }},
                "Iterate": "grand"
            }
        ],
        "Outputs": ["parents", "children", "grandchildren"]
    });
    let mut token: Option<String> = None;
    let mut parent_items = 0;
    let mut child_items = 0;
    let mut grandchild_items = 0;
    for page_index in 0..10 {
        let mut page = request.clone();
        if let Some(previous) = token.take() {
            page["NextSequenceToken"] = json!(previous);
        }
        let parsed = ReadSequenceRequest::try_from(page).unwrap();
        let response = store
            .operations
            .read_sequence(parsed)
            .await
            .unwrap_or_else(|error| panic!("read sequence page {page_index}: {error}"));
        let body = serde_json::to_value(&response).unwrap();
        for node in body["Nodes"].as_array().unwrap() {
            match node["Name"].as_str().unwrap() {
                "parents" | "children" => {
                    let count = node["Invocations"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|invocation| {
                            invocation["Result"]["Query"]["Count"].as_u64().unwrap() as usize
                        })
                        .sum::<usize>();
                    if node["Name"] == "parents" {
                        parent_items += count;
                    } else {
                        child_items += count;
                    }
                }
                "grandchildren" => {
                    grandchild_items += node["Invocations"].as_array().unwrap().len()
                }
                name => panic!("unexpected node {name}"),
            }
        }
        token = response.next_sequence_token;
        if token.is_none() {
            break;
        }
    }
    assert!(token.is_none(), "read sequence did not finish");
    assert_eq!(parent_items, 2);
    assert_eq!(child_items, 4);
    assert_eq!(grandchild_items, 4);
    store.finish().await;
}
