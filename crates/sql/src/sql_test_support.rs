use std::{fs, path::PathBuf};

use storage_types::{
    AttributeDefinition, BillingMode, CreateGlobalSecondaryIndex, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, ReadSequenceRequest,
    TableName,
};
use tempfile::{Builder, TempDir};

fn test_data_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let path = workspace_root.join("run-artifacts/sql-data");
    fs::create_dir_all(&path).expect("create sql test data directory");
    path
}

pub(crate) fn temp_dir(prefix: &str) -> TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in(test_data_dir())
        .expect("create sql test directory")
}

pub(crate) fn mapped_gsi_table_request_with_projection(
    table_name: TableName,
    projection_type: ProjectionType,
) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
        table_name,
        vec![
            attribute_definition("pk"),
            attribute_definition("sk"),
            attribute_definition("gpk"),
            attribute_definition("gsk"),
        ],
        vec![
            key_schema("pk", KeyType::Hash),
            key_schema("sk", KeyType::Range),
        ],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: storage_types::IndexName::new("by_group"),
        key_schema: vec![
            key_schema("gpk", KeyType::Hash),
            key_schema("gsk", KeyType::Range),
        ],
        projection: Projection {
            projection_type: Some(projection_type),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    request.max_indexers = storage_types::MaxIndexers::try_new(1).expect("indexer capacity");
    request
}

pub(crate) fn mapped_gsi_read_sequence_request(
    parents: &TableName,
    children: &TableName,
) -> ReadSequenceRequest {
    mapped_gsi_read_sequence_request_with_cardinality(parents, children, true)
}

pub(crate) fn mapped_gsi_read_sequence_one_request(
    parents: &TableName,
    children: &TableName,
) -> ReadSequenceRequest {
    mapped_gsi_read_sequence_request_with_cardinality(parents, children, false)
}

fn mapped_gsi_read_sequence_request_with_cardinality(
    parents: &TableName,
    children: &TableName,
    iterates: bool,
) -> ReadSequenceRequest {
    let select = if iterates {
        "$.Query.Items[*].customer_id"
    } else {
        "$.Query.Items[0].customer_id"
    };
    let mut child = serde_json::json!({
        "Name": "children", "Operation": {"Get": {
            "TableName": children,
            "Key": {
                "pk": {"S": "group"},
                "sk": {"FromInput": "customer"}
            }
        }}, "Inputs": {
            "customer": {
                "From": {"Node": "parents", "Select": select},
                "MappedKeySource": {"AttributeName": "customer_id", "Indexer": 0},
                "Cardinality": if iterates { "MANY" } else { "ONE" },
                "OnMissing": if iterates { "SKIP" } else { "ERROR" }
            }
        }
    });
    if iterates {
        child["Iterate"] = serde_json::Value::String("customer".to_string());
    }
    serde_json::from_value(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": parents,
                "IndexName": "by_group",
                "KeyConditionExpression": "gpk = :gpk",
                "ExpressionAttributeValues": {":gpk": {"S": "segment"}}
            }}},
            child
        ]
    }))
    .expect("mapped GSI request")
}

pub(crate) fn mapped_gsi_parent_item(
    sort: &str,
    customer: Option<&str>,
) -> std::collections::HashMap<String, storage_types::AttributeValue> {
    let mut item = std::collections::HashMap::from([
        (
            "pk".to_string(),
            storage_types::AttributeValue::S("group".to_string()),
        ),
        (
            "sk".to_string(),
            storage_types::AttributeValue::S(sort.to_string()),
        ),
        (
            "gpk".to_string(),
            storage_types::AttributeValue::S("segment".to_string()),
        ),
        (
            "gsk".to_string(),
            storage_types::AttributeValue::S(sort.to_string()),
        ),
    ]);
    if let Some(customer) = customer {
        item.insert(
            "customer_id".to_string(),
            storage_types::AttributeValue::S(customer.to_string()),
        );
    }
    item
}

fn attribute_definition(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: KeyAttributeType::S,
    }
}

fn key_schema(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}
