//! Duplicate/canonical numeric keys, malformed keys, missing tables,
//! reserved-word projections, and simultaneous errors preserve the exact
//! manager-level variants and reason positions.

use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use storage_types::{
    AttributeValue, CreateTableRequest, KeyAttributes, StorageEnum, TableName, TransactGetItem,
    TransactGetItemsRequest, TransactGetRequest, context::WrappedError as _,
};

use super::test_support::TestStore;

fn create_composite_table_request(name: &str) -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new(name),
        vec![
            storage_types::AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: storage_types::KeyAttributeType::S,
            },
            storage_types::AttributeDefinition {
                attribute_name: "sk".into(),
                attribute_type: storage_types::KeyAttributeType::S,
            },
        ],
        vec![
            storage_types::KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: storage_types::KeyType::Hash,
            },
            storage_types::KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: storage_types::KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
}

fn composite_key(pk: &str, sk: &str) -> KeyAttributes {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
    ])
    .into()
}

#[tokio::test]
async fn given_invalid_transact_get_requests_when_executed_then_error_precedence_is_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let manager = &store.operations;
        manager
            .create_table(create_composite_table_request("tg_priority"))
            .await
            .expect("create priority table: failed");

        let duplicate = TransactGetItemsRequest {
            transact_items: vec![
                transact_get("tg_priority", composite_key("p", "s"), None, None),
                transact_get("tg_priority", composite_key("p", "s"), None, None),
            ],
            return_consumed_capacity: None,
        };
        let error = manager
            .transact_get_items(duplicate)
            .await
            .expect_err("duplicate keys must fail");
        assert!(
            matches!(
                error.to_enum(),
                StorageEnum::Validation { message, .. }
                if message
                    == "Transaction request cannot include multiple operations on one item"
            ),
            "duplicate keys changed variant: {error:?}"
        );

        let arn_duplicate = TransactGetItemsRequest {
            transact_items: vec![
                transact_get("tg_priority", composite_key("p", "s"), None, None),
                transact_get(
                    "arn:aws:dynamodb:eu-central-1:123456789012:table/tg_priority",
                    composite_key("p", "s"),
                    None,
                    None,
                ),
            ],
            return_consumed_capacity: None,
        };
        let error = manager
            .transact_get_items(arn_duplicate)
            .await
            .expect_err("ARN/plain duplicates must fail");
        assert!(
            matches!(
                error.to_enum(),
                StorageEnum::Validation { message, .. }
                if message
                    == "Transaction request cannot include multiple operations on one item"
            ),
            "ARN duplicate changed variant: {error:?}"
        );

        manager
            .create_table(CreateTableRequest::new(
                TableName::new("tg_numeric"),
                vec![storage_types::AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: storage_types::KeyAttributeType::N,
                }],
                vec![storage_types::KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: storage_types::KeyType::Hash,
                }],
                storage_types::BillingMode::PayPerRequest,
            ))
            .await
            .expect("create numeric table: failed");
        let numeric_key = |raw: &str| TransactGetItem {
            get: TransactGetRequest {
                table_name: TableName::new("tg_numeric"),
                key: HashMap::from([("pk".to_string(), AttributeValue::N(raw.to_string()))]).into(),
                projection_expression: None,
                expression_attribute_names: None,
            },
        };
        let error = manager
            .transact_get_items(TransactGetItemsRequest {
                transact_items: vec![numeric_key("1"), numeric_key("1.0")],
                return_consumed_capacity: None,
            })
            .await
            .expect_err("canonical numeric duplicates must fail");
        assert!(
            matches!(
                error.to_enum(),
                StorageEnum::Validation { message, .. }
                if message
                    == "Transaction request cannot include multiple operations on one item"
            ),
            "canonical numeric duplicate changed variant: {error:?}"
        );

        let malformed = |key: KeyAttributes| TransactGetItemsRequest {
            transact_items: vec![
                transact_get("tg_priority", composite_key("p", "s"), None, None),
                transact_get("tg_priority", composite_key("p", "s"), None, None),
                transact_get("tg_priority", key, None, None),
            ],
            return_consumed_capacity: None,
        };
        let bad_key: KeyAttributes = HashMap::from([
            ("pk".to_string(), AttributeValue::N("1".to_string())),
            ("sk".to_string(), AttributeValue::S("s".to_string())),
        ])
        .into();
        let error = manager
            .transact_get_items(malformed(bad_key))
            .await
            .expect_err("malformed trailing key must cancel");
        assert!(
            matches!(
                error.to_enum(),
                StorageEnum::TransactionCanceled { reasons }
                if reasons.as_slice()
                    == [
                        "None".to_string(),
                        "None".to_string(),
                        "ValidationError\tThe provided key element does not match the schema"
                            .to_string(),
                    ]
            ),
            "trailing key reasons changed: {error:?}"
        );

        let leading = TransactGetItemsRequest {
            transact_items: vec![
                transact_get(
                    "tg_priority",
                    HashMap::from([
                        ("pk".to_string(), AttributeValue::N("1".to_string())),
                        ("sk".to_string(), AttributeValue::S("s".to_string())),
                    ])
                    .into(),
                    None,
                    None,
                ),
                transact_get("tg_priority", composite_key("p", "s"), None, None),
                transact_get("tg_priority", composite_key("p", "s"), None, None),
            ],
            return_consumed_capacity: None,
        };
        let error = manager
            .transact_get_items(leading)
            .await
            .expect_err("malformed leading key must cancel");
        assert!(
            matches!(
                error.to_enum(),
                StorageEnum::TransactionCanceled { reasons }
                if reasons.as_slice()
                    == [
                        "ValidationError\tThe provided key element does not match the schema"
                            .to_string(),
                        "None".to_string(),
                        "None".to_string(),
                    ]
            ),
            "leading key reasons changed: {error:?}"
        );

        for (label, request) in [
            (
                "missing-first",
                TransactGetItemsRequest {
                    transact_items: vec![
                        transact_get("tg_missing_priority", composite_key("p", "s"), None, None),
                        transact_get("tg_priority", composite_key("p", "s"), None, None),
                        transact_get("tg_priority", composite_key("p", "s"), None, None),
                    ],
                    return_consumed_capacity: None,
                },
            ),
            (
                "missing-last",
                TransactGetItemsRequest {
                    transact_items: vec![
                        transact_get("tg_priority", composite_key("p", "s"), None, None),
                        transact_get("tg_priority", composite_key("p", "s"), None, None),
                        transact_get("tg_missing_priority", composite_key("p", "s"), None, None),
                    ],
                    return_consumed_capacity: None,
                },
            ),
            (
                "missing-with-malformed-key",
                TransactGetItemsRequest {
                    transact_items: vec![
                        transact_get(
                            "tg_priority",
                            HashMap::from([
                                ("pk".to_string(), AttributeValue::N("1".to_string())),
                                ("sk".to_string(), AttributeValue::S("s".to_string())),
                            ])
                            .into(),
                            None,
                            None,
                        ),
                        transact_get("tg_missing_priority", composite_key("p", "s"), None, None),
                    ],
                    return_consumed_capacity: None,
                },
            ),
        ] {
            let error = manager
                .transact_get_items(request)
                .await
                .expect_err("missing table must fail closed");
            assert!(
                matches!(error.to_enum(), StorageEnum::ResourceNotFound { .. }),
                "{label} changed variant: {error:?}"
            );
        }

        for (label, projection_expression, expression_attribute_names, expected) in [
            (
                "reserved keyword",
                Some("COMMENT"),
                None,
                "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved \
                 keyword: COMMENT",
            ),
            (
                "alias without sigil",
                Some("#ok"),
                Some(HashMap::from([("ok".to_string(), "ok".to_string())])),
                "ExpressionAttributeNames contains invalid key: Syntax error; key: \"ok\"",
            ),
            (
                "unused alias",
                Some("#ok"),
                Some(HashMap::from([
                    ("#ok".to_string(), "ok".to_string()),
                    ("#unused".to_string(), "COMMENT".to_string()),
                ])),
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}",
            ),
            (
                "undefined alias",
                Some("#missing"),
                None,
                "Invalid ProjectionExpression: An expression attribute name used in the document \
                 path is not defined; attribute name: #missing",
            ),
        ] {
            let error = manager
                .transact_get_items(TransactGetItemsRequest {
                    transact_items: vec![transact_get(
                        "tg_priority",
                        composite_key("p", "s"),
                        projection_expression,
                        expression_attribute_names,
                    )],
                    return_consumed_capacity: None,
                })
                .await
                .expect_err("invalid projection must fail pure validation");
            // Request validation owns the wire message; HTTP mapping must
            // preserve it rather than add a second validation prefix.
            let response = http_error::HttpApiError::from(error);
            assert_eq!(response.status_code, 400, "{label}");
            assert_eq!(
                response.error_type, "com.amazon.coral.validate#ValidationException",
                "{label}"
            );
            assert_eq!(response.message, expected, "{label}");
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

fn transact_get(
    table: &str,
    key: KeyAttributes,
    projection_expression: Option<&str>,
    expression_attribute_names: Option<HashMap<String, String>>,
) -> TransactGetItem {
    TransactGetItem {
        get: TransactGetRequest {
            table_name: TableName::new(table),
            key,
            projection_expression: projection_expression.map(str::to_string),
            expression_attribute_names: expression_attribute_names.map(Into::into),
        },
    }
}
