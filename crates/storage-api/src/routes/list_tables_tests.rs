use serde_json::json;
use storage_types::TableName;

use crate::{
    routes::routes_tests::{for_each_conformance_backend, handle_create_table, handle_list_tables},
    types::Response,
};

#[tokio::test]
async fn list_tables_hides_internal_storage_tables() {
    for_each_conformance_backend(|_backend, db| async move {
        let create_table_payload = json!({
            "TableName": "zzz_user_table",
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        });

        let create_result =
            handle_create_table(db.clone(), create_table_payload.try_into().unwrap()).await;
        assert!(
            create_result.is_ok(),
            "CreateTable should succeed: {create_result:?}"
        );

        let list_payload = json!({
            "Limit": 1
        });

        let list_result = handle_list_tables(db.clone(), list_payload.try_into().unwrap()).await;
        assert!(
            list_result.is_ok(),
            "ListTables should succeed: {list_result:?}"
        );

        match list_result.unwrap() {
            Response::ListTables(response) => {
                assert_eq!(response.table_names.len(), 1, "Should return 1 table");
                assert_eq!(response.table_names[0], TableName::new("zzz_user_table"));
                assert!(
                    response
                        .table_names
                        .iter()
                        .all(|name| !matches!(name.as_ref(), "sys" | "ana" | "rep")),
                    "internal storage tables should be hidden from ListTables responses"
                );
            }
            other => panic!("Expected ListTables response, got: {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn list_tables_returns_alphabetical_order_for_unsorted_creation() {
    for_each_conformance_backend(|_backend, db| async move {
        for table_name in ["Users", "Orders", "Products"] {
            let create_table_payload = json!({
                "TableName": table_name,
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ]
            });

            let create_result =
                handle_create_table(db.clone(), create_table_payload.try_into().unwrap()).await;
            assert!(
                create_result.is_ok(),
                "CreateTable should succeed for {table_name}: {create_result:?}"
            );
        }

        let list_payload = json!({});
        let list_result = handle_list_tables(db, list_payload.try_into().unwrap()).await;
        assert!(
            list_result.is_ok(),
            "ListTables should succeed: {list_result:?}"
        );

        match list_result.unwrap() {
            Response::ListTables(response) => {
                assert_eq!(
                    response.table_names,
                    vec![
                        TableName::new("Orders"),
                        TableName::new("Products"),
                        TableName::new("Users")
                    ]
                );
            }
            other => panic!("Expected ListTables response, got: {other:?}"),
        }
    })
    .await;
}
