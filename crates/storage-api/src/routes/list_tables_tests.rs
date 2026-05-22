use serde_json::json;
use storage::{DatabaseManager, Tables};
use storage_types::TableName;

use crate::{
    routes::routes_support_tests::{create_test_db, handle_create_table, handle_list_tables},
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn list_tables_with_invalid_exclusive_start() {
    let db = create_test_db_manager().await;
    for table_name in ["TableA", "TableB", "TableC"] {
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
    let list_payload = json!({
        "ExclusiveStartTableName": "NonExistentTable"
    });

    let list_result = handle_list_tables(db.clone(), list_payload.try_into().unwrap()).await;
    assert!(
        list_result.is_ok(),
        "ListTables should succeed: {list_result:?}"
    );

    match list_result.unwrap() {
        Response::ListTables(response) => {
            assert_eq!(response.table_names.len(), 3, "Should return all 3 tables");
            assert!(response.table_names.contains(&TableName::new("TableA")));
            assert!(response.table_names.contains(&TableName::new("TableB")));
            assert!(response.table_names.contains(&TableName::new("TableC")));
            assert_eq!(
                response.table_names,
                vec![
                    TableName::new("TableA"),
                    TableName::new("TableB"),
                    TableName::new("TableC")
                ]
            );
            assert!(response.last_evaluated_table_name.is_none());
        }
        other => panic!("Expected ListTables response, got: {other:?}"),
    }
}

#[tokio::test]
async fn list_tables_rejects_limit_above_dynamodb_maximum() {
    let db = create_test_db_manager().await;
    let list_payload = json!({
        "Limit": 10000
    });

    let err = storage_types::ListTablesRequest::try_from(list_payload)
        .expect_err("ListTables should reject Limit > 100");
    assert_eq!(err, "Limit must be between 1 and 100");

    drop(db);
}

#[tokio::test]
async fn list_tables_hides_internal_storage_tables() {
    let db = create_test_db_manager().await;

    Tables::create_sys_jobs_table(&db)
        .await
        .expect("create sys_jobs table");
    Tables::create_sys_storage_replication_table(&db)
        .await
        .expect("create sys_storage_replication table");

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
                    .all(|name| !Tables::should_hide_from_list_tables(name)),
                "internal storage tables should be hidden from ListTables responses"
            );
        }
        other => panic!("Expected ListTables response, got: {other:?}"),
    }
}

#[tokio::test]
async fn list_tables_returns_alphabetical_order_for_unsorted_creation() {
    let db = create_test_db_manager().await;

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
}
