//! A deleted table cannot consume a neighboring table's history namespace.

use serde_json::json;
use storage_types::{DeleteTableRequest, PutItemRequest, TableName};

use super::{
    cdc_stream_tests::{create_named_table, system_records},
    image_retention_tests::table_records,
    test_support::{TestStore, unique_table_name},
};

#[tokio::test]
async fn given_neighboring_stream_table_names_when_one_is_deleted_then_other_stream_remains_writable()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let base = unique_table_name();
        let deleted = TableName::new(&format!("{base}_one"));
        let live = TableName::new(&format!("{base}Xone"));
        for table in [&deleted, &live] {
            create_named_table(&store, table.clone()).await?;
            store
                .operations
                .put_item(
                    PutItemRequest::try_from(json!({
                        "TableName": table, "Item": {"pk": {"S": "first"}}
                    }))
                    .unwrap(),
                )
                .await?;
        }
        anyhow::ensure!(system_records(&store, 10).await?.len() == 2);
        store
            .operations
            .delete_table(DeleteTableRequest {
                table_name: deleted,
            })
            .await?;
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": live, "Item": {"pk": {"S": "second"}}
                }))
                .unwrap(),
            )
            .await?;
        let records = table_records(&store, &live).await?;
        anyhow::ensure!(records.len() == 2);
        for (record, key) in records.iter().zip(["first", "second"]) {
            anyhow::ensure!(serde_json::to_value(&record.item)? == json!({"pk": {"S": key}}));
        }
        let system = system_records(&store, 1).await?;
        anyhow::ensure!(system.len() == 2);
        anyhow::ensure!(
            system
                .iter()
                .all(|row| row.source_table_name.as_ref() == Some(&live))
        );
        Ok(())
    }
    .await;
    store.finish().await;
    result
}
