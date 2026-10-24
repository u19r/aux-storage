use storage_common::ttl::{TtlSweepPageInput, TtlSweepRequest};
use storage_types::{
    AttributeDefinition, KeyAttributeType, KeySchemaElement, KeyType, StorageEnum, StorageError,
    TableName, TimestampMillis, context::WrappedError,
};

use crate::DatabaseManager;

const SCAN_TIME: i64 = 1_700_000_000_000;

fn request(table_name: &str) -> TtlSweepRequest {
    let input =
        TtlSweepPageInput::try_new("ttl", TimestampMillis::from_timestamp(SCAN_TIME), 25, None)
            .expect("valid ttl sweep input");
    TtlSweepRequest {
        table_name: TableName::new(table_name),
        input,
    }
}

fn assert_validation(error: StorageError, message: &str) {
    match error.to_enum() {
        StorageEnum::Validation {
            message: actual, ..
        } => assert_eq!(actual, message),
        other => panic!("expected validation {message:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn ttl_job_rejects_work_without_a_durable_policy() {
    let manager = DatabaseManager::new_for_test().await.expect("manager");
    manager
        .create_table(&storage_types::CreateTableRequest::new(
            TableName::new("ttl_without_policy"),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .expect("table without ttl policy");
    let error = manager
        .run_ttl_sweep(request("ttl_without_policy"))
        .await
        .err()
        .expect("missing policy must fail closed");
    assert_validation(error, "ttl sweep requires an enabled ttl policy");
}
