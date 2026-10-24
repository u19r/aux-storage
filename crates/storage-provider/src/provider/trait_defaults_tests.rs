use storage_types::{
    StorageEnum, TableName, TimeToLiveSpecification, UpdateTimeToLiveRequest,
    context::WrappedError as _,
};

use super::trait_defaults;

#[tokio::test]
async fn given_provider_without_ttl_support_when_defaults_are_called_then_original_errors_are_preserved()
 {
    let table = TableName::new("unsupported_table");
    let update = trait_defaults::update_time_to_live(UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "expires_at".to_owned(),
            enabled: true,
        },
    })
    .await
    .err()
    .unwrap();
    let describe = trait_defaults::describe_time_to_live(&table)
        .await
        .err()
        .unwrap();
    for error in [update, describe] {
        assert!(
            matches!(error.to_enum(), StorageEnum::InternalServerError { message }
            if message == "Time to live configuration is not supported by this backend")
        );
    }
}
