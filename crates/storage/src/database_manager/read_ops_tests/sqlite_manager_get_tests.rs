use std::sync::Arc;

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, StorageEnum, TableName, context::WrappedError as _,
};

use crate::DatabaseManager;

struct CountedBackend {
    backend: sql_backend::sqlite::SqliteBackend,
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}

impl sql_backend::SqlBackend for CountedBackend {
    type Attempt = <sql_backend::sqlite::SqliteBackend as sql_backend::SqlBackend>::Attempt;

    fn attempt_capacity(
        &self,
    ) -> storage_types::StorageResult<storage_types::NativeAttemptCapacity> {
        self.backend.attempt_capacity()
    }
    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> storage_types::StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity>
    {
        self.backend.record_capacity(limits)
    }

    fn engine(&self) -> sql_backend::SqlEngine {
        self.backend.engine()
    }

    fn with_attempt<R, F>(
        &self,
        mode: sql_backend::AttemptMode,
        operation: F,
    ) -> impl std::future::Future<Output = storage_types::StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(
                &'a mut Self::Attempt,
            )
                -> futures::future::BoxFuture<'a, storage_types::StorageResult<R>>
            + Send
            + 'static,
    {
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.backend.with_attempt(mode, operation)
    }
}

#[tokio::test]
async fn given_manager_get_when_cache_cold_and_warm_then_one_native_attempt_supplies_item() {
    let path = crate::storage_test_support_tests::unique_path("manager-get-attempt");
    let table = TableName::new("manager_get_scope");
    let seed = DatabaseManager::new_for_test_with_config(storage_provider::StorageConfig {
        connection_string: Some(path.to_string_lossy().into_owned()),
        ..super::super::construction::immediate_sqlite_test_config()
    })
    .await
    .expect("seed manager");
    seed.create_table(&CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create table");
    seed.put_item(
        crate::PutItemInput::builder()
            .table_name(table.clone())
            .item(std::collections::HashMap::from([
                ("pk".to_owned(), AttributeValue::S("item#1".to_owned())),
                ("value".to_owned(), AttributeValue::S("one".to_owned())),
            ]))
            .build(),
    )
    .await
    .expect("seed item");
    drop(seed);

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = CountedBackend {
        backend: sql_backend::sqlite::SqliteBackend::initialize(&path)
            .await
            .expect("open SQLite"),
        attempts: attempts.clone(),
    };
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(backend)
            .await
            .expect("initialize store"),
    );
    let manager = super::super::construction_tests::database_manager_from_bundle(
        crate::provider_bundle::StorageProviderBundle::try_from_initialized_store(
            store,
            Default::default(),
        )
        .expect("initialized bundle"),
    )
    .expect("manager over one initialized store");
    for temperature in ["cold", "warm"] {
        attempts.store(0, std::sync::atomic::Ordering::SeqCst);
        let item = manager
            .get_item(
                table.clone(),
                std::collections::HashMap::from([(
                    "pk".to_owned(),
                    AttributeValue::S("item#1".to_owned()),
                )]),
            )
            .await
            .expect("manager Get")
            .expect("item exists");
        assert_eq!(
            item.to_attribute_map().expect("decode item").get("value"),
            Some(&AttributeValue::S("one".to_owned()))
        );
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "{temperature} Get must own one native attempt"
        );
    }
    drop(manager);
    std::fs::remove_file(path).expect("remove owned database");
}

#[tokio::test]
async fn given_invalid_manager_get_keys_when_consistency_varies_then_validation_precedes_metadata()
{
    let manager = DatabaseManager::new_for_test()
        .await
        .expect("create SQLite manager");
    let string_table = TableName::new("manager_key_validation");
    manager
        .create_table(&CreateTableRequest::new(
            string_table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create string table");
    let number_table = TableName::new("manager_number_validation");
    manager
        .create_table(&CreateTableRequest::new(
            number_table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::N,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .expect("create number table");

    for consistent_read in [false, true] {
        let empty_key = manager
            .get_item_with_consistent_read(
                string_table.clone(),
                std::collections::HashMap::new(),
                consistent_read,
            )
            .await
            .expect_err("empty key must be rejected before provider metadata");
        assert!(matches!(
            empty_key.to_enum(),
            StorageEnum::Validation { .. }
        ));

        let missing_table = manager
            .get_item_with_consistent_read(
                TableName::new("manager_missing_table"),
                std::collections::HashMap::new(),
                consistent_read,
            )
            .await
            .expect_err("empty key takes precedence over missing table");
        assert!(matches!(
            missing_table.to_enum(),
            StorageEnum::Validation { .. }
        ));
    }

    for consistent_read in [false, true] {
        let wrong_type = manager
            .get_item_with_consistent_read(
                string_table.clone(),
                std::collections::HashMap::from([("pk".to_string(), AttributeValue::BOOL(true))]),
                consistent_read,
            )
            .await
            .expect_err("wrong key type must be rejected by shared binding");
        assert!(matches!(
            wrong_type.to_enum(),
            StorageEnum::Validation { .. }
        ));

        let malformed_number = manager
            .get_item_with_consistent_read(
                number_table.clone(),
                std::collections::HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::N("not-a-number".to_string()),
                )]),
                consistent_read,
            )
            .await
            .expect_err("malformed numeric key must be rejected by shared binding");
        assert!(matches!(
            malformed_number.to_enum(),
            StorageEnum::RawValidation { .. }
        ));
    }
}

#[tokio::test]
async fn given_table_stream_when_cursor_is_malformed_then_public_read_rejects_it() {
    let manager = crate::DatabaseManager::new_for_test()
        .await
        .expect("SQLite manager");
    let error = manager
        .get_stream_records_for_table_name(
            &crate::Tables::sys_namespaces(),
            Some("not-hex"),
            Some(10),
        )
        .await
        .expect_err("malformed cursor must fail");
    assert!(matches!(
        storage_types::context::WrappedError::to_enum(&error),
        storage_types::StorageEnum::InternalServerError { .. }
    ));
}
