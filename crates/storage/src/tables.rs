use std::time::Duration;

use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, StorageEnum, StorageError,
    StorageResult, StreamSpecification, StreamViewType, TableName, TableNamespace, TableStatus,
    UpdateTableRequest, context::WrappedError as _,
};
use tokio::time::sleep;

use crate::{
    DatabaseManager,
    constants::{TABLE_ACTIVE_RETRY_ATTEMPTS, TABLE_ACTIVE_RETRY_DELAY_MS},
};

pub struct Tables;
impl Tables {
    #[must_use]
    pub fn sys_namespaces() -> TableName {
        TableName::new("sys")
    }

    pub async fn create_sys_namespaces_table(db: &DatabaseManager) -> StorageResult<()> {
        create_sys_namespaces(db).await
    }

    #[must_use]
    pub fn sys_analytics() -> TableName {
        TableName::new("ana")
    }
    pub async fn create_sys_analytics_table(db: &DatabaseManager) -> StorageResult<()> {
        create_sys_analytics(db).await
    }

    #[must_use]
    pub fn sys_jobs() -> TableName {
        TableName::new("job")
    }
    pub async fn create_sys_jobs_table(db: &DatabaseManager) -> StorageResult<()> {
        create_sys_jobs(db).await
    }

    #[must_use]
    pub fn sys_storage_replication() -> TableName {
        TableName::new("rep")
    }
    pub async fn create_sys_storage_replication_table(db: &DatabaseManager) -> StorageResult<()> {
        create_sys_storage_replication(db).await
    }

    #[must_use]
    pub fn should_hide_from_list_tables(table_name: &TableName) -> bool {
        *table_name == Self::sys_namespaces()
            || *table_name == Self::sys_analytics()
            || *table_name == Self::sys_jobs()
            || *table_name == Self::sys_storage_replication()
    }

    #[must_use]
    pub fn should_exclude_from_multi_region_replication(table_name: &TableName) -> bool {
        *table_name == Self::sys_storage_replication()
    }

    #[must_use]
    pub fn namespace(namespace: &TableNamespace) -> TableName {
        TableName::new(&format!("n{}", namespace.storage_key()))
    }

    #[must_use]
    pub fn shared_namespace(location_code: u16) -> TableName {
        TableName::new(&format!("s{location_code:05}"))
    }

    #[must_use]
    pub fn parse_namespace_table_name(table_name: &TableName) -> Option<TableNamespace> {
        let suffix = table_name.as_ref().strip_prefix('n')?;
        let storage_key = if suffix.starts_with(TableNamespace::PREFIX)
            || suffix.eq_ignore_ascii_case("system")
        {
            suffix.to_string()
        } else {
            format!("{}{suffix}", TableNamespace::PREFIX)
        };
        TableNamespace::parse_str(&storage_key).ok()
    }

    #[must_use]
    pub fn parse_shared_table_location(table_name: &TableName) -> Option<u16> {
        let suffix = table_name.as_ref().strip_prefix('s')?;
        suffix.parse::<u16>().ok()
    }
    pub async fn create_namespace_table(
        db: &DatabaseManager,
        namespace: &TableNamespace,
    ) -> StorageResult<()> {
        create_namespace(db, namespace).await
    }

    pub async fn create_shared_namespace_table(
        db: &DatabaseManager,
        location_code: u16,
    ) -> StorageResult<()> {
        create_shared_namespace(db, location_code).await
    }
}

async fn table_exists(db: &DatabaseManager, table_name: &TableName) -> Result<bool, StorageError> {
    match db.get_table_info(table_name).await {
        Ok(_) => Ok(true),
        Err(e) => {
            if matches!(
                e.to_enum(),
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
            ) {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

async fn wait_for_table_active(
    db: &DatabaseManager,
    table_name: &TableName,
) -> Result<(), StorageError> {
    let delay = Duration::from_millis(TABLE_ACTIVE_RETRY_DELAY_MS);
    for _ in 0..TABLE_ACTIVE_RETRY_ATTEMPTS {
        match db.get_table_info(table_name).await {
            Ok(info) => match info.table_status {
                TableStatus::Active => return Ok(()),
                TableStatus::Creating | TableStatus::Updating => {}
                other => {
                    return Err(StorageError::internal(&format!(
                        "table {table_name} unexpected status: {other:?}"
                    )));
                }
            },
            Err(err) => {
                if !matches!(
                    err.to_enum(),
                    StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                ) {
                    return Err(err);
                }
            }
        }
        sleep(delay).await;
    }
    Err(StorageError::internal(&format!(
        "table {table_name} did not become ACTIVE after {TABLE_ACTIVE_RETRY_ATTEMPTS} attempts"
    )))
}

async fn create_table_if_missing(
    db: &DatabaseManager,
    request: &CreateTableRequest,
) -> Result<(), StorageError> {
    match db.create_table(request).await {
        Ok(()) => Ok(()),
        Err(err) => {
            if matches!(err.to_enum(), StorageEnum::TableAlreadyExists { .. }) {
                return Ok(());
            }
            Err(err)
        }
    }
}

async fn raw_table_exists(
    db: &DatabaseManager,
    table_name: &TableName,
) -> Result<bool, StorageError> {
    match db.storage_provider().get_table_info(table_name).await {
        Ok(_) => Ok(true),
        Err(error) => {
            if matches!(
                error.to_enum(),
                StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
            ) {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }
}

async fn wait_for_raw_table_active(
    db: &DatabaseManager,
    table_name: &TableName,
) -> Result<(), StorageError> {
    let delay = Duration::from_millis(TABLE_ACTIVE_RETRY_DELAY_MS);
    for _ in 0..TABLE_ACTIVE_RETRY_ATTEMPTS {
        match db.storage_provider().get_table_info(table_name).await {
            Ok(info) => match info.table_status {
                TableStatus::Active => return Ok(()),
                TableStatus::Creating | TableStatus::Updating => {}
                other => {
                    return Err(StorageError::internal(&format!(
                        "table {table_name} unexpected status: {other:?}"
                    )));
                }
            },
            Err(error) => {
                if !matches!(
                    error.to_enum(),
                    StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
                ) {
                    return Err(error);
                }
            }
        }
        sleep(delay).await;
    }
    Err(StorageError::internal(&format!(
        "table {table_name} did not become ACTIVE after {TABLE_ACTIVE_RETRY_ATTEMPTS} attempts"
    )))
}

async fn create_table_if_missing_raw(
    db: &DatabaseManager,
    request: &CreateTableRequest,
) -> Result<(), StorageError> {
    match db.storage_provider().create_table(request).await {
        Ok(()) => Ok(()),
        Err(error) => {
            if matches!(error.to_enum(), StorageEnum::TableAlreadyExists { .. }) {
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

async fn create_sys_namespaces(db: &DatabaseManager) -> Result<(), StorageError> {
    let table_name = Tables::sys_namespaces();
    if table_exists(db, &table_name).await? {
        ensure_table_stream_enabled(db, &table_name).await?;
        return Ok(());
    }
    create_table_if_missing(
        db,
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi1pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi2pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi3pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi3sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        )
        .with_global_secondary_indexes(Some(vec![
            CreateGlobalSecondaryIndex {
                index_name: IndexName::new("gsi1"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi1pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(storage_types::ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            },
            CreateGlobalSecondaryIndex {
                // sparse ST preload index (items with gsi2pk="ST#1")
                index_name: IndexName::new("gsi2"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi2pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(storage_types::ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            },
            CreateGlobalSecondaryIndex {
                // cutover scheduling index (window scan by effective timestamp)
                index_name: IndexName::new("gsi3"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi3pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "gsi3sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(storage_types::ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            },
        ]))
        .with_stream_specification(Some(system_table_stream_specification())),
    )
    .await?;
    wait_for_table_active(db, &table_name).await?;
    Ok(())
}

async fn ensure_table_stream_enabled(
    db: &DatabaseManager,
    table_name: &TableName,
) -> Result<(), StorageError> {
    let table_info = db.get_table_info(table_name).await?;
    if table_info
        .stream_specification
        .as_ref()
        .is_some_and(|stream| stream.stream_enabled)
    {
        return Ok(());
    }

    db.update_table(UpdateTableRequest {
        table_name: table_name.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        replica_updates: None,
        sse_specification: None,
        stream_specification: Some(system_table_stream_specification()),
        table_class: None,
        aux_stream_duration_hours: None,
        aux_default_item_stream_duration_hours: None,
    })
    .await?;
    wait_for_table_active(db, table_name).await?;
    Ok(())
}

async fn ensure_raw_table_stream_enabled(
    db: &DatabaseManager,
    table_name: &TableName,
) -> Result<(), StorageError> {
    let table_info = db.storage_provider().get_table_info(table_name).await?;
    if table_info
        .stream_specification
        .as_ref()
        .is_some_and(|stream| stream.stream_enabled)
    {
        return Ok(());
    }

    db.storage_provider()
        .update_table(UpdateTableRequest {
            table_name: table_name.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: None,
            sse_specification: None,
            stream_specification: Some(system_table_stream_specification()),
            table_class: None,
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
        })
        .await?;
    wait_for_raw_table_active(db, table_name).await?;
    Ok(())
}

fn system_table_stream_specification() -> StreamSpecification {
    StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }
}

async fn create_sys_analytics(db: &DatabaseManager) -> Result<(), StorageError> {
    let table_name = Tables::sys_analytics();
    if table_exists(db, &table_name).await? {
        return Ok(());
    }
    create_table_if_missing(
        db,
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi1pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi2pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gsi2sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        )
        .with_global_secondary_indexes(Some(vec![
            CreateGlobalSecondaryIndex {
                index_name: IndexName::new("gsi1"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi1pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(storage_types::ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            },
            CreateGlobalSecondaryIndex {
                index_name: IndexName::new("gsi2"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi2pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "gsi2sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(storage_types::ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            },
        ])),
    )
    .await?;
    wait_for_table_active(db, &table_name).await?;
    Ok(())
}

async fn create_sys_jobs(db: &DatabaseManager) -> Result<(), StorageError> {
    let table_name = Tables::sys_jobs();
    if table_exists(db, &table_name).await? {
        return Ok(());
    }
    create_table_if_missing(
        db,
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        )
        .with_stream_specification(Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        })),
    )
    .await?;
    wait_for_table_active(db, &table_name).await?;
    Ok(())
}

async fn create_sys_storage_replication(db: &DatabaseManager) -> Result<(), StorageError> {
    let table_name = Tables::sys_storage_replication();
    if table_exists(db, &table_name).await? {
        return Ok(());
    }
    create_table_if_missing(
        db,
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        )
        .with_stream_specification(Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        })),
    )
    .await?;
    wait_for_table_active(db, &table_name).await?;
    Ok(())
}

async fn create_namespace(
    db: &DatabaseManager,
    namespace: &TableNamespace,
) -> Result<(), StorageError> {
    create_namespace_table_named(db, Tables::namespace(namespace)).await
}

async fn create_namespace_table_named(
    db: &DatabaseManager,
    table_name: TableName,
) -> Result<(), StorageError> {
    if table_exists(db, &table_name).await? {
        ensure_table_stream_enabled(db, &table_name).await?;
        return Ok(());
    }
    // Unified namespace table holds SCIM users, groups, SAML config, MFA records,
    // etc.
    create_table_if_missing(
        db,
        &namespace_table_request(table_name.clone())
            .with_stream_specification(Some(system_table_stream_specification())),
    )
    .await?;
    wait_for_table_active(db, &table_name).await?;
    Ok(())
}

async fn create_shared_namespace(
    db: &DatabaseManager,
    location_code: u16,
) -> Result<(), StorageError> {
    let table_name = Tables::shared_namespace(location_code);
    if raw_table_exists(db, &table_name).await? {
        ensure_raw_table_stream_enabled(db, &table_name).await?;
        return Ok(());
    }
    create_table_if_missing_raw(
        db,
        &namespace_table_request(table_name.clone())
            .with_stream_specification(Some(system_table_stream_specification())),
    )
    .await?;
    wait_for_raw_table_active(db, &table_name).await?;
    Ok(())
}

fn namespace_table_request(table_name: TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name,
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi2pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi2sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi3pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi3sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi4pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi4sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi5pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi5sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![
        CreateGlobalSecondaryIndex {
            // generic gsi1
            index_name: IndexName::new("gsi1"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi1pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi1sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(storage_types::ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            // generic gsi2
            index_name: IndexName::new("gsi2"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi2pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi2sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(storage_types::ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            // generic gsi3
            index_name: IndexName::new("gsi3"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi3pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi3sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(storage_types::ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            // users by primary org
            index_name: IndexName::new("gsi4"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi4pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi4sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(storage_types::ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            // generic gsi5
            index_name: IndexName::new("gsi5"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi5pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi5sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(storage_types::ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
    ]))
}
