use std::sync::Arc;

use storage_provider::DynamoOperations;
use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateTableRequest, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, MaxIndexers, Projection, StorageEnum,
    StorageError, StorageResult, StreamSpecification, StreamViewType, TableName, TableNamespace,
    context::WrappedError as _,
};

use crate::{DatabaseManager, database_manager::ROUTED_DEFAULT_CONNECTION_ID};

pub struct Tables;
impl Tables {
    #[must_use]
    pub fn sys_namespaces() -> TableName {
        TableName::new("sys")
    }

    pub async fn create_sys_namespaces_table(db: &DatabaseManager) -> StorageResult<()> {
        let mut request = sys_namespaces_table_request(Self::sys_namespaces())?;
        request.stream_specification = Some(system_table_stream_specification());
        ensure_system_table(db, request).await
    }

    #[must_use]
    pub fn sys_analytics() -> TableName {
        TableName::new("ana")
    }
    pub async fn create_sys_analytics_table(db: &DatabaseManager) -> StorageResult<()> {
        ensure_system_table(db, sys_analytics_table_request()).await
    }

    #[must_use]
    pub fn namespace(namespace: &TableNamespace) -> TableName {
        TableName::new(&format!("n{}", namespace.storage_key()))
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

    pub async fn create_namespace_table(
        db: &DatabaseManager,
        namespace: &TableNamespace,
    ) -> StorageResult<()> {
        create_namespace_table_named(db, Self::namespace(namespace)).await
    }
}

async fn ensure_system_table(
    db: &DatabaseManager,
    request: CreateTableRequest,
) -> StorageResult<()> {
    let table_name = request.table_name.clone();
    let stream_specification = request.stream_specification.clone();
    let max_indexers = request.max_indexers;
    let result = db
        .run_dynamo_control(
            ROUTED_DEFAULT_CONNECTION_ID,
            |dynamo: Arc<dyn DynamoOperations>| async move { dynamo.create_table(request).await },
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if matches!(error.to_enum(), StorageEnum::TableAlreadyExists { .. }) => {
            ensure_system_table_configuration(db, table_name, stream_specification, max_indexers)
                .await
        }
        Err(error) => Err(error),
    }
}

async fn ensure_system_table_configuration(
    db: &DatabaseManager,
    table_name: TableName,
    stream_specification: Option<StreamSpecification>,
    max_indexers: MaxIndexers,
) -> StorageResult<()> {
    let table = db.get_table_info_for_control(&table_name).await?;
    let stream_specification = stream_specification.filter(|required| {
        !table.stream_specification.as_ref().is_some_and(|current| {
            current.stream_enabled == required.stream_enabled
                && current.stream_view_type == required.stream_view_type
        })
    });
    let max_indexers = (max_indexers.get() > table.max_indexers.get()).then_some(max_indexers);
    if stream_specification.is_none() && max_indexers.is_none() {
        return Ok(());
    }
    db.update_table_for_control(storage_types::UpdateTableRequest {
        stream_specification,
        max_indexers,
        ..storage_types::UpdateTableRequest::new(table_name)
    })
    .await?;
    Ok(())
}

fn sys_namespaces_table_request(table_name: TableName) -> Result<CreateTableRequest, StorageError> {
    let mut request = CreateTableRequest::new(
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
    );
    request.global_secondary_indexes = Some(vec![
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
    ]);
    request.max_indexers = MaxIndexers::try_new(2)?;
    Ok(request)
}

fn system_table_stream_specification() -> StreamSpecification {
    StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }
}

fn sys_analytics_table_request() -> CreateTableRequest {
    let mut analytics_request = CreateTableRequest::new(
        Tables::sys_analytics(),
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
    );
    analytics_request.global_secondary_indexes = Some(vec![
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
    ]);
    analytics_request
}

async fn create_namespace_table_named(
    db: &DatabaseManager,
    table_name: TableName,
) -> Result<(), StorageError> {
    let mut request = namespace_table_request(table_name)?;
    request.stream_specification = Some(system_table_stream_specification());
    ensure_system_table(db, request).await
}

fn namespace_table_request(table_name: TableName) -> Result<CreateTableRequest, StorageError> {
    let mut request = CreateTableRequest::new(
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
    );
    request.global_secondary_indexes = Some(vec![
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
    ]);
    request.max_indexers = MaxIndexers::try_new(2)?;
    Ok(request)
}
