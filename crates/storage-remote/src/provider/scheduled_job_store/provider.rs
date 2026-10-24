use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use async_trait::async_trait;
use bg_job_store::{ScheduledJobStore, backend::create_scheduled_job_store};
use bytes::Bytes;
use http_request::Url;
use scheduled_bg_jobs::store::{
    CommitOutcome, RangePage, RangeRequest, StoreCapabilities, StoreCapabilitiesInput, StoreKey,
    StoreLevel, StoreTransaction,
};
use storage_provider::{RawOrderedJobStore, RawOrderedJobStoreError, StorageProvider};
use storage_types::{
    AttributeDefinition, BillingMode, CreateTableRequest, KeyAttributeType, KeySchemaElement,
    KeyType, StorageEnum, StorageError, StorageResult, StreamSpecification, TableName, TableStatus,
    context::WrappedError,
};

use super::{commit, constants::*, read};
use crate::provider::implementation::RemoteStorageProvider;

#[derive(Clone)]
struct RemoteRawOrderedJobStore {
    provider: Arc<RemoteStorageProvider>,
    capabilities: StoreCapabilities,
}

impl RemoteRawOrderedJobStore {
    fn new(provider: RemoteStorageProvider) -> StorageResult<Self> {
        Ok(Self {
            provider: Arc::new(provider),
            capabilities: remote_capabilities()?,
        })
    }

    async fn verify_table(&self) -> Result<(), RawOrderedJobStoreError> {
        let response = self
            .provider
            .invoke::<_, storage_types::DescribeTableResponse>(
                "DynamoDB_20120810.DescribeTable",
                &storage_types::DescribeTableRequest {
                    table_name: job_table(),
                },
            )
            .await
            .map_err(RawOrderedJobStoreError::Backend)?;
        validate_job_table(&response.table)
    }
}

/// Validates private-network HTTP endpoints for the scheduled-job connector.
///
/// This is shared with the public storage facade so both construction paths
/// reject missing, malformed, or non-HTTP endpoints before work starts.
pub fn validate_scheduled_job_http_endpoints(endpoint_urls: &[String]) -> StorageResult<()> {
    if endpoint_urls.is_empty() {
        return Err(StorageError::validation(
            "remote scheduled-job endpoints must not be empty",
        ));
    }

    endpoint_urls
        .iter()
        .try_for_each(|endpoint| validate_scheduled_job_http_endpoint(endpoint))
}

fn validate_scheduled_job_http_endpoint(endpoint: &str) -> StorageResult<()> {
    let parsed = Url::parse(endpoint.trim()).map_err(|_| {
        StorageError::validation("remote scheduled-job store requires valid HTTP endpoints")
    })?;
    if !parsed.scheme().eq_ignore_ascii_case("http") || parsed.host_str().is_none() {
        return Err(StorageError::validation(
            "remote scheduled-job store requires valid private-network HTTP endpoints",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(StorageError::validation(
            "remote scheduled-job store requires endpoints without credentials",
        ));
    }
    if parsed.fragment().is_some() {
        return Err(StorageError::validation(
            "remote scheduled-job store endpoints must not contain fragments",
        ));
    }
    Ok(())
}

/// Connects a supported scheduled-job store to an existing private-network
/// DynamoDB-compatible endpoint and verifies the reserved `job` table before
/// returning it. This is a library boundary, not an HTTP route; all
/// operations continue to use the existing DynamoDB actions.
pub async fn connect_scheduled_job_store(
    settings: storage_provider::RemoteStorageSettings,
) -> StorageResult<ScheduledJobStore> {
    let store = build_scheduled_job_store(settings).await?;
    store
        .bootstrap()
        .await
        .map_err(|_| StorageError::internal("bootstrap scheduled job store"))?;
    Ok(store)
}

/// Builds an unbootstrapped scheduled-job store for an owner that controls
/// initialization ordering, such as `DatabaseManager`.
pub async fn build_scheduled_job_store(
    mut settings: storage_provider::RemoteStorageSettings,
) -> StorageResult<ScheduledJobStore> {
    validate_http_settings(&settings)?;
    apply_default_timeout(&mut settings);
    let mut provider = RemoteStorageProvider::new(settings).await?;
    provider.enable_scheduled_job_http_redirects();
    scheduled_job_store(provider)
}

pub(super) fn apply_default_timeout(settings: &mut storage_provider::RemoteStorageSettings) {
    let default_timeout_ms = MAX_TRANSACTION_DURATION.as_millis() as u64;
    let timeouts = settings.timeouts.get_or_insert_with(Default::default);
    timeouts
        .connect_timeout_ms
        .get_or_insert(default_timeout_ms);
    timeouts
        .request_timeout_ms
        .get_or_insert(default_timeout_ms);
}

fn scheduled_job_store(provider: RemoteStorageProvider) -> StorageResult<ScheduledJobStore> {
    Ok(create_scheduled_job_store(Arc::new(
        RemoteRawOrderedJobStore::new(provider)?,
    )))
}

fn validate_http_settings(settings: &storage_provider::RemoteStorageSettings) -> StorageResult<()> {
    if settings.tls {
        return Err(StorageError::validation(
            "scheduled-job store requires a private-network HTTP endpoint",
        ));
    }
    validate_scheduled_job_http_endpoints(&settings.endpoint_urls)
}

#[async_trait]
impl RawOrderedJobStore for RemoteRawOrderedJobStore {
    fn bind_native_admission(
        &self,
        _: std::sync::Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()> {
        Err(storage_types::StorageError::unsupported(
            "remote job stores do not bind local native admission",
        ))
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.capabilities
    }

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError> {
        let table = job_table();
        ensure_http_endpoint(&self.provider)?;
        ensure_job_table(&self.provider, &table).await?;
        self.provider
            .wait_for_table_active(&table)
            .await
            .map_err(RawOrderedJobStoreError::Backend)?;
        self.verify_table().await
    }

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
        read::get(Arc::clone(&self.provider), key).await
    }

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        read::get_many(Arc::clone(&self.provider), keys).await
    }

    async fn read_range(
        &self,
        request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError> {
        read::range(Arc::clone(&self.provider), request).await
    }

    async fn commit(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError> {
        commit::run(Arc::clone(&self.provider), transaction).await
    }
}

async fn ensure_job_table(
    provider: &RemoteStorageProvider,
    table: &TableName,
) -> Result<(), RawOrderedJobStoreError> {
    if provider
        .table_exists(table)
        .await
        .map_err(RawOrderedJobStoreError::Backend)?
    {
        return Ok(());
    }
    let request = job_table_request(table.clone());
    match provider
        .invoke::<_, storage_types::CreateTableResponse>("DynamoDB_20120810.CreateTable", &request)
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if matches!(error.to_enum(), StorageEnum::TableAlreadyExists { .. }) => Ok(()),
        Err(error) => Err(RawOrderedJobStoreError::Backend(error)),
    }
}

fn job_table_request(table: TableName) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
        table,
        vec![
            AttributeDefinition {
                attribute_name: KEY_ATTRIBUTE.to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: SORT_ATTRIBUTE.to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: KEY_ATTRIBUTE.to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: SORT_ATTRIBUTE.to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    );
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: false,
        stream_view_type: None,
    });
    request
}

fn ensure_http_endpoint(provider: &RemoteStorageProvider) -> Result<(), RawOrderedJobStoreError> {
    if provider.endpoints.iter().any(|endpoint| {
        !endpoint
            .uri
            .scheme_str()
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http"))
    }) {
        return Err(RawOrderedJobStoreError::Backend(StorageError::validation(
            "scheduled-job store requires a private-network HTTP endpoint",
        )));
    }
    Ok(())
}

pub(super) fn job_table() -> TableName {
    TableName::new(JOB_TABLE_NAME)
}

fn validate_job_table(
    table: &storage_types::TableDescription,
) -> Result<(), RawOrderedJobStoreError> {
    if table.table_name != job_table() || table.table_status != TableStatus::Active {
        return Err(RawOrderedJobStoreError::Corrupt(
            "reserved scheduled-job table has an invalid identity or status",
        ));
    }
    if table.key_schema.len() != 2
        || !table
            .key_schema
            .iter()
            .any(|key| key.attribute_name == KEY_ATTRIBUTE && key.key_type == KeyType::Hash)
        || !table
            .key_schema
            .iter()
            .any(|key| key.attribute_name == SORT_ATTRIBUTE && key.key_type == KeyType::Range)
    {
        return Err(RawOrderedJobStoreError::Corrupt(
            "reserved scheduled-job table key schema is invalid",
        ));
    }
    let exact_definitions = table.attribute_definitions.len() == 2
        && table.attribute_definitions.iter().any(|definition| {
            definition.attribute_name == KEY_ATTRIBUTE
                && matches!(definition.attribute_type, KeyAttributeType::S)
        })
        && table.attribute_definitions.iter().any(|definition| {
            definition.attribute_name == SORT_ATTRIBUTE
                && matches!(definition.attribute_type, KeyAttributeType::S)
        });
    if !exact_definitions {
        return Err(RawOrderedJobStoreError::Corrupt(
            "reserved scheduled-job table attribute schema is invalid",
        ));
    }
    Ok(())
}

fn remote_capabilities() -> StorageResult<StoreCapabilities> {
    StoreCapabilities::try_from(StoreCapabilitiesInput {
        // The endpoint contract is a coherent DynamoDB-compatible service:
        // strong reads, atomic conditional TransactWriteItems, and durable
        // operation receipts establish the cross-process/node store semantics.
        level: StoreLevel::Distributed,
        max_key_bytes: nonzero_usize(MAX_KEY_BYTES)?,
        max_value_bytes: nonzero_usize(MAX_VALUE_BYTES)?,
        max_get_many_keys: nonzero_u32(MAX_GET_MANY_KEYS)?,
        max_get_many_key_bytes: nonzero_usize(MAX_GET_MANY_KEY_BYTES)?,
        max_transaction_bytes: nonzero_usize(MAX_TRANSACTION_BYTES)?,
        max_transaction_keys: nonzero_u32(MAX_TRANSACTION_KEYS)?,
        max_range_items: nonzero_u32(MAX_RANGE_ITEMS)?,
        max_range_bytes: nonzero_usize(MAX_RANGE_BYTES)?,
        max_transaction_duration: MAX_TRANSACTION_DURATION,
    })
    .map_err(|_| StorageError::internal("invalid remote scheduled-job capabilities"))
}

fn nonzero_usize(value: usize) -> StorageResult<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| StorageError::internal("remote scheduled-job limit must be non-zero"))
}

fn nonzero_u32(value: u32) -> StorageResult<NonZeroU32> {
    NonZeroU32::new(value)
        .ok_or_else(|| StorageError::internal("remote scheduled-job limit must be non-zero"))
}
