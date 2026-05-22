use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use bg_jobs::{BackgroundJobName, JobLockAttempt, JobLockError, JobLockResult, JobLockStore};
use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, StorageEnum, StorageError, StorageResult, TableName,
    UpdateItemRequest, WireAttributeDecode, WireItem,
};

use crate::constants::{
    JOB_LOCK_ATTR_JOB_ID, JOB_LOCK_ATTR_LEASE_UNTIL_MS, JOB_LOCK_ATTR_LEASED_BY, JOB_LOCK_KEY_PK,
    JOB_LOCK_KEY_SK, JOB_LOCK_PK_PREFIX, JOB_LOCK_SK, SYS_JOBS_TABLE,
};

pub struct SysJobLockStore {
    storage: Arc<dyn StorageProvider>,
    worker_id: String,
    table_ready: OnceLock<()>,
}

impl SysJobLockStore {
    pub async fn new(
        storage: Arc<dyn StorageProvider>,
        worker_id: impl Into<String>,
    ) -> StorageResult<Self> {
        Ok(Self {
            storage,
            worker_id: worker_id.into(),
            table_ready: OnceLock::new(),
        })
    }

    pub(crate) fn table_name() -> TableName {
        TableName::new(SYS_JOBS_TABLE)
    }

    pub(crate) fn key_map(job_id: BackgroundJobName) -> KeyAttributes {
        KeyAttributes::from([
            (
                JOB_LOCK_KEY_PK.to_string(),
                AttributeValue::S(format!("{JOB_LOCK_PK_PREFIX}{job_id}")),
            ),
            (
                JOB_LOCK_KEY_SK.to_string(),
                AttributeValue::S(JOB_LOCK_SK.to_string()),
            ),
        ])
    }

    async fn fetch_lease_until(
        &self,
        job_id: BackgroundJobName,
    ) -> Result<Option<i64>, StorageError> {
        let item = self
            .storage
            .get_item(Self::table_name(), Self::key_map(job_id), true)
            .await?;
        item.map(|wire_item| decode_lease_until_ms(&wire_item))
            .transpose()
            .map(Option::flatten)
    }

    pub(crate) fn acquire_update_request(
        job_id: BackgroundJobName,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> UpdateItemRequest {
        let mut expr_values = HashMap::new();
        expr_values.insert(
            ":worker".to_string(),
            AttributeValue::S(worker_id.to_string()),
        );
        expr_values.insert(
            ":lease".to_string(),
            AttributeValue::N(lease_until_ms.to_string()),
        );
        expr_values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));
        expr_values.insert(":job_id".to_string(), AttributeValue::S(job_id.to_string()));

        UpdateItemRequest::builder()
            .table_name(Self::table_name())
            .key(Self::key_map(job_id))
            .update_expression(format!(
                "SET {JOB_LOCK_ATTR_LEASED_BY} = :worker, {JOB_LOCK_ATTR_LEASE_UNTIL_MS} = \
                 :lease, {JOB_LOCK_ATTR_JOB_ID} = :job_id"
            ))
            .condition_expression(Some(format!(
                "(attribute_not_exists({JOB_LOCK_ATTR_LEASE_UNTIL_MS}) OR \
                 {JOB_LOCK_ATTR_LEASE_UNTIL_MS} < :now)"
            )))
            .expression_attribute_values(Some(expr_values))
            .build()
    }

    pub(crate) fn renew_update_request(
        job_id: BackgroundJobName,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> UpdateItemRequest {
        let mut expr_values = HashMap::new();
        expr_values.insert(
            ":worker".to_string(),
            AttributeValue::S(worker_id.to_string()),
        );
        expr_values.insert(
            ":lease".to_string(),
            AttributeValue::N(lease_until_ms.to_string()),
        );
        expr_values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));

        UpdateItemRequest::builder()
            .table_name(Self::table_name())
            .key(Self::key_map(job_id))
            .update_expression(format!(
                "SET {JOB_LOCK_ATTR_LEASED_BY} = :worker, {JOB_LOCK_ATTR_LEASE_UNTIL_MS} = :lease"
            ))
            .condition_expression(Some(format!(
                "{JOB_LOCK_ATTR_LEASED_BY} = :worker AND {JOB_LOCK_ATTR_LEASE_UNTIL_MS} >= :now"
            )))
            .expression_attribute_values(Some(expr_values))
            .build()
    }

    async fn ensure_table_exists(&self) -> Result<(), StorageError> {
        if self.table_ready.get().is_some() {
            return Ok(());
        }
        let table = Self::table_name();
        if self.storage.table_exists(&table).await? {
            let _ = self.table_ready.set(());
            return Ok(());
        }
        let request = CreateTableRequest::new(
            table,
            vec![
                AttributeDefinition {
                    attribute_name: JOB_LOCK_KEY_PK.to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: JOB_LOCK_KEY_SK.to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: JOB_LOCK_KEY_PK.to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: JOB_LOCK_KEY_SK.to_string(),
                    key_type: KeyType::Range,
                },
            ],
            storage_types::BillingMode::PayPerRequest,
        );
        self.storage.create_table(&request).await?;
        let _ = self.table_ready.set(());
        Ok(())
    }

    pub(crate) fn map_storage_error(error: StorageError) -> JobLockError {
        match error.as_ref() {
            StorageEnum::ConditionalCheckFailed
            | StorageEnum::ResourceExists { .. }
            | StorageEnum::ResourceNotFound { .. }
            | StorageEnum::IndexNotFound { .. }
            | StorageEnum::TableAlreadyExists { .. }
            | StorageEnum::TableNotFound { .. }
            | StorageEnum::TransactionConflict { .. }
            | StorageEnum::TransactionInProgress { .. }
            | StorageEnum::InternalServerError { .. } => {
                JobLockError::contention(error.to_string())
            }
            _ => JobLockError::store(error.to_string()),
        }
    }
}

pub(crate) fn decode_lease_until_ms(item: &WireItem) -> Result<Option<i64>, StorageError> {
    let values = item.scalar_attributes(&[JOB_LOCK_ATTR_LEASE_UNTIL_MS])?;
    values
        .first()
        .and_then(|value| value.as_deref())
        .map(|raw| <i64 as WireAttributeDecode>::decode(Some(raw), JOB_LOCK_ATTR_LEASE_UNTIL_MS))
        .transpose()
}

#[async_trait]
impl JobLockStore for SysJobLockStore {
    async fn try_acquire(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<JobLockAttempt> {
        self.ensure_table_exists()
            .await
            .map_err(Self::map_storage_error)?;
        let request = Self::acquire_update_request(job_id, &self.worker_id, lease_until_ms, now_ms);
        match self.storage.update_item(request).await {
            Ok(_) => Ok(JobLockAttempt::Acquired { lease_until_ms }),
            Err(err) => {
                if matches!(err.as_ref(), StorageEnum::ConditionalCheckFailed) {
                    let lease_until_ms = self
                        .fetch_lease_until(job_id)
                        .await
                        .map_err(Self::map_storage_error)?;
                    Ok(JobLockAttempt::Conflict { lease_until_ms })
                } else {
                    Err(Self::map_storage_error(err))
                }
            }
        }
    }

    async fn renew(
        &self,
        job_id: BackgroundJobName,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> JobLockResult<bool> {
        self.ensure_table_exists()
            .await
            .map_err(Self::map_storage_error)?;
        let request = Self::renew_update_request(job_id, &self.worker_id, lease_until_ms, now_ms);
        match self.storage.update_item(request).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if matches!(err.as_ref(), StorageEnum::ConditionalCheckFailed) {
                    Ok(false)
                } else {
                    Err(Self::map_storage_error(err))
                }
            }
        }
    }
}
