//! One scheduled-job implementation over the three native SQL drivers.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use bg_job_store::{
    ScheduledJobStore, ScheduledJobStoreConfig, backend::create_scheduled_job_store,
};
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    CommitOutcome, RangePage, RangeRequest, StoreCapabilities, StoreKey, StoreTransaction,
};
use storage_common::point_results::PointPlacement;
use storage_provider::{RawOrderedJobStore, RawOrderedJobStoreError};
use storage_types::{StorageError, StorageResult};

use super::{commit, limits, range, sql_statements};
use crate::{
    AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlEngine, SqlStore, SqlValue,
    constants::SCHEDULED_JOB_POINT_READ_BYTES as POINT_READ_BYTES,
};

pub(crate) async fn initialize<A: SqlAttempt>(
    attempt: &mut A,
    engine: SqlEngine,
) -> StorageResult<()> {
    for statement in sql_statements::schema(engine)? {
        attempt.execute(statement).await?;
    }
    Ok(())
}

pub(crate) struct SqlRawOrderedJobStore<B> {
    pub(super) backend: Arc<B>,
    pub(super) capabilities: StoreCapabilities,
    pub(super) admission:
        std::sync::OnceLock<Arc<dyn storage_provider::native_admission::NativeAdmission>>,
    pub(super) engine: SqlEngine,
}

impl<B: SqlBackend + 'static> bg_job_store::ScheduledJobStoreSource for SqlStore<B> {
    fn scheduled_job_store(
        &self,
        config: ScheduledJobStoreConfig,
    ) -> StorageResult<ScheduledJobStore> {
        let raw = SqlRawOrderedJobStore {
            backend: Arc::clone(&self.backend),
            capabilities: limits::capabilities(self.backend.engine(), config)?,
            engine: self.backend.engine(),
            admission: std::sync::OnceLock::new(),
        };
        Ok(create_scheduled_job_store(Arc::new(raw)))
    }
}

impl<B: SqlBackend + 'static> SqlRawOrderedJobStore<B> {
    pub(super) async fn read<R: Send + 'static>(
        &self,
        operation: impl for<'a> FnOnce(
            &'a mut B::Attempt,
        ) -> futures::future::BoxFuture<'a, StorageResult<R>>
        + Send
        + 'static,
    ) -> Result<R, RawOrderedJobStoreError> {
        let backend = Arc::clone(&self.backend);
        storage_provider::job_store::run_native_job_read(
            self.capabilities.max_transaction_duration(),
            self.admission.get().cloned(),
            move || async move { backend.with_attempt(AttemptMode::ReadOnly, operation).await },
        )
        .await
    }
}

#[async_trait]
impl<B: SqlBackend + 'static> RawOrderedJobStore for SqlRawOrderedJobStore<B> {
    fn bind_native_admission(
        &self,
        admission: Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> StorageResult<()> {
        self.admission.set(admission).map_err(|_| {
            storage_types::StorageError::internal("scheduled-job native admission is already bound")
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.capabilities
    }

    async fn bootstrap(&self) -> Result<(), RawOrderedJobStoreError> {
        Ok(())
    }

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, RawOrderedJobStoreError> {
        Ok(self.get_many(&[key]).await?.into_iter().next().flatten())
    }

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let max_bytes = POINT_READ_BYTES
            .get()
            .checked_mul(keys.len())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                RawOrderedJobStoreError::Backend(StorageError::internal(
                    "scheduled job batch byte limit overflowed",
                ))
            })?;
        let statement = sql_statements::points(self.engine, keys)?;
        let page = self
            .read(move |attempt| {
                Box::pin(async move {
                    attempt
                        .query(statement, ReadProtection::Snapshot, max_bytes)
                        .await
                })
            })
            .await?;
        decode_many(page, keys, |value: &Bytes| Ok(value.clone()))
    }

    async fn read_range(
        &self,
        request: &RangeRequest,
    ) -> Result<RangePage, RawOrderedJobStoreError> {
        range::read(self, request.clone()).await
    }

    async fn commit(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, RawOrderedJobStoreError> {
        commit::commit(self, transaction.clone()).await
    }
}

pub(crate) fn decode_many<Value: From<Vec<u8>>>(
    page: crate::SqlQueryPage,
    keys: &[StoreKey],
    copy: impl Fn(&Value) -> StorageResult<Value>,
) -> Result<Vec<Option<Value>>, RawOrderedJobStoreError> {
    if page.more || page.rows.len() > keys.len() {
        return Err(RawOrderedJobStoreError::Corrupt(
            "scheduled job batch bounds",
        ));
    }
    let mut values = Vec::new();
    values.try_reserve_exact(keys.len()).map_err(|_| {
        RawOrderedJobStoreError::Backend(
            storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
        )
    })?;
    values.resize_with(keys.len(), || None);
    for row in page.rows {
        let columns: [SqlValue; 2] = row
            .into_values()
            .try_into()
            .map_err(|_| RawOrderedJobStoreError::Corrupt("scheduled job batch row"))?;
        let [SqlValue::Blob(key), SqlValue::Blob(value)] = columns else {
            return Err(RawOrderedJobStoreError::Corrupt("scheduled job batch row"));
        };
        PointPlacement::new(keys, &mut values, |requested: &StoreKey, _: &Value| {
            requested.as_bytes() == key
        })
        .place(Value::from(value), &copy)
        .map_err(|error| {
            if error.native_attempt_rejection()
                == Some(storage_types::NativeAttemptRejection::Capacity)
            {
                RawOrderedJobStoreError::Backend(error)
            } else {
                RawOrderedJobStoreError::Corrupt("scheduled job batch association")
            }
        })?;
    }
    Ok(values)
}
