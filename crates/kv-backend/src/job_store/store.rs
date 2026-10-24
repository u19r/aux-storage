//! One scheduled-job implementation over either native KV driver.

use std::sync::Arc;

use async_trait::async_trait;
use bg_job_store::{
    ScheduledJobStore, ScheduledJobStoreConfig, backend::create_scheduled_job_store,
};
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    CommitOutcome, RangePage, RangeRequest, StoreCapabilities, StoreKey, StoreTransaction,
};
use storage_provider::{RawOrderedJobStore, RawOrderedJobStoreError};
use storage_types::StorageResult;

use super::{commit, limits, range};
use crate::{
    AttemptMode, KvAttempt, KvBackend, KvStore, ReadProtection,
    constants::JOB_DATA_PREFIX as DATA_PREFIX,
};

pub(crate) fn data_key(key: &[u8]) -> StorageResult<Vec<u8>> {
    let count = DATA_PREFIX
        .len()
        .checked_add(key.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut physical = Vec::new();
    physical
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    physical.extend_from_slice(DATA_PREFIX);
    physical.extend_from_slice(key);
    Ok(physical)
}

pub(crate) fn data_keys<'a>(
    keys: impl ExactSizeIterator<Item = &'a [u8]>,
) -> StorageResult<Vec<Vec<u8>>> {
    let mut physical = Vec::new();
    physical
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for key in keys {
        physical.push(data_key(key)?);
    }
    Ok(physical)
}

pub(crate) struct KvRawOrderedJobStore<B> {
    pub(super) backend: Arc<B>,
    pub(super) capabilities: StoreCapabilities,
    pub(super) admission:
        std::sync::OnceLock<Arc<dyn storage_provider::native_admission::NativeAdmission>>,
}

impl<B: KvBackend + 'static> bg_job_store::ScheduledJobStoreSource for KvStore<B> {
    fn scheduled_job_store(
        &self,
        config: ScheduledJobStoreConfig,
    ) -> StorageResult<ScheduledJobStore> {
        let capabilities = limits::capabilities(self.backend.as_ref(), config)?;
        let raw = KvRawOrderedJobStore {
            backend: Arc::clone(&self.backend),
            capabilities,
            admission: std::sync::OnceLock::new(),
        };
        Ok(create_scheduled_job_store(Arc::new(raw)))
    }
}

impl<B: KvBackend + 'static> KvRawOrderedJobStore<B> {
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
impl<B: KvBackend + 'static> RawOrderedJobStore for KvRawOrderedJobStore<B> {
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
        let value = self
            .read(move |attempt| {
                Box::pin(async move {
                    attempt
                        .get_admitted(
                            &data_key(key.as_bytes())?,
                            ReadProtection::Snapshot,
                            limits::point_allowance(1)?,
                        )
                        .await
                })
            })
            .await?;
        Ok(value.map(Bytes::from))
    }

    async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, RawOrderedJobStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let physical = data_keys(keys.iter().map(StoreKey::as_bytes))?;
        let allowance = limits::point_allowance(keys.len())?;
        let values = self
            .read(move |attempt| {
                Box::pin(async move {
                    attempt
                        .get_batch_admitted(physical, ReadProtection::Snapshot, allowance)
                        .await
                })
            })
            .await?;
        let mut output = Vec::new();
        output.try_reserve_exact(values.len()).map_err(|_| {
            RawOrderedJobStoreError::Backend(
                storage_types::InfrastructureResultKind::NativeCapacityRejected.into(),
            )
        })?;
        output.extend(values.into_iter().map(|value| value.map(Bytes::from)));
        Ok(output)
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
