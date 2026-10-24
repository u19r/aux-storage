use std::{future::Future, sync::Arc};

use bytes::Bytes;
use scheduled_bg_jobs::{
    ValidationError,
    store::{
        CommitOutcome, JobStore, RangeDirection, RangePage, RangeRequest, StoreCapabilities,
        StoreContractError, StoreDataError, StoreFailure, StoreKey, StoreTransaction,
    },
};
use storage_provider::RawOrderedJobStore;

use crate::ScheduledJobStoreError;

/// The supported adapter from a provider-owned raw store to the upstream
/// scheduled-job store contract.
#[derive(Clone)]
pub struct ScheduledJobStore {
    raw: Arc<dyn RawOrderedJobStore>,
}

impl ScheduledJobStore {
    pub(crate) fn new(raw: Arc<dyn RawOrderedJobStore>) -> Self {
        Self { raw }
    }

    pub fn capabilities(&self) -> StoreCapabilities {
        self.raw.capabilities()
    }

    pub fn bind_native_admission(
        &self,
        admission: Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<()> {
        self.raw.bind_native_admission(admission)
    }

    /// Bootstraps the provider-owned store before it is exposed by storage.
    #[doc(hidden)]
    pub async fn bootstrap(&self) -> Result<(), ScheduledJobStoreError> {
        self.raw.bootstrap().await.map_err(Into::into)
    }

    pub async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, ScheduledJobStoreError> {
        if key.as_bytes().len() > self.capabilities().max_key_bytes().get() {
            return Err(StoreContractError::CapabilityExceeded("key bytes").into());
        }
        let value = self
            .raw
            .get(key)
            .await
            .map_err(ScheduledJobStoreError::from)?;
        validate_value(value, self.capabilities().max_value_bytes().get())
    }

    pub async fn get_many(
        &self,
        keys: &[StoreKey],
    ) -> Result<Vec<Option<Bytes>>, ScheduledJobStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.capabilities().validate_get_many(keys)?;
        let values = self
            .raw
            .get_many(keys)
            .await
            .map_err(ScheduledJobStoreError::from)?;
        if values.len() != keys.len() {
            return Err(corrupt("get-many cardinality"));
        }
        let max_value_bytes = self.capabilities().max_value_bytes().get();
        values
            .into_iter()
            .map(|value| validate_value(value, max_value_bytes))
            .collect()
    }

    pub async fn read_range(
        &self,
        request: RangeRequest,
    ) -> Result<RangePage, ScheduledJobStoreError> {
        let capabilities = self.capabilities();
        capabilities.validate_range(&request)?;
        let page = self
            .raw
            .read_range(&request)
            .await
            .map_err(ScheduledJobStoreError::from)?;
        validate_range_page(&request, page, capabilities)
    }

    pub async fn commit(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<CommitOutcome, ScheduledJobStoreError> {
        self.capabilities().validate_transaction(transaction)?;
        self.raw
            .commit(transaction)
            .await
            .map_err(ScheduledJobStoreError::from)
    }
}

impl JobStore for ScheduledJobStore {
    type Error = ScheduledJobStoreError;

    fn capabilities(&self) -> StoreCapabilities {
        self.capabilities()
    }

    fn get(
        &self,
        key: StoreKey,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send + '_ {
        ScheduledJobStore::get(self, key)
    }

    fn get_many<'a>(
        &'a self,
        keys: &'a [StoreKey],
    ) -> impl Future<Output = Result<Vec<Option<Bytes>>, Self::Error>> + Send + 'a {
        ScheduledJobStore::get_many(self, keys)
    }

    fn read_range(
        &self,
        request: RangeRequest,
    ) -> impl Future<Output = Result<RangePage, Self::Error>> + Send + '_ {
        ScheduledJobStore::read_range(self, request)
    }

    fn commit<'a>(
        &'a self,
        transaction: &'a StoreTransaction,
    ) -> impl Future<Output = Result<CommitOutcome, Self::Error>> + Send + 'a {
        ScheduledJobStore::commit(self, transaction)
    }
}

fn validate_value(
    value: Option<Bytes>,
    max_value_bytes: usize,
) -> Result<Option<Bytes>, ScheduledJobStoreError> {
    if value
        .as_ref()
        .is_some_and(|value| value.len() > max_value_bytes)
    {
        return Err(corrupt("value bytes"));
    }
    Ok(value)
}

fn validate_range_page(
    request: &RangeRequest,
    page: RangePage,
    capabilities: StoreCapabilities,
) -> Result<RangePage, ScheduledJobStoreError> {
    if page.entries.len() > request.item_limit().get() as usize {
        return Err(corrupt("range item count"));
    }
    if page.total_bytes() > request.byte_limit().get() {
        return Err(corrupt("range byte count"));
    }
    let is_forward = request.direction() == RangeDirection::Forward;
    if request.continuation().is_some_and(|cursor| {
        page.entries.first().is_some_and(|first| {
            (is_forward && first.key <= *cursor) || (!is_forward && first.key >= *cursor)
        })
    }) {
        return Err(corrupt("range continuation ordering"));
    }
    for entry in &page.entries {
        if entry.key.as_bytes().len() > capabilities.max_key_bytes().get() {
            return Err(corrupt("range key bytes"));
        }
        if entry.value.len() > capabilities.max_value_bytes().get() {
            return Err(corrupt("range value bytes"));
        }
        if request.start().is_some_and(|start| entry.key < *start)
            || request.end().is_some_and(|end| entry.key >= *end)
        {
            return Err(corrupt("range bounds"));
        }
    }
    if page.entries.windows(2).any(|entries| {
        (is_forward && entries[0].key >= entries[1].key)
            || (!is_forward && entries[0].key <= entries[1].key)
    }) {
        return Err(corrupt("range ordering"));
    }

    validate_range_continuation(request, &page)?;
    Ok(page)
}

fn validate_range_continuation(
    request: &RangeRequest,
    page: &RangePage,
) -> Result<(), ScheduledJobStoreError> {
    if let Some(cursor) = page.continuation.as_ref() {
        let Some(last) = page.entries.last() else {
            return Err(
                StoreContractError::Validation(ValidationError::InvalidValue {
                    field: "range continuation",
                })
                .into(),
            );
        };
        if cursor != &last.key
            || request.start().is_some_and(|start| cursor < start)
            || request.end().is_some_and(|end| cursor >= end)
        {
            return Err(corrupt("range continuation"));
        }
    }
    Ok(())
}

fn corrupt(field: &'static str) -> ScheduledJobStoreError {
    ScheduledJobStoreError::Backend(StoreFailure::Corrupt(StoreDataError::Malformed(field)))
}
