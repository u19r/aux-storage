use std::{future::Future, pin::Pin, sync::Arc};

use super::source::SourceKey;
use crate::{
    DatabaseManager,
    types::{BatchGetItemRequest, BatchGetItemResponse},
};

/// Retains source capacity until native work and its response are destroyed.
#[derive(Clone)]
pub struct PointReadReservation {
    _batch: Arc<lru_ttl_cache::RefreshBatch<SourceKey>>,
}

impl PointReadReservation {
    pub(super) fn new(batch: Arc<lru_ttl_cache::RefreshBatch<SourceKey>>) -> Self {
        Self { _batch: batch }
    }
}

/// Implementations retain the reservation inside any detached operation and
/// its successful output until the response payload is destroyed.
pub trait PointReadClient: Send + Sync {
    fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
        reservation: PointReadReservation,
    ) -> Pin<Box<dyn Future<Output = crate::types::StorageResult<BatchGetItemResponse>> + Send + '_>>;
}

pub(crate) struct DatabasePointReadClient(pub(crate) std::sync::Weak<DatabaseManager>);

impl PointReadClient for DatabasePointReadClient {
    fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
        reservation: PointReadReservation,
    ) -> Pin<Box<dyn Future<Output = crate::types::StorageResult<BatchGetItemResponse>> + Send + '_>>
    {
        Box::pin(async move {
            let db = self
                .0
                .upgrade()
                .ok_or_else(|| crate::types::StorageError::service_unavailable(1))?;
            db.batch_get_item_retaining(request, reservation)
                .await
                .map(|(response, _reservation)| response)
        })
    }
}
