use storage_types::{DurablePointReadProof, KeyAttributes, StorageResult, TableName, WireItem};

use crate::{
    cache_coordinator::StorageCacheServices,
    cache_read_observability::{
        StorageCacheReadOperation, StorageCacheReadOutcome, record_storage_cache_read_outcome,
    },
    point_read_cache::{
        AuthoritativePointReadHit, AuthoritativePointReadPurpose, AuthoritativePointReadResult,
        PointReadGetRequest, PointReadGetResult,
    },
};

pub(crate) struct PreparedPointReadCacheRead {
    request: Option<PointReadGetRequest>,
    hit: Option<Option<WireItem>>,
}

impl PreparedPointReadCacheRead {
    pub(crate) fn cache_hit(&self) -> Option<Option<WireItem>> {
        self.hit.clone()
    }

    pub(crate) fn request(&self) -> Option<&PointReadGetRequest> {
        self.request.as_ref()
    }

    pub(crate) fn record_db_miss(&self) {
        if self.request.is_some() {
            record_storage_cache_read_outcome(
                StorageCacheReadOperation::GetItem,
                StorageCacheReadOutcome::Miss,
            );
        }
    }
}

pub(crate) struct StoragePointReadCacheRuntime<'a> {
    services: &'a StorageCacheServices,
}

impl<'a> StoragePointReadCacheRuntime<'a> {
    pub(crate) fn new(services: &'a StorageCacheServices) -> Self {
        Self { services }
    }

    pub(crate) async fn prepare_get(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<PreparedPointReadCacheRead> {
        let can_use_point_read_cache =
            !consistent_read || self.services.authoritative_strong_point_reads_enabled();
        let request = can_use_point_read_cache.then(|| PointReadGetRequest {
            table_name: table_name.clone(),
            key: key.clone(),
        });

        let hit = match (consistent_read, request.as_ref()) {
            (true, Some(request)) => match self
                .services
                .get_authoritative_point_read(request, AuthoritativePointReadPurpose::StrongGet)
                .await?
            {
                AuthoritativePointReadResult::Hit(hit) => {
                    record_storage_cache_read_outcome(
                        StorageCacheReadOperation::GetItem,
                        StorageCacheReadOutcome::Hit,
                    );
                    Some(authoritative_hit_to_wire_item(*hit))
                }
                AuthoritativePointReadResult::Miss => None,
            },
            (false, Some(request)) => match self.services.get_eventual_point_read(request).await? {
                PointReadGetResult::Hit(item) => {
                    record_storage_cache_read_outcome(
                        StorageCacheReadOperation::GetItem,
                        StorageCacheReadOutcome::Hit,
                    );
                    Some(*item)
                }
                PointReadGetResult::Miss => None,
            },
            (_, None) => None,
        };

        Ok(PreparedPointReadCacheRead { request, hit })
    }

    pub(crate) fn strong_read_through_warming_enabled(&self) -> bool {
        self.services.strong_read_through_warming_enabled()
    }

    pub(crate) async fn warm_authoritative_read(
        &self,
        request: &PointReadGetRequest,
        proof: DurablePointReadProof,
    ) -> StorageResult<()> {
        self.services
            .warm_authoritative_point_read(request, proof)
            .await
    }
}

fn authoritative_hit_to_wire_item(hit: AuthoritativePointReadHit) -> Option<WireItem> {
    match hit {
        AuthoritativePointReadHit::Present { item, .. } => Some(*item),
        AuthoritativePointReadHit::Absent { .. } => None,
    }
}
