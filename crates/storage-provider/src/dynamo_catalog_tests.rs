use std::{num::NonZeroU64, sync::Arc};

use storage_types::{
    BoundTableInfo, StorageEnum, StorageResult, StoredTableInfo, TableName,
    context::WrappedError as _,
};

use crate::{
    CatalogAttempt, CatalogStamp, TableMetadataCache,
    dynamo_catalog::resolve_prepared_table_in_attempt,
};

struct Catalog {
    stamp: Option<CatalogStamp>,
    table: Arc<BoundTableInfo>,
    metadata_reads: usize,
}

impl Catalog {
    fn new() -> Self {
        let info: StoredTableInfo = serde_json::from_value(serde_json::json!({
            "table_name": "prepared", "table_status": "ACTIVE", "created_at": 0,
            "attribute_definitions": [], "key_schema": [], "max_indexers": 0,
            "global_secondary_indexes": null, "table_size_bytes": 0, "item_count": 0,
            "stream_specification": null
        }))
        .unwrap();
        let table = Arc::new(BoundTableInfo {
            info: Arc::new(info),
            history_id: 1_u32.into(),
        });
        Self {
            stamp: Some(CatalogStamp {
                history_id: table.history_id,
                revision: NonZeroU64::new(1).unwrap(),
            }),
            table,
            metadata_reads: 0,
        }
    }
}

impl CatalogAttempt for Catalog {
    async fn advance_catalog_stamp(
        &mut self,
        _: &TableName,
        expected: CatalogStamp,
    ) -> StorageResult<CatalogStamp> {
        if self.stamp != Some(expected) {
            return Err(StorageEnum::TransactionConflict {
                message: "catalog changed".into(),
            }
            .into());
        }
        let revision = expected
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| storage_types::StorageError::internal("catalog revision overflow"))?;
        let next = CatalogStamp {
            history_id: expected.history_id,
            revision,
        };
        self.stamp = Some(next);
        Ok(next)
    }

    async fn read_catalog_stamp(&mut self, _: &TableName) -> StorageResult<Option<CatalogStamp>> {
        Ok(self.stamp)
    }
    async fn read_table_at_stamp(
        &mut self,
        _: &TableName,
        stamp: CatalogStamp,
    ) -> StorageResult<Arc<BoundTableInfo>> {
        assert!(self.stamp == Some(stamp));
        self.metadata_reads += 1;
        Ok(Arc::clone(&self.table))
    }
    async fn read_catalog_stamps(
        &mut self,
        names: &[TableName],
    ) -> StorageResult<Vec<Option<CatalogStamp>>> {
        Ok(vec![self.stamp; names.len()])
    }
    async fn read_tables_at_stamps(
        &mut self,
        _: &[(TableName, CatalogStamp)],
    ) -> StorageResult<Vec<Arc<BoundTableInfo>>> {
        panic!("single-table preparation must not batch metadata")
    }
}

#[tokio::test]
async fn given_current_prepared_metadata_when_writer_confirms_then_cached_table_is_reused() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.info.table_name.clone();
    let (prepared, publication) = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name)
        .await
        .and_then(|decision| decision.into_result())
        .unwrap();
    cache.publish(publication);
    let cached = cache.prepared_candidate(&name).unwrap();
    assert!(Arc::ptr_eq(&prepared.table, &cached.table));
    cached.confirm(&mut catalog, &cache).await.unwrap();
    assert_eq!(catalog.metadata_reads, 1);
}

#[tokio::test]
async fn given_changed_catalog_when_writer_confirms_then_preparation_conflicts_and_cache_is_invalidated()
 {
    for replacement in 0..3 {
        let mut catalog = Catalog::new();
        let cache = TableMetadataCache::new();
        let name = catalog.table.info.table_name.clone();
        let (prepared, publication) =
            resolve_prepared_table_in_attempt(&mut catalog, &cache, &name)
                .await
                .and_then(|decision| decision.into_result())
                .unwrap();
        cache.publish(publication);
        catalog.stamp = match replacement {
            0 => None,
            1 => Some(CatalogStamp {
                history_id: catalog.table.history_id,
                revision: NonZeroU64::new(2).unwrap(),
            }),
            _ => Some(CatalogStamp {
                history_id: 2_u32.into(),
                revision: NonZeroU64::new(1).unwrap(),
            }),
        };
        let error = prepared.confirm(&mut catalog, &cache).await.unwrap_err();
        assert!(matches!(
            error.to_enum(),
            StorageEnum::TransactionConflict { .. }
        ));
        assert!(cache.prepared_candidate(&name).is_none());
        assert_eq!(catalog.metadata_reads, 1);
    }
}
