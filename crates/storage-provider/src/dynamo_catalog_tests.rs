use std::{num::NonZeroU64, sync::Arc};

use storage_types::{
    BoundTableInfo, StorageEnum, StorageResult, StoredTableInfo, TableName,
    context::WrappedError as _,
};

use crate::{
    CatalogAttempt, CatalogStamp, RequestTableBindings, TableMetadataCache,
    dynamo_catalog::resolve_prepared_table_in_attempt,
};

struct Catalog {
    stamp: Option<CatalogStamp>,
    table: Arc<BoundTableInfo>,
    metadata_reads: usize,
    batch_reads: usize,
    fail_metadata: bool,
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
            batch_reads: 0,
            fail_metadata: false,
        }
    }
}

impl CatalogAttempt for Catalog {
    async fn store_external_cleanup_barrier(
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
        if std::mem::take(&mut self.fail_metadata) {
            return Err(StorageEnum::TransactionConflict {
                message: "injected metadata abort".into(),
            }
            .into());
        }
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
        requests: &[(TableName, CatalogStamp)],
    ) -> StorageResult<Vec<Arc<BoundTableInfo>>> {
        self.batch_reads += 1;
        let mut tables = Vec::new();
        for (name, stamp) in requests {
            tables.push(self.read_table_at_stamp(name, *stamp).await?);
        }
        Ok(tables)
    }
}

#[tokio::test]
async fn given_stamp_resolved_before_metadata_abort_when_name_is_recreated_then_retry_stays_bound()
{
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.table_name.clone();
    catalog.fail_metadata = true;
    let error = bindings
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionConflict { .. }
    ));
    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    let error = bindings
        .clone()
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .err()
        .unwrap();
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
    assert_eq!(
        catalog.metadata_reads, 1,
        "replacement metadata must not be loaded"
    );
    assert_eq!(
        catalog.batch_reads, 0,
        "single-table preparation must not batch metadata"
    );
}

#[tokio::test]
async fn given_current_prepared_metadata_when_writer_confirms_then_cached_table_is_reused() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.info.table_name.clone();
    let (prepared, publication) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .and_then(|decision| decision.into_result())
    .unwrap();
    cache.publish(publication);
    let cached = cache.prepared_candidate(&name).unwrap();
    assert!(Arc::ptr_eq(&prepared.table, &cached.table));
    cached
        .confirm(&mut catalog, &cache, &RequestTableBindings::default())
        .await
        .unwrap();
    assert_eq!(catalog.metadata_reads, 1);
}

#[tokio::test]
async fn given_changed_catalog_when_writer_confirms_then_preparation_conflicts_and_cache_is_invalidated()
 {
    for replacement in 0..3 {
        let mut catalog = Catalog::new();
        let cache = TableMetadataCache::new();
        let name = catalog.table.info.table_name.clone();
        let (prepared, publication) = resolve_prepared_table_in_attempt(
            &mut catalog,
            &cache,
            &name,
            &RequestTableBindings::default(),
        )
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
        let error = prepared
            .confirm(&mut catalog, &cache, &RequestTableBindings::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error.to_enum(),
            StorageEnum::TransactionConflict { .. }
        ));
        assert!(cache.prepared_candidate(&name).is_none());
        assert_eq!(catalog.metadata_reads, 1);
    }
}

#[tokio::test]
async fn given_bound_request_when_retry_observes_recreation_then_it_cannot_retarget_the_name() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.info.table_name.clone();
    let (prepared, _) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .unwrap()
    .into_result()
    .unwrap();
    prepared
        .confirm(&mut catalog, &cache, &bindings)
        .await
        .unwrap();

    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2_u32.into(),
    });
    catalog.stamp = Some(CatalogStamp {
        history_id: catalog.table.history_id,
        revision: NonZeroU64::new(1).unwrap(),
    });
    let (replacement, _) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .unwrap()
    .into_result()
    .unwrap();
    let error = replacement
        .confirm(&mut catalog, &cache, &bindings.clone())
        .await
        .unwrap_err();
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
}

#[tokio::test]
async fn given_bound_request_when_schema_revision_changes_then_same_id_can_retry() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.info.table_name.clone();
    let (prepared, _) = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
        .await
        .unwrap()
        .into_result()
        .unwrap();
    prepared
        .confirm(&mut catalog, &cache, &bindings)
        .await
        .unwrap();
    catalog.stamp.as_mut().unwrap().revision = NonZeroU64::new(2).unwrap();
    let (refreshed, _) = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
        .await
        .unwrap()
        .into_result()
        .unwrap();
    refreshed
        .confirm(&mut catalog, &cache, &bindings)
        .await
        .unwrap();
}

#[tokio::test]
async fn given_stale_candidate_when_confirmation_fails_then_request_has_not_bound_its_id() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.info.table_name.clone();
    let (stale, _) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .unwrap()
    .into_result()
    .unwrap();
    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2_u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    assert!(
        stale
            .confirm(&mut catalog, &cache, &bindings)
            .await
            .is_err()
    );
    let (fresh, _) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .unwrap()
    .into_result()
    .unwrap();
    fresh
        .confirm(&mut catalog, &cache, &bindings)
        .await
        .unwrap();
}

#[tokio::test]
async fn given_resolved_metadata_request_when_table_is_recreated_then_retry_rejects_new_id() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.info.table_name.clone();
    let (_, publication) = bindings
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .unwrap();
    cache.publish(publication);
    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2_u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    let error = bindings
        .clone()
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .err()
        .unwrap();
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
}

#[tokio::test]
async fn given_batch_stamps_before_metadata_abort_when_a_name_is_recreated_then_retry_rejects_it_before_another_metadata_batch()
 {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let names = [catalog.table.table_name.clone()];
    catalog.fail_metadata = true;
    let error = crate::resolve_tables_batch_in_attempt(&mut catalog, &cache, &names, &bindings)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionConflict { .. }
    ));
    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    let results =
        crate::resolve_tables_batch_in_attempt(&mut catalog, &cache, &names, &bindings.clone())
            .await
            .unwrap();
    assert!(matches!(
        results.into_iter().next().unwrap().err().unwrap().to_enum(),
        StorageEnum::TableNotFound { .. }
    ));
    assert_eq!(catalog.metadata_reads, 1);
    assert_eq!(
        catalog.batch_reads, 1,
        "the replacement cannot reach metadata I/O"
    );
}

#[tokio::test]
async fn given_cold_item_preparation_aborts_after_its_stamp_when_name_is_recreated_then_writer_cannot_accept_the_replacement()
 {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let name = catalog.table.table_name.clone();
    catalog.fail_metadata = true;
    let error = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error.to_enum(),
        StorageEnum::TransactionConflict { .. }
    ));
    catalog.table = Arc::new(BoundTableInfo {
        info: Arc::clone(&catalog.table.info),
        history_id: 2u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    let result =
        match resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings).await {
            Ok(decision) => match decision.into_result() {
                Ok((prepared, _)) => prepared.confirm(&mut catalog, &cache, &bindings).await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
    assert!(matches!(
        result.unwrap_err().to_enum(),
        StorageEnum::TableNotFound { .. }
    ));
    assert_eq!(
        catalog.metadata_reads, 1,
        "the replacement must not be loaded"
    );
}

#[tokio::test]
async fn given_cached_metadata_when_confirmation_observes_recreation_then_retry_cannot_retarget_a_second_recreation()
 {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.table_name.clone();
    let (stale, publication) = resolve_prepared_table_in_attempt(
        &mut catalog,
        &cache,
        &name,
        &RequestTableBindings::default(),
    )
    .await
    .unwrap()
    .into_result()
    .unwrap();
    cache.publish(publication);
    let bindings = RequestTableBindings::default();
    for id in [2u32, 3] {
        catalog.table = Arc::new(BoundTableInfo {
            info: Arc::clone(&catalog.table.info),
            history_id: id.into(),
        });
        catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
        if id == 2 {
            assert!(matches!(
                stale
                    .confirm(&mut catalog, &cache, &bindings)
                    .await
                    .unwrap_err()
                    .to_enum(),
                StorageEnum::TransactionConflict { .. }
            ));
        }
    }
    let error = bindings
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .err()
        .expect("the stamp observed during stale preparation confirmation must remain bound");
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
    assert_eq!(
        catalog.metadata_reads, 1,
        "replacement metadata was loaded after the first authoritative ID changed"
    );
}

#[test]
fn given_a_rejected_batch_confirmation_when_all_stamps_were_observed_then_later_names_remain_pinned()
 {
    let first = TableName::new("first");
    let second = TableName::new("second");
    let stamp = |id: u32| CatalogStamp {
        history_id: id.into(),
        revision: NonZeroU64::new(1).unwrap(),
    };
    for already_bound in [false, true] {
        let cache = TableMetadataCache::new();
        let bindings = RequestTableBindings::default();
        if already_bound {
            bindings.observe(&first, 1u32.into()).unwrap();
        }
        let error = bindings
            .confirm_observations(
                &cache,
                [
                    crate::dynamo_table_bindings::CatalogConfirmation {
                        name: &first,
                        prepared: Some(stamp(1)),
                        observed: Some(stamp(2)),
                    },
                    crate::dynamo_table_bindings::CatalogConfirmation {
                        name: &second,
                        prepared: Some(stamp(3)),
                        observed: Some(stamp(4)),
                    },
                ],
            )
            .unwrap_err();
        if already_bound {
            assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
        } else {
            assert!(matches!(
                error.to_enum(),
                StorageEnum::TransactionConflict { .. }
            ));
        }
        bindings.observe(&second, 4u32.into()).unwrap();
        assert!(matches!(
            bindings
                .observe(&second, 5u32.into())
                .unwrap_err()
                .to_enum(),
            StorageEnum::TableNotFound { .. }
        ));
    }
}
