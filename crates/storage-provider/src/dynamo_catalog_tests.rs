use std::{num::NonZeroU64, sync::Arc};

use storage_types::{
    BoundTableInfo, StorageEnum, StorageResult, StoredTableInfo, TableName,
    context::WrappedError as _,
};

use crate::{
    CatalogAttempt, CatalogStamp, RequestTableBindings, TableMetadataCache,
    dynamo_catalog::resolve_prepared_table_in_attempt,
};

async fn confirm_prepared<A: CatalogAttempt>(
    prepared: &crate::dynamo_catalog::PreparedTable,
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &RequestTableBindings,
) -> StorageResult<crate::SemanticDecision<Option<crate::CachePublication>>> {
    let name = &prepared.table.info.table_name;
    let generation = cache.generation();
    let version = attempt.read_catalog_version().await?;
    let observed = if version.is_some() && version == prepared.catalog_version {
        Some(prepared.stamp)
    } else {
        attempt.read_catalog_stamp(name).await?
    };
    let decision = bindings.confirm_write_observations(
        cache,
        [crate::dynamo_table_bindings::CatalogConfirmation {
            name,
            prepared: Some(prepared.stamp),
            observed,
        }],
    )?;
    Ok(decision.map(|()| prepared.cache_update_for_version(name, version, generation)))
}

struct Catalog {
    stamp: Option<CatalogStamp>,
    table: Arc<BoundTableInfo>,
    metadata_reads: usize,
    batch_reads: usize,
    fail_metadata: bool,
    version: Option<storage_types::CatalogVersion>,
    stamp_reads: usize,
    stamp_batches: usize,
    route_reads: usize,
    installed_routes: Option<Arc<stream_provider::SystemStreamLineageCatalog>>,
    /// PERF-FDB-1 fake: `pipeline_catalog_version` answers this.
    pipeline: bool,
    /// Set by `pipeline_catalog_version`, consumed by the next
    /// `read_primary_pairs`.
    pending: bool,
    /// What `pipelined_catalog_version` returns.
    carried: Option<Option<storage_types::CatalogVersion>>,
    /// `read_primary_pairs` fails after its "wave" (it carried the version).
    fail_pairs_after_wave: bool,
    /// `read_primary_pairs` fails before its "wave" (it carries nothing).
    fail_pairs_before_wave: bool,
    version_reads: usize,
    /// The version the caller expects; the engine drops data of a moved
    /// version.
    expected: Option<storage_types::CatalogVersion>,
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
            binding: Default::default(),
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
            version: None,
            stamp_reads: 0,
            stamp_batches: 0,
            route_reads: 0,
            installed_routes: None,
            pipeline: false,
            pending: false,
            carried: None,
            fail_pairs_after_wave: false,
            fail_pairs_before_wave: false,
            version_reads: 0,
            expected: None,
        }
    }
}

impl CatalogAttempt for Catalog {
    async fn read_writer_routes(
        &mut self,
    ) -> StorageResult<Option<Arc<stream_provider::SystemStreamLineageCatalog>>> {
        self.route_reads += 1;
        Ok(Some(Arc::new(
            stream_provider::SystemStreamLineageCatalog::initial_roots(
                storage_types::SystemStreamScope::try_new("scope", 1).unwrap(),
                storage_types::InitialStreamLineages::ONE,
            )
            .unwrap(),
        )))
    }
    fn install_writer_routes(
        &mut self,
        routes: Arc<stream_provider::SystemStreamLineageCatalog>,
    ) -> StorageResult<()> {
        self.installed_routes = Some(routes);
        Ok(())
    }
    async fn read_catalog_version(
        &mut self,
    ) -> StorageResult<Option<storage_types::CatalogVersion>> {
        self.version_reads += 1;
        Ok(self.version)
    }
    fn pipeline_catalog_version(
        &mut self,
        expected: storage_types::CatalogVersion,
    ) -> StorageResult<bool> {
        self.expected = Some(expected);
        self.pending = self.pipeline;
        Ok(self.pipeline)
    }
    fn pipelined_catalog_version(&mut self) -> Option<Option<storage_types::CatalogVersion>> {
        self.expected = None;
        self.carried.take()
    }
    async fn store_external_cleanup_barrier(
        &mut self,
        _: &TableName,
        expected: CatalogStamp,
    ) -> StorageResult<CatalogStamp> {
        if self.stamp != Some(expected) {
            return Err(
                StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                    storage_types::ConflictCause::Logical("catalog changed"),
                ))
                .into(),
            );
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
        self.stamp_reads += 1;
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
            return Err(
                StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                    storage_types::ConflictCause::Logical("injected metadata abort"),
                ))
                .into(),
            );
        }
        Ok(Arc::clone(&self.table))
    }
    async fn read_catalog_stamps(
        &mut self,
        names: &[TableName],
    ) -> StorageResult<storage_types::Exact<Option<CatalogStamp>>> {
        self.stamp_reads += names.len();
        self.stamp_batches += 1;
        storage_types::Exact::try_new(names.len(), vec![self.stamp; names.len()])
    }
    async fn read_tables_at_stamps(
        &mut self,
        requests: &[(TableName, CatalogStamp)],
    ) -> StorageResult<storage_types::Exact<Arc<BoundTableInfo>>> {
        self.batch_reads += 1;
        let mut tables = Vec::new();
        for (name, stamp) in requests {
            tables.push(self.read_table_at_stamp(name, *stamp).await?);
        }
        storage_types::Exact::try_new(requests.len(), tables)
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
        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
    ));
    catalog.table = Arc::new(BoundTableInfo {
        binding: Default::default(),
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
    confirm_prepared(
        &cached,
        &mut catalog,
        &cache,
        &RequestTableBindings::default(),
    )
    .await
    .and_then(crate::SemanticDecision::into_result)
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
        let error = confirm_prepared(
            &prepared,
            &mut catalog,
            &cache,
            &RequestTableBindings::default(),
        )
        .await
        .and_then(crate::SemanticDecision::into_result)
        .map(|_| ())
        .unwrap_err();
        assert_eq!(
            error.attempt_kind(),
            Some(storage_types::AttemptKind::Conflict)
        );
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
    confirm_prepared(&prepared, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
        .unwrap();

    catalog.table = Arc::new(BoundTableInfo {
        binding: Default::default(),
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
    let error = confirm_prepared(&replacement, &mut catalog, &cache, &bindings.clone())
        .await
        .and_then(crate::SemanticDecision::into_result)
        .map(|_| ())
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
    confirm_prepared(&prepared, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
        .unwrap();
    catalog.stamp.as_mut().unwrap().revision = NonZeroU64::new(2).unwrap();
    let (refreshed, _) = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
        .await
        .unwrap()
        .into_result()
        .unwrap();
    confirm_prepared(&refreshed, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
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
        binding: Default::default(),
        info: Arc::clone(&catalog.table.info),
        history_id: 2_u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    assert!(
        confirm_prepared(&stale, &mut catalog, &cache, &bindings)
            .await
            .and_then(crate::SemanticDecision::into_result)
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
    confirm_prepared(&fresh, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
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
        binding: Default::default(),
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
        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
    ));
    catalog.table = Arc::new(BoundTableInfo {
        binding: Default::default(),
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
        StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
    ));
    catalog.table = Arc::new(BoundTableInfo {
        binding: Default::default(),
        info: Arc::clone(&catalog.table.info),
        history_id: 2u32.into(),
    });
    catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
    let result =
        match resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings).await {
            Ok(decision) => match decision.into_result() {
                Ok((prepared, _)) => confirm_prepared(&prepared, &mut catalog, &cache, &bindings)
                    .await
                    .and_then(crate::SemanticDecision::into_result)
                    .map(|_| ()),
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
            binding: Default::default(),
            info: Arc::clone(&catalog.table.info),
            history_id: id.into(),
        });
        catalog.stamp.as_mut().unwrap().history_id = catalog.table.history_id;
        if id == 2 {
            assert_eq!(
                confirm_prepared(&stale, &mut catalog, &cache, &bindings)
                    .await
                    .and_then(crate::SemanticDecision::into_result)
                    .map(|_| ())
                    .unwrap_err()
                    .attempt_kind(),
                Some(storage_types::AttemptKind::Conflict)
            );
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
            assert_eq!(
                error.attempt_kind(),
                Some(storage_types::AttemptKind::Conflict)
            );
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

#[tokio::test]
async fn given_token_checked_cache_when_suffix_changes_then_stamp_is_checked_and_retag_is_deferred()
{
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.table_name.clone();
    let bindings = RequestTableBindings::default();
    let mut bytes = [1; 10];
    catalog.version = Some(storage_types::CatalogVersion::FoundationDb(bytes));
    let (first, publication) =
        resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
            .await
            .unwrap()
            .into_result()
            .unwrap();
    cache.publish(publication);
    let reads = catalog.stamp_reads;
    let (equal, _) = resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
        .await
        .unwrap()
        .into_result()
        .unwrap();
    assert!(Arc::ptr_eq(&first.table, &equal.table));
    assert_eq!(catalog.stamp_reads, reads);
    bytes[9] += 1;
    catalog.version = Some(storage_types::CatalogVersion::FoundationDb(bytes));
    let retag = confirm_prepared(&equal, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
        .unwrap();
    assert!(retag.is_some());
    assert_eq!(catalog.stamp_reads, reads + 1);
    assert!(cache.prepared_candidate(&name).unwrap().catalog_version != catalog.version);
    cache.publish(retag);
    assert!(cache.prepared_candidate(&name).unwrap().catalog_version == catalog.version);
    assert_eq!(catalog.metadata_reads, 1);
    catalog.version = None;
    confirm_prepared(&equal, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
        .unwrap();
    assert_eq!(catalog.stamp_reads, reads + 2);
}

#[tokio::test]
async fn given_deferred_token_retag_when_local_generation_changes_then_publication_is_discarded() {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.table_name.clone();
    let bindings = RequestTableBindings::default();
    let (prepared, publication) =
        resolve_prepared_table_in_attempt(&mut catalog, &cache, &name, &bindings)
            .await
            .unwrap()
            .into_result()
            .unwrap();
    cache.publish(publication);
    catalog.version = Some(storage_types::CatalogVersion::RocksDb(
        NonZeroU64::new(1).unwrap(),
    ));
    let retag = confirm_prepared(&prepared, &mut catalog, &cache, &bindings)
        .await
        .and_then(crate::SemanticDecision::into_result)
        .unwrap();
    cache.invalidate(&name);
    cache.publish(retag);
    assert!(cache.prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_warm_routes_when_token_matches_then_arc_is_reused_and_changes_reload() {
    let cache = TableMetadataCache::new();
    let mut catalog = Catalog::new();
    catalog.version = Some(storage_types::CatalogVersion::RocksDb(
        NonZeroU64::new(1).unwrap(),
    ));
    let publication = crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
        .await
        .unwrap();
    let first = catalog.installed_routes.clone().unwrap();
    cache.publish(publication);
    assert!(
        crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
            .await
            .unwrap()
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &first,
        catalog.installed_routes.as_ref().unwrap()
    ));
    assert_eq!(catalog.route_reads, 1);
    catalog.version = Some(storage_types::CatalogVersion::RocksDb(
        NonZeroU64::new(2).unwrap(),
    ));
    let publication = crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
        .await
        .unwrap();
    assert_eq!(catalog.route_reads, 2);
    cache.invalidate_routes();
    cache.publish(publication);
    crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
        .await
        .unwrap();
    assert_eq!(catalog.route_reads, 3);
    catalog.version = None;
    crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
        .await
        .unwrap();
    crate::prepare_writer_routes_in_attempt(&mut catalog, Some(&cache))
        .await
        .unwrap();
    assert_eq!(catalog.route_reads, 5);
}

impl crate::ItemReadAttempt for Catalog {
    async fn read_primary_item(
        &mut self,
        _: &BoundTableInfo,
        _: &storage_types::KeyAttributes,
    ) -> StorageResult<Option<storage_types::WireItem>> {
        panic!("default batch must use the existing pair port")
    }
    async fn read_range_page(
        &mut self,
        _: &BoundTableInfo,
        _: &crate::BoundRangeReadPlan<'_>,
        _: &crate::BoundRangeReadPage<'_>,
    ) -> StorageResult<crate::RangeReadPage<crate::RangeReadItem>> {
        panic!("catalog fake has no range reads")
    }
    async fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &storage_types::KeyAttributes)],
    ) -> StorageResult<storage_types::Exact<Option<storage_types::WireItem>>> {
        if self.fail_pairs_before_wave {
            return Err(storage_types::StorageError::internal(
                "fake failure before the wave",
            ));
        }
        self.batch_reads += 1;
        if std::mem::take(&mut self.pending) {
            self.carried = Some(self.version);
        }
        if let (Some(expected), Some(carried)) = (self.expected, self.carried)
            && carried != Some(expected)
        {
            return Err(storage_types::StorageError::logical_conflict(
                "fake moved before decode",
            ));
        }
        if self.fail_pairs_after_wave {
            return Err(storage_types::StorageError::internal("fake stale decode"));
        }
        storage_types::Exact::try_new(reads.len(), vec![None; reads.len()])
    }
}

#[tokio::test]
async fn given_catalog_candidates_when_default_points_run_then_outputs_and_scoped_allocations_are_recorded()
 {
    use storage_types::CatalogVersion;

    use crate::{ItemReadAttempt as _, PrimaryReadBatch, TableReadCandidate};
    for case in ["equal", "suffix", "missing_version", "missing_stamp"] {
        let mut catalog = Catalog::new();
        let version = CatalogVersion::FoundationDb([1; 10]);
        catalog.version = Some(version);
        if case == "suffix" {
            let mut bytes = [1; 10];
            bytes[9] = 2;
            catalog.version = Some(CatalogVersion::FoundationDb(bytes));
        } else if case == "missing_version" {
            catalog.version = None;
        } else if case == "missing_stamp" {
            catalog.version = None;
            catalog.stamp = None;
        }
        let table = Arc::clone(&catalog.table);
        let stamp = CatalogStamp {
            history_id: table.history_id,
            revision: NonZeroU64::new(1).unwrap(),
        };
        let candidates = [TableReadCandidate {
            table: &table,
            stamp,
            catalog_version: Some(version),
        }];
        let key = storage_types::KeyAttributes::default();
        let reads = [(&*table, &key)];
        let guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "metadata_default_phase",
            file!(),
            line!(),
            Some(case),
        );
        let result = catalog
            .finish_primary_read(PrimaryReadBatch {
                tables: &candidates,
                reads: &reads,
            })
            .await
            .unwrap();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert_eq!(result.catalog_version, catalog.version);
        assert!(result.stamps.unwrap() == [catalog.stamp]);
        assert_eq!(catalog.stamp_reads, usize::from(case != "equal"));
        assert_eq!(catalog.batch_reads, usize::from(case != "missing_stamp"));
        if case == "missing_stamp" {
            assert!(result.result.is_err());
        } else {
            let items = result.result.unwrap();
            assert_eq!(items.len(), 1);
            assert!(items[0].is_none());
        }
    }
}

#[tokio::test]
async fn given_mixed_candidates_when_stamps_are_observed_then_one_fallback_batch_preserves_positions()
 {
    use storage_types::CatalogVersion;

    use crate::TableReadCandidate;
    let mut catalog = Catalog::new();
    let table = Arc::clone(&catalog.table);
    let version = CatalogVersion::FoundationDb([1; 10]);
    let mut suffix = [1; 10];
    suffix[9] = 2;
    let warm_stamp = CatalogStamp {
        history_id: table.history_id,
        revision: NonZeroU64::new(2).unwrap(),
    };
    let candidate = TableReadCandidate {
        table: &table,
        stamp: warm_stamp,
        catalog_version: Some(version),
    };
    let names =
        ["warm", "cold", "stale", "unversioned"].map(|name| TableName::try_from(name).unwrap());
    let candidates = [
        Some(candidate),
        None,
        Some(TableReadCandidate {
            catalog_version: Some(CatalogVersion::FoundationDb(suffix)),
            ..candidate
        }),
        Some(TableReadCandidate {
            catalog_version: None,
            ..candidate
        }),
    ];
    let stamps = crate::dynamo_catalog::observe_catalog_stamps(
        &mut catalog,
        names.iter().zip(candidates),
        Some(version),
    )
    .await
    .unwrap();
    assert!(
        stamps
            == [
                Some(warm_stamp),
                catalog.stamp,
                catalog.stamp,
                catalog.stamp
            ]
    );
    assert_eq!(catalog.stamp_reads, 3);
    assert_eq!(catalog.stamp_batches, 1);
}

#[tokio::test]
async fn given_duplicate_names_when_tables_are_resolved_then_one_observation_and_metadata_load_are_reused()
 {
    let mut catalog = Catalog::new();
    let name = catalog.table.table_name.clone();
    let cache = TableMetadataCache::new();
    let bindings = RequestTableBindings::default();
    let results = crate::dynamo_catalog::resolve_prepared_tables_in_attempt(
        &mut catalog,
        &cache,
        &[name.clone(), name],
        &bindings,
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 2);
    let tables = results
        .into_iter()
        .map(|result| result.unwrap().0)
        .collect::<Vec<_>>();
    assert!(Arc::ptr_eq(&tables[0].table, &tables[1].table));
    assert!(tables[0].stamp == tables[1].stamp);
    assert_eq!(catalog.stamp_reads, 1);
    assert_eq!(catalog.stamp_batches, 1);
    assert_eq!(catalog.metadata_reads, 1);
    assert_eq!(catalog.batch_reads, 1);
}

#[tokio::test]
async fn given_provisional_multi_read_key_error_when_authority_changes_then_confirmation_precedes_error_acceptance()
 {
    for operation in ["batch", "transact"] {
        for changed in [false, true] {
            let mut catalog = Catalog::new();
            let info = Arc::make_mut(&mut Arc::make_mut(&mut catalog.table).info);
            info.key_schema = vec![storage_types::KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: storage_types::KeyType::Hash,
            }];
            info.attribute_definitions = vec![storage_types::AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: storage_types::KeyAttributeType::S,
            }];
            let cache = TableMetadataCache::new();
            let name = catalog.table.info.table_name.clone();
            let (_, publication) = RequestTableBindings::default()
                .resolve_table(&mut catalog, &cache, &name)
                .await
                .unwrap();
            cache.publish(publication);
            if changed {
                catalog.stamp.as_mut().unwrap().revision = NonZeroU64::new(2).unwrap();
            }
            let metadata_reads = catalog.metadata_reads;
            let result = if operation == "batch" {
                let plan = crate::BatchGetPlan::from(
                    serde_json::from_value::<storage_types::BatchGetItemRequest>(
                        serde_json::json!({
                            "RequestItems": {name.as_ref(): {"Keys": [{"wrong": {"S": "key"}}]}}
                        }),
                    )
                    .unwrap(),
                );
                crate::dynamo_batch_get::finish_batch_get_item_in_attempt(
                    &mut catalog,
                    &cache,
                    &plan,
                    &RequestTableBindings::default(),
                )
                .await
                .map(|_| ())
            } else {
                let plan = crate::TransactGetPlan::try_from(serde_json::from_value::<storage_types::TransactGetItemsRequest>(serde_json::json!({
                    "TransactItems": [{"Get": {"TableName": name, "Key": {"wrong": {"S": "key"}}}}]
                })).unwrap()).unwrap();
                crate::dynamo_transact_get::finish_transact_get_items_in_attempt(
                    &mut catalog,
                    &cache,
                    &plan,
                    &RequestTableBindings::default(),
                )
                .await
                .map(|_| ())
            };
            let error = result.unwrap_err();
            assert_eq!(
                error.attempt_kind() == Some(storage_types::AttemptKind::Conflict),
                changed,
                "{operation}: {error:?}"
            );
            assert_eq!(
                catalog.metadata_reads, metadata_reads,
                "warm path does not reload metadata"
            );
            assert_eq!(
                catalog.batch_reads,
                usize::from(!changed),
                "changed authority prevents even the empty pair call"
            );
        }
    }
}

#[test]
fn given_cached_identity_when_attempt_is_discarded_then_fresh_recreation_can_bind() {
    let bindings = RequestTableBindings::default();
    let name = TableName::try_from("cached-identity").unwrap();
    let cached = bindings.for_write_attempt(storage_types::ReadVersionPolicy::Cached);
    cached.observe(&name, 1_u32.into()).unwrap();
    drop(cached);
    bindings.observe(&name, 2_u32.into()).unwrap();
    assert!(bindings.observe(&name, 1_u32.into()).is_err());
}

#[test]
fn given_fresh_identity_when_cached_attempt_sees_another_incarnation_then_real_pin_survives() {
    let bindings = RequestTableBindings::default();
    let name = TableName::try_from("fresh-identity").unwrap();
    bindings.observe(&name, 2_u32.into()).unwrap();
    let cached = bindings.for_write_attempt(storage_types::ReadVersionPolicy::Cached);
    assert!(cached.observe(&name, 1_u32.into()).is_err());
    drop(cached);
    bindings.observe(&name, 2_u32.into()).unwrap();
    assert!(bindings.observe(&name, 1_u32.into()).is_err());
}

#[test]
fn given_validated_cached_identity_when_promoted_then_recreation_cannot_retarget() {
    let bindings = RequestTableBindings::default();
    let name = TableName::try_from("promoted-identity").unwrap();
    let cached = bindings.for_write_attempt(storage_types::ReadVersionPolicy::Cached);
    cached.observe(&name, 1_u32.into()).unwrap();
    cached.promote();
    bindings.observe(&name, 1_u32.into()).unwrap();
    assert!(bindings.observe(&name, 2_u32.into()).is_err());
}

#[test]
fn given_cached_mismatch_against_real_pin_when_confirmed_then_rejection_stays_nested() {
    use crate::dynamo_table_bindings::CatalogConfirmation;
    let bindings = RequestTableBindings::default();
    let name = TableName::try_from("confirmed-identity").unwrap();
    bindings.observe(&name, 2_u32.into()).unwrap();
    let stamp = CatalogStamp {
        history_id: 1_u32.into(),
        revision: NonZeroU64::new(1).unwrap(),
    };
    let cached = bindings.for_write_attempt(storage_types::ReadVersionPolicy::Cached);
    let decision = cached
        .confirm_write_observations(
            &TableMetadataCache::new(),
            [CatalogConfirmation {
                name: &name,
                prepared: Some(stamp),
                observed: Some(stamp),
            }],
        )
        .unwrap();
    assert!(matches!(decision, crate::SemanticDecision::Rejected(_)));
    bindings.observe(&name, 2_u32.into()).unwrap();
}

fn missing_relation() -> storage_types::StorageError {
    storage_types::StorageError::from(storage_types::AttemptFailure::FreshValidation(
        storage_types::FreshCause::MissingRelation(storage_types::NativeCause {
            engine: storage_types::NativeEngine::Postgres,
            code: storage_types::NativeCode::sql_state("42P01"),
            phase: storage_types::NativeCallPhase::Statement,
        }),
    ))
}

fn error_of<T>(result: StorageResult<T>) -> storage_types::StorageError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("expected an error"),
    }
}

/// A cache holding the fixture table, its name, candidate and generation.
async fn warm_candidate() -> (
    TableMetadataCache,
    TableName,
    crate::dynamo_catalog::PreparedTable,
    u64,
) {
    let mut catalog = Catalog::new();
    let cache = TableMetadataCache::new();
    let name = catalog.table.info.table_name.clone();
    let (_, publication) = RequestTableBindings::default()
        .resolve_table(&mut catalog, &cache, &name)
        .await
        .unwrap();
    cache.publish(publication);
    let (candidate, generation) = cache.candidate(&name);
    (cache, name, candidate.unwrap(), generation)
}

fn changed_stamp(stamp: CatalogStamp) -> CatalogStamp {
    let mut changed = stamp;
    changed.revision = NonZeroU64::new(changed.revision.get() + 1).unwrap();
    changed
}

fn exact_stamps(
    stamps: Option<Vec<Option<CatalogStamp>>>,
) -> Option<storage_types::Exact<Option<CatalogStamp>>> {
    stamps.map(|stamps| storage_types::Exact::try_new(stamps.len(), stamps).unwrap())
}

fn read_with(
    cache: &TableMetadataCache,
    candidate: &crate::dynamo_catalog::PreparedTable,
    generation: u64,
    stamps: Option<Vec<Option<CatalogStamp>>>,
    error: storage_types::StorageError,
) -> storage_types::StorageError {
    error_of(crate::dynamo_read_execution::confirm_read_observation(
        cache,
        &RequestTableBindings::default(),
        &[(candidate.clone(), generation)],
        crate::ReadObservationResult {
            catalog_version: None,
            stamps: exact_stamps(stamps),
            result: Err::<(), _>(error),
        },
    ))
}

fn write_with(
    cache: &TableMetadataCache,
    name: &TableName,
    generation: u64,
    stamps: Option<Vec<Option<CatalogStamp>>>,
    candidates: Vec<usize>,
    error: storage_types::StorageError,
) -> storage_types::StorageError {
    let tables = crate::dynamo_write_preparation::PreparedWriteTables::from_prepared(
        cache.prepared_candidate(name).unwrap(),
    );
    error_of(tables.accept_observations(
        cache,
        &RequestTableBindings::default(),
        crate::WriteObservationBatchResult {
            receipt: None,
            catalog_version: None,
            stamps: exact_stamps(stamps),
            observations: Err(crate::WriteObservationFailure { candidates, error }),
            publications: Vec::new(),
        },
        generation,
    ))
}

#[tokio::test]
async fn given_cached_read_candidate_when_read_reports_missing_relation_then_fresh_validation_is_returned_and_cache_kept()
 {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = read_with(
        &cache,
        &candidate,
        generation,
        Some(vec![Some(candidate.stamp)]),
        missing_relation(),
    );
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_changed_stamp_when_read_reports_missing_relation_then_conflict_answers_first() {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = read_with(
        &cache,
        &candidate,
        generation,
        Some(vec![Some(changed_stamp(candidate.stamp))]),
        missing_relation(),
    );
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(cache.prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_no_stamps_when_read_reports_missing_relation_then_fresh_validation_is_returned() {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = read_with(&cache, &candidate, generation, None, missing_relation());
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_cached_read_candidate_when_read_reports_other_error_then_error_is_kept_and_cache_kept()
 {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = read_with(
        &cache,
        &candidate,
        generation,
        Some(vec![Some(candidate.stamp)]),
        storage_types::StorageError::internal("decode"),
    );
    assert_eq!(error.attempt_kind(), None);
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_prepared_write_table_when_observation_reports_missing_relation_then_fresh_validation_is_returned_and_cache_kept()
 {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = write_with(
        &cache,
        &name,
        generation,
        Some(vec![Some(candidate.stamp)]),
        vec![0],
        missing_relation(),
    );
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_changed_stamp_when_write_observation_reports_missing_relation_then_conflict_answers_first()
 {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = write_with(
        &cache,
        &name,
        generation,
        Some(vec![Some(changed_stamp(candidate.stamp))]),
        vec![0],
        missing_relation(),
    );
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
}

#[tokio::test]
async fn given_no_stamps_when_write_observation_reports_missing_relation_then_fresh_validation_is_returned()
 {
    let (cache, name, _, generation) = warm_candidate().await;
    let error = write_with(&cache, &name, generation, None, vec![0], missing_relation());
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
}

#[tokio::test]
async fn given_no_candidate_when_write_observation_reports_missing_relation_then_internal_and_cache_kept()
 {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = write_with(
        &cache,
        &name,
        generation,
        Some(vec![Some(candidate.stamp)]),
        Vec::new(),
        missing_relation(),
    );
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message }
            if message == "missing relation outside the prepared write tables"
    ));
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_prepared_write_table_when_observation_reports_other_error_then_error_is_kept() {
    let (cache, name, candidate, generation) = warm_candidate().await;
    let error = write_with(
        &cache,
        &name,
        generation,
        Some(vec![Some(candidate.stamp)]),
        vec![0],
        storage_types::StorageError::internal("decode"),
    );
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { message } if message == "decode"
    ));
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_missing_relation_when_retry_owner_invalidates_then_named_tables_are_dropped() {
    let (cache, name, _, _) = warm_candidate().await;
    crate::dynamo_read_execution::invalidate_on_missing_relation(
        &cache,
        &missing_relation(),
        [&name],
    );
    assert!(cache.prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_other_known_abort_when_retry_owner_invalidates_then_cache_is_kept() {
    let (cache, name, _, _) = warm_candidate().await;
    for error in [
        storage_types::StorageError::from(storage_types::AttemptFailure::FRESH_VALIDATION),
        storage_types::StorageError::logical_conflict("x"),
        storage_types::StorageError::from(storage_types::AttemptFailure::CAPACITY),
        storage_types::StorageError::internal("x"),
    ] {
        crate::dynamo_read_execution::invalidate_on_missing_relation(&cache, &error, [&name]);
        assert!(cache.prepared_candidate(&name).is_some(), "{error:?}");
    }
}

/// A session whose cold-cache resolution of the fixture table returned a
/// publication.
async fn resolved_session() -> (
    crate::dynamo_table_bindings::CatalogSession,
    Catalog,
    TableName,
    Option<crate::CachePublication>,
) {
    let mut catalog = Catalog::new();
    let name = catalog.table.info.table_name.clone();
    let session =
        crate::dynamo_table_bindings::CatalogSession::new(Arc::new(TableMetadataCache::new()));
    let (_, publication) = session.resolve_table(&mut catalog, &name).await.unwrap();
    assert!(publication.is_some());
    (session, catalog, name, publication)
}

#[tokio::test]
async fn given_answered_try_when_session_settles_then_publications_are_published() {
    let (session, _, name, publication) = resolved_session().await;
    assert!(session.cache().prepared_candidate(&name).is_none());

    let value = session.settle(Ok((7_u32, publication)), [&name]).unwrap();

    assert_eq!(value, 7);
    assert!(session.cache().prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_failed_try_when_session_settles_then_nothing_is_published() {
    let (session, _, name, _publication) = resolved_session().await;

    let error = session
        .settle(
            Err::<(u32, Option<crate::CachePublication>), _>(
                storage_types::StorageError::logical_conflict("x"),
            ),
            [&name],
        )
        .unwrap_err();

    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(session.cache().prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_missing_relation_when_session_settles_then_named_tables_are_invalidated() {
    let (session, _, name, publication) = resolved_session().await;
    session.settle(Ok(((), publication)), [&name]).unwrap();
    assert!(session.cache().prepared_candidate(&name).is_some());

    let error = session
        .settle(
            Err::<((), Option<crate::CachePublication>), _>(missing_relation()),
            [&name],
        )
        .unwrap_err();

    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(session.cache().prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_other_error_when_session_settles_then_cache_is_kept() {
    let (session, _, name, publication) = resolved_session().await;
    session.settle(Ok(((), publication)), [&name]).unwrap();

    let error = session
        .settle(
            Err::<((), Option<crate::CachePublication>), _>(
                storage_types::StorageError::logical_conflict("x"),
            ),
            [&name],
        )
        .unwrap_err();
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(session.cache().prepared_candidate(&name).is_some());

    let error = session
        .settle(
            Err::<((), Option<crate::CachePublication>), _>(storage_types::StorageError::from(
                storage_types::AttemptFailure::FRESH_VALIDATION,
            )),
            [&name],
        )
        .unwrap_err();
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(session.cache().prepared_candidate(&name).is_some());

    let error = session
        .settle(
            Err::<((), Option<crate::CachePublication>), _>(
                storage_types::StorageError::commit_unknown("lost reply"),
            ),
            [&name],
        )
        .unwrap_err();
    assert!(error.is_commit_unknown());
    assert!(session.cache().prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_session_clones_when_one_binds_a_table_then_the_other_sees_the_identity() {
    let mut catalog = Catalog::new();
    let name = catalog.table.info.table_name.clone();
    let session =
        crate::dynamo_table_bindings::CatalogSession::new(Arc::new(TableMetadataCache::new()));
    let other = session.clone();

    // The identity differs from the fixture table's history id (1).
    other.bindings().observe(&name, 2_u32.into()).unwrap();

    let error = error_of(session.resolve_table(&mut catalog, &name).await);
    assert!(matches!(error.to_enum(), StorageEnum::TableNotFound { .. }));
}

// PERF-FDB-1 pipelined authority (U1-U9).

type PipelinedRead =
    crate::ReadObservationResult<storage_types::Exact<Option<storage_types::WireItem>>>;

/// A pipelining fake whose catalog holds `v1`, the fixture table and its stamp.
fn pipelined_fixture() -> (
    Catalog,
    storage_types::CatalogVersion,
    storage_types::CatalogVersion,
) {
    let mut catalog = Catalog::new();
    catalog.pipeline = true;
    let v1 = storage_types::CatalogVersion::FoundationDb([1; 10]);
    let v2 = storage_types::CatalogVersion::FoundationDb([2; 10]);
    catalog.version = Some(v1);
    (catalog, v1, v2)
}

async fn finish_pipelined_points(
    catalog: &mut Catalog,
    candidate_version: Option<storage_types::CatalogVersion>,
) -> PipelinedRead {
    use crate::TableReadCandidate;
    let table = Arc::clone(&catalog.table);
    let stamp = CatalogStamp {
        history_id: table.history_id,
        revision: NonZeroU64::new(1).unwrap(),
    };
    let candidates = [TableReadCandidate {
        table: &table,
        stamp,
        catalog_version: candidate_version,
    }];
    let key = storage_types::KeyAttributes::default();
    let reads = [(&*table, &key)];
    finish_points_for(catalog, &candidates, &reads).await
}

async fn finish_points_for(
    catalog: &mut Catalog,
    candidates: &[crate::TableReadCandidate<'_>],
    reads: &[(&BoundTableInfo, &storage_types::KeyAttributes)],
) -> PipelinedRead {
    use crate::{ItemReadAttempt as _, PrimaryReadBatch};
    catalog
        .finish_primary_read(PrimaryReadBatch {
            tables: candidates,
            reads,
        })
        .await
        .unwrap()
}

/// A cache publishing the fixture table tagged `v1`, with its candidate.
fn published_candidate(
    catalog: &Catalog,
    v1: storage_types::CatalogVersion,
) -> (
    TableMetadataCache,
    TableName,
    crate::dynamo_catalog::PreparedTable,
    u64,
) {
    let table = Arc::clone(&catalog.table);
    let stamp = CatalogStamp {
        history_id: table.history_id,
        revision: NonZeroU64::new(1).unwrap(),
    };
    let name = table.info.table_name.clone();
    let cache = TableMetadataCache::new();
    let generation = cache.generation();
    let prepared = crate::dynamo_catalog::PreparedTable {
        table,
        stamp,
        catalog_version: None,
    };
    cache.publish(prepared.cache_update_for_version(&name, Some(v1), generation));
    let (candidate, generation) = cache.candidate(&name);
    (cache, name, candidate.unwrap(), generation)
}

fn confirm_points(
    cache: &TableMetadataCache,
    candidate: &crate::dynamo_catalog::PreparedTable,
    generation: u64,
    result: PipelinedRead,
) -> StorageResult<(
    storage_types::Exact<Option<storage_types::WireItem>>,
    Vec<crate::CachePublication>,
)> {
    crate::dynamo_read_execution::confirm_read_observation(
        cache,
        &RequestTableBindings::default(),
        &[(candidate.clone(), generation)],
        result,
    )
}

#[tokio::test]
async fn given_pipelined_equal_version_when_points_run_then_items_need_no_version_or_stamp_wave() {
    let (mut catalog, v1, _) = pipelined_fixture();
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    let stamp = CatalogStamp {
        history_id: catalog.table.history_id,
        revision: NonZeroU64::new(1).unwrap(),
    };
    assert_eq!(result.catalog_version, Some(v1));
    assert!(
        result
            .stamps
            .as_ref()
            .is_some_and(|stamps| *stamps == [Some(stamp)])
    );
    assert_eq!(result.result.unwrap().len(), 1);
    assert_eq!(catalog.version_reads, 0);
    assert_eq!(catalog.stamp_reads, 0);
    assert_eq!(catalog.batch_reads, 1);
}

#[tokio::test]
async fn given_pipelined_moved_version_and_equal_stamp_when_points_run_then_stamps_confirm_and_items_are_read_again()
 {
    let (mut catalog, v1, v2) = pipelined_fixture();
    let (cache, _, candidate, generation) = published_candidate(&catalog, v1);
    catalog.version = Some(v2);
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    assert_eq!(result.catalog_version, Some(v2));
    assert!(
        result
            .stamps
            .as_ref()
            .is_some_and(|stamps| *stamps == [catalog.stamp])
    );
    assert!(result.result.is_ok());
    assert_eq!(
        catalog.batch_reads, 2,
        "the dropped read, then the read again"
    );
    assert_eq!(catalog.stamp_batches, 1);
    assert_eq!(catalog.version_reads, 0);
    let (items, publications) = confirm_points(&cache, &candidate, generation, result).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(publications.len(), 1);
}

#[tokio::test]
async fn given_pipelined_moved_version_and_changed_stamp_when_points_run_then_items_are_discarded_and_conflict_answers()
 {
    let (mut catalog, v1, v2) = pipelined_fixture();
    let (cache, name, candidate, generation) = published_candidate(&catalog, v1);
    catalog.version = Some(v2);
    catalog.stamp = catalog.stamp.map(changed_stamp);
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    assert!(result.result.is_err());
    assert!(result.stamps.is_some());
    assert_eq!(catalog.batch_reads, 1);
    let error = error_of(confirm_points(&cache, &candidate, generation, result));
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(cache.prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_pipelined_moved_version_when_decode_would_fail_then_it_never_runs_and_stamp_conflict_answers()
 {
    let (mut catalog, v1, v2) = pipelined_fixture();
    let (cache, name, candidate, generation) = published_candidate(&catalog, v1);
    catalog.version = Some(v2);
    catalog.stamp = catalog.stamp.map(changed_stamp);
    catalog.fail_pairs_after_wave = true;
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    assert!(result.stamps.is_some());
    let message = result
        .result
        .as_ref()
        .err()
        .expect("read is rejected")
        .to_string();
    assert!(!message.contains("fake stale decode"), "{message}");
    let error = error_of(confirm_points(&cache, &candidate, generation, result));
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(cache.prepared_candidate(&name).is_none());
}

#[tokio::test]
async fn given_pipelined_moved_version_and_equal_stamp_when_decode_fails_on_read_again_then_genuine_error_returns()
 {
    let (mut catalog, v1, v2) = pipelined_fixture();
    let (cache, name, candidate, generation) = published_candidate(&catalog, v1);
    catalog.version = Some(v2);
    catalog.fail_pairs_after_wave = true;
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    let stamp = candidate.stamp;
    assert_eq!(catalog.batch_reads, 2);
    assert!(
        result
            .stamps
            .as_ref()
            .is_some_and(|stamps| *stamps == [Some(stamp)])
    );
    let error = error_of(confirm_points(&cache, &candidate, generation, result));
    assert!(error.to_string().contains("fake stale decode"), "{error}");
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_pipelined_read_failing_under_equal_version_when_confirmed_then_genuine_error_returns_and_cache_stays()
 {
    let (mut catalog, v1, _) = pipelined_fixture();
    let (cache, name, candidate, generation) = published_candidate(&catalog, v1);
    catalog.fail_pairs_after_wave = true;
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    let stamp = candidate.stamp;
    assert!(
        result
            .stamps
            .as_ref()
            .is_some_and(|stamps| *stamps == [Some(stamp)])
    );
    let error = error_of(confirm_points(&cache, &candidate, generation, result));
    assert!(error.to_string().contains("fake stale decode"), "{error}");
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_pipelined_read_failing_before_its_wave_when_confirmed_then_error_is_unchanged() {
    let (mut catalog, v1, v2) = pipelined_fixture();
    let (cache, name, candidate, generation) = published_candidate(&catalog, v1);
    catalog.version = Some(v2);
    catalog.fail_pairs_before_wave = true;
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    assert_eq!(result.catalog_version, None);
    assert!(result.stamps.is_none());
    let error = error_of(confirm_points(&cache, &candidate, generation, result));
    assert!(
        error.to_string().contains("fake failure before the wave"),
        "{error}"
    );
    assert!(cache.prepared_candidate(&name).is_some());
}

#[tokio::test]
async fn given_candidate_without_version_when_points_run_then_version_first_order_is_kept() {
    let (mut catalog, _, _) = pipelined_fixture();
    let result = finish_pipelined_points(&mut catalog, None).await;
    assert!(!catalog.pending);
    assert_eq!(catalog.version_reads, 1);
    assert_eq!(catalog.stamp_reads, 1);
    assert!(result.result.is_ok());
}

#[tokio::test]
async fn given_engine_without_pipelining_when_points_run_then_version_first_order_is_kept() {
    let (mut catalog, v1, _) = pipelined_fixture();
    catalog.pipeline = false;
    let result = finish_pipelined_points(&mut catalog, Some(v1)).await;
    assert!(result.result.is_ok());
    assert_eq!(catalog.version_reads, 1);
    assert_eq!(catalog.stamp_reads, 0);
    assert_eq!(catalog.batch_reads, 1);
}

#[tokio::test]
async fn given_candidates_with_different_versions_when_points_run_then_version_first_order_is_kept()
{
    use crate::TableReadCandidate;
    let (mut catalog, v1, v2) = pipelined_fixture();
    let first = Arc::clone(&catalog.table);
    let mut info = (*first.info).clone();
    info.table_name = TableName::new("prepared_2");
    let second = Arc::new(BoundTableInfo {
        binding: Default::default(),
        info: Arc::new(info),
        history_id: 2_u32.into(),
    });
    let stamp = |table: &BoundTableInfo| CatalogStamp {
        history_id: table.history_id,
        revision: NonZeroU64::new(1).unwrap(),
    };
    let candidates = [
        TableReadCandidate {
            table: &first,
            stamp: stamp(&first),
            catalog_version: Some(v1),
        },
        TableReadCandidate {
            table: &second,
            stamp: stamp(&second),
            catalog_version: Some(v2),
        },
    ];
    let key = storage_types::KeyAttributes::default();
    let reads = [(&*first, &key), (&*second, &key)];
    finish_points_for(&mut catalog, &candidates, &reads).await;
    assert!(!catalog.pending);
    assert_eq!(catalog.version_reads, 1);
}
