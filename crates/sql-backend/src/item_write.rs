//! Protected SQL item and deleted-revision observations for a native write.

use storage_backfill::GsiProjectionLifecycle;
use storage_common::{TtlConfigRecord, TtlIndexWrite, TtlIndexWriteBatch};
use storage_provider::{
    AcceptedUpsert, CATALOG_READ_BYTES, ItemPrimaryEffects, ItemWriteObservation,
    ItemWriteObservationAttempt, ItemWriteObservationRead, StreamTrimScope, StreamTrimState,
    UpsertAttempt, combine_write_observations, lifecycle_tables_for_writes,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, StoredWireItem, TableName};

use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlRow, SqlStatement, SqlValue,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    gsi::{decode_gsi_lifecycle, gsi_lifecycle_statement},
    item_read::point_read_statement,
    item_tombstone::{
        decode::{decode_deleted, decode_provenance, decode_revision},
        sql_statements::deleted_observation_statement,
    },
    retention, stream_history, ttl_policy,
};

pub(crate) const WRITE_COLUMNS: &str =
    "item_descriptor, __aux_indexers, __aux_revision, __aux_provenance, __aux_ttl_epoch";

struct WriteObservationReads<'a> {
    statements: Vec<(SqlStatement, ReadProtection)>,
    lifecycle_tables: Vec<&'a BoundTableInfo>,
}

fn plan_write_observation_reads<'a>(
    engine: impl SqlLayout,
    reads: &[ItemWriteObservationRead<'a>],
) -> StorageResult<WriteObservationReads<'a>> {
    let lifecycle_tables = lifecycle_tables_for_writes(reads)?;
    let count = reads
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(lifecycle_tables.len()))
        .and_then(|count| {
            count.checked_add(usize::from(
                engine.engine() == SqlEngine::Postgres && !reads.is_empty(),
            ))
        })
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    if engine.engine() == SqlEngine::Postgres && !reads.is_empty() {
        statements.push((
            crate::item_lock::item_lock_statement(
                reads.iter().map(ItemWriteObservationRead::target),
            )?,
            ReadProtection::Snapshot,
        ));
    }
    for read in reads {
        let (table, key) = read.target();
        statements.push((
            point_read_statement(engine, table, key, WRITE_COLUMNS)?,
            ReadProtection::Conflict,
        ));
        statements.push((
            deleted_observation_statement(engine, table, key)?,
            ReadProtection::Conflict,
        ));
    }
    statements.extend(
        lifecycle_tables
            .iter()
            .filter_map(|table| gsi_lifecycle_statement(engine, table))
            .map(|statement| (statement, ReadProtection::Snapshot)),
    );
    Ok(WriteObservationReads {
        statements,
        lifecycle_tables,
    })
}

fn decode_write_observations(
    reads: &[ItemWriteObservationRead<'_>],
    lifecycle_tables: &[&BoundTableInfo],
    observed: (Vec<SqlQueryPage>, Vec<Option<StoredWireItem>>),
    lifecycles: &mut Vec<(TableName, GsiProjectionLifecycle)>,
) -> StorageResult<Vec<ItemWriteObservation>> {
    let (pages, hydrated) = observed;
    let mut hydrated = hydrated.into_iter();
    let mut pages = pages.into_iter();
    let mut items = Vec::new();
    items
        .try_reserve_exact(reads.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for _read in reads {
        let live = decode_one(
            pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL live observation page is missing"))?,
        )?;
        let deleted =
            decode_one(pages.next().ok_or_else(|| {
                StorageError::internal("SQL revision observation page is missing")
            })?)?;
        if live.is_some() && deleted.is_some() {
            return Err(StorageError::internal(
                "SQL key has both a live item and a tombstone",
            ));
        }
        let stored = hydrated
            .next()
            .ok_or_else(|| StorageError::internal("Missing SQL observation hydration slot"))?;
        items.push(match live {
            Some(row) => decode_live(
                row,
                stored.ok_or_else(|| StorageError::internal("Missing SQL observation content"))?,
            )?,
            None => match deleted {
                Some(row) => decode_deleted(row)?,
                None => ItemWriteObservation::Absent,
            },
        });
    }
    for table in lifecycle_tables {
        let page = pages
            .next()
            .ok_or_else(|| StorageError::internal("SQL GSI observation page is missing"))?;
        let lifecycle = decode_gsi_lifecycle(page, table)?;
        lifecycles.push((table.info.table_name.clone(), lifecycle));
    }
    Ok(items)
}

impl<A: SqlAttempt, L: SqlLayout> ItemWriteObservationAttempt for SqlCatalogAttempt<'_, A, L> {
    fn item_write_cache(&self) -> Option<&storage_provider::TableMetadataCache> {
        self.metadata_cache.as_deref()
    }

    async fn read_item_write_prerequisites(
        &mut self,
        table: &BoundTableInfo,
        retention_scope: Option<&StreamTrimScope>,
        needs_ttl: bool,
    ) -> StorageResult<(Option<TtlConfigRecord>, Option<StreamTrimState>)> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            // The history scope and TTL policy are independent prerequisites.
            // Later members reuse this attempt's history observation.
            let needs_history = catalog.dynamo_history.is_none();
            let mut statements = Vec::new();
            statements
                .try_reserve_exact(
                    usize::from(needs_ttl)
                        + usize::from(retention_scope.is_some())
                        + usize::from(needs_history),
                )
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            if needs_ttl {
                statements.push((
                    ttl_policy::ttl_config_statement(catalog.engine, table),
                    ReadProtection::Snapshot,
                ));
            }
            if let Some(scope) = retention_scope {
                statements.push((
                    retention::state_statement(catalog.engine, &scope.scope_id),
                    ReadProtection::Snapshot,
                ));
            }
            if needs_history {
                // Routing metadata uses the attempt snapshot without row locks.
                statements.push((
                    stream_history::history_statement(
                        catalog.engine,
                        stream_history::HistoryRead::Writer,
                    ),
                    ReadProtection::Snapshot,
                ));
            }
            let pages = if statements.is_empty() {
                Vec::new()
            } else {
                catalog
                    .attempt
                    .query_batch_protected(statements, CATALOG_READ_BYTES)
                    .await?
            };
            if pages.len()
                != usize::from(needs_ttl)
                    + usize::from(retention_scope.is_some())
                    + usize::from(needs_history)
            {
                return Err(StorageError::internal(
                    "SQL item write prerequisite count differs",
                ));
            }
            let mut pages = pages.into_iter();
            let ttl = if needs_ttl {
                let page = pages.next().ok_or_else(|| {
                    StorageError::internal("SQL TTL prerequisite page is missing")
                })?;
                ttl_policy::decode_ttl_config_page(page)?
            } else {
                None
            };
            let retention = retention_scope
                .map(|scope| {
                    pages.next().map(|page| (scope, page)).ok_or_else(|| {
                        StorageError::internal("SQL retention prerequisite page is missing")
                    })
                })
                .transpose()?
                .map(|(scope, page)| {
                    if page.more || page.rows.len() > 1 {
                        return Err(StorageError::internal("SQL retention state is not unique"));
                    }
                    page.rows
                        .into_iter()
                        .next()
                        .map(|row| retention::decode_trim_state(row, scope))
                        .transpose()
                })
                .transpose()?
                .flatten();
            if needs_history {
                let page = pages.next().ok_or_else(|| {
                    StorageError::internal("SQL history prerequisite page is missing")
                })?;
                if catalog.engine.engine() == SqlEngine::Postgres {
                    catalog.dynamo_positions = crate::catalog::DynamoStreamPositions::Native(
                        crate::stream_allocation::NativeStreamAllocation::from_history(&page)?,
                    );
                }
                catalog.dynamo_history =
                    Some(stream_history::decode_history_page(page, catalog.engine)?);
            }
            Ok((ttl, retention))
        }
        .await;
        operation.complete(result)
    }

    async fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> StorageResult<Vec<(ItemWriteObservation, GsiProjectionLifecycle)>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let WriteObservationReads {
                statements,
                lifecycle_tables,
            } = plan_write_observation_reads(catalog.engine, reads)?;
            if statements.is_empty() {
                return Ok(Vec::new());
            }
            let expected = statements.len();
            let pages = catalog
                .attempt
                .query_batch_protected(statements, CATALOG_READ_BYTES)
                .await?;
            if pages.len() != expected {
                return Err(StorageError::internal(
                    "SQL write observation page count differs",
                ));
            }
            let mut pages = pages;
            if catalog.engine.engine() == SqlEngine::Postgres {
                pages.remove(0);
            }
            let mut requests = Vec::new();
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (index, read) in reads.iter().enumerate() {
                let page = pages
                    .get(index * 2)
                    .ok_or_else(|| StorageError::internal("Missing SQL live observation page"))?;
                if page.more || page.rows.len() > 1 {
                    return Err(StorageError::internal("SQL live observation is not unique"));
                }
                requests.push((
                    read.target().0,
                    page.rows
                        .first()
                        .map(|row| row.value(crate::ColumnIndex::new(0)))
                        .transpose()?,
                    read.target().0.max_indexers,
                ));
            }
            let bound = reads
                .len()
                .checked_mul(CATALOG_READ_BYTES.get())
                .ok_or_else(|| StorageError::internal("Primary observation bound overflow"))?;
            let hydrated = crate::item_read::hydrate_item_images(catalog, requests, bound).await?;
            for (read, item) in reads.iter().zip(&hydrated) {
                if let Some(item) = item
                    && !item.item.has_verified_keys(read.target().1)?
                {
                    return Err(StorageError::internal(
                        "SQL observation content differs from physical key",
                    ));
                }
            }
            let mut lifecycles = Vec::new();
            lifecycles
                .try_reserve_exact(lifecycle_tables.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let items = decode_write_observations(
                reads,
                &lifecycle_tables,
                (pages, hydrated),
                &mut lifecycles,
            )?;
            catalog.bind_gsi_targets(
                lifecycles
                    .iter()
                    .flat_map(|(_, lifecycle)| lifecycle.writable_targets().iter().cloned()),
            )?;
            combine_write_observations(reads, items, lifecycles)
        }
        .await;
        operation.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> UpsertAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &ItemPrimaryEffects,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let mut prepared =
                std::mem::replace(&mut catalog.prepared_writes, Ok(Default::default()))?;
            let image = match prepared
                .primary
                .iter()
                .position(|write| write.matches(item))
            {
                Some(position) => prepared.primary.swap_remove(position),
                None => crate::write_preparation::PreparedPrimaryWrite::try_new(
                    catalog.engine,
                    item.into(),
                )?,
            };
            catalog.prepared_writes = Ok(prepared);
            let (primary, tombstone) = image
                .prepare(catalog, effects, item.old_item.as_ref())
                .await?;
            let ttl = ttl_index_statements(catalog.engine, &item.table, writes)?;
            let count = ttl
                .len()
                .checked_add(2)
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut statements = Vec::new();
            statements
                .try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            statements.push(primary);
            statements.push(tombstone);
            statements.extend(ttl);
            catalog.append_dynamo_statements(
                SqlDynamoWriteKind::Primary,
                statements
                    .into_iter()
                    .enumerate()
                    .map(|(index, statement)| {
                        (
                            statement,
                            if index == 0 {
                                SqlDynamoCount::ExactlyOne
                            } else {
                                SqlDynamoCount::AtMostOne
                            },
                        )
                    }),
            )?;
            Ok(())
        }
        .await;
        operation.complete(result)
    }
}

pub(crate) async fn apply_ttl_index_writes<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    writes: impl IntoIterator<Item = TtlIndexWrite>,
) -> StorageResult<()> {
    let statements = ttl_index_statements(engine, table, writes)?;
    let expected = statements.len();
    let counts = attempt.execute_batch(statements).await?;
    if counts.len() != expected || counts.into_iter().any(|count| count > 1) {
        return Err(StorageError::internal(
            "SQL TTL write changed the wrong row count",
        ));
    }
    Ok(())
}

pub(crate) fn ttl_index_statements(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    writes: impl IntoIterator<Item = TtlIndexWrite>,
) -> StorageResult<Vec<SqlStatement>> {
    let table = engine.ttl_table_name(&table.table_name, table.history_id);
    let mut statements = Vec::new();
    for write in writes {
        let (upsert, key) = match write {
            TtlIndexWrite::Delete(key) => (false, key),
            TtlIndexWrite::Upsert(key) => (true, key),
        };
        let (value, token): (i64, String) = key.into();
        let first = engine.engine().placeholder(1);
        let second = engine.engine().placeholder(2);
        let sql = if upsert {
            // Postgres joins a concurrent TTL sweep row lock; SQLite and
            // Turso settle the conflict through their native transaction.
            let conflict = match engine.engine() {
                SqlEngine::Postgres => {
                    "ON CONFLICT (ttl_value, key_token) DO UPDATE SET ttl_value = \
                     EXCLUDED.ttl_value WHERE FALSE"
                }
                SqlEngine::Sqlite | SqlEngine::Turso => "ON CONFLICT DO NOTHING",
            };
            format!(
                "INSERT INTO \"{table}\" (ttl_value, key_token) VALUES ({first}, {second}) \
                 {conflict}"
            )
        } else {
            format!("DELETE FROM \"{table}\" WHERE ttl_value = {first} AND key_token = {second}")
        };
        let mut binds = Vec::new();
        binds
            .try_reserve_exact(2)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        binds.push(SqlValue::Integer(value));
        binds.push(SqlValue::Text(token));
        statements
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        statements.push(SqlStatement::new(sql, binds));
    }
    Ok(statements)
}

fn decode_one(page: SqlQueryPage) -> StorageResult<Option<SqlRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL write observation returned multiple rows",
        ));
    }
    Ok(page.rows.into_iter().next())
}

pub(crate) fn decode_live(
    row: SqlRow,
    stored: StoredWireItem,
) -> StorageResult<ItemWriteObservation> {
    let [_descriptor, indexers, revision, provenance, ttl_epoch] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL item observation width is invalid"))?;
    let SqlValue::Text(indexers) = indexers else {
        return Err(StorageError::internal(
            "SQL stored indexers have invalid type",
        ));
    };
    let indexers: Vec<String> = serde_json::from_str(&indexers)?;
    if indexers != stored.indexers {
        return Err(StorageError::internal(
            "SQL native indexers differ from retained content",
        ));
    }
    let revision = decode_revision(revision)?;
    let provenance = decode_provenance(provenance)?;
    let ttl_epoch = match ttl_epoch {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value),
        _ => {
            return Err(StorageError::internal(
                "SQL stored TTL epoch has invalid type",
            ));
        }
    };
    Ok(ItemWriteObservation::Live {
        stored: Box::new(stored),
        revision,
        provenance,
        ttl_epoch,
    })
}
