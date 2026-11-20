//! One physical SQL catalog layout and decoder for all three native drivers.

use std::{num::NonZeroU64, sync::Arc};

use serde::de::DeserializeOwned;
use storage_common::point_results::place_value;
use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, CatalogNamePage, CatalogStamp, CatalogStreamPage,
    CreateTableLayout, ListingAttempt,
};
use storage_types::{
    BoundTableInfo, GsiPhase, MaxIndexers, MissExpectation, StorageEnum, StorageError,
    StorageResult, StoredGsiBinding, StoredTableInfo, StreamRetentionDuration, SystemStreamShardId,
    TableBinding, TableHistoryId, TableName, TableStatus, TimestampMillis,
    context::WrappedError as _, storage_serde,
};
use stream_provider::SystemStreamHistory;

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlRow,
    SqlStatement, SqlValue,
    item_schema::{render_gsi_index, render_item_table, render_ttl_index},
    stream_history,
};

/// An operation lends its native SQL attempt to this catalog lowerer. Write
/// operations use conflict protection; read operations use their snapshot.
pub struct SqlCatalogAttempt<'a, A, L: SqlLayout = SqlEngine> {
    pub(crate) attempt: &'a mut A,
    pub(crate) engine: L,
    pub(crate) protection: ReadProtection,
    pub(crate) catalog_lock: crate::catalog_lock::CatalogLockState,
    pub(crate) dynamo_writes: PendingDynamoWrites,
    primary_upserts: crate::write_batch::UpsertGroups,
    primary_deletes: crate::write_batch::DeleteGroups,
    pub(crate) dynamo_history: Option<SystemStreamHistory>,
    pub(crate) dynamo_positions: DynamoStreamPositions,
    pub(crate) prepared_writes: StorageResult<crate::write_preparation::SqlPreparedWrites>,
    pub(crate) deferred_ddl: Vec<crate::ddl::SqlDdl>,
    /// `ALTER TABLE` statements of this attempt, run as its last batch.
    pub(crate) layout_ddl: Vec<SqlStatement>,
    pub(crate) queue_wakes: queue_provider::QueueWakeSlots,
    /// The queue port's catalog hints and this attempt's start time (W7 P7);
    /// `None` outside `with_attempt`.
    pub(crate) queue_catalog_hints: Option<(
        std::sync::Arc<queue_provider::QueueCatalogHints>,
        storage_types::TimestampMillis,
    )>,
    /// A hinted catalog row that no read of this attempt has verified yet.
    pub(crate) queue_catalog_check: Option<queue_provider::QueueCatalogRecord>,
}

pub(crate) enum PendingDynamoWrites {
    KeepOpen(Vec<(SqlStatement, SqlDynamoCount, SqlDynamoWriteKind)>),
    Commit(Vec<crate::SqlCountedWrite>),
}

impl PendingDynamoWrites {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::KeepOpen(writes) => writes.is_empty(),
            Self::Commit(writes) => writes.is_empty(),
        }
    }

    pub(crate) fn stage(
        &mut self,
        ordinary: impl FnOnce() -> StorageResult<(SqlStatement, SqlDynamoCount, SqlDynamoWriteKind)>,
        counted: impl FnOnce() -> StorageResult<crate::SqlCountedWrite>,
    ) -> StorageResult<()> {
        match self {
            Self::KeepOpen(writes) => {
                writes
                    .try_reserve(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.push(ordinary()?);
            }
            Self::Commit(writes) => {
                writes
                    .try_reserve(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.push(counted()?);
            }
        }
        Ok(())
    }
}

pub(crate) enum DynamoStreamPositions {
    Sequential(Vec<(SystemStreamShardId, NonZeroU64, NonZeroU64)>),
    Native(crate::stream_allocation::StreamEventNumbers, i64),
}

#[derive(Clone, Copy)]
pub(crate) enum SqlDynamoCount {
    ExactlyOne,
    ExactlyRows(usize),
    AtMostRows(usize),
    /// Any count (a cleanup delete of every row of a scope): no assertion.
    Unbounded,
}

impl SqlDynamoCount {
    fn rejects(self, changed: u64) -> bool {
        match self {
            Self::ExactlyOne => changed != 1,
            Self::ExactlyRows(rows) => changed != u64::try_from(rows).unwrap_or(u64::MAX),
            Self::AtMostRows(rows) => changed > u64::try_from(rows).unwrap_or(u64::MAX),
            Self::Unbounded => false,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SqlDynamoWriteKind {
    SystemSequence,
    Primary,
    Stream,
    Receipt,
    Pubsub,
}

impl SqlDynamoWriteKind {
    pub(crate) fn error_factory(self) -> fn() -> StorageError {
        match self {
            Self::Receipt => crate::transact_write::receipt_count_error,
            Self::Pubsub => || StorageError::write_miss("pubsub_record", MissExpectation::Present),
            Self::SystemSequence => || StorageEnum::HistoryUnavailable.into(),
            Self::Primary => {
                || StorageError::internal("SQL primary write affected the wrong row count")
            }
            Self::Stream => || {
                StorageError::internal("SQL Dynamo stream write affected an unexpected row count")
            },
        }
    }

    fn error(self) -> StorageError {
        (self.error_factory())()
    }
}

impl<'a, A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'a, A, L> {
    pub fn new(attempt: &'a mut A, engine: L, request: storage_types::AttemptRequest) -> Self {
        Self {
            attempt,
            engine,
            protection: request.mode.read_protection(),
            catalog_lock: crate::catalog_lock::CatalogLockState::new(engine.engine(), request),
            dynamo_writes: PendingDynamoWrites::KeepOpen(Vec::new()),
            primary_upserts: crate::write_batch::UpsertGroups::default(),
            primary_deletes: crate::write_batch::DeleteGroups::default(),
            dynamo_history: None,
            dynamo_positions: DynamoStreamPositions::Sequential(Vec::new()),
            prepared_writes: Ok(Default::default()),
            deferred_ddl: Vec::new(),
            layout_ddl: Vec::new(),
            queue_wakes: Default::default(),
            queue_catalog_hints: None,
            queue_catalog_check: None,
        }
    }

    pub(crate) fn new_complete_write(
        attempt: &'a mut A,
        engine: L,
        request: storage_types::AttemptRequest,
    ) -> Self {
        let mut catalog = Self::new(attempt, engine, request);
        if engine.engine() == crate::SqlEngine::Postgres {
            catalog.dynamo_writes = PendingDynamoWrites::Commit(Vec::new());
        }
        catalog
    }

    pub(crate) fn stage_primary_upsert(
        &mut self,
        row: crate::write_batch::PreparedUpsertRow,
    ) -> StorageResult<()> {
        self.primary_upserts.stage(row)
    }

    pub(crate) fn stage_primary_delete(
        &mut self,
        row: crate::write_batch::PreparedDeleteRow,
    ) -> StorageResult<()> {
        self.primary_deletes.stage(row)
    }

    /// Stage one write of a pubsub attempt (W3 C8). Postgres (`Commit`
    /// mode) sends it with `COMMIT`; other engines run the staged writes as
    /// one batch just before commit. A different row count is a conflict.
    pub(crate) fn stage_pubsub_write(
        &mut self,
        statement: SqlStatement,
        count: SqlDynamoCount,
    ) -> StorageResult<()> {
        match &mut self.dynamo_writes {
            PendingDynamoWrites::KeepOpen(writes) => {
                writes
                    .try_reserve(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.push((statement, count, SqlDynamoWriteKind::Pubsub));
            }
            PendingDynamoWrites::Commit(writes) => {
                writes
                    .try_reserve(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.push(crate::statement::counted_write(
                    statement,
                    count,
                    SqlDynamoWriteKind::Pubsub.error_factory(),
                )?);
            }
        }
        Ok(())
    }

    /// `true` in Postgres `Commit` mode: writes staged now go with `COMMIT`.
    pub(crate) fn carries_writes_to_commit(&self) -> bool {
        matches!(self.dynamo_writes, PendingDynamoWrites::Commit(_))
    }

    pub(crate) async fn flush_dynamo_statements(&mut self) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if matches!(catalog.dynamo_writes, PendingDynamoWrites::Commit(_)) {
                return Err(StorageError::internal(
                    "complete SQL write cannot flush before finalization",
                ));
            }
            catalog.render_dynamo_writes()?;
            let PendingDynamoWrites::KeepOpen(planned) = &mut catalog.dynamo_writes else {
                unreachable!()
            };
            let planned = std::mem::take(planned);
            if planned.is_empty() {
                return Ok(());
            }
            let mut expected = Vec::new();
            let mut statements = Vec::new();
            expected
                .try_reserve_exact(planned.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            statements
                .try_reserve_exact(planned.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            for (statement, count, kind) in planned {
                expected.push((count, kind));
                statements.push(statement);
            }
            let counts = catalog.attempt.execute_batch_exact(statements).await?;
            for (count, (expected, kind)) in counts.into_iter().zip(expected) {
                if expected.rejects(count) {
                    return Err(kind.error());
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    fn render_dynamo_writes(&mut self) -> StorageResult<()> {
        let empty = match self.dynamo_writes {
            PendingDynamoWrites::KeepOpen(_) => PendingDynamoWrites::KeepOpen(Vec::new()),
            PendingDynamoWrites::Commit(_) => PendingDynamoWrites::Commit(Vec::new()),
        };
        let tail = std::mem::replace(&mut self.dynamo_writes, empty);
        let positions = match &mut self.dynamo_positions {
            DynamoStreamPositions::Sequential(positions) => std::mem::take(positions),
            DynamoStreamPositions::Native(..) => Vec::new(),
        };
        for (shard, first, next) in positions {
            self.dynamo_writes.stage(
                || {
                    Ok((
                        stream_history::sequence_advance_statement(
                            self.engine,
                            shard.as_str(),
                            first,
                            next,
                        )?,
                        SqlDynamoCount::ExactlyOne,
                        SqlDynamoWriteKind::SystemSequence,
                    ))
                },
                || {
                    stream_history::counted_sequence_advance(
                        self.engine,
                        shard.as_str(),
                        first,
                        next,
                    )
                },
            )?;
        }
        std::mem::take(&mut self.primary_upserts).for_each_batch(|shape, relation, rows| {
            match &mut self.dynamo_writes {
                PendingDynamoWrites::KeepOpen(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    let count = SqlDynamoCount::ExactlyRows(rows.len());
                    writes.push((
                        shape.render(self.engine, relation, rows)?,
                        count,
                        SqlDynamoWriteKind::Primary,
                    ));
                }
                PendingDynamoWrites::Commit(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    writes.push(shape.render_counted(self.engine, relation, rows)?);
                }
            }
            Ok(())
        })?;
        std::mem::take(&mut self.primary_deletes).for_each_batch(|shape, relation, rows| {
            match &mut self.dynamo_writes {
                PendingDynamoWrites::KeepOpen(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    let count = SqlDynamoCount::AtMostRows(rows.len());
                    writes.push((
                        shape.render(self.engine, relation, rows)?,
                        count,
                        SqlDynamoWriteKind::Primary,
                    ));
                }
                PendingDynamoWrites::Commit(writes) => {
                    writes
                        .try_reserve(1)
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    writes.push(shape.render_counted(self.engine, relation, rows)?);
                }
            }
            Ok(())
        })?;
        match (&mut self.dynamo_writes, tail) {
            (PendingDynamoWrites::KeepOpen(writes), PendingDynamoWrites::KeepOpen(tail)) => {
                writes
                    .try_reserve(tail.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.extend(tail);
            }
            (PendingDynamoWrites::Commit(writes), PendingDynamoWrites::Commit(tail)) => {
                writes
                    .try_reserve(tail.len())
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                writes.extend(tail);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub(crate) async fn finish_dynamo_statements(&mut self) -> StorageResult<()> {
        if matches!(self.dynamo_writes, PendingDynamoWrites::KeepOpen(_)) {
            return self.flush_dynamo_statements().await;
        }
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog.render_dynamo_writes()?;
            let PendingDynamoWrites::Commit(writes) = &mut catalog.dynamo_writes else {
                unreachable!()
            };
            match catalog
                .attempt
                .finish_write_batch(
                    std::mem::take(writes),
                    std::num::NonZeroUsize::MIN.saturating_add(15),
                )
                .await?
            {
                Ok(_) => Ok(()),
                Err(
                    crate::SqlWriteBatchFailure::CountAssertion { error, .. }
                    | crate::SqlWriteBatchFailure::Other { error, .. },
                ) => Err(error),
            }
        }
        .await;
        settlement.complete(result)
    }

    async fn query_one(
        &mut self,
        statement: SqlStatement,
        protection: ReadProtection,
    ) -> StorageResult<Option<SqlRow>> {
        let page = self
            .attempt
            .query(statement, protection, CATALOG_READ_BYTES)
            .await?;
        single_catalog_row(page)
    }

    /// Read one catalog row of `name` in the window that takes its catalog
    /// lock. `protection` is the row protection without the lock.
    async fn query_one_after_catalog_lock(
        &mut self,
        name: &TableName,
        statement: SqlStatement,
        protection: ReadProtection,
    ) -> StorageResult<Option<SqlRow>> {
        let protection = self.catalog_lock.row_protection(protection);
        let [page] = self
            .query_with_catalog_lock(&[name], vec![(statement, protection)], CATALOG_READ_BYTES)
            .await?
            .into_array::<1>()?;
        single_catalog_row(page)
    }

    async fn query_catalog_batch(
        &mut self,
        names: &[TableName],
        statement: SqlStatement,
        max_bytes: std::num::NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        let protection = self.catalog_lock.row_protection(self.protection.shared());
        let names = names.iter().collect::<Vec<_>>();
        let [page] = self
            .query_with_catalog_lock(&names, vec![(statement, protection)], max_bytes)
            .await?
            .into_array::<1>()?;
        Ok(page)
    }

    /// Lower one validated table image to the catalog and physical item/GSI
    /// relations inside the caller's native write attempt. Stream retention
    /// and GSI publication are separate effects in that same attempt.
    pub async fn read_table_layout(
        &mut self,
        table: &StoredTableInfo,
    ) -> StorageResult<CreateTableLayout<()>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let statement = SqlStatement::new(
                format!(
                    "SELECT id FROM {relation_tables} WHERE table_name = {} AND table_status <> \
                     'DELETING'",
                    catalog.engine.engine().placeholder(1),
                    relation_tables = catalog.engine.relation_name("tables"),
                ),
                vec![SqlValue::Text(table.table_name.to_string())],
            );
            let row = catalog
                .query_one_after_catalog_lock(
                    &table.table_name,
                    statement,
                    ReadProtection::Conflict,
                )
                .await?;
            Ok(CreateTableLayout {
                name_in_use: row.is_some(),
                seed: (),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub async fn insert_table_layout(
        &mut self,
        table: &StoredTableInfo,
        history_id: TableHistoryId,
        regional: Option<std::sync::Arc<storage_types::RegionalBinding>>,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut binding = initial_binding(table, history_id)?;
            binding.regional = regional;
            let layout = vec![
                insert_table_statement(catalog.engine, table, history_id, &binding)?,
                SqlStatement::new(
                    render_item_table(catalog.engine, table, &binding, history_id),
                    vec![],
                ),
                SqlStatement::new(render_ttl_index(catalog.engine, history_id), vec![]),
                crate::stream_write::sql_statements::initialize_sequence(
                    catalog.engine,
                    &crate::stream_write::sql_statements::user_sequence_name(
                        &history_id.append_stream_name(),
                    ),
                ),
            ];
            // The table is empty inside its creation change, so a plain
            // CREATE INDEX builds each GSI partial index (W6F section 4.3).
            let dependent_layout = binding
                .gsis
                .iter()
                .filter(|gsi| !gsi.name.is_ttl_index())
                .map(|gsi| {
                    render_gsi_index(catalog.engine, table, history_id, gsi)
                        .map(|sql| SqlStatement::new(sql, vec![]))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let counts = catalog
                .execute_catalog_change(&table.table_name, layout)
                .await?;
            if counts.first() != Some(&1) {
                return Err(StorageError::internal(
                    "SQL table layout batch did not insert its catalog record",
                ));
            }
            if !dependent_layout.is_empty() {
                let expected = dependent_layout.len();
                let counts = catalog.attempt.execute_batch(dependent_layout).await?;
                if counts.len() != expected {
                    return Err(StorageError::internal(
                        "SQL dependent layout batch returned an incomplete result",
                    ));
                }
            }
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }
}

/// CreateTable registers an active build for each GSI: an empty table needs
/// no backfill (`register_initial_gsis`; `register_gsis` checks it).
fn initial_binding(
    table: &StoredTableInfo,
    history_id: TableHistoryId,
) -> StorageResult<TableBinding> {
    let mut binding = TableBinding::for_info(table);
    for index in table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
    {
        binding = binding.with_added_gsi(
            index,
            &table.attribute_definitions,
            crate::physical_name::initial_gsi_incarnation(history_id, &index.index_name),
            GsiPhase::Active,
        )?;
    }
    Ok(binding)
}

pub(crate) fn encode_gsi_binding(binding: &TableBinding) -> StorageResult<SqlValue> {
    Ok(SqlValue::Blob(storage_serde::to_bytes(
        &StoredGsiBinding::from(binding),
    )?))
}

fn insert_table_statement(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    history_id: TableHistoryId,
    binding: &TableBinding,
) -> StorageResult<SqlStatement> {
    let placeholders = (1..=17)
        .map(|index| engine.engine().placeholder(index))
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {relation_tables} (id, table_name, table_status, created_at, \
         attribute_definitions, key_schema, max_indexers, global_secondary_indexes, \
         table_size_bytes, item_count, stream_specification, stream_enabled, \
         deletion_protection_enabled, table_stream_duration_hours, gsi_blob, \
         regional_binding_blob, stream_enabled_at) VALUES ({})",
        placeholders.join(", "),
        relation_tables = engine.relation_name("tables"),
    );
    let optional_json = |value: Option<String>| value.map_or(SqlValue::Null, SqlValue::Text);
    let values = vec![
        SqlValue::Text(uuid::Uuid::from_bytes(*history_id.as_ref()).to_string()),
        SqlValue::Text(table.table_name.to_string()),
        SqlValue::Text(String::from(&table.table_status)),
        SqlValue::Integer(table.created_at.timestamp_millis()),
        SqlValue::Text(serde_json::to_string(&table.attribute_definitions)?),
        SqlValue::Text(serde_json::to_string(&table.key_schema)?),
        SqlValue::Integer(i64::from(table.max_indexers.get())),
        optional_json(
            table
                .global_secondary_indexes
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ),
        SqlValue::Integer(
            i64::try_from(table.table_size_bytes)
                .map_err(|_| StorageError::internal("table size overflows SQL catalog"))?,
        ),
        SqlValue::Integer(
            i64::try_from(table.item_count)
                .map_err(|_| StorageError::internal("item count overflows SQL catalog"))?,
        ),
        optional_json(
            table
                .stream_specification
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        ),
        SqlValue::Boolean(table.has_enabled_stream()),
        SqlValue::Boolean(table.deletion_protection_enabled),
        SqlValue::Integer(table.table_stream_duration.as_hours_wire_value()),
        encode_gsi_binding(binding)?,
        binding
            .regional
            .as_deref()
            .map(storage_types::storage_serde::to_bytes)
            .transpose()?
            .map_or(SqlValue::Null, SqlValue::Blob),
        encode_stream_enabled_at(table),
    ];
    Ok(SqlStatement::new(sql, values))
}

pub(crate) fn encode_stream_enabled_at(table: &StoredTableInfo) -> SqlValue {
    table.stream_enabled_at.map_or(SqlValue::Null, |stamp| {
        SqlValue::Integer(stamp.timestamp_millis())
    })
}

impl<A: SqlAttempt, L: SqlLayout> CatalogAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn store_external_cleanup_barrier(
        &mut self,
        name: &TableName,
        expected: CatalogStamp,
    ) -> StorageResult<CatalogStamp> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let revision = expected
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| StorageError::internal("catalog revision exhausted"))?;
            let next = i64::try_from(revision.get())
                .map_err(|_| StorageError::internal("catalog revision overflow"))?;
            let statement = SqlStatement::new(
                format!(
                    "UPDATE {} SET catalog_revision = {}, external_cleanup_required = 1 WHERE \
                     table_name = {} AND id = {} AND catalog_revision = {} AND table_status <> \
                     'DELETING'",
                    catalog.engine.relation_name("tables"),
                    catalog.engine.engine().placeholder(1),
                    catalog.engine.engine().placeholder(2),
                    catalog.engine.engine().placeholder(3),
                    catalog.engine.engine().placeholder(4)
                ),
                vec![
                    SqlValue::Integer(next),
                    SqlValue::Text(name.to_string()),
                    SqlValue::Text(
                        uuid::Uuid::from_bytes(*expected.history_id.as_ref()).to_string(),
                    ),
                    SqlValue::Integer(next - 1),
                ],
            );
            if catalog
                .execute_catalog_change(name, vec![statement])
                .await?
                != [1]
            {
                return Err(StorageError::write_miss(
                    "catalog_barrier",
                    MissExpectation::Revision,
                ));
            }
            Ok(CatalogStamp {
                revision,
                ..expected
            })
        }
        .await;
        settlement.complete(result)
    }

    async fn lock_catalog_for_change(&mut self, name: &TableName) -> StorageResult<()> {
        if self.protection == ReadProtection::Snapshot {
            return Ok(());
        }
        self.take_exclusive_catalog_lock(name).await
    }

    async fn read_catalog_stamp(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<CatalogStamp>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .query_one_after_catalog_lock(
                    name,
                    catalog_stamp_statement(catalog.engine, name),
                    catalog.protection.shared(),
                )
                .await?
                .as_ref()
                .map(decode_stamp)
                .transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn read_catalog_stamps(
        &mut self,
        names: &[TableName],
    ) -> StorageResult<storage_types::Exact<Option<CatalogStamp>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if names.is_empty() {
                return Ok(storage_types::Exact::empty());
            }
            let statement =
                catalog_batch_statement(catalog.engine, names, "id, catalog_revision, table_name");
            let page = catalog
                .query_catalog_batch(names, statement, catalog_batch_bytes(names.len())?)
                .await?;
            collect_catalog_rows(names, page, 2, decode_stamp)
        }
        .await;
        settlement.complete(result)
    }

    async fn read_table_at_stamp(
        &mut self,
        name: &TableName,
        stamp: CatalogStamp,
    ) -> StorageResult<Arc<BoundTableInfo>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let statement = table_at_stamp_statement(catalog.engine, name, stamp)?;
            let row = catalog
                .query_one_after_catalog_lock(name, statement, catalog.protection.shared())
                .await?
                .ok_or_else(|| {
                    let diagnostic = "catalog changed during table binding";
                    tracing::warn!(
                        backend = "sql",
                        operation = "catalog.read_table",
                        error_kind = "missing_bound_table",
                        error_code = "InternalServerError",
                        diagnostic,
                        "bound table metadata is missing"
                    );
                    StorageError::internal(diagnostic)
                })?;
            let table = decode_table(&row).inspect_err(|error| {
                let diagnostic = match error.to_enum() {
                    storage_types::StorageEnum::InternalServerError { message } => message.as_str(),
                    _ => "catalog metadata decode failed",
                };
                tracing::warn!(
                    backend = "sql",
                    operation = "catalog.read_table",
                    error_kind = "corrupt_bound_table",
                    error_code = "InternalServerError",
                    diagnostic,
                    "bound table metadata is corrupt"
                );
            })?;
            Ok(Arc::new(table))
        }
        .await;
        settlement.complete(result)
    }

    async fn read_tables_at_stamps(
        &mut self,
        requests: &[(TableName, CatalogStamp)],
    ) -> StorageResult<storage_types::Exact<Arc<BoundTableInfo>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if requests.is_empty() {
                return Ok(storage_types::Exact::empty());
            }
            let names = requests
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            let statement = catalog_batch_statement(
                catalog.engine,
                &names,
                &format!("{TABLE_COLUMNS}, catalog_revision"),
            );
            let page = catalog
                .query_catalog_batch(&names, statement, catalog_batch_bytes(names.len())?)
                .await?;
            let tables = collect_catalog_rows(&names, page, 1, |row| {
                Ok((
                    Arc::new(decode_table(row)?),
                    decode_revision(row, TABLE_COLUMN_COUNT)?,
                ))
            })?;
            tables.try_zip_map(requests, |(_, stamp), table| {
                let (table, revision) = table.ok_or_else(|| {
                    StorageError::internal("catalog changed during table binding")
                })?;
                if table.history_id != stamp.history_id || revision != stamp.revision {
                    return Err(StorageError::internal(
                        "catalog changed during table binding",
                    ));
                }
                Ok(table)
            })
        }
        .await;
        settlement.complete(result)
    }
}

pub(crate) fn terminal_catalog_statement(
    engine: impl SqlLayout,
    names: &[TableName],
) -> SqlStatement {
    catalog_batch_statement(engine, names, "id, catalog_revision, table_name")
}

pub(crate) fn decode_terminal_catalog(
    names: &[TableName],
    page: SqlQueryPage,
) -> StorageResult<storage_types::Exact<Option<CatalogStamp>>> {
    collect_catalog_rows(names, page, 2, decode_stamp)
}

fn catalog_batch_statement(
    engine: impl SqlLayout,
    names: &[TableName],
    columns: &str,
) -> SqlStatement {
    let placeholders = engine.engine().placeholders(names.len());
    let sql = format!(
        "SELECT {columns} FROM {relation_tables} WHERE table_name IN ({placeholders}) AND \
         table_status <> 'DELETING'",
        relation_tables = engine.relation_name("tables"),
    );
    SqlStatement::new(
        sql,
        names
            .iter()
            .map(|name| SqlValue::Text(name.to_string()))
            .collect(),
    )
}

pub(crate) fn catalog_batch_bytes(count: usize) -> StorageResult<std::num::NonZeroUsize> {
    CATALOG_READ_BYTES
        .get()
        .checked_mul(count)
        .and_then(std::num::NonZeroUsize::new)
        .ok_or_else(|| StorageError::internal("catalog metadata batch exceeds address space"))
}

fn collect_catalog_rows<T: Clone>(
    names: &[TableName],
    page: SqlQueryPage,
    name_column: usize,
    decode: impl Fn(&SqlRow) -> StorageResult<T>,
) -> StorageResult<storage_types::Exact<Option<T>>> {
    if page.more || page.rows.len() > names.len() {
        return Err(StorageError::internal(
            "catalog batch exceeded its row or byte bound",
        ));
    }
    let mut results = (0..names.len()).map(|_| None).collect::<Vec<_>>();
    for row in page.rows {
        let name = text_column(&row, name_column)?;
        let value = decode(&row)?;
        place_value(
            names,
            &mut results,
            |candidate, _| candidate.as_ref() == name,
            value,
        )?;
    }
    storage_types::Exact::try_new(names.len(), results)
}

fn single_catalog_row(page: SqlQueryPage) -> StorageResult<Option<SqlRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "catalog lookup returned multiple rows",
        ));
    }
    Ok(page.rows.into_iter().next())
}

pub(crate) fn catalog_stamp_statement(engine: impl SqlLayout, name: &TableName) -> SqlStatement {
    let sql = format!(
        "SELECT id, catalog_revision FROM {relation_tables} WHERE table_name = {} AND \
         table_status <> 'DELETING'",
        engine.engine().placeholder(1),
        relation_tables = engine.relation_name("tables"),
    );
    SqlStatement::new(sql, vec![SqlValue::Text(name.to_string())])
}

/// The catalog columns of one bound table, in `decode_table` order.
const TABLE_COLUMNS: &str = "id, table_name, table_status, created_at, attribute_definitions, \
                             key_schema, max_indexers, global_secondary_indexes, \
                             table_size_bytes, item_count, stream_specification, \
                             deletion_protection_enabled, table_stream_duration_hours, gsi_blob, \
                             ttl_blob, regional_binding_blob, stream_enabled_at";
const TABLE_COLUMN_COUNT: usize = 17;

fn table_at_stamp_statement(
    engine: impl SqlLayout,
    name: &TableName,
    stamp: CatalogStamp,
) -> StorageResult<SqlStatement> {
    let revision = i64::try_from(stamp.revision.get())
        .map_err(|_| StorageError::internal("catalog revision overflow"))?;
    let id = uuid::Uuid::from_bytes(*stamp.history_id.as_ref()).to_string();
    let sql = format!(
        "SELECT {TABLE_COLUMNS} FROM {relation_tables} WHERE table_name = {} AND id = {} AND \
         catalog_revision = {} AND table_status <> 'DELETING'",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        relation_tables = engine.relation_name("tables"),
    );
    Ok(SqlStatement::new(
        sql,
        vec![
            SqlValue::Text(name.to_string()),
            SqlValue::Text(id),
            SqlValue::Integer(revision),
        ],
    ))
}

impl<A: SqlAttempt, L: SqlLayout> ListingAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<storage_types::TableStreamIdentity>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let row = catalog
                .query_one(
                    SqlStatement::new(
                        format!(
                            "SELECT id, stream_enabled_at FROM {relation_tables} WHERE table_name \
                             = {} AND stream_enabled = TRUE",
                            catalog.engine.engine().placeholder(1),
                            relation_tables = catalog.engine.relation_name("tables"),
                        ),
                        vec![SqlValue::Text(name.to_string())],
                    ),
                    catalog.catalog_lock.listing_protection(catalog.protection),
                )
                .await?;
            row.map(|row| decode_stream_identity(&row, 0)).transpose()
        }
        .await;
        settlement.complete(result)
    }

    async fn list_stream_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogStreamPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut binds = Vec::with_capacity(2);
            let after_clause = if let Some(after) = after {
                binds.push(SqlValue::Text(after.to_string()));
                format!(
                    "AND table_name > {}",
                    catalog.engine.engine().placeholder(1)
                )
            } else {
                String::new()
            };
            binds.push(SqlValue::Integer(i64::from(limit)));
            let sql = format!(
                "SELECT table_name, id, stream_enabled_at FROM {relation_tables} WHERE \
                 stream_enabled = TRUE {after_clause} ORDER BY table_name LIMIT {}",
                catalog.engine.engine().placeholder(binds.len()),
                relation_tables = catalog.engine.relation_name("tables"),
            );
            let page = catalog
                .attempt
                .query(
                    SqlStatement::new(sql, binds),
                    catalog.catalog_lock.listing_protection(catalog.protection),
                    CATALOG_READ_BYTES,
                )
                .await?;
            let entries = page
                .rows
                .into_iter()
                .map(|row| {
                    let name = match row.value(ColumnIndex::new(0))? {
                        SqlValue::Text(name) => TableName::new(name),
                        _ => {
                            return Err(StorageError::internal(
                                "stream catalog name has invalid type",
                            ));
                        }
                    };
                    Ok((name, decode_stream_identity(&row, 1)?))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(CatalogStreamPage {
                entries,
                more: page.more,
            })
        }
        .await;
        settlement.complete(result)
    }

    async fn list_name_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogNamePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut binds = Vec::with_capacity(2);
            let where_clause = if let Some(after) = after {
                binds.push(SqlValue::Text(after.to_string()));
                format!(
                    "WHERE table_name > {} AND table_status <> 'DELETING'",
                    catalog.engine.engine().placeholder(1)
                )
            } else {
                "WHERE table_status <> 'DELETING'".to_string()
            };
            binds.push(SqlValue::Integer(i64::from(limit)));
            let sql = format!(
                "SELECT table_name FROM {relation_tables} {where_clause} ORDER BY table_name \
                 LIMIT {}",
                catalog.engine.engine().placeholder(binds.len()),
                relation_tables = catalog.engine.relation_name("tables"),
            );
            let page = catalog
                .attempt
                .query(
                    SqlStatement::new(sql, binds),
                    catalog.catalog_lock.listing_protection(catalog.protection),
                    CATALOG_READ_BYTES,
                )
                .await?;
            let names = page
                .rows
                .into_iter()
                .map(|row| match row.value(ColumnIndex::new(0))? {
                    SqlValue::Text(name) => Ok(TableName::new(name)),
                    _ => Err(StorageError::internal(
                        "catalog name column has invalid type",
                    )),
                })
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(CatalogNamePage {
                names,
                more: page.more,
            })
        }
        .await;
        settlement.complete(result)
    }
}

/// Install the one current catalog format before publishing a SQL store.
pub async fn initialize_catalog<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(render_catalog_schema(engine), vec![]))
        .await?;
    attempt
        .execute(crate::stream_label_clock_sql_statements::create_clock(
            engine,
        ))
        .await?;
    attempt
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE INDEX IF NOT EXISTS "["tables_stream_enabled_name"],
                " ON "["tables"],
                " (stream_enabled, table_name)"
            )
            .to_string(),
            vec![],
        ))
        .await?;
    attempt
        .execute(SqlStatement::new(render_table_jobs_schema(engine), vec![]))
        .await?;
    attempt
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE UNIQUE INDEX IF NOT EXISTS "["tables_active_name"],
                " ON "["tables"],
                " (table_name) WHERE table_status <> 'DELETING'"
            )
            .to_string(),
            vec![],
        ))
        .await?;
    Ok(())
}

fn render_catalog_schema(engine: impl SqlLayout) -> String {
    let text = engine.engine().text_type();
    let int = engine.engine().integer_type();
    let blob = engine.engine().blob_type();
    let boolean = engine
        .engine()
        .boolean_column("deletion_protection_enabled");
    let stream_boolean = engine.engine().boolean_column("stream_enabled");
    format!(
        "CREATE TABLE IF NOT EXISTS {relation_tables} (id {text} NOT NULL, table_name {text} NOT \
         NULL, table_status {text} NOT NULL, created_at {int} NOT NULL, attribute_definitions \
         {text} NOT NULL, key_schema {text} NOT NULL, max_indexers {int} NOT NULL, \
         global_secondary_indexes {text}, table_size_bytes {int} NOT NULL DEFAULT 0, item_count \
         {int} NOT NULL DEFAULT 0, stream_specification {text}, stream_enabled {stream_boolean}, \
         deletion_protection_enabled {boolean}, table_stream_duration_hours {int} NOT NULL \
         DEFAULT 72, gsi_blob {blob}, ttl_blob {blob}, regional_binding_blob {blob}, \
         stream_enabled_at {int}, external_cleanup_required {int} NOT NULL DEFAULT 0 CHECK \
         (external_cleanup_required IN (0, 1)), catalog_revision {int} NOT NULL DEFAULT 1 CHECK \
         (catalog_revision > 0), PRIMARY KEY (id)){}",
        engine.engine().table_suffix(),
        relation_tables = engine.relation_name("tables"),
    )
}

/// Job state that changes often stays out of the binding (W6 A15).
fn render_table_jobs_schema(engine: impl SqlLayout) -> String {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    format!(
        "CREATE TABLE IF NOT EXISTS {relation} (table_id {text} NOT NULL, job {text} NOT NULL, \
         state {blob} NOT NULL, PRIMARY KEY (table_id, job)){}",
        engine.engine().table_suffix(),
        relation = engine.relation_name("table_jobs"),
    )
}

pub(crate) fn decode_stamp(row: &SqlRow) -> StorageResult<CatalogStamp> {
    let id = uuid::Uuid::parse_str(text_column(row, 0)?)
        .map_err(|_| StorageError::internal("invalid catalog table incarnation"))?;
    Ok(CatalogStamp {
        history_id: TableHistoryId::from(id),
        revision: decode_revision(row, 1)?,
    })
}

fn decode_revision(row: &SqlRow, column: usize) -> StorageResult<NonZeroU64> {
    u64::try_from(integer_column(row, column)?)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| StorageError::internal("invalid catalog revision"))
}

fn decode_table(row: &SqlRow) -> StorageResult<BoundTableInfo> {
    let id = uuid::Uuid::parse_str(text_column(row, 0)?)
        .map_err(|_| StorageError::internal("invalid catalog table incarnation"))?;
    let table_stream_duration = StreamRetentionDuration::try_from(integer_column(row, 12)?)
        .map_err(|_| StorageError::internal("invalid catalog stream duration"))?;
    let max_indexers = u8::try_from(integer_column(row, 6)?)
        .map_err(|_| StorageError::internal("invalid catalog max_indexers"))?;
    let info = StoredTableInfo {
        table_name: TableName::new(text_column(row, 1)?),
        table_status: TableStatus::from(text_column(row, 2)?),
        created_at: TimestampMillis::from(integer_column(row, 3)?),
        attribute_definitions: required_json(row, 4)?,
        key_schema: required_json(row, 5)?,
        max_indexers: MaxIndexers::try_new(max_indexers)?,
        global_secondary_indexes: optional_json(row, 7)?,
        table_size_bytes: u64::try_from(integer_column(row, 8)?).unwrap_or_default(),
        item_count: u64::try_from(integer_column(row, 9)?).unwrap_or_default(),
        stream_specification: optional_json(row, 10)?,
        stream_enabled_at: match row.value(ColumnIndex::new(16))? {
            SqlValue::Null => None,
            SqlValue::Integer(stamp) => Some(TimestampMillis::from_timestamp(*stamp)),
            _ => {
                return Err(StorageError::internal(
                    "catalog stream enable stamp has invalid type",
                ));
            }
        },
        deletion_protection_enabled: integer_column(row, 11)? != 0,
        table_stream_duration,
    };
    let gsis: StoredGsiBinding = optional_blob(row, 13)?.unwrap_or_default();
    let ttl =
        optional_blob::<storage_provider::TtlConfigRecord>(row, 14)?.map(|config| config.binding());
    let regional = optional_blob(row, 15)?.map(Arc::new);
    let binding = TableBinding {
        gsis: gsis.gsis.into(),
        gsi_high_water: gsis.high_water,
        ttl,
        regional,
        ..TableBinding::for_info(&info)
    };
    Ok(BoundTableInfo {
        history_id: TableHistoryId::from(id),
        info: Arc::new(info),
        binding,
    })
}

fn optional_blob<T: DeserializeOwned>(row: &SqlRow, index: usize) -> StorageResult<Option<T>> {
    match row.value(ColumnIndex::new(index))? {
        SqlValue::Null => Ok(None),
        SqlValue::Blob(bytes) => storage_serde::from_bytes(bytes).map(Some),
        _ => Err(StorageError::internal(
            "catalog binding column has invalid type",
        )),
    }
}

fn text_column(row: &SqlRow, index: usize) -> StorageResult<&str> {
    match row.value(ColumnIndex::new(index))? {
        SqlValue::Text(value) => Ok(value),
        _ => Err(StorageError::internal(
            "catalog text column has invalid type",
        )),
    }
}

fn integer_column(row: &SqlRow, index: usize) -> StorageResult<i64> {
    match row.value(ColumnIndex::new(index))? {
        SqlValue::Integer(value) => Ok(*value),
        _ => Err(StorageError::internal(
            "catalog integer column has invalid type",
        )),
    }
}

fn required_json<T: DeserializeOwned>(row: &SqlRow, index: usize) -> StorageResult<T> {
    serde_json::from_str(text_column(row, index)?)
        .map_err(|_| StorageError::internal("invalid required catalog JSON"))
}

fn optional_json<T: DeserializeOwned>(row: &SqlRow, index: usize) -> StorageResult<Option<T>> {
    match row.value(ColumnIndex::new(index))? {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value)
            if value.trim().is_empty() || value.trim() == "null" || value.trim() == "\"null\"" =>
        {
            Ok(None)
        }
        SqlValue::Text(value) => serde_json::from_str(value)
            .map_err(|_| StorageError::internal("invalid optional catalog JSON")),
        _ => Err(StorageError::internal(
            "catalog JSON column has invalid type",
        )),
    }
}

fn decode_stream_identity(
    row: &crate::SqlRow,
    first: usize,
) -> StorageResult<storage_types::TableStreamIdentity> {
    let history_id = uuid::Uuid::parse_str(text_column(row, first)?)
        .map_err(|_| StorageError::internal("stream catalog identity is invalid"))?
        .into();
    let enabled_at = TimestampMillis::from_timestamp(integer_column(row, first + 1)?);
    Ok(storage_types::TableStreamIdentity {
        history_id,
        enabled_at,
    })
}
