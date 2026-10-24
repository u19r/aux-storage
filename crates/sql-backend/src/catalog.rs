//! One physical SQL catalog layout and decoder for all three native drivers.

use std::{num::NonZeroU64, sync::Arc};

use serde::de::DeserializeOwned;
use storage_backfill::GsiBuildTarget;
use storage_common::point_results::place_value;
use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, CatalogNamePage, CatalogStamp, CatalogStreamPage,
    CreateTableLayout, ListStreamsAttempt, ListTablesAttempt, TableMetadataCache,
    TableNameReservation,
};
use storage_types::{
    BoundTableInfo, MaxIndexers, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    StreamRetentionDuration, SystemStreamPosition, SystemStreamShardId, TableHistoryId, TableName,
    TableStatus, TimestampMillis, context::WrappedError as _,
};
use stream_provider::SystemStreamHistory;

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlRow,
    SqlStatement, SqlValue,
    item_schema::{
        render_gsi_read_index, render_gsi_table, render_item_table, render_ttl_index_table,
    },
    stream_history,
};

/// An operation lends its native SQL attempt to this catalog lowerer. Write
/// operations use conflict protection; read operations use their snapshot.
pub struct SqlCatalogAttempt<'a, A, L: SqlLayout = SqlEngine> {
    pub(crate) attempt: &'a mut A,
    pub(crate) engine: L,
    pub(crate) protection: ReadProtection,
    dynamo_statements: Vec<(SqlStatement, SqlDynamoCount, SqlDynamoWriteKind)>,
    pub(crate) dynamo_history: Option<SystemStreamHistory>,
    pub(crate) dynamo_positions: DynamoStreamPositions,
    pub(crate) metadata_cache: Option<Arc<TableMetadataCache>>,
    pub(crate) prepared_writes: StorageResult<crate::write_preparation::SqlPreparedWrites>,
    gsi_targets: Vec<GsiBuildTarget>,
}

pub(crate) enum DynamoStreamPositions {
    Sequential(Vec<(SystemStreamShardId, NonZeroU64, NonZeroU64)>),
    Native(crate::stream_allocation::NativeStreamAllocation),
}

#[derive(Clone, Copy)]
pub(crate) enum SqlDynamoCount {
    ExactlyOne,
    AtMostOne,
}

#[derive(Clone, Copy)]
pub(crate) enum SqlDynamoWriteKind {
    SystemSequence,
    Primary,
    Gsi,
    Stream,
    Retention,
}

impl SqlDynamoWriteKind {
    fn error(self) -> StorageError {
        match self {
            Self::SystemSequence => StorageEnum::HistoryUnavailable.into(),
            Self::Primary => {
                StorageError::internal("SQL primary write affected the wrong row count")
            }
            Self::Gsi => StorageError::internal("SQL GSI write changed the wrong row count"),
            Self::Stream => {
                StorageError::internal("SQL Dynamo stream write affected an unexpected row count")
            }
            Self::Retention => {
                StorageError::internal("SQL retention write affected the wrong row count")
            }
        }
    }
}

impl<'a, A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'a, A, L> {
    pub fn new(attempt: &'a mut A, engine: L, protection: ReadProtection) -> Self {
        Self {
            attempt,
            engine,
            protection,
            dynamo_statements: Vec::new(),
            dynamo_history: None,
            dynamo_positions: DynamoStreamPositions::Sequential(Vec::new()),
            metadata_cache: None,
            prepared_writes: Ok(Default::default()),
            gsi_targets: Vec::new(),
        }
    }

    pub(crate) fn with_metadata_cache(mut self, cache: Arc<TableMetadataCache>) -> Self {
        self.metadata_cache = Some(cache);
        self
    }

    pub(crate) fn bind_gsi_targets(
        &mut self,
        targets: impl IntoIterator<Item = GsiBuildTarget>,
    ) -> StorageResult<()> {
        for target in targets {
            self.gsi_targets
                .try_reserve(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            self.gsi_targets.retain(|known| {
                known.table_name != target.table_name || known.index_name != target.index_name
            });
            self.gsi_targets.push(target);
        }
        Ok(())
    }

    pub(crate) fn gsi_target(
        &self,
        table: &TableName,
        index: &storage_types::IndexName,
    ) -> StorageResult<&GsiBuildTarget> {
        self.gsi_targets
            .iter()
            .find(|target| &target.table_name == table && &target.index_name == index)
            .ok_or_else(|| StorageError::internal("SQL GSI physical target was not bound"))
    }

    pub(crate) fn gsi_targets(&self) -> &[GsiBuildTarget] {
        &self.gsi_targets
    }

    pub(crate) fn reserve_stream_position(
        &mut self,
        routing_key: &[u8],
    ) -> StorageResult<SystemStreamPosition> {
        let history = self
            .dynamo_history
            .as_mut()
            .ok_or_else(|| StorageError::internal("SQL stream history was not prepared"))?;
        let positions = match &mut self.dynamo_positions {
            DynamoStreamPositions::Native(allocation) => {
                return history.native_numeric_position(routing_key, allocation.reserve()?);
            }
            DynamoStreamPositions::Sequential(positions) => positions,
        };
        let allocation = history.reserve_write(routing_key)?;
        let shard = allocation.position.shard_id();
        if let Some((_, _, next)) = positions.iter_mut().find(|(known, _, _)| known == shard) {
            *next = allocation.next_offset;
        } else {
            positions.push((
                shard.clone(),
                allocation.expected_offset,
                allocation.next_offset,
            ));
        }
        Ok(allocation.position)
    }

    pub(crate) fn append_dynamo_statements(
        &mut self,
        kind: SqlDynamoWriteKind,
        statements: impl IntoIterator<
            Item = (SqlStatement, SqlDynamoCount),
            IntoIter: ExactSizeIterator,
        >,
    ) -> StorageResult<()> {
        let statements = statements.into_iter();
        if self
            .dynamo_statements
            .try_reserve(statements.len())
            .is_err()
        {
            let error = storage_types::InfrastructureResultKind::NativeCapacityRejected.into();
            self.attempt.reject_settlement_error(&error);
            return Err(error);
        }
        self.dynamo_statements
            .extend(statements.map(|(statement, count)| (statement, count, kind)));
        Ok(())
    }

    pub(crate) async fn flush_dynamo_statements(&mut self) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut planned = Vec::new();
            let positions = match &mut catalog.dynamo_positions {
                DynamoStreamPositions::Sequential(positions) => std::mem::take(positions),
                DynamoStreamPositions::Native(_) => Vec::new(),
            };
            let count = positions
                .len()
                .checked_add(catalog.dynamo_statements.len())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            planned
                .try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (shard, first, next) in positions {
                let statement = stream_history::sequence_advance_statement(
                    catalog.engine,
                    shard.as_str(),
                    first,
                    next,
                )?;
                planned.push((
                    statement,
                    SqlDynamoCount::ExactlyOne,
                    SqlDynamoWriteKind::SystemSequence,
                ));
            }
            planned.extend(std::mem::take(&mut catalog.dynamo_statements));
            if planned.is_empty() {
                return Ok(());
            }
            let mut expected = Vec::new();
            let mut statements = Vec::new();
            expected
                .try_reserve_exact(planned.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            statements
                .try_reserve_exact(planned.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (statement, count, kind) in planned {
                expected.push((count, kind));
                statements.push(statement);
            }
            let counts = catalog.attempt.execute_batch(statements).await?;
            if counts.len() != expected.len() {
                return Err(StorageError::internal(
                    "SQL Dynamo write batch count differs",
                ));
            }
            for (count, (expected, kind)) in counts.into_iter().zip(expected) {
                let invalid = match expected {
                    SqlDynamoCount::ExactlyOne => count != 1,
                    SqlDynamoCount::AtMostOne => count > 1,
                };
                if invalid {
                    if matches!(kind, SqlDynamoWriteKind::Gsi) && count == 0 {
                        return Err(StorageEnum::TransactionConflict {
                            message: "SQL GSI projection changed during write".to_owned(),
                        }
                        .into());
                    }
                    return Err(kind.error());
                }
            }
            Ok(())
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

    /// Lower one validated table image to the catalog and physical item/GSI
    /// relations inside the caller's native write attempt. Stream retention
    /// and GSI publication are separate effects in that same attempt.
    pub async fn read_table_layout(
        &mut self,
        table: &StoredTableInfo,
    ) -> StorageResult<CreateTableLayout<bool>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let lock = catalog
                .query_one(
                    SqlStatement::new(
                        format!(
                            "SELECT locked_until FROM {relation_tables} WHERE table_name = {}",
                            catalog.engine.engine().placeholder(1),
                            relation_tables = catalog.engine.relation_name("tables"),
                        ),
                        vec![SqlValue::Text(table.table_name.to_string())],
                    ),
                    ReadProtection::Conflict,
                )
                .await?;
            let reservation = match lock.as_ref() {
                None => TableNameReservation::Available,
                Some(row) => match row.value(ColumnIndex::new(0))? {
                    SqlValue::Integer(millis) => {
                        TableNameReservation::LockedUntil(TimestampMillis::from_timestamp(*millis))
                    }
                    SqlValue::Null => TableNameReservation::Active,
                    _ => return Err(StorageError::internal("invalid SQL name reservation")),
                },
            };
            Ok(CreateTableLayout {
                reservation,
                seed: lock.is_some(),
            })
        }
        .await;
        settlement.complete(result)
    }

    pub async fn insert_table_layout(
        &mut self,
        table: &StoredTableInfo,
        history_id: TableHistoryId,
        clear_expired_lock: bool,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let name = &table.table_name;
            if clear_expired_lock {
                let deleted = catalog
                    .attempt
                    .execute(SqlStatement::new(
                        format!(
                            "DELETE FROM {relation_tables} WHERE table_name = {} AND locked_until \
                             <= {}",
                            catalog.engine.engine().placeholder(1),
                            catalog.engine.engine().placeholder(2),
                            relation_tables = catalog.engine.relation_name("tables"),
                        ),
                        vec![
                            SqlValue::Text(name.to_string()),
                            SqlValue::Integer(table.created_at.timestamp_millis()),
                        ],
                    ))
                    .await?;
                if deleted == 0 {
                    return Ok(false);
                }
                if deleted != 1 {
                    return Err(StorageError::internal(
                        "SQL name reservation deletion affected multiple rows",
                    ));
                }
            }
            let mut layout = vec![
                insert_table_statement(catalog.engine, table, history_id)?,
                SqlStatement::new(render_item_table(catalog.engine, table, history_id), vec![]),
                SqlStatement::new(
                    render_ttl_index_table(catalog.engine, table, history_id),
                    vec![],
                ),
                crate::stream_write::sql_statements::initialize_sequence(
                    catalog.engine,
                    &crate::stream_write::sql_statements::user_sequence_name(
                        &storage_types::StreamName::table_stream(name),
                    ),
                ),
            ];
            let mut dependent_layout = Vec::new();
            for index in table
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter(|index| !index.index_name.is_ttl_index())
            {
                let incarnation =
                    crate::physical_name::initial_gsi_incarnation(history_id, &index.index_name);
                layout.push(SqlStatement::new(
                    render_gsi_table(catalog.engine, table, index, &incarnation),
                    vec![],
                ));
                dependent_layout.push(SqlStatement::new(
                    render_gsi_read_index(catalog.engine, table, index, &incarnation),
                    vec![],
                ));
            }
            let expected = layout.len();
            let counts = catalog.attempt.execute_batch(layout).await?;
            if counts.len() != expected || counts.first() != Some(&1) {
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

fn insert_table_statement(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    history_id: TableHistoryId,
) -> StorageResult<SqlStatement> {
    let placeholders = (1..=15)
        .map(|index| engine.engine().placeholder(index))
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {relation_tables} (id, table_name, table_status, created_at, \
         attribute_definitions, key_schema, max_indexers, global_secondary_indexes, \
         table_size_bytes, item_count, stream_specification, stream_enabled, \
         deletion_protection_enabled, table_stream_duration_hours, \
         default_item_stream_duration_hours) VALUES ({})",
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
        SqlValue::Boolean(
            table
                .stream_specification
                .as_ref()
                .is_some_and(|spec| spec.stream_enabled),
        ),
        SqlValue::Boolean(table.deletion_protection_enabled),
        SqlValue::Integer(table.table_stream_duration.as_hours_wire_value()),
        SqlValue::Integer(table.default_item_stream_duration.as_hours_wire_value()),
    ];
    Ok(SqlStatement::new(sql, values))
}

impl<A: SqlAttempt, L: SqlLayout> CatalogAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn advance_catalog_stamp(
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
                    "UPDATE {} SET catalog_revision = {} WHERE table_name = {} AND id = {} AND \
                     catalog_revision = {} AND locked_until IS NULL",
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
            if catalog.attempt.execute(statement).await? != 1 {
                return Err(StorageEnum::TransactionConflict {
                    message: "catalog barrier revision changed".into(),
                }
                .into());
            }
            Ok(CatalogStamp {
                revision,
                ..expected
            })
        }
        .await;
        settlement.complete(result)
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
                .query_one(
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
    ) -> StorageResult<Vec<Option<CatalogStamp>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if names.is_empty() {
                return Ok(Vec::new());
            }
            let statement =
                catalog_batch_statement(catalog.engine, names, "id, catalog_revision, table_name");
            let page = catalog
                .attempt
                .query(
                    statement,
                    catalog.protection.shared(),
                    catalog_batch_bytes(names.len())?,
                )
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
                .query_one(statement, catalog.protection.shared())
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
    ) -> StorageResult<Vec<Arc<BoundTableInfo>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let names = requests
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            let statement = catalog_batch_statement(
                catalog.engine,
                &names,
                "id, table_name, table_status, created_at, attribute_definitions, key_schema, \
                 max_indexers, global_secondary_indexes, table_size_bytes, item_count, \
                 stream_specification, deletion_protection_enabled, table_stream_duration_hours, \
                 default_item_stream_duration_hours, catalog_revision",
            );
            let page = catalog
                .attempt
                .query(
                    statement,
                    catalog.protection.shared(),
                    catalog_batch_bytes(names.len())?,
                )
                .await?;
            let tables = collect_catalog_rows(&names, page, 1, |row| {
                Ok((Arc::new(decode_table(row)?), decode_revision(row, 14)?))
            })?;
            requests
                .iter()
                .zip(tables)
                .map(|((_, stamp), table)| {
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
                .collect()
        }
        .await;
        settlement.complete(result)
    }
}

fn catalog_batch_statement(
    engine: impl SqlLayout,
    names: &[TableName],
    columns: &str,
) -> SqlStatement {
    let placeholders = engine.engine().placeholders(names.len());
    let sql = format!(
        "SELECT {columns} FROM {relation_tables} WHERE table_name IN ({placeholders}) AND \
         locked_until IS NULL",
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

fn catalog_batch_bytes(count: usize) -> StorageResult<std::num::NonZeroUsize> {
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
) -> StorageResult<Vec<Option<T>>> {
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
    Ok(results)
}

fn single_catalog_row(page: SqlQueryPage) -> StorageResult<Option<SqlRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "catalog lookup returned multiple rows",
        ));
    }
    Ok(page.rows.into_iter().next())
}

fn catalog_stamp_statement(engine: impl SqlLayout, name: &TableName) -> SqlStatement {
    let sql = format!(
        "SELECT id, catalog_revision FROM {relation_tables} WHERE table_name = {} AND \
         locked_until IS NULL",
        engine.engine().placeholder(1),
        relation_tables = engine.relation_name("tables"),
    );
    SqlStatement::new(sql, vec![SqlValue::Text(name.to_string())])
}

fn table_at_stamp_statement(
    engine: impl SqlLayout,
    name: &TableName,
    stamp: CatalogStamp,
) -> StorageResult<SqlStatement> {
    let revision = i64::try_from(stamp.revision.get())
        .map_err(|_| StorageError::internal("catalog revision overflow"))?;
    let id = uuid::Uuid::from_bytes(*stamp.history_id.as_ref()).to_string();
    let sql = format!(
        "SELECT id, table_name, table_status, created_at, attribute_definitions, key_schema, \
         max_indexers, global_secondary_indexes, table_size_bytes, item_count, \
         stream_specification, deletion_protection_enabled, table_stream_duration_hours, \
         default_item_stream_duration_hours FROM {relation_tables} WHERE table_name = {} AND id = \
         {} AND catalog_revision = {} AND locked_until IS NULL",
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

impl<A: SqlAttempt, L: SqlLayout> ListTablesAttempt for SqlCatalogAttempt<'_, A, L> {
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
                    "WHERE table_name > {} AND locked_until IS NULL",
                    catalog.engine.engine().placeholder(1)
                )
            } else {
                "WHERE locked_until IS NULL".to_string()
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
                    catalog.protection,
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

impl<A: SqlAttempt, L: SqlLayout> ListStreamsAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<TimestampMillis>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let row = catalog
                .query_one(
                    SqlStatement::new(
                        format!(
                            "SELECT created_at FROM {relation_tables} WHERE table_name = {} AND \
                             stream_enabled = TRUE",
                            catalog.engine.engine().placeholder(1),
                            relation_tables = catalog.engine.relation_name("tables"),
                        ),
                        vec![SqlValue::Text(name.to_string())],
                    ),
                    catalog.protection,
                )
                .await?;
            row.map(|row| match row.value(ColumnIndex::new(0))? {
                SqlValue::Integer(value) => Ok(TimestampMillis::from_timestamp(*value)),
                _ => Err(StorageError::internal(
                    "stream catalog timestamp has invalid type",
                )),
            })
            .transpose()
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
                "SELECT table_name, created_at FROM {relation_tables} WHERE stream_enabled = TRUE \
                 {after_clause} ORDER BY table_name LIMIT {}",
                catalog.engine.engine().placeholder(binds.len()),
                relation_tables = catalog.engine.relation_name("tables"),
            );
            let page = catalog
                .attempt
                .query(
                    SqlStatement::new(sql, binds),
                    catalog.protection,
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
                    let created_at = match row.value(ColumnIndex::new(1))? {
                        SqlValue::Integer(value) => TimestampMillis::from_timestamp(*value),
                        _ => {
                            return Err(StorageError::internal(
                                "stream catalog timestamp has invalid type",
                            ));
                        }
                    };
                    Ok((name, created_at))
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
         DEFAULT 72, default_item_stream_duration_hours {int} NOT NULL DEFAULT 72, \
         ttl_config_blob {blob}, catalog_revision {int} NOT NULL DEFAULT 1 CHECK \
         (catalog_revision > 0), locked_until {int}, PRIMARY KEY (id), UNIQUE (table_name)){}",
        engine.engine().table_suffix(),
        relation_tables = engine.relation_name("tables"),
    )
}

fn decode_stamp(row: &SqlRow) -> StorageResult<CatalogStamp> {
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
    let default_item_stream_duration = StreamRetentionDuration::try_from(integer_column(row, 13)?)
        .map_err(|_| StorageError::internal("invalid catalog item stream duration"))?;
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
        deletion_protection_enabled: integer_column(row, 11)? != 0,
        table_stream_duration,
        default_item_stream_duration,
    };
    Ok((info, id).into())
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
