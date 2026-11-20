//! Regional pair state of SQL tables: the binding port and the shared marker
//! relation.

use std::{
    fmt::Write as _,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use regional_replication::message::LineageRef;
use storage_provider::{
    LineageCursor, MarkerPage, ProgressDirection, RegionalCatalogAttempt, RegionalMarkerAttempt,
    RegionalProgressAttempt,
};
use storage_types::{
    BoundTableInfo, KeyAttributes, PairId, RegionId, RegionalBinding, RegionalMarker,
    RegionalOrder, StorageError, StorageResult, SystemStreamLineageId, SystemStreamOffset,
    SystemStreamShardId, regional_marker_item_key, storage_serde,
};

use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlRow, SqlStatement, SqlValue,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    item_observation::match_observation_rows,
};

pub(crate) async fn initialize_regional_markers<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let blob = engine.engine().blob_type();
    let int = engine.engine().integer_type();
    attempt
        .execute(SqlStatement::new(
            format!(
                "CREATE TABLE IF NOT EXISTS {relation} (table_id {blob} NOT NULL, item_key {blob} \
                 NOT NULL, stamp {int} NOT NULL, region {int} NOT NULL, expiry {int} NOT NULL, \
                 received {int} NOT NULL, PRIMARY KEY (table_id, item_key)){}",
                engine.engine().table_suffix(),
                relation = engine.relation_name("regional_markers"),
            ),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn initialize_regional_progress<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let (blob, int, text) = (
        engine.engine().blob_type(),
        engine.engine().integer_type(),
        engine.engine().text_type(),
    );
    attempt
        .execute(SqlStatement::new(
            format!(
                "CREATE TABLE IF NOT EXISTS {relation} (table_id {blob} NOT NULL, pair_id {blob} \
                 NOT NULL, direction {int} NOT NULL, shard_id {text} NOT NULL, lineage_id {text} \
                 NOT NULL, through_text {text}, sealed {int} NOT NULL, PRIMARY KEY (table_id, \
                 pair_id, direction, shard_id, lineage_id)){}",
                engine.engine().table_suffix(),
                relation = engine.relation_name("regional_progress"),
            ),
            vec![],
        ))
        .await?;
    Ok(())
}

fn table_id(table: &BoundTableInfo) -> SqlValue {
    SqlValue::Blob(table.history_id.as_ref().to_vec())
}

fn item_key(table: &BoundTableInfo, key: &KeyAttributes) -> StorageResult<SqlValue> {
    Ok(SqlValue::Blob(regional_marker_item_key(table, key)?))
}

/// One statement for the paired write targets of one table: rows `(ordinal,
/// stamp, region, expiry, received)`; ordinals index `keys` (VALUES, as the
/// live observation, avoids Turso's OR-expression limit).
pub(crate) fn marker_observation_statement<'k>(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    keys: impl Iterator<Item = &'k KeyAttributes>,
) -> StorageResult<SqlStatement> {
    let mut binds = vec![table_id(table)];
    let mut sql = String::from("WITH requested(__aux_ordinal, item_key) AS (VALUES ");
    for (ordinal, key) in keys.enumerate() {
        binds.push(item_key(table, key)?);
        let placeholder = engine.engine().placeholder(binds.len());
        if ordinal > 0 {
            sql.push_str(", ");
        }
        match engine.engine() {
            SqlEngine::Postgres => write!(sql, "({ordinal}, CAST({placeholder} AS BYTEA))")?,
            _ => write!(sql, "({ordinal}, {placeholder})")?,
        }
    }
    write!(
        sql,
        ") SELECT requested.__aux_ordinal, marker.stamp, marker.region, marker.expiry, \
         marker.received FROM requested JOIN {relation} AS marker ON marker.table_id = {} AND \
         marker.item_key = requested.item_key",
        engine.engine().placeholder(1),
        relation = engine.relation_name("regional_markers"),
    )?;
    Ok(SqlStatement::new(sql, binds))
}

/// A row after `match_observation_rows` removed the ordinal: `[stamp, region,
/// expiry, received]`.
pub(crate) fn decode_marker_row(row: SqlRow) -> StorageResult<RegionalMarker> {
    let invalid = || StorageError::internal("SQL regional marker row is invalid");
    decode_marker_values(row.into_values().try_into().map_err(|_| invalid())?)
}

fn decode_marker_values(values: [SqlValue; 4]) -> StorageResult<RegionalMarker> {
    let invalid = || StorageError::internal("SQL regional marker row is invalid");
    let [stamp, region, expiry, received] = values;
    let integer = |value: SqlValue| match value {
        SqlValue::Integer(value) => Ok(value),
        _ => Err(invalid()),
    };
    let flag = |value: i64| match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid()),
    };
    let stamp = u64::try_from(integer(stamp)?).map_err(|_| invalid())?;
    let region = u16::try_from(integer(region)?)
        .ok()
        .and_then(std::num::NonZeroU16::new)
        .ok_or_else(invalid)?;
    Ok(RegionalMarker {
        order: RegionalOrder::from_parts(stamp, RegionId(region), flag(integer(expiry)?)?),
        received: flag(integer(received)?)?,
    })
}

fn marker_write_sql(engine: impl SqlLayout) -> String {
    let p = |n| engine.engine().placeholder(n);
    format!(
        "INSERT INTO {relation} (table_id, item_key, stamp, region, expiry, received) VALUES ({}, \
         {}, {}, {}, {}, {}) ON CONFLICT (table_id, item_key) DO UPDATE SET stamp = \
         excluded.stamp, region = excluded.region, expiry = excluded.expiry, received = \
         excluded.received",
        p(1),
        p(2),
        p(3),
        p(4),
        p(5),
        p(6),
        relation = engine.relation_name("regional_markers"),
    )
}

fn marker_delete_sql(engine: impl SqlLayout) -> String {
    format!(
        "DELETE FROM {relation} WHERE table_id = {} AND item_key = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation = engine.relation_name("regional_markers"),
    )
}

fn marker_binds(
    table: &BoundTableInfo,
    key: &KeyAttributes,
    marker: Option<RegionalMarker>,
) -> StorageResult<Vec<SqlValue>> {
    let mut binds = vec![table_id(table), item_key(table, key)?];
    if let Some(marker) = marker {
        let stamp = i64::try_from(marker.order.stamp())
            .map_err(|_| StorageError::internal("regional order stamp exceeds SQL integer"))?;
        binds.extend([
            SqlValue::Integer(stamp),
            SqlValue::Integer(i64::from(marker.order.region().0.get())),
            SqlValue::Integer(i64::from(marker.order.is_expiry())),
            SqlValue::Integer(i64::from(marker.received)),
        ]);
    }
    Ok(binds)
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    /// Stage one statement whose binds are rebuilt for each staging form
    /// (`SqlValue` is not `Clone`).
    fn stage_counted(
        &mut self,
        sql: String,
        binds: impl Fn() -> StorageResult<Vec<SqlValue>>,
        count: SqlDynamoCount,
    ) -> StorageResult<()> {
        self.dynamo_writes.stage(
            || {
                Ok((
                    SqlStatement::new(sql.clone(), binds()?),
                    count,
                    SqlDynamoWriteKind::Primary,
                ))
            },
            || {
                let mut text = crate::statement::SqlText::start_counted_write()?;
                text.append(format_args!("{sql}"))?;
                text.finish_counted_write(
                    binds()?,
                    count,
                    SqlDynamoWriteKind::Primary.error_factory(),
                )
            },
        )
    }
}

impl<A: SqlAttempt, L: SqlLayout> RegionalCatalogAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn store_regional_binding(
        &mut self,
        table: &BoundTableInfo,
        regional: Option<Arc<RegionalBinding>>,
    ) -> StorageResult<()> {
        let engine = self.engine;
        let blob = regional
            .as_deref()
            .map(storage_serde::to_bytes)
            .transpose()?
            .map_or(SqlValue::Null, SqlValue::Blob);
        let sql = format!(
            "UPDATE {relation_tables} SET regional_binding_blob = {}, catalog_revision = \
             catalog_revision + 1 WHERE table_name = {} AND id = {} AND catalog_revision < {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            i64::MAX,
            relation_tables = engine.relation_name("tables"),
        );
        let changed = self
            .execute_catalog_change(
                &table.info.table_name,
                vec![SqlStatement::new(
                    sql,
                    vec![
                        blob,
                        SqlValue::Text(table.info.table_name.to_string()),
                        SqlValue::Text(
                            uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string(),
                        ),
                    ],
                )],
            )
            .await?;
        if changed != [1] {
            return Err(StorageError::internal(
                "SQL table incarnation changed during a pair change",
            ));
        }
        Ok(())
    }
}

fn progress_relation(engine: impl SqlLayout) -> String {
    engine.relation_name("regional_progress").to_string()
}

fn pair_value(pair: PairId) -> SqlValue {
    SqlValue::Blob(pair.as_bytes().to_vec())
}

pub(crate) fn progress_lock_identity(
    table: &BoundTableInfo,
    pair: PairId,
    direction: ProgressDirection,
    lineage: &LineageRef,
) -> String {
    let shard = lineage.shard.as_str();
    format!(
        "regional-progress:{}:{}:{}:{}:{shard}:{}",
        uuid::Uuid::from_bytes(*table.history_id.as_ref()).simple(),
        uuid::Uuid::from_bytes(*pair.as_bytes()).simple(),
        direction as u8,
        shard.len(),
        lineage.lineage.as_str(),
    )
}

/// Rows `(ordinal, through_text, sealed)` for the requested lineages (VALUES
/// join, as REG-4).
pub(crate) fn progress_read_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    pair: PairId,
    direction: ProgressDirection,
    lineages: &[LineageRef],
) -> StorageResult<SqlStatement> {
    let p = |n: usize| engine.engine().placeholder(n);
    let mut binds = vec![
        table_id(table),
        pair_value(pair),
        SqlValue::Integer(i64::from(direction as u8)),
    ];
    let mut sql = String::from("WITH requested(__aux_ordinal, shard_id, lineage_id) AS (VALUES ");
    for (ordinal, lineage) in lineages.iter().enumerate() {
        binds.push(SqlValue::Text(lineage.shard.as_str().to_owned()));
        let shard = p(binds.len());
        binds.push(SqlValue::Text(lineage.lineage.as_str().to_owned()));
        let name = p(binds.len());
        if ordinal > 0 {
            sql.push_str(", ");
        }
        match engine.engine() {
            SqlEngine::Postgres => write!(
                sql,
                "({ordinal}, CAST({shard} AS TEXT), CAST({name} AS TEXT))"
            )?,
            _ => write!(sql, "({ordinal}, {shard}, {name})")?,
        }
    }
    write!(
        sql,
        ") SELECT requested.__aux_ordinal, progress.through_text, progress.sealed FROM requested \
         JOIN {relation} AS progress ON progress.table_id = {} AND progress.pair_id = {} AND \
         progress.direction = {} AND progress.shard_id = requested.shard_id AND \
         progress.lineage_id = requested.lineage_id",
        p(1),
        p(2),
        p(3),
        relation = progress_relation(engine),
    )?;
    Ok(SqlStatement::new(sql, binds))
}

pub(crate) fn decode_progress_values(
    through: SqlValue,
    sealed: SqlValue,
) -> StorageResult<LineageCursor> {
    let invalid = || StorageError::internal("SQL regional progress row is invalid");
    let through = match through {
        SqlValue::Null => None,
        SqlValue::Text(text) => Some(SystemStreamOffset::try_new(text).map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    let sealed = match sealed {
        SqlValue::Integer(0) => false,
        SqlValue::Integer(1) => true,
        _ => return Err(invalid()),
    };
    Ok(LineageCursor { through, sealed })
}

fn progress_binds(
    table: &BoundTableInfo,
    pair: PairId,
    direction: ProgressDirection,
    lineage: &LineageRef,
    cursor: &LineageCursor,
) -> Vec<SqlValue> {
    vec![
        table_id(table),
        pair_value(pair),
        SqlValue::Integer(i64::from(direction as u8)),
        SqlValue::Text(lineage.shard.as_str().to_owned()),
        SqlValue::Text(lineage.lineage.as_str().to_owned()),
        cursor.through.as_ref().map_or(SqlValue::Null, |offset| {
            SqlValue::Text(offset.as_str().to_owned())
        }),
        SqlValue::Integer(i64::from(cursor.sealed)),
    ]
}

fn progress_write_sql(engine: impl SqlLayout) -> String {
    let p = |n| engine.engine().placeholder(n);
    format!(
        "INSERT INTO {relation} (table_id, pair_id, direction, shard_id, lineage_id, \
         through_text, sealed) VALUES ({}, {}, {}, {}, {}, {}, {}) ON CONFLICT (table_id, \
         pair_id, direction, shard_id, lineage_id) DO UPDATE SET through_text = \
         excluded.through_text, sealed = excluded.sealed",
        p(1),
        p(2),
        p(3),
        p(4),
        p(5),
        p(6),
        p(7),
        relation = progress_relation(engine),
    )
}

impl<A: SqlAttempt, L: SqlLayout> RegionalProgressAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineages: &[LineageRef],
    ) -> StorageResult<Vec<Option<LineageCursor>>> {
        if lineages.is_empty() {
            return Ok(Vec::new());
        }
        let identities = lineages
            .iter()
            .map(|lineage| progress_lock_identity(table, pair, direction, lineage))
            .collect::<Vec<_>>();
        let mut window = Vec::new();
        let prefix = self
            .catalog_lock
            .progress_lock_prefix(&identities, &mut window);
        window.push((
            progress_read_statement(self.engine, table, pair, direction, lineages)?,
            ReadProtection::Snapshot,
        ));
        let max_bytes = std::num::NonZeroUsize::new(lineages.len() * 1_024)
            .ok_or_else(|| StorageError::internal("regional progress read bound"))?;
        let pages = self
            .attempt
            .query_batch_protected_exact(window, max_bytes)
            .await?;
        let mut pages = pages.into_iter();
        self.catalog_lock.finish_lock_prefix(prefix, &mut pages)?;
        let page = pages
            .next()
            .ok_or_else(|| StorageError::internal("SQL regional progress page is missing"))?;
        if page.more {
            return Err(StorageError::internal(
                "SQL regional progress exceeded its page bound",
            ));
        }
        match_observation_rows(lineages.len(), page.rows)?
            .into_iter()
            .map(|row| {
                row.map(|row| {
                    let [through, sealed]: [SqlValue; 2] =
                        row.into_values().try_into().map_err(|_| {
                            StorageError::internal("SQL regional progress row is invalid")
                        })?;
                    decode_progress_values(through, sealed)
                })
                .transpose()
            })
            .collect()
    }

    async fn list_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
    ) -> StorageResult<Vec<(LineageRef, LineageCursor)>> {
        let p = |n| self.engine.engine().placeholder(n);
        let statement = SqlStatement::new(
            format!(
                "SELECT shard_id, lineage_id, through_text, sealed FROM {relation} WHERE table_id \
                 = {} AND pair_id = {} AND direction = {} ORDER BY shard_id, lineage_id",
                p(1),
                p(2),
                p(3),
                relation = progress_relation(self.engine),
            ),
            vec![
                table_id(table),
                pair_value(pair),
                SqlValue::Integer(i64::from(direction as u8)),
            ],
        );
        let page = self
            .attempt
            .query(
                statement,
                ReadProtection::Snapshot,
                std::num::NonZeroUsize::new(4 * 1024 * 1024)
                    .ok_or_else(|| StorageError::internal("bound"))?,
            )
            .await?;
        if page.more {
            return Err(StorageError::internal(
                "a table has more regional progress rows than the list bound",
            ));
        }
        let invalid = || StorageError::internal("SQL regional progress row is invalid");
        page.rows
            .into_iter()
            .map(|row| {
                let [shard, lineage, through, sealed]: [SqlValue; 4] =
                    row.into_values().try_into().map_err(|_| invalid())?;
                let (SqlValue::Text(shard), SqlValue::Text(lineage)) = (shard, lineage) else {
                    return Err(invalid());
                };
                Ok((
                    LineageRef {
                        shard: SystemStreamShardId::try_new(shard).map_err(|_| invalid())?,
                        lineage: SystemStreamLineageId::try_new(lineage).map_err(|_| invalid())?,
                    },
                    decode_progress_values(through, sealed)?,
                ))
            })
            .collect()
    }

    fn stage_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineage: &LineageRef,
        cursor: &LineageCursor,
    ) -> StorageResult<()> {
        let sql = progress_write_sql(self.engine);
        self.stage_counted(
            sql,
            || Ok(progress_binds(table, pair, direction, lineage, cursor)),
            SqlDynamoCount::ExactlyOne,
        )
    }

    fn clear_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
    ) -> StorageResult<()> {
        let sql = format!(
            "DELETE FROM {relation} WHERE table_id = {} AND pair_id = {}",
            self.engine.engine().placeholder(1),
            self.engine.engine().placeholder(2),
            relation = progress_relation(self.engine),
        );
        self.stage_counted(
            sql,
            || Ok(vec![table_id(table), pair_value(pair)]),
            SqlDynamoCount::Unbounded,
        )
    }

    fn clear_table_regional_progress(&mut self, table: &BoundTableInfo) -> StorageResult<()> {
        let sql = format!(
            "DELETE FROM {relation} WHERE table_id = {}",
            self.engine.engine().placeholder(1),
            relation = progress_relation(self.engine),
        );
        self.stage_counted(sql, || Ok(vec![table_id(table)]), SqlDynamoCount::Unbounded)
    }
}

impl<A: SqlAttempt, L: SqlLayout> RegionalMarkerAttempt for SqlCatalogAttempt<'_, A, L> {
    /// Every path: the store is an upsert (one row); the removal deletes the
    /// marker this attempt observed (one row). Another count is an internal
    /// error: the catalog lock (pair end, abandon) and the item lock exclude a
    /// concurrent removal.
    fn stage_regional_marker(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
        marker: Option<RegionalMarker>,
    ) -> StorageResult<()> {
        let sql = if marker.is_some() {
            marker_write_sql(self.engine)
        } else {
            marker_delete_sql(self.engine)
        };
        self.stage_counted(
            sql,
            || marker_binds(table, key, marker),
            SqlDynamoCount::ExactlyOne,
        )
    }

    async fn list_regional_markers(
        &mut self,
        table: &BoundTableInfo,
        after: Option<&[u8]>,
        limit: NonZeroU32,
    ) -> StorageResult<MarkerPage> {
        let p = |n| self.engine.engine().placeholder(n);
        let mut binds = vec![table_id(table)];
        let after_clause = match after {
            Some(after) => {
                binds.push(SqlValue::Blob(after.to_vec()));
                format!(" AND item_key > {}", p(2))
            }
            None => String::new(),
        };
        let statement = SqlStatement::new(
            format!(
                "SELECT item_key, stamp, region, expiry, received FROM {relation} WHERE table_id \
                 = {}{after_clause} ORDER BY item_key LIMIT {}",
                p(1),
                u64::from(limit.get()) + 1,
                relation = self.engine.relation_name("regional_markers"),
            ),
            binds,
        );
        let page = self
            .attempt
            .query(
                statement,
                ReadProtection::Snapshot,
                NonZeroUsize::new(8 * 1024 * 1024)
                    .ok_or_else(|| StorageError::internal("bound"))?,
            )
            .await?;
        if page.more {
            return Err(StorageError::internal(
                "SQL marker page exceeded its byte bound",
            ));
        }
        let invalid = || StorageError::internal("SQL regional marker row is invalid");
        let mut markers = page
            .rows
            .into_iter()
            .map(|row| {
                let [key, stamp, region, expiry, received]: [SqlValue; 5] =
                    row.into_values().try_into().map_err(|_| invalid())?;
                let SqlValue::Blob(key) = key else {
                    return Err(invalid());
                };
                Ok((
                    key,
                    decode_marker_values([stamp, region, expiry, received])?,
                ))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let more = markers.len() > limit.get() as usize;
        markers.truncate(limit.get() as usize);
        Ok(MarkerPage { markers, more })
    }

    fn clear_regional_markers(&mut self, table: &BoundTableInfo) -> StorageResult<()> {
        let sql = format!(
            "DELETE FROM {relation} WHERE table_id = {}",
            self.engine.engine().placeholder(1),
            relation = self.engine.relation_name("regional_markers"),
        );
        self.stage_counted(sql, || Ok(vec![table_id(table)]), SqlDynamoCount::Unbounded)
    }
}
