//! Physical SQL reclamation after a deleted incarnation leaves the public
//! catalog.

use storage_provider::{
    CATALOG_READ_BYTES, CatalogAttempt, TableReclaimAttempt, TableReclaimPhase, TableReclaimStep,
    TableReclaimTarget,
};
use storage_types::{StorageError, StorageResult, TimestampMillis, storage_serde};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
};

pub(crate) async fn initialize_table_reclaim<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let text = engine.engine().text_type();
    let int = engine.engine().integer_type();
    let blob = engine.engine().blob_type();
    attempt
        .execute(SqlStatement::new(
            format!(
                "CREATE TABLE IF NOT EXISTS {relation_sys_table_reclaim} (history_id {text} \
                 PRIMARY KEY, ready_at {int} NOT NULL, payload {blob} NOT NULL){}",
                engine.engine().table_suffix(),
                relation_sys_table_reclaim = engine.relation_name("sys_table_reclaim"),
            ),
            vec![],
        ))
        .await?;
    attempt
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE INDEX IF NOT EXISTS "["sys_table_reclaim_due"],
                " ON "["sys_table_reclaim"],
                " (ready_at, history_id)"
            )
            .to_owned(),
            vec![],
        ))
        .await?;
    Ok(())
}

impl<A: SqlAttempt, L: SqlLayout> TableReclaimAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_reclaim_target(
        &mut self,
        observed_at: TimestampMillis,
    ) -> StorageResult<Option<TableReclaimTarget>> {
        let page = self
            .attempt
            .query(
                SqlStatement::new(
                    format!(
                        "SELECT history_id, ready_at, payload FROM {relation_sys_table_reclaim} \
                         WHERE ready_at <= {} ORDER BY ready_at, history_id LIMIT 1",
                        self.engine.engine().placeholder(1),
                        relation_sys_table_reclaim = self.engine.relation_name("sys_table_reclaim"),
                    ),
                    vec![SqlValue::Integer(observed_at.timestamp_millis())],
                ),
                ReadProtection::Conflict,
                CATALOG_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL reclaim discovery returned multiple targets",
            ));
        }
        let Some(row) = page.rows.into_iter().next() else {
            return Ok(None);
        };
        let SqlValue::Text(id) = row.value(ColumnIndex::new(0))? else {
            return Err(StorageError::internal(
                "SQL reclaim identity has invalid type",
            ));
        };
        let SqlValue::Integer(ready_at) = row.value(ColumnIndex::new(1))? else {
            return Err(StorageError::internal(
                "SQL reclaim deadline has invalid type",
            ));
        };
        let SqlValue::Blob(bytes) = row.value(ColumnIndex::new(2))? else {
            return Err(StorageError::internal(
                "SQL reclaim payload has invalid type",
            ));
        };
        let target: TableReclaimTarget = storage_serde::from_bytes(bytes)?;
        let expected_id = uuid::Uuid::from_bytes(*target.history_id.as_ref()).to_string();
        let expected_ready = if target.phase == TableReclaimPhase::NameReservation {
            target.name_locked_until.timestamp_millis()
        } else {
            i64::MIN
        };
        if id != &expected_id || *ready_at != expected_ready {
            return Err(StorageError::internal(
                "SQL reclaim target differs from its marker",
            ));
        }
        if self
            .read_catalog_stamp(&target.table_name)
            .await?
            .is_some_and(|stamp| stamp.history_id == target.history_id)
        {
            return Err(StorageError::internal(
                "SQL reclaim marker targets an active incarnation",
            ));
        }
        Ok(Some(target))
    }

    async fn delete_reclaim_page(
        &mut self,
        target: &TableReclaimTarget,
    ) -> StorageResult<TableReclaimStep> {
        let name = &target.table_name;
        let history = target.history_id;
        match target.phase {
            TableReclaimPhase::MetadataCleanup => {
                let mut statements = target
                    .gsi_targets
                    .iter()
                    .map(|gsi| {
                        SqlStatement::new(
                            format!(
                                "DELETE FROM {relation_sys_gsi_history_holds} WHERE table_name = \
                                 {} AND index_name = {} AND index_incarnation = {} AND lineage_id \
                                 IN (SELECT lineage_id FROM {relation_sys_gsi_history_holds} \
                                 WHERE table_name = {} AND index_name = {} AND index_incarnation \
                                 = {} ORDER BY lineage_id LIMIT 64)",
                                self.engine.engine().placeholder(1),
                                self.engine.engine().placeholder(2),
                                self.engine.engine().placeholder(3),
                                self.engine.engine().placeholder(4),
                                self.engine.engine().placeholder(5),
                                self.engine.engine().placeholder(6),
                                relation_sys_gsi_history_holds =
                                    self.engine.relation_name("sys_gsi_history_holds"),
                            ),
                            vec![
                                SqlValue::Text(name.to_string()),
                                SqlValue::Text(gsi.index_name.to_string()),
                                SqlValue::Blob(gsi.incarnation.as_ref().to_vec()),
                                SqlValue::Text(name.to_string()),
                                SqlValue::Text(gsi.index_name.to_string()),
                                SqlValue::Blob(gsi.incarnation.as_ref().to_vec()),
                            ],
                        )
                    })
                    .collect::<Vec<_>>();
                statements.push(SqlStatement::new(
                    format!(
                        "DELETE FROM {relation_sys_stream_trim_due_markers} WHERE (due_bucket, \
                         scope_id, policy_version) IN (SELECT due_bucket, scope_id, \
                         policy_version FROM {relation_sys_stream_trim_due_markers} WHERE \
                         scope_id = {} LIMIT 64)",
                        self.engine.engine().placeholder(1),
                        relation_sys_stream_trim_due_markers =
                            self.engine.relation_name("sys_stream_trim_due_markers"),
                    ),
                    vec![SqlValue::Text(storage_provider::table_retention_scope_id(
                        history,
                    ))],
                ));
                let expected = statements.len();
                let counts = self.attempt.execute_batch(statements).await?;
                if counts.len() != expected {
                    return Err(StorageError::internal("SQL metadata reclaim count differs"));
                }
                if counts.into_iter().any(|count| count == 64) {
                    return Ok(TableReclaimStep::Advanced);
                }
            }
            TableReclaimPhase::PrimaryItems => {
                drop_relation(self.attempt, self.engine.item_table_name(name, history)).await?;
            }
            TableReclaimPhase::PrimaryTombstones => {
                let count = self
                    .attempt
                    .execute(SqlStatement::new(
                        format!(
                            "DELETE FROM {relation} WHERE table_id = {} AND key_json IN (SELECT \
                             key_json FROM {relation} WHERE table_id = {} ORDER BY key_json LIMIT \
                             64)",
                            self.engine.engine().placeholder(1),
                            self.engine.engine().placeholder(2),
                            relation = self.engine.relation_name("item_tombstones"),
                        ),
                        vec![
                            SqlValue::Blob(history.as_ref().to_vec()),
                            SqlValue::Blob(history.as_ref().to_vec()),
                        ],
                    ))
                    .await?;
                if count == 64 {
                    return Ok(TableReclaimStep::Advanced);
                }
            }
            TableReclaimPhase::GsiProjections => {
                let statements = target
                    .gsi_targets
                    .iter()
                    .map(|gsi| {
                        let relation =
                            self.engine
                                .gsi_table_name(name, &gsi.index_name, &gsi.incarnation);
                        SqlStatement::new(format!("DROP TABLE IF EXISTS \"{relation}\""), vec![])
                    })
                    .collect();
                if !target.gsi_targets.is_empty() {
                    self.attempt.execute_batch(statements).await?;
                }
            }
            TableReclaimPhase::TtlIndex => {
                drop_relation(self.attempt, self.engine.ttl_table_name(name, history)).await?;
            }
            TableReclaimPhase::NameReservation => {
                let id = uuid::Uuid::from_bytes(*history.as_ref()).to_string();
                self.attempt
                    .execute(SqlStatement::new(
                        format!(
                            "DELETE FROM {relation_tables} WHERE id = {} AND locked_until = {}",
                            self.engine.engine().placeholder(1),
                            self.engine.engine().placeholder(2),
                            relation_tables = self.engine.relation_name("tables"),
                        ),
                        vec![
                            SqlValue::Text(id),
                            SqlValue::Integer(target.name_locked_until.timestamp_millis()),
                        ],
                    ))
                    .await?;
            }
            TableReclaimPhase::GsiTombstones | TableReclaimPhase::GsiNameLocks => {}
        }
        Ok(TableReclaimStep::Complete)
    }

    async fn store_reclaim_phase(
        &mut self,
        target: &TableReclaimTarget,
        next: Option<TableReclaimPhase>,
    ) -> StorageResult<()> {
        let id = uuid::Uuid::from_bytes(*target.history_id.as_ref()).to_string();
        let statement = if let Some(phase) = next {
            let mut next_target = target.clone();
            next_target.phase = phase;
            let ready_at = if phase == TableReclaimPhase::NameReservation {
                target.name_locked_until.timestamp_millis()
            } else {
                i64::MIN
            };
            SqlStatement::new(
                format!(
                    "UPDATE {relation_sys_table_reclaim} SET ready_at = {}, payload = {} WHERE \
                     history_id = {}",
                    self.engine.engine().placeholder(1),
                    self.engine.engine().placeholder(2),
                    self.engine.engine().placeholder(3),
                    relation_sys_table_reclaim = self.engine.relation_name("sys_table_reclaim"),
                ),
                vec![
                    SqlValue::Integer(ready_at),
                    SqlValue::Blob(storage_serde::to_bytes(&next_target)?),
                    SqlValue::Text(id),
                ],
            )
        } else {
            SqlStatement::new(
                format!(
                    "DELETE FROM {relation_sys_table_reclaim} WHERE history_id = {}",
                    self.engine.engine().placeholder(1),
                    relation_sys_table_reclaim = self.engine.relation_name("sys_table_reclaim"),
                ),
                vec![SqlValue::Text(id)],
            )
        };
        if self.attempt.execute(statement).await? != 1 {
            return Err(StorageError::internal(
                "SQL reclaim target changed during phase update",
            ));
        }
        Ok(())
    }
}

async fn drop_relation<A: SqlAttempt>(attempt: &mut A, relation: String) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(
            format!("DROP TABLE IF EXISTS \"{relation}\""),
            vec![],
        ))
        .await?;
    Ok(())
}
