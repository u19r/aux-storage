//! Guarded schema effects that must run after the owning transaction commits.

use storage_provider::maintenance::{NativeIndexAction, NativeIndexWork};
use storage_types::{StorageResult, TableHistoryId};

use crate::{SqlAttempt as _, SqlBackend, SqlEngine, SqlLayout, SqlStatement, SqlStore, SqlValue};

/// One immutable native object, its fresh precondition, and its DDL statement.
pub struct SqlDdl {
    #[cfg(feature = "postgres")]
    pub(crate) object: String,
    pub(crate) guard: SqlStatement,
    pub(crate) statement: SqlStatement,
    #[cfg(feature = "postgres")]
    pub(crate) lock_timeout: Option<std::time::Duration>,
}

impl SqlDdl {
    pub(crate) fn for_index(engine: impl SqlLayout, work: NativeIndexWork) -> StorageResult<Self> {
        let name = engine.gsi_index_name(work.table.history_id, &work.index);
        let exists = index_exists_clause(engine, &name);
        let (sql, guard) = match work.action {
            NativeIndexAction::Create => {
                let sql = crate::item_schema::render_gsi_index(
                    engine,
                    &work.table.info,
                    work.table.history_id,
                    &work.index,
                )?;
                let sql = if engine.engine() == SqlEngine::Postgres {
                    sql.replacen("CREATE INDEX ", "CREATE INDEX CONCURRENTLY ", 1)
                } else {
                    sql
                };
                let guard = SqlStatement::new(
                    format!(
                        "SELECT EXISTS (SELECT 1 FROM {} WHERE id = {} AND table_status <> \
                         'DELETING' AND gsi_blob = {}) AND NOT ({exists})",
                        engine.relation_name("tables"),
                        engine.engine().placeholder(1),
                        engine.engine().placeholder(2)
                    ),
                    vec![
                        SqlValue::Text(
                            uuid::Uuid::from_bytes(*work.table.history_id.as_ref()).to_string(),
                        ),
                        crate::catalog::encode_gsi_binding(&work.table.binding)?,
                    ],
                );
                (sql, guard)
            }
            NativeIndexAction::Drop => {
                return Ok(Self::drop_index(engine, work.table.history_id, &work.index));
            }
        };
        Ok(Self {
            #[cfg(feature = "postgres")]
            object: name,
            guard,
            statement: SqlStatement::new(sql, vec![]),
            #[cfg(feature = "postgres")]
            lock_timeout: None,
        })
    }

    pub(crate) fn drop_index(
        engine: impl SqlLayout,
        history: TableHistoryId,
        index: &storage_types::BoundGsi,
    ) -> Self {
        let name = engine.gsi_index_name(history, index);
        let exists = index_exists_clause(engine, &name);
        Self {
            #[cfg(feature = "postgres")]
            object: name.clone(),
            guard: SqlStatement::new(format!("SELECT {exists}"), vec![]),
            statement: SqlStatement::new(
                format!(
                    "DROP INDEX {}IF EXISTS \"{name}\"",
                    if engine.engine() == SqlEngine::Postgres {
                        "CONCURRENTLY "
                    } else {
                        ""
                    }
                ),
                vec![],
            ),
            #[cfg(feature = "postgres")]
            lock_timeout: None,
        }
    }

    pub(crate) fn drop_column(
        engine: impl SqlLayout,
        history: TableHistoryId,
        column: &storage_types::KeyColumn,
    ) -> Self {
        let table = engine.item_table_name(history);
        let column = crate::item_schema::key_column_identifier(column);
        // A waiting ACCESS EXCLUSIVE lock queues later writers. Give up and
        // retry the durable retirement job instead of creating that queue.
        Self {
            #[cfg(feature = "postgres")]
            object: table.clone(),
            guard: SqlStatement::new(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND \
                 attname = $2 AND NOT attisdropped)",
                vec![
                    SqlValue::Text(table.clone()),
                    SqlValue::Text(column.replace("\"\"", "\"")),
                ],
            ),
            statement: SqlStatement::new(
                format!("ALTER TABLE \"{table}\" DROP COLUMN \"{column}\""),
                vec![],
            ),
            #[cfg(feature = "postgres")]
            lock_timeout: Some(crate::constants::POSTGRES_DDL_LOCK_TIMEOUT),
        }
    }
}

fn index_exists_clause(engine: impl SqlLayout, name: &str) -> String {
    let literal = name.replace('\'', "''");
    if engine.engine() == SqlEngine::Postgres {
        format!("to_regclass('{literal}') IS NOT NULL")
    } else {
        format!("EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = '{literal}')")
    }
}

impl<B: SqlBackend> SqlStore<B> {
    pub(crate) async fn execute_deferred_ddl(
        backend: &B,
        effects: Vec<SqlDdl>,
    ) -> StorageResult<()> {
        for effect in effects {
            if backend.engine() == SqlEngine::Postgres {
                backend.execute_autocommit_ddl(effect).await?;
            } else {
                backend
                    .with_schema_attempt(move |attempt| {
                        Box::pin(async move {
                            let page = attempt
                                .query(
                                    effect.guard,
                                    crate::ReadProtection::Snapshot,
                                    storage_provider::CATALOG_READ_BYTES,
                                )
                                .await?;
                            if page.rows.first().is_some_and(|row| {
                                matches!(
                                    row.value(crate::ColumnIndex::new(0)),
                                    Ok(SqlValue::Integer(1))
                                )
                            }) {
                                attempt.execute(effect.statement).await?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
            }
        }
        Ok(())
    }
}
