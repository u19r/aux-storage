use std::borrow::Cow;

use crate::{SqlEngine, SqlStatement, SqlValue, sql_template::sql_template};

#[test]
fn given_fixed_template_when_dialects_selected_then_preserve_explicit_slots_and_borrow_sql() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let sql = sql_template!(engine, "SELECT " {2}, ", " {1}, ", " {2});
        let (sql, binds) =
            SqlStatement::new(sql, vec![SqlValue::Integer(11), SqlValue::Integer(22)]).into_parts();
        assert!(matches!(sql, Cow::Borrowed(_)));
        assert_eq!(sql, format!("SELECT {prefix}2, {prefix}1, {prefix}2"));
        assert_eq!(binds.len(), 2);
    }
}

#[test]
fn given_sequence_advance_when_statement_rendered_then_preserve_full_sql_and_bind_order() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let (sql, binds) = crate::stream_history::sequence_advance_statement(
            engine,
            "shard-01",
            std::num::NonZeroU64::new(41).unwrap(),
            std::num::NonZeroU64::new(42).unwrap(),
        )
        .unwrap()
        .into_parts();
        assert!(matches!(sql, Cow::Borrowed(_)));
        assert_eq!(
            sql,
            format!(
                "UPDATE sys_system_stream_sequences SET next_offset = {prefix}1 WHERE scope_key = \
                 'system' AND shard_id = {prefix}2 AND next_offset = {prefix}3"
            )
        );
        assert!(
            matches!(binds.as_slice(), [SqlValue::Integer(42), SqlValue::Text(shard), SqlValue::Integer(41)] if shard == "shard-01")
        );
    }
}

#[derive(Default)]
struct CaptureAttempt {
    statements: Vec<SqlStatement>,
    rejected: bool,
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
}

impl crate::SqlAttempt for CaptureAttempt {
    fn reject_settlement(&mut self) {
        self.rejected = true;
    }
    fn reject_settlement_error(&mut self, _: &storage_types::StorageError) {
        self.reject_settlement();
    }
    fn require_settlement(&self) -> storage_types::StorageResult<()> {
        if self.rejected {
            return Err(storage_types::StorageError::internal(
                "statement fixture rejected settlement",
            ));
        }
        Ok(())
    }
    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        &mut self.ownership
    }

    async fn query(
        &mut self,
        statement: SqlStatement,
        _: crate::ReadProtection,
        _: std::num::NonZeroUsize,
    ) -> storage_types::StorageResult<crate::SqlQueryPage> {
        self.statements.push(statement);
        Ok(crate::SqlQueryPage::new())
    }
    async fn execute(&mut self, statement: SqlStatement) -> storage_types::StorageResult<u64> {
        self.statements.push(statement);
        Ok(1)
    }
    async fn execute_batch(
        &mut self,
        statements: Vec<SqlStatement>,
    ) -> storage_types::StorageResult<Vec<u64>> {
        let rows = vec![1; statements.len()];
        self.statements.extend(statements);
        Ok(rows)
    }
    async fn query_batch_protected(
        &mut self,
        statements: Vec<(SqlStatement, crate::ReadProtection)>,
        _: std::num::NonZeroUsize,
    ) -> storage_types::StorageResult<Vec<crate::SqlQueryPage>> {
        Ok(statements
            .into_iter()
            .map(|(statement, _)| {
                self.statements.push(statement);
                crate::SqlQueryPage::new()
            })
            .collect())
    }
}

#[test]
fn given_catalog_lookups_when_fixed_sql_selected_then_preserve_full_text_and_bind_values() {
    futures::executor::block_on(async {
        let internal_id = storage_types::StreamName::new(b"internal-stream");
        for (engine, prefix) in [
            (SqlEngine::Sqlite, "?"),
            (SqlEngine::Turso, "?"),
            (SqlEngine::Postgres, "$"),
        ] {
            let mut attempt = CaptureAttempt::default();
            assert!(
                crate::queue_catalog::read_queue_by_name(
                    &mut attempt,
                    engine,
                    crate::ReadProtection::Snapshot,
                    "queue"
                )
                .await
                .unwrap()
                .is_none()
            );
            let queues = crate::queue_catalog::read_queues_by_url(
                &mut attempt,
                engine,
                crate::ReadProtection::Snapshot,
                vec!["url".to_owned()],
            )
            .await
            .unwrap();
            assert_eq!(queues.len(), 1);
            assert!(queues[0].is_none());
            for lookup in [
                stream_provider::StreamCatalogLookup::Name("stream"),
                stream_provider::StreamCatalogLookup::Id(&internal_id),
            ] {
                assert!(
                    crate::stream_catalog::read_stream_record(
                        &mut attempt,
                        engine,
                        crate::ReadProtection::Snapshot,
                        lookup
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
            }
            let expected = [
                format!(
                    "SELECT queue_name, queue_url, incarnation, row_blob FROM sys_queue_catalog \
                     WHERE queue_name = {prefix}1"
                ),
                format!(
                    "SELECT queue_name, queue_url, incarnation, row_blob FROM sys_queue_catalog \
                     WHERE queue_url = {prefix}1"
                ),
                format!("SELECT row_blob FROM sys_user_stream_catalog WHERE name = {prefix}1"),
                format!(
                    "SELECT row_blob FROM sys_user_stream_catalog WHERE internal_id = {prefix}1"
                ),
            ];
            for (index, (statement, expected)) in
                attempt.statements.into_iter().zip(expected).enumerate()
            {
                let (sql, binds) = statement.into_parts();
                assert!(matches!(sql, Cow::Borrowed(_)));
                assert_eq!(sql, expected);
                match index {
                    0 => assert!(
                        matches!(binds.as_slice(), [SqlValue::Text(value)] if value == "queue")
                    ),
                    1 => assert!(
                        matches!(binds.as_slice(), [SqlValue::Text(value)] if value == "url")
                    ),
                    2 => assert!(
                        matches!(binds.as_slice(), [SqlValue::Text(value)] if value == "stream")
                    ),
                    3 => assert!(
                        matches!(binds.as_slice(), [SqlValue::Blob(value)] if value.as_slice() == internal_id.as_ref())
                    ),
                    _ => unreachable!(),
                }
            }
        }
    });
}

#[cfg(feature = "sqlite")]
#[test]
fn given_fixed_templates_when_sqlite_prepares_and_binds_then_preserve_parameter_semantics() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE sys_system_stream_sequences (scope_key TEXT, shard_id TEXT, next_offset \
             INTEGER); INSERT INTO sys_system_stream_sequences VALUES ('system', 'shard-01', 41)",
        )
        .unwrap();
    let (sql, binds) = crate::stream_history::sequence_advance_statement(
        SqlEngine::Sqlite,
        "shard-01",
        std::num::NonZeroU64::new(41).unwrap(),
        std::num::NonZeroU64::new(42).unwrap(),
    )
    .unwrap()
    .into_parts();
    let mut prepared = connection.prepare(&sql).unwrap();
    assert_eq!(prepared.parameter_count(), binds.len());
    assert_eq!(
        prepared
            .execute(rusqlite::params![42, "shard-01", 41])
            .unwrap(),
        1
    );
    assert_eq!(
        prepared
            .execute(rusqlite::params![42, "shard-01", 41])
            .unwrap(),
        0
    );
    let value: i64 = connection
        .query_row(
            "SELECT next_offset FROM sys_system_stream_sequences",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, 42);
    let sql = sql_template!(SqlEngine::Sqlite, "SELECT " {2}, ", " {1}, ", " {2});
    let values: (i64, i64, i64) = connection
        .query_row(&sql, [11, 22], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(values, (22, 11, 22));
}
