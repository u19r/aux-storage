use std::{hint::black_box, num::NonZeroU32};

use alloc_counter::AllocationGuard;
use storage_types::{StreamItemId, StreamName};
use stream_provider::{ReadDirection, StreamPartitioningMode, StreamReadRequest, StreamRowKind};

use super::sql_statements;
use crate::SqlEngine;

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_stream_read_shapes_when_statements_rendered_then_report_cpu_and_allocations() {
    let stream = StreamRowKind::User(StreamName::new(b"performance-stream"));
    let start = StreamItemId::random();
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        for (shape, exclusive_start) in [("first", None), ("continued", Some(start))] {
            let request = StreamReadRequest {
                partitioning_mode: StreamPartitioningMode::Single,
                exclusive_start,
                direction: ReadDirection::Forward,
                limit: NonZeroU32::new(32).unwrap(),
            };
            let label = format!("{name}_stream_{shape}");
            let (sql, binds) = sql_statements::read_rows(engine, &stream, &request).into_parts();
            assert_eq!(binds.len(), if exclusive_start.is_some() { 3 } else { 2 });
            assert!(matches!(sql, std::borrow::Cow::Owned(_)));
            assert!(sql.contains("ORDER BY item_id ASC LIMIT"));
            for _ in 0..100 {
                drop(black_box(sql_statements::read_rows(
                    engine, &stream, &request,
                )));
            }
            let guard = AllocationGuard::start(
                module_path!(),
                "sql_stream_statement",
                file!(),
                line!(),
                Some(&label),
            );
            let started = cpu_time::ProcessTime::now();
            for _ in 0..100_000 {
                drop(black_box(sql_statements::read_rows(
                    black_box(engine),
                    black_box(&stream),
                    black_box(&request),
                )));
            }
            let cpu = started.elapsed();
            alloc_counter::emit_report(&guard.finish()).unwrap();
            eprintln!(
                "sql_stream_statement label={label} iterations=100000 cpu_ns={}",
                cpu.as_nanos()
            );
        }
    }
}

#[cfg(feature = "sqlite")]
#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_sqlite_stream_read_when_native_attempts_run_then_report_cpu_and_allocations() {
    use crate::{AttemptMode, SqlAttempt as _, SqlBackend as _, SqlStatement};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let backend = crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap();
        backend
            .with_attempt(AttemptMode::ReadWrite, |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "CREATE TABLE sys_dynamo_stream_items (stream_name BLOB, item_id \
                             BLOB, row_blob BLOB, PRIMARY KEY (stream_name, item_id))"
                                .to_owned(),
                            vec![],
                        ))
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        for (label, exclusive_start, is_point) in [
            ("sqlite_native_first", None, false),
            (
                "sqlite_native_continued",
                Some(StreamItemId::random()),
                false,
            ),
            ("sqlite_native_point", Some(StreamItemId::random()), true),
        ] {
            for _ in 0..20 {
                read_empty_stream(&backend, exclusive_start, is_point).await;
            }
            let guard = AllocationGuard::start(
                module_path!(),
                "sql_native_stream",
                file!(),
                line!(),
                Some(label),
            );
            let started = cpu_time::ProcessTime::now();
            for _ in 0..2000 {
                read_empty_stream(&backend, exclusive_start, is_point).await;
            }
            let cpu = started.elapsed();
            alloc_counter::emit_report(&guard.finish()).unwrap();
            eprintln!(
                "sql_native_stream label={label} iterations=2000 cpu_ns={}",
                cpu.as_nanos()
            );
        }
    });

    async fn read_empty_stream(
        backend: &crate::sqlite::SqliteBackend,
        exclusive_start: Option<StreamItemId>,
        is_point: bool,
    ) {
        use stream_provider::StreamRowReadKind;

        use crate::{AttemptMode, ReadProtection, SqlBackend as _};
        let page = backend
            .with_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let stream = StreamRowKind::User(StreamName::new(b"performance-stream"));
                    let request = StreamReadRequest {
                        partitioning_mode: StreamPartitioningMode::Single,
                        exclusive_start,
                        direction: ReadDirection::Forward,
                        limit: NonZeroU32::new(32).unwrap(),
                    };
                    super::read::read_stream(
                        attempt,
                        SqlEngine::Sqlite,
                        ReadProtection::Snapshot,
                        stream.row_read(if is_point {
                            StreamRowReadKind::Point {
                                partitioning_mode: StreamPartitioningMode::Single,
                                item_id: exclusive_start.unwrap(),
                            }
                        } else {
                            StreamRowReadKind::Page(&request)
                        }),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        match black_box(page) {
            stream_provider::StreamReadResult::Items(page) => {
                assert!(page.rows.is_empty());
                assert!(!page.more);
            }
            stream_provider::StreamReadResult::Item(item) => assert!(item.is_none()),
            _ => panic!("unexpected read kind"),
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_point_read_sql_when_rendered_then_report_cpu_and_allocations() {
    let item_id = StreamItemId::random();
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        for (kind, stream) in [
            ("user", StreamRowKind::User(StreamName::new(b"stream"))),
            ("table", StreamRowKind::Table(StreamName::new(b"table"))),
        ] {
            let label = format!("{name}_point_{kind}");
            for _ in 0..100 {
                drop(black_box(sql_statements::read_item(
                    engine, &stream, item_id,
                )));
            }
            let guard = AllocationGuard::start(
                module_path!(),
                "sql_point_statement",
                file!(),
                line!(),
                Some(&label),
            );
            let started = cpu_time::ProcessTime::now();
            for _ in 0..100_000 {
                drop(black_box(sql_statements::read_item(
                    black_box(engine),
                    black_box(&stream),
                    black_box(item_id),
                )));
            }
            let cpu = started.elapsed();
            alloc_counter::emit_report(&guard.finish()).unwrap();
            eprintln!(
                "sql_point_statement label={label} iterations=100000 cpu_ns={}",
                cpu.as_nanos()
            );
        }
    }
}

#[test]
fn given_stream_point_kinds_when_sql_rendered_then_preserve_full_sql_and_bind_order() {
    let item_id = StreamItemId::random();
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for stream in [
            StreamRowKind::User(StreamName::new(b"stream")),
            StreamRowKind::Table(StreamName::new(b"table")),
        ] {
            let suffix =
                if engine == SqlEngine::Postgres && matches!(stream, StreamRowKind::User(_)) {
                    " AND item_id < (decode('58494431', 'hex') || \
                     int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint))"
                } else {
                    ""
                };
            let (sql, binds) = sql_statements::read_item(engine, &stream, item_id).into_parts();
            assert!(matches!(sql, std::borrow::Cow::Borrowed(_)));
            assert_eq!(
                sql,
                format!(
                    "SELECT item_id, row_blob FROM sys_dynamo_stream_items WHERE stream_name = \
                     {prefix}1 AND item_id = {prefix}2{suffix}"
                )
            );
            assert!(
                matches!(binds.as_slice(), [crate::SqlValue::Blob(name), crate::SqlValue::Blob(id)] if name.as_slice() == stream.value().as_ref() && id.as_slice() == item_id.as_bytes())
            );
        }
    }
}
