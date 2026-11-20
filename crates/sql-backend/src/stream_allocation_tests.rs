use super::stream_allocation::{StreamEventNumbers, mapped_offset};

#[test]
fn given_native_transaction_when_reserving_records_then_event_numbers_are_unique_and_bounded() {
    let mut allocation = StreamEventNumbers::default();
    for event_number in 0..65_536 {
        assert_eq!(u32::from(allocation.reserve().unwrap()), event_number);
    }
    assert!(allocation.reserve().is_err());
}

#[test]
fn given_invalid_native_transaction_when_mapping_offset_then_fail_without_wrapping() {
    assert!(mapped_offset(0).is_err());
    assert!(mapped_offset(u64::MAX).is_err());
    assert!(mapped_offset(i64::MAX as u64 / 65_536 + 1).is_err());
}

#[test]
fn given_postgres_writer_when_loading_topology_then_native_id_is_not_assigned() {
    use super::stream_history::{HistoryRead, history_statement};
    let (writer, _) =
        history_statement(crate::SqlEngine::Postgres, HistoryRead::Writer).into_parts();
    let (reader, _) =
        history_statement(crate::SqlEngine::Postgres, HistoryRead::Reader).into_parts();
    assert!(!writer.contains("sys_dynamo_stream_records"));
    assert!(!reader.contains("sys_stream_entries"));
    assert!(!reader.contains("sys_system_stream_sequences"));
    assert!(!writer.contains("sys_system_stream_sequences"));
    assert!(!reader.contains("sys_dynamo_stream_records"));
    assert!(!writer.contains("pg_current_xact_id()"));
    assert!(!writer.contains("sys_stream_entries"));
    assert!(!reader.contains("pg_current_xact_id()"));
}

#[test]
fn given_restored_history_when_clock_is_lower_then_later_batches_follow_every_old_event() {
    let old_high = 99 * 65_536 + 65_535;
    let offset = super::stream_allocation::restored_transaction_offset(0, 1, old_high, 0).unwrap();
    assert_eq!(offset, 98);
    assert!(mapped_offset(2 + offset as u64).unwrap() > old_high);
    assert!(mapped_offset(3 + offset as u64).unwrap() > old_high);
}

#[test]
fn given_existing_offset_when_reopening_then_preserve_it_and_cover_native_user_ids() {
    use super::stream_allocation::restored_transaction_offset;
    assert_eq!(restored_transaction_offset(98, 1000, 1, 0).unwrap(), 98);
    assert_eq!(restored_transaction_offset(0, 1, 1, 100).unwrap(), 99);
    assert!(restored_transaction_offset(0, 1, 0, i64::MAX as u64).is_err());
    assert!(restored_transaction_offset(u64::MAX, 1, 0, 0).is_err());
}

#[test]
fn given_postgres_split_when_loading_topology_then_only_parent_tail_is_read() {
    use super::stream_history::{HistoryRead, history_statement};
    let parent = storage_types::SystemStreamLineageId::try_new("parent").unwrap();
    let (sql, params) =
        history_statement(crate::SqlEngine::Postgres, HistoryRead::Split(&parent)).into_parts();
    assert!(!sql.contains("sys_system_stream_sequences"));
    assert!(sql.contains("l.lineage_id = $1 AND r.shard_id = l.shard_id"));
    assert!(sql.contains("l.retired_offset + 1"));
    assert!(sql.contains("ORDER BY r.offset_order DESC, r.offset_text DESC LIMIT 1"));
    assert!(matches!(params.as_slice(), [crate::SqlValue::Text(value)] if value == "parent"));
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_user_stream_ids_when_node_starts_then_positions_use_index_ranges_and_cover_ids() {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let schema = format!("stream_positions_{}", uuid::Uuid::new_v4().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let separator = if dsn.contains('?') { '&' } else { '?' };
    let scoped = format!("{dsn}{separator}options=-csearch_path%3D{schema}");
    open_positions_store(&scoped).await;
    admin
        .batch_execute(&format!(
            "SET search_path TO {schema}; INSERT INTO sys_dynamo_stream_items (stream_name, \
             item_id, row_blob, created_at) VALUES ('\\x01', decode('58494431', 'hex') || \
             int8send(5), '\\x00', 0), ('\\x02', decode('58494431', 'hex') || \
             int8send(1000000000000), '\\x00', 0), ('\\x03', decode('58494431', 'hex') || \
             int8send(1000000000000000) || '\\x00'::bytea, '\\x00', 0)"
        ))
        .await
        .unwrap();
    admin
        .batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = admin
        .query(
            &format!(
                "EXPLAIN {}",
                super::stream_allocation::POSTGRES_STREAM_POSITIONS
            ),
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    admin.batch_execute("RESET enable_seqscan").await.unwrap();
    assert!(
        !plan.contains("Seq Scan on sys_dynamo_stream_items"),
        "the user high must be read by index range:\n{plan}"
    );
    open_positions_store(&scoped).await;
    let (offset, next) = observe_positions(&admin).await;
    assert!(
        offset + next > 1_000_000_000_000,
        "the next native user ID must follow every restored one"
    );
    assert!(
        offset + next < 1_000_000_000_000_000,
        "an opaque ID is not a native clock authority"
    );
    open_positions_store(&scoped).await;
    assert_eq!(
        observe_positions(&admin).await.0,
        offset,
        "an unchanged high keeps the offset"
    );
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_sealed_lineage_end_below_its_start_when_reading_history_then_unavailable() {
    use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, StorageEnum};

    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let schema = format!("stream_history_{}", uuid::Uuid::new_v4().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let separator = if dsn.contains('?') { '&' } else { '?' };
    let scoped = format!("{dsn}{separator}options=-csearch_path%3D{schema}");
    let backend = crate::postgres::PostgresBackend::initialize(
        &scoped,
        2,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    let corrupted = admin
        .batch_execute(&format!(
            "SET search_path TO {schema}; UPDATE sys_system_stream_lineage SET sealed = TRUE, \
             sealed_after_offset = initial_sequence, initial_sequence = initial_sequence + 1 \
             WHERE scope_key = 'system'"
        ))
        .await;
    match corrupted {
        // A CHECK forbids the state: the sealed branch is unreachable on SQL.
        Err(error) if error.code().map(|code| code.code()) == Some("23514") => {}
        Err(error) => panic!("unexpected corruption failure: {error}"),
        Ok(()) => {
            let result = storage_provider::DynamoPort::with_attempt(
                &store,
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(
                    storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
                ),
                |attempt| {
                    Box::pin(async move {
                        stream_provider::StreamAttempt::read_system_page_start(attempt)
                            .await
                            .map(|_| ())
                    })
                },
            )
            .await;
            let Err(error) = result else {
                panic!("a sealed lineage end below its start must be unavailable");
            };
            assert!(matches!(error.as_ref(), StorageEnum::HistoryUnavailable));
        }
    }
    drop(store);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[cfg(feature = "postgres")]
async fn open_positions_store(dsn: &str) {
    let backend = crate::postgres::PostgresBackend::initialize(
        dsn,
        2,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    drop(
        crate::SqlStore::initialize_schema(
            backend,
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap(),
    );
}

#[cfg(feature = "postgres")]
async fn observe_positions(admin: &tokio_postgres::Client) -> (i64, i64) {
    let row = admin
        .query_one(
            "SELECT transaction_offset, pg_snapshot_xmax(pg_current_snapshot())::text::bigint \
             FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap();
    (row.get::<_, i64>(0), row.get::<_, i64>(1))
}
