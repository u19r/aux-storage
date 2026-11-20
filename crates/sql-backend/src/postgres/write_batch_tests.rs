use std::num::{NonZeroU64, NonZeroUsize};

use storage_types::{AttemptPlan, DeadlineClass, StorageError};

use super::test_support::NativeFixture;
use crate::{
    AttemptMode, SqlAttempt, SqlBackend, SqlEngine, SqlStatement, SqlValue, SqlWriteBatchFailure,
};

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(4096).unwrap()
}
fn statement(sql: &'static str) -> SqlStatement {
    SqlStatement::new(sql, vec![])
}

async fn open_fixture() -> NativeFixture {
    let fixture = NativeFixture::open().await;
    eprintln!("owned terminal write fixture schema: {}", fixture.schema);
    fixture
        .admin
        .batch_execute(&format!(
            "SET search_path TO {}; CREATE TABLE sys_system_stream_scope (slot bigint NOT NULL); \
             CREATE TABLE counted_primary (value bigint PRIMARY KEY, payload bigint NOT NULL); \
             CREATE TABLE counted_delete (value text NOT NULL); CREATE TABLE \
             sys_transact_receipts (token text PRIMARY KEY, receipt_blob bytea NOT NULL); CREATE \
             TABLE sys_system_stream_sequences (scope_key text, shard_id text, next_offset \
             bigint); INSERT INTO sys_system_stream_sequences VALUES ('system', 'shard', 1); \
             CREATE TABLE sys_stream_entries (table_id bytea, shard_id text, lineage_id text, \
             offset_order bytea, offset_text text, entry_blob bytea, created_at bigint); CREATE \
             FUNCTION skip_counted() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; \
             END $$",
            fixture.schema,
        ))
        .await
        .unwrap();
    fixture
}

fn upsert(value: i64) -> crate::SqlCountedWrite {
    crate::write_batch::UpsertShape::try_new(
        vec!["value".into(), "payload".into()],
        vec![false, false],
        vec!["value".into()],
    )
    .unwrap()
    .render_counted(
        SqlEngine::Postgres,
        "counted_primary",
        vec![vec![SqlValue::Integer(value), SqlValue::Integer(7)]],
    )
    .unwrap()
}

fn sequence(expected: u64) -> crate::SqlCountedWrite {
    crate::stream_history::counted_sequence_advance(
        SqlEngine::Postgres,
        "shard",
        NonZeroU64::new(expected).unwrap(),
        NonZeroU64::new(3).unwrap(),
    )
    .unwrap()
}

fn assert_error(failure: SqlWriteBatchFailure, expected_index: usize, expected: &str) {
    match failure {
        SqlWriteBatchFailure::CountAssertion {
            statement_index,
            error,
        } => {
            assert_eq!(statement_index, expected_index);
            assert!(
                format!("{:?}", error.as_ref()).contains(expected),
                "{error:?}"
            );
        }
        SqlWriteBatchFailure::Other { error, .. } => {
            panic!("expected owned count failure: {:?}", error.as_ref())
        }
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_counted_writes_when_healthy_then_commit_and_finish_once() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_write_batch(
                            vec![
                                upsert(1),
                                sequence(1),
                                crate::transact_write_statements::counted_receipt(
                                    SqlEngine::Postgres,
                                    "healthy",
                                    vec![1],
                                )
                                .unwrap(),
                            ],
                            budget(),
                        )
                        .await?;
                    let pages = result.ok().expect("healthy counts");
                    assert_eq!(pages.len(), 3);
                    Ok(())
                })
            },
        )
        .await;
    let () = result.unwrap_or_else(|error| panic!("{:?}", error.as_ref()));
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_false_count_rules_when_terminal_then_earlier_effects_roll_back() {
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(
            "CREATE TRIGGER skip_primary BEFORE INSERT ON counted_primary FOR EACH ROW EXECUTE \
             FUNCTION skip_counted(); INSERT INTO counted_delete VALUES ('5'), ('5')",
        )
        .await
        .unwrap();
    for (counted, expected) in [
        (upsert(1), "SQL primary write affected the wrong row count"),
        (
            crate::write_batch::DeleteShape::try_new(vec!["value".into()], vec![false])
                .unwrap()
                .render_counted(
                    SqlEngine::Postgres,
                    "counted_delete",
                    vec![vec![SqlValue::Text("5".into())]],
                )
                .unwrap(),
            "SQL primary write affected the wrong row count",
        ),
        (sequence(2), "HistoryUnavailable"),
    ] {
        let result = fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(fixture.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt
                            .execute(statement("INSERT INTO native_commit_probe VALUES (2)"))
                            .await?;
                        let failure = attempt
                            .finish_write_batch(vec![counted], budget())
                            .await?
                            .err()
                            .expect("false count");
                        assert_error(failure, 0, expected);
                        Ok(())
                    })
                },
            )
            .await;
        assert!(result.is_err());
        assert!(!result.unwrap_err().is_commit_unknown());
        assert_eq!(fixture.count_records().await, 0);
    }
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_delete", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_receipt_count_failure_when_callback_replaces_error_then_receipt_cause_survives() {
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(
            "CREATE TRIGGER skip_receipt BEFORE INSERT ON sys_transact_receipts FOR EACH ROW \
             EXECUTE FUNCTION skip_counted()",
        )
        .await
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let failure = attempt
                        .finish_write_batch(
                            vec![
                                upsert(3),
                                crate::transact_write_statements::counted_receipt(
                                    SqlEngine::Postgres,
                                    "rejected",
                                    vec![1],
                                )
                                .unwrap(),
                            ],
                            budget(),
                        )
                        .await?
                        .err()
                        .expect("receipt count");
                    assert_error(
                        failure,
                        1,
                        "SQL transact token receipt write affected another row count",
                    );
                    Err::<(), _>(StorageError::internal("later callback failure"))
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert!(
        format!("{:?}", result.as_ref())
            .contains("SQL transact token receipt write affected another row count")
    );
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_later_window_count_failure_when_drained_then_all_windows_roll_back() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let mut writes = (0..257).map(upsert).collect::<Vec<_>>();
                    writes.push(sequence(2));
                    writes.push(upsert(999));
                    assert_error(
                        attempt
                            .finish_write_batch(writes, budget())
                            .await?
                            .err()
                            .expect("middle failure"),
                        257,
                        "HistoryUnavailable",
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!result.unwrap_err().is_commit_unknown());
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_insufficient_fixed_result_budget_when_caught_then_no_mutation_occurs() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_write_batch(vec![upsert(1)], NonZeroUsize::new(1).unwrap())
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

fn stream_write(bias: i64, native: bool) -> crate::SqlCountedWrite {
    use storage_types::{
        AttributeMap, AttributeValue, KeyAttributes, StreamBinding, StreamViewType,
        TimestampMillis, WireItem,
    };
    let mut table = crate::test_support::table();
    table.binding.stream = Some(StreamBinding {
        view: StreamViewType::KeysOnly,
        topology_revision: 0,
        xid_bias: 0,
    });
    let keys = KeyAttributes::default();
    let route =
        stream_provider::DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys)
            .unwrap();
    let item = route
        .verify_item(
            WireItem::try_from(AttributeMap::from_iter([(
                "body".into(),
                AttributeValue::S("value".into()),
            )]))
            .unwrap(),
        )
        .unwrap();
    let entry = stream_provider::StreamEntryInput::try_new(
        &table,
        &route,
        None,
        Some((&item, &[])),
        TimestampMillis::from_timestamp(1),
        None,
    )
    .unwrap();
    let lineage =
        stream_provider::SystemStreamLineage::try_new("shard", "lineage", None, false).unwrap();
    let scope = storage_types::SystemStreamScope::try_new("scope", 1).unwrap();
    if native {
        crate::stream_write::sql_statements::counted_native_entry(
            SqlEngine::Postgres,
            &table,
            &lineage,
            (&entry, &scope),
            (1, bias),
        )
        .unwrap()
    } else {
        let position = storage_types::SystemStreamPosition::try_new(
            lineage.shard_id().as_str(),
            lineage.lineage_id().as_str(),
            "00000000000000000001",
        )
        .unwrap();
        crate::stream_write::sql_statements::counted_entry(
            SqlEngine::Postgres,
            &table,
            &position,
            (&entry, &scope),
        )
        .unwrap()
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_native_stream_when_asserted_then_nested_cte_and_overflow_keep_their_meaning() {
    let fixture = open_fixture().await;
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_write_batch(vec![stream_write(0, true)], budget())
                            .await?
                            .is_ok()
                    );
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let failure = attempt
                        .finish_write_batch(vec![upsert(1), stream_write(i64::MAX, true)], budget())
                        .await?
                        .err()
                        .expect("overflow");
                    match failure {
                        SqlWriteBatchFailure::Other {
                            statement_index: 1,
                            error,
                        } => assert!(format!("{:?}", error.as_ref()).contains("22003")),
                        _ => panic!("overflow must retain native classification"),
                    }
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!result.unwrap_err().is_commit_unknown());
    fixture
        .admin
        .batch_execute(
            "CREATE TRIGGER skip_stream BEFORE INSERT ON sys_stream_entries FOR EACH ROW EXECUTE \
             FUNCTION skip_counted()",
        )
        .await
        .unwrap();
    for native in [true, false] {
        let result = fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(fixture.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        assert_error(
                            attempt
                                .finish_write_batch(
                                    vec![upsert(2), stream_write(0, native)],
                                    budget(),
                                )
                                .await?
                                .err()
                                .expect("stream count"),
                            1,
                            "SQL Dynamo stream write affected an unexpected row count",
                        );
                        Ok(())
                    })
                },
            )
            .await;
        assert!(result.is_err());
        assert!(!result.unwrap_err().is_commit_unknown());
    }
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM sys_stream_entries", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_wrong_bind_type_when_terminal_is_caught_then_all_mutations_remain_undispatched() {
    let fixture = open_fixture().await;
    let malformed = crate::write_batch::UpsertShape::try_new(
        vec!["value".into(), "payload".into()],
        vec![false, false],
        vec!["value".into()],
    )
    .unwrap()
    .render_counted(
        SqlEngine::Postgres,
        "counted_primary",
        vec![vec![
            SqlValue::Text("wrong type".into()),
            SqlValue::Integer(7),
        ]],
    )
    .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_write_batch(vec![upsert(1), malformed], budget())
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct TerminalFaultProbe {
    queries: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for TerminalFaultProbe {
    fn fail(&self, call: crate::SqlFaultCall<'_>) -> Option<StorageError> {
        if matches!(call.operation, crate::SqlFaultOperation::Query)
            && self
                .queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 1
        {
            Some(StorageError::internal("rejected before terminal dispatch"))
        } else {
            None
        }
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_later_local_fault_when_terminal_is_caught_then_no_mutation_dispatches() {
    let fixture = open_fixture().await;
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(std::sync::Arc::new(TerminalFaultProbe::default()));
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_write_batch(vec![upsert(1), upsert(2)], budget())
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_unowned_division_when_caught_then_original_native_cause_remains() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let failure = attempt
                        .query_batch_open(
                            vec![(statement("SELECT 1 / 0"), crate::ReadProtection::Snapshot)],
                            budget(),
                        )
                        .await?
                        .failure
                        .unwrap();
                    assert!(format!("{:?}", failure.error.as_ref()).contains("22012"));
                    Err::<(), _>(StorageError::internal("later callback failure"))
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert!(format!("{:?}", result.as_ref()).contains("22012"));
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct SettlementProbe {
    lose_ack: bool,
    lose_abort: bool,
    commits: std::sync::atomic::AtomicUsize,
    aborts: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for SettlementProbe {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }
    fn before_wave<'a>(
        &'a self,
        statements: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            for statement in statements {
                if statement.sql == "COMMIT" {
                    self.commits
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if statement.sql == "ROLLBACK" {
                    self.aborts
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if self.lose_abort {
                        return Err(StorageError::commit_unknown(
                            "suppressed terminal abort acknowledgement",
                        ));
                    }
                }
            }
            Ok(())
        })
    }
    fn at_commit_boundary(
        &self,
        boundary: crate::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if self.lose_ack
                && matches!(
                    boundary,
                    crate::SqlCommitBoundary::AfterDurableAcknowledgement
                )
            {
                return Err(StorageError::internal(
                    "suppressed terminal acknowledgement",
                ));
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_terminal_ack_loss_when_callback_catches_then_unknown_beats_success() {
    let fixture = open_fixture().await;
    let probe = std::sync::Arc::new(SettlementProbe {
        lose_ack: true,
        ..Default::default()
    });
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let writes = vec![upsert(1)];
                    assert!(
                        attempt
                            .finish_write_batch(writes, budget())
                            .await
                            .err()
                            .unwrap()
                            .is_commit_unknown()
                    );
                    Err::<(), _>(
                        storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical("later callback conflict"),
                            ),
                        )
                        .into(),
                    )
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert!(result.is_commit_unknown());
    assert_eq!(probe.commits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(backend);
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_inflight_terminal_cancel_when_callback_catches_then_unknown_and_no_second_settlement()
 {
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(
            "CREATE FUNCTION delay_counted() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM \
             pg_sleep(0.1); RETURN NEW; END $$; CREATE TRIGGER delay_primary BEFORE INSERT ON \
             counted_primary FOR EACH ROW EXECUTE FUNCTION delay_counted()",
        )
        .await
        .unwrap();
    let probe = std::sync::Arc::new(SettlementProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        tokio::time::timeout(
                            std::time::Duration::from_millis(25),
                            attempt.finish_write_batch(vec![upsert(1)], budget())
                        )
                        .await
                        .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert!(result.is_commit_unknown());
    assert_eq!(probe.commits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(backend);
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_committed_write_when_callback_returns_conflict_then_replay_is_forbidden() {
    let fixture = open_fixture().await;
    let probe = std::sync::Arc::new(SettlementProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    for callback_error in [
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
            storage_types::ConflictCause::Logical("later callback conflict"),
        ))
        .into(),
        storage_types::AttemptFailure::FRESH_VALIDATION.into(),
        storage_types::AttemptFailure::TRANSACTION_BYTES.into(),
        storage_types::AttemptFailure::CAPACITY.into(),
        storage_types::AttemptFailure::Unavailable(storage_types::NativeUnavailable::Deadline(
            storage_types::AttemptPhase::Statement,
        ))
        .into(),
    ] {
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        assert!(
                            attempt
                                .finish_write_batch(vec![upsert(1)], budget())
                                .await?
                                .is_ok()
                        );
                        Err::<(), _>(callback_error)
                    })
                },
            )
            .await
            .err()
            .unwrap();
        assert!(!result.is_commit_unknown());
        assert!(
            !result.class().is_retryable(),
            "committed write must never become retryable: {:?}",
            result.as_ref()
        );
        assert!(
            !result
                .attempt_kind()
                .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
        );
    }
    let durable = fixture
        .admin
        .query_one("SELECT count(*) FROM counted_primary", &[])
        .await
        .unwrap()
        .get::<_, i64>(0);
    let commits = probe.commits.load(std::sync::atomic::Ordering::SeqCst);
    let aborts = probe.aborts.load(std::sync::atomic::Ordering::SeqCst);
    drop(backend);
    fixture.close().await;
    assert_eq!(durable, 1);
    assert_eq!(commits, 5);
    assert_eq!(aborts, 0);
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_nonfinal_count_failure_when_abort_reply_is_lost_then_known_abort_has_one_abort() {
    let fixture = open_fixture().await;
    for lose_abort in [false, true] {
        let probe = std::sync::Arc::new(SettlementProbe {
            lose_abort,
            ..Default::default()
        });
        let backend = fixture
            .open_independent_backend()
            .await
            .with_fault_probe(probe.clone());
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        let mut writes = (0..257).map(upsert).collect::<Vec<_>>();
                        writes.push(sequence(2));
                        writes.extend((258..520).map(upsert));
                        let result = attempt.finish_write_batch(writes, budget()).await;
                        if let Ok(result) = result {
                            assert_error(
                                result.err().expect("nonfinal count"),
                                257,
                                "HistoryUnavailable",
                            );
                        }
                        Ok(())
                    })
                },
            )
            .await;
        // No COMMIT was sent: a lost ROLLBACK answer is still a known abort.
        assert!(result.is_err());
        assert!(!result.unwrap_err().is_commit_unknown());
        assert_eq!(backend.pool.status().size, usize::from(!lose_abort));
        assert_eq!(probe.commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(probe.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(backend);
    }
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_terminal_prepare_failure_when_prior_effect_exists_then_known_abort_preserves_index()
{
    let fixture = open_fixture().await;
    let missing = crate::write_batch::UpsertShape::try_new(
        vec!["value".into(), "payload".into()],
        vec![false, false],
        vec!["value".into()],
    )
    .unwrap()
    .render_counted(
        SqlEngine::Postgres,
        "native_missing_relation",
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(7)]],
    )
    .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (3)"))
                        .await?;
                    match attempt
                        .finish_write_batch(vec![upsert(1), missing], budget())
                        .await?
                        .err()
                        .expect("prepare failure")
                    {
                        SqlWriteBatchFailure::Other {
                            statement_index: 1,
                            error,
                        } => assert!(format!("{:?}", error.as_ref()).contains("42P01")),
                        _ => panic!("prepare error must not be an assertion"),
                    }
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!result.unwrap_err().is_commit_unknown());
    assert_eq!(fixture.count_records().await, 0);
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_committed_read_when_callback_returns_conflict_then_read_retry_is_unchanged() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(statement("SELECT 1::bigint"), vec![], budget())
                        .await?;
                    Err::<(), _>(
                        storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical("read callback conflict"),
                            ),
                        )
                        .into(),
                    )
                })
            },
        )
        .await
        .err()
        .unwrap();
    fixture.close().await;
    assert!(result.class().is_retryable());
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_complete_catalog_when_intermediate_flush_is_caught_then_settlement_rejects() {
    use storage_provider::DynamoStreamWriteAttempt as _;
    let fixture = open_fixture().await;
    let budgets = fixture.backend.attempt_deadlines();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(budgets),
            move |attempt| {
                Box::pin(async move {
                    let mut catalog = crate::catalog::SqlCatalogAttempt::new_complete_write(
                        attempt,
                        SqlEngine::Postgres,
                        AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                            .start(budgets),
                    );
                    assert!(catalog.flush_dynamo_write().await.is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(
        result.is_err(),
        "a caught intermediate flush cannot acknowledge completion"
    );
    assert_eq!(fixture.count_records().await, 0);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_borrowed_catalog_when_receipt_flushes_then_attempt_stays_open() {
    use storage_provider::{DynamoStreamWriteAttempt as _, DynamoTransactWriteAttempt as _};
    let fixture = open_fixture().await;
    let budgets = fixture.backend.attempt_deadlines();
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write).start(budgets),
            move |attempt| {
                Box::pin(async move {
                    let mut catalog = crate::catalog::SqlCatalogAttempt::new(
                        attempt,
                        SqlEngine::Postgres,
                        AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                            .start(budgets),
                    );
                    catalog.stage_transact_receipt(
                        storage_provider::ReceiptSlot::Request("borrowed"),
                        vec![3],
                    )?;
                    catalog.flush_dynamo_write().await?;
                    catalog
                        .attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (9)"))
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(fixture.count_records().await, 1);
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT receipt_blob FROM sys_transact_receipts WHERE token = 'borrowed'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![3]
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_warning_in_counted_write_when_commit_acknowledges_then_result_is_unknown() {
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(
            "CREATE FUNCTION warn_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE \
             WARNING 'fixture'; RETURN NEW; END $$; CREATE TRIGGER warn_write BEFORE INSERT ON \
             counted_primary FOR EACH ROW EXECUTE FUNCTION warn_write()",
        )
        .await
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move { attempt.finish_write_batch(vec![upsert(1)], budget()).await })
            },
        )
        .await;
    assert!(result.err().unwrap().is_commit_unknown());
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    fixture.close().await;
}

/// A failed count assertion followed by another counted statement in the
/// same pipeline: the follow-on `25P02` must not replace the write miss.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_count_assertion_when_followed_by_counted_statement_then_root_error_wins() {
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(
            "CREATE TRIGGER skip_primary BEFORE INSERT ON counted_primary FOR EACH ROW EXECUTE \
             FUNCTION skip_counted()",
        )
        .await
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let failure = attempt
                        .finish_write_batch(vec![upsert(1), upsert(2)], budget())
                        .await?
                        .err()
                        .expect("false count");
                    assert!(matches!(
                        failure,
                        SqlWriteBatchFailure::CountAssertion {
                            statement_index: 0,
                            ..
                        }
                    ));
                    // The root (the count assertion, internal for a primary
                    // write) wins; the follow-on 25P02 of the second statement
                    // does not.
                    let settled = attempt.require_settlement().expect_err("rejected attempt");
                    assert!(
                        settled
                            .to_string()
                            .contains("SQL primary write affected the wrong row count"),
                        "{settled}"
                    );
                    assert!(!settled.to_string().contains("25P02"), "{settled}");
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!result.unwrap_err().is_commit_unknown());
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct CountedResultLoss {
    fired: std::sync::atomic::AtomicBool,
    rollbacks: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for CountedResultLoss {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn fail_counted_result(&self, sql: &str) -> Option<StorageError> {
        (sql.contains("counted_primary")
            && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst))
        .then(|| StorageError::internal("probe: counted result lost"))
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if statements
                .iter()
                .any(|statement| statement.sql == "ROLLBACK")
            {
                self.rollbacks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_final_counted_result_lost_locally_after_commit_when_settled_then_unknown_unclean_and_committed()
 {
    let fixture = open_fixture().await;
    let probe = std::sync::Arc::new(CountedResultLoss::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = storage_common::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_provider::DYNAMO_WRITE_RETRY,
        backend.attempt_deadlines(),
        AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
            .scoped(storage_types::AttemptScope::Data),
        |request| {
            let calls = calls.clone();
            backend.with_attempt(request, move |attempt| {
                Box::pin(async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    attempt
                        .finish_write_batch(vec![upsert(1)], budget())
                        .await?
                        .map_err(|_| StorageError::internal("unexpected rejection"))?;
                    Ok(())
                })
            })
        },
    )
    .await;
    let error = result.unwrap_err();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::CommitUnknown)
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "never replayed"
    );
    assert_eq!(
        probe.rollbacks.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the transaction ended at COMMIT"
    );
    assert_eq!(
        backend.pool.status().size,
        0,
        "the unclean client was discarded"
    );
    assert_eq!(
        fixture
            .admin
            .query_one("SELECT count(*) FROM counted_primary", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1,
        "committed"
    );
    drop(backend);
    fixture.close().await;
}
