use std::num::NonZeroUsize;

use storage_types::{AttemptPlan, DeadlineClass};

use super::test_support::NativeFixture;
use crate::{AttemptMode, ColumnIndex, ReadProtection, SqlAttempt, SqlBackend, SqlStatement};

fn statement(sql: &'static str) -> SqlStatement {
    SqlStatement::new(sql, vec![])
}

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(4096).unwrap()
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_large_final_read_when_windows_run_then_one_snapshot_and_order_survive() {
    let fixture = NativeFixture::open().await;
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_read_batch(
                            statement("SELECT pg_current_snapshot()::text"),
                            (0..(crate::constants::POSTGRES_PIPELINE_STATEMENTS + 5))
                                .map(|_| {
                                    (
                                        statement("SELECT pg_current_snapshot()::text"),
                                        ReadProtection::Snapshot,
                                    )
                                })
                                .collect(),
                            budget(),
                        )
                        .await?;
                    let observation = result.observation.unwrap();
                    let crate::SqlValue::Text(snapshot) =
                        observation.rows[0].value(ColumnIndex::new(0))?
                    else {
                        panic!("snapshot text")
                    };
                    let pages = result.reads.map_err(|error| error.error)?;
                    assert_eq!(
                        pages.len(),
                        crate::constants::POSTGRES_PIPELINE_STATEMENTS + 5
                    );
                    for page in pages {
                        let crate::SqlValue::Text(actual) =
                            page.rows[0].value(ColumnIndex::new(0))?
                        else {
                            panic!("snapshot text")
                        };
                        assert_eq!(actual, snapshot);
                    }
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_schema_failure_when_prepared_then_original_index_and_completed_prefix_survive() {
    let fixture = NativeFixture::open().await;
    for (following, expected_index, prefix) in [
        (0, 0, false),
        (1, 1, false),
        (
            crate::constants::POSTGRES_PIPELINE_STATEMENTS,
            crate::constants::POSTGRES_PIPELINE_STATEMENTS,
            true,
        ),
    ] {
        let outcome = fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(fixture.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let (observation, reads) = if following == 0 {
                            (
                                statement("SELECT value FROM native_missing_relation"),
                                vec![],
                            )
                        } else {
                            let mut reads = (1..following)
                                .map(|_| (statement("SELECT 1::bigint"), ReadProtection::Snapshot))
                                .collect::<Vec<_>>();
                            reads.push((
                                statement("SELECT value FROM native_missing_relation"),
                                ReadProtection::Snapshot,
                            ));
                            (statement("SELECT 7::bigint"), reads)
                        };
                        let result = attempt
                            .finish_read_batch(observation, reads, budget())
                            .await?;
                        assert_eq!(result.observation.is_some(), prefix);
                        let failure = result.reads.err().unwrap();
                        assert_eq!(failure.statement_index, expected_index);
                        assert!(
                            failure.error.attempt_kind()
                                == Some(storage_types::AttemptKind::FreshValidation)
                        );
                        assert!(
                            attempt.require_settlement().is_err(),
                            "native rejection remains sticky"
                        );
                        Ok(31)
                    })
                },
            )
            .await;
        let error = outcome.err().unwrap();
        assert_eq!(
            error.attempt_kind(),
            Some(storage_types::AttemptKind::FreshValidation)
        );
        assert!(!error.is_commit_unknown());
    }
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let caught = attempt
                        .finish_read_batch(
                            statement("SELECT missing_column FROM native_commit_probe"),
                            vec![],
                            budget(),
                        )
                        .await?;
                    let failure = caught.reads.err().unwrap();
                    assert_eq!(failure.statement_index, 0);
                    assert!(
                        failure.error.attempt_kind()
                            == Some(storage_types::AttemptKind::FreshValidation)
                    );
                    Ok(31)
                })
            },
        )
        .await;
    assert_eq!(
        result.err().unwrap().attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation),
        "ordinary lending must reject a caught native error with its cause"
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_execution_failure_when_final_window_drains_then_prefix_and_original_failure_survive()
{
    let fixture = NativeFixture::open().await;
    let outcome = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_read_batch(
                            statement("SELECT 11::bigint"),
                            vec![
                                (statement("SELECT 1 / 0"), ReadProtection::Snapshot),
                                (statement("SELECT 3::bigint"), ReadProtection::Snapshot),
                            ],
                            budget(),
                        )
                        .await?;
                    assert_eq!(result.observation.unwrap().rows.len(), 1);
                    let failure = result.reads.err().unwrap();
                    assert_eq!(failure.statement_index, 1);
                    assert!(
                        failure.error.attempt_kind()
                            != Some(storage_types::AttemptKind::FreshValidation)
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(outcome.is_err());
    assert!(!outcome.unwrap_err().is_commit_unknown());
    // The sole pooled client can be reused after acknowledged failed
    // settlement.
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT 9::bigint"),
                            ReadProtection::Snapshot,
                            budget(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_local_decode_failure_when_commit_acknowledges_then_caught_error_is_outer_rejection()
{
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_read_batch(statement("SELECT ARRAY[1,2]"), vec![], budget())
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(
        result.is_err(),
        "committed local failure cannot become Aborted or Completed"
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_abandoned_terminal_read_when_callback_tries_io_then_known_failure_discards_client() {
    let fixture = NativeFixture::open().await;
    let previous_pid = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let observed_pid = previous_pid.clone();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .query(
                            statement("SELECT pg_backend_pid()::bigint"),
                            ReadProtection::Snapshot,
                            budget(),
                        )
                        .await?;
                    let crate::SqlValue::Integer(pid) = page.rows[0].value(ColumnIndex::new(0))?
                    else {
                        panic!("backend pid")
                    };
                    observed_pid.store(*pid, std::sync::atomic::Ordering::SeqCst);
                    let canceled = tokio::time::timeout(
                        std::time::Duration::from_millis(30),
                        attempt.finish_read_batch(
                            statement("SELECT 1::bigint FROM pg_sleep(0.15)"),
                            vec![],
                            budget(),
                        ),
                    )
                    .await;
                    assert!(canceled.is_err());
                    let error = attempt
                        .query(
                            statement("SELECT 2::bigint"),
                            ReadProtection::Snapshot,
                            budget(),
                        )
                        .await
                        .err()
                        .unwrap();
                    assert!(
                        !error.is_commit_unknown(),
                        "a read never reports an unknown outcome"
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(!result.err().unwrap().is_commit_unknown());
    let page = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT pg_backend_pid()::bigint"),
                            ReadProtection::Snapshot,
                            budget(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    let crate::SqlValue::Integer(pid) = page.rows[0].value(ColumnIndex::new(0)).unwrap() else {
        panic!("backend pid")
    };
    assert_ne!(
        *pid,
        previous_pid.load(std::sync::atomic::Ordering::SeqCst),
        "abandoned client must be discarded"
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_invalid_terminal_protection_when_caught_then_attempt_stays_rejected() {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_read_batch(
                                statement("SELECT 1::bigint"),
                                vec![(statement("SELECT 2::bigint"), ReadProtection::Shared),],
                                budget()
                            )
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct SettlementProbe {
    events: std::sync::Mutex<Vec<crate::SqlNativeEvent>>,
    suppress_commit: bool,
    block_begin: bool,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for SettlementProbe {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }
    fn observe(&self, event: crate::SqlNativeEvent) {
        self.events.lock().unwrap().push(event);
    }
    fn before_wave<'a>(
        &'a self,
        statements: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if self.block_begin
                && statements
                    .iter()
                    .any(|statement| statement.sql.starts_with("START TRANSACTION"))
            {
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
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
            if self.suppress_commit
                && boundary == crate::SqlCommitBoundary::AfterDurableAcknowledgement
            {
                return Err(storage_types::StorageError::internal(
                    "suppressed acknowledgement",
                ));
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_final_read_when_settled_then_one_execution_wave_and_no_outer_settlement() {
    use crate::SqlNativeEvent;
    let fixture = NativeFixture::open().await;
    let probe = std::sync::Arc::new(SettlementProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(statement("SELECT 1::bigint"), vec![], budget())
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    {
        let events = probe.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SqlNativeEvent::Begin))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SqlNativeEvent::Commit))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SqlNativeEvent::Rollback))
                .count(),
            0
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SqlNativeEvent::Wave { statements: 3, .. }))
                .count(),
            1
        );
    }
    drop(backend);
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_read_commit_answer_suppressed_when_callback_catches_then_known_failure_discards_value()
 {
    let fixture = NativeFixture::open().await;
    let probe = std::sync::Arc::new(SettlementProbe {
        suppress_commit: true,
        ..Default::default()
    });
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        !attempt
                            .finish_read_batch(statement("SELECT 1::bigint"), vec![], budget())
                            .await
                            .err()
                            .unwrap()
                            .is_commit_unknown()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(!result.err().unwrap().is_commit_unknown());
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_final_read_work_limit_when_output_exhausts_it_then_caught_error_stays_outer() {
    let fixture = NativeFixture::open().await;
    fixture
        .backend
        .bind_work_limits(storage_types::NativeWorkLimits {
            data_bytes: NonZeroUsize::new(96).unwrap(),
            call_guards: NonZeroUsize::new(10).unwrap(),
        })
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .finish_read_batch(
                                statement("SELECT repeat('x', 128)"),
                                vec![],
                                budget()
                            )
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
struct WindowWriteProbe {
    admin: tokio_postgres::Client,
    insert: String,
    waves: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for WindowWriteProbe {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }
    fn before_wave<'a>(
        &'a self,
        _: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if self.waves.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                self.admin.batch_execute(&self.insert).await.unwrap();
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_writer_between_terminal_windows_when_following_reads_run_then_snapshot_excludes_new_row()
 {
    let fixture = NativeFixture::open().await;
    let (admin, connection) = tokio_postgres::connect(
        &std::env::var("TEST_POSTGRES_DSN").unwrap(),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let probe = std::sync::Arc::new(WindowWriteProbe {
        admin,
        insert: format!(
            "INSERT INTO {}.native_commit_probe VALUES (1)",
            fixture.schema
        ),
        waves: Default::default(),
    });
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_read_batch(
                            statement("SELECT count(*)::bigint FROM native_commit_probe"),
                            (0..crate::constants::POSTGRES_PIPELINE_STATEMENTS)
                                .map(|_| {
                                    (
                                        statement(
                                            "SELECT count(*)::bigint FROM native_commit_probe",
                                        ),
                                        ReadProtection::Snapshot,
                                    )
                                })
                                .collect(),
                            budget(),
                        )
                        .await?;
                    let observation = result.observation.unwrap();
                    let pages = result.reads.map_err(|error| error.error)?;
                    for page in std::iter::once(observation).chain(pages) {
                        assert!(matches!(
                            page.rows[0].value(ColumnIndex::new(0))?,
                            crate::SqlValue::Integer(0)
                        ));
                    }
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(probe.waves.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(fixture.count_records().await, 1);
    drop(backend);
    drop(probe);
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_finished_read_when_another_query_is_caught_then_no_io_or_second_settlement_occurs() {
    let fixture = NativeFixture::open().await;
    let probe = std::sync::Arc::new(SettlementProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(statement("SELECT 1::bigint"), vec![], budget())
                        .await?;
                    assert!(
                        attempt
                            .query(
                                statement("SELECT 2::bigint"),
                                ReadProtection::Snapshot,
                                budget()
                            )
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    {
        let events = probe.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, crate::SqlNativeEvent::Wave { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, crate::SqlNativeEvent::Commit))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, crate::SqlNativeEvent::Rollback))
                .count(),
            0
        );
    }
    drop(backend);
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_blocked_lazy_begin_when_timeout_is_caught_then_guard_rejects_later_io_and_settlement()
 {
    let fixture = NativeFixture::open().await;
    let probe = std::sync::Arc::new(SettlementProbe {
        block_begin: true,
        ..Default::default()
    });
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        tokio::time::timeout(
                            std::time::Duration::from_millis(30),
                            attempt.query(
                                statement("SELECT 1::bigint"),
                                ReadProtection::Snapshot,
                                budget()
                            )
                        )
                        .await
                        .is_err()
                    );
                    assert!(
                        attempt
                            .query(
                                statement("SELECT 2::bigint"),
                                ReadProtection::Snapshot,
                                budget()
                            )
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.err().unwrap().is_attempt_abandoned());
    {
        let events = probe.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    crate::SqlNativeEvent::Begin
                        | crate::SqlNativeEvent::Commit
                        | crate::SqlNativeEvent::Statement { .. }
                ))
                .count(),
            0
        );
        // Lazy BEGIN: the cancelled wave wrote nothing, so no ROLLBACK is
        // needed.
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, crate::SqlNativeEvent::Rollback))
                .count(),
            0
        );
    }
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_ordinary_lazy_begin_when_guard_budget_is_one_or_two_then_admission_counts_begin_and_query()
 {
    for guards in [1, 2] {
        let fixture = NativeFixture::open().await;
        fixture
            .backend
            .bind_work_limits(storage_types::NativeWorkLimits {
                data_bytes: NonZeroUsize::new(1024).unwrap(),
                call_guards: NonZeroUsize::new(guards).unwrap(),
            })
            .unwrap();
        let result = fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(fixture.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let query = attempt
                            .query(
                                statement("SELECT 1::bigint"),
                                ReadProtection::Snapshot,
                                budget(),
                            )
                            .await;
                        if guards == 1 {
                            assert!(query.is_err());
                        } else {
                            query?;
                        }
                        Ok(())
                    })
                },
            )
            .await;
        if guards == 1 {
            assert!(matches!(
                result.err().unwrap().attempt_kind(),
                Some(
                    storage_types::AttemptKind::Capacity
                        | storage_types::AttemptKind::TransactionBytes
                )
            ));
        } else {
            result.unwrap();
        }
        fixture.close().await;
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_missing_or_stale_authority_when_combined_get_runs_then_item_scan_is_not_executed() {
    use crate::{ColumnIndex, ReadProtection, SqlAttempt, SqlBackend, SqlStatement, SqlValue};
    let fixture = crate::postgres::test_support::NativeFixture::open().await;
    let mut table = crate::test_support::table();
    let info = std::sync::Arc::make_mut(&mut table.info);
    info.key_schema.push(storage_types::KeySchemaElement {
        attribute_name: "key_0_".into(),
        key_type: storage_types::KeyType::Hash,
    });
    info.attribute_definitions
        .push(storage_types::AttributeDefinition {
            attribute_name: "key_0_".into(),
            attribute_type: storage_types::KeyAttributeType::S,
        });
    let mut key = storage_types::KeyAttributes::new();
    key.insert("key_0_", storage_types::AttributeValue::S("value".into()));
    let physical = crate::SqlEngine::Postgres.item_table_name(table.history_id);
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.tables (id text, catalog_revision bigint, table_name text, \
             table_status text); CREATE TABLE {}.\"{physical}\" (\"key_0_\" text PRIMARY KEY, \
             item_value bytea); INSERT INTO {}.\"{physical}\" VALUES ('value', 'image')",
            fixture.schema, fixture.schema, fixture.schema
        ))
        .await
        .unwrap();
    let history = uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string();
    for (case, actual_history, actual_revision) in [
        ("missing", None, 1),
        ("revision", Some(history.clone()), 2),
        ("history", Some(uuid::Uuid::new_v4().to_string()), 1),
        ("current", Some(history.clone()), 1),
    ] {
        fixture
            .admin
            .execute(&format!("DELETE FROM {}.tables", fixture.schema), &[])
            .await
            .unwrap();
        if let Some(actual_history) = actual_history {
            fixture
                .admin
                .execute(
                    &format!(
                        "INSERT INTO {}.tables VALUES ($1, $2, 'table', 'ACTIVE')",
                        fixture.schema
                    ),
                    &[&actual_history, &i64::from(actual_revision)],
                )
                .await
                .unwrap();
        }
        for layout in ["combined", "separate", "range"] {
            let stamp = storage_provider::CatalogStamp {
                history_id: table.history_id,
                revision: std::num::NonZeroU64::new(1).unwrap(),
            };
            let statement = if layout == "combined" {
                crate::item_read::combined_point_statement(
                    crate::SqlEngine::Postgres,
                    stamp,
                    &table,
                    &key,
                )
            } else if layout == "range" {
                let candidate = storage_provider::TableReadCandidate {
                    table: &table,
                    stamp,
                    catalog_version: None,
                };
                crate::item_observation::guard_observation_statement(
                    crate::SqlEngine::Postgres,
                    SqlStatement::new(
                        format!(
                            "SELECT key_0_, item_value FROM \"{physical}\" ORDER BY key_0_ LIMIT \
                             101"
                        ),
                        vec![],
                    ),
                    &[(&table.info.table_name, Some(candidate))],
                )
            } else {
                crate::item_read::guarded_point_statement(
                    crate::SqlEngine::Postgres,
                    stamp,
                    &table,
                    &key,
                )
            }
            .unwrap();
            let (sql, binds) = statement.into_parts();
            let page = fixture
                .backend
                .with_attempt(
                    storage_types::AttemptPlan::new(
                        crate::AttemptMode::ReadOnly,
                        storage_types::DeadlineClass::PointRead,
                    )
                    .start(fixture.backend.attempt_deadlines()),
                    move |attempt| {
                        Box::pin(async move {
                            attempt
                                .query(
                                    SqlStatement::new(
                                        format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) {sql}"),
                                        binds,
                                    ),
                                    ReadProtection::Snapshot,
                                    std::num::NonZeroUsize::new(65536).unwrap(),
                                )
                                .await
                        })
                    },
                )
                .await
                .unwrap();
            let plan = page
                .rows
                .iter()
                .map(|row| match row.value(ColumnIndex::new(0)).unwrap() {
                    SqlValue::Text(value) => value.as_str(),
                    _ => panic!("text plan"),
                })
                .collect::<Vec<_>>()
                .join("\n");
            println!("N7_GUARDED_POINT_PLAN layout={layout} {case}\n{plan}");
            let item_scan = plan
                .lines()
                .find(|line| line.contains(&physical) && line.contains("Scan"))
                .expect("physical item scan in plan");
            if case == "current" {
                assert!(!item_scan.contains("never executed"));
            } else {
                assert!(item_scan.contains("never executed"), "{case}: {plan}");
            }
        }
    }
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
struct CapacityAfterSchema;

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for CapacityAfterSchema {
    fn fail(&self, call: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        (call.operation == crate::SqlFaultOperation::Query && call.sql == "SELECT 3::bigint")
            .then(|| storage_types::AttemptFailure::CAPACITY.into())
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_execution_schema_failure_and_later_capacity_rejection_when_final_read_settles_then_stronger_outer_error_preserves_carrier()
 {
    let fixture = NativeFixture::open().await;
    // This is a genuine execution SQLSTATE, after the actual prefix has run.
    // A missing relation at Parse would prevent the complete first window.
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE FUNCTION {}.native_schema_failure() RETURNS bigint LANGUAGE plpgsql AS 'BEGIN \
             RAISE EXCEPTION USING ERRCODE = ''42703'', MESSAGE = ''test missing column''; END'",
            fixture.schema
        ))
        .await
        .unwrap();
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(std::sync::Arc::new(CapacityAfterSchema));
    let outcome = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_read_batch(
                            statement("SELECT 11::bigint"),
                            vec![
                                (
                                    statement("SELECT native_schema_failure()"),
                                    ReadProtection::Snapshot,
                                ),
                                (statement("SELECT 3::bigint"), ReadProtection::Snapshot),
                            ],
                            budget(),
                        )
                        .await?;
                    assert!(
                        result.observation.is_some(),
                        "actual prefix survives execution failure"
                    );
                    let failure = result.reads.err().unwrap();
                    assert_eq!(failure.statement_index, 1);
                    assert!(
                        failure.error.attempt_kind()
                            == Some(storage_types::AttemptKind::FreshValidation),
                        "genuine candidate schema carrier survives"
                    );
                    Ok(failure)
                })
            },
        )
        .await;
    let error = outcome.err().unwrap();
    assert!(matches!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Capacity | storage_types::AttemptKind::TransactionBytes)
    ));
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_warning_in_final_read_when_commit_acknowledges_then_read_completes() {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE FUNCTION {}.warn_read() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RAISE \
             WARNING 'fixture'; RETURN 1; END $$",
            fixture.schema
        ))
        .await
        .unwrap();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(statement("SELECT warn_read()"), vec![], budget())
                        .await
                })
            },
        )
        .await;
    // A read commits nothing; the warning only retires the connection.
    result.unwrap();
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_missing_relation_in_final_read_when_step_returns_ok_then_attempt_returns_fresh_validation()
 {
    let fixture = NativeFixture::open().await;
    let error = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .finish_read_batch(
                            statement("SELECT 1::bigint"),
                            vec![(
                                statement("SELECT value FROM native_missing_relation"),
                                ReadProtection::Snapshot,
                            )],
                            budget(),
                        )
                        .await?;
                    let failure = result.reads.err().unwrap();
                    assert_eq!(failure.statement_index, 1);
                    assert_eq!(
                        failure.error.attempt_kind(),
                        Some(storage_types::AttemptKind::FreshValidation)
                    );
                    Ok(())
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    assert!(!error.is_commit_unknown());
}
