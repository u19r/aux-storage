use std::num::NonZeroUsize;

use storage_types::{AttemptPlan, DeadlineClass, StorageError};

use super::test_support::NativeFixture;
use crate::{AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlStatement};

fn statement(sql: &'static str) -> SqlStatement {
    SqlStatement::new(sql, vec![])
}

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(4096).unwrap()
}

async fn open_fixture() -> NativeFixture {
    let fixture = NativeFixture::open().await;
    eprintln!("owned write observation fixture schema: {}", fixture.schema);
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    fixture
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_observations_when_successful_then_attempt_remains_open_for_mutation() {
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
                        .query_batch_open(
                            (0..261)
                                .map(|_| (statement("SELECT 7::bigint"), ReadProtection::Snapshot))
                                .collect(),
                            budget(),
                        )
                        .await?;
                    assert!(result.failure.is_none());
                    assert_eq!(result.pages.len(), 261);
                    attempt.require_settlement()?;
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (7)"))
                        .await?;
                    Ok(17)
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(result, 17);
    let rows = fixture
        .admin
        .query(
            &format!("SELECT value FROM {}.native_commit_probe", fixture.schema),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_prepare_failure_when_windowed_then_only_prior_window_prefix_survives() {
    let fixture = open_fixture().await;
    for index in [0, 1, crate::constants::POSTGRES_PIPELINE_STATEMENTS + 1] {
        let result = fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(fixture.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let mut statements = (0..index)
                            .map(|_| (statement("SELECT 7::bigint"), ReadProtection::Snapshot))
                            .collect::<Vec<_>>();
                        statements.push((
                            statement("SELECT value FROM native_missing_relation"),
                            ReadProtection::Snapshot,
                        ));
                        let result = attempt.query_batch_open(statements, budget()).await?;
                        assert_eq!(
                            result.pages.len(),
                            index / crate::constants::POSTGRES_PIPELINE_STATEMENTS
                                * crate::constants::POSTGRES_PIPELINE_STATEMENTS
                        );
                        let failure = result.failure.unwrap();
                        assert_eq!(failure.statement_index, index);
                        assert!(
                            failure.error.attempt_kind()
                                == Some(storage_types::AttemptKind::FreshValidation)
                        );
                        assert!(
                            attempt
                                .execute(statement("INSERT INTO native_commit_probe VALUES (8)"))
                                .await
                                .is_err()
                        );
                        Ok(23)
                    })
                },
            )
            .await;
        assert!(result.is_err());
        assert!(!result.unwrap_err().is_commit_unknown());
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_execution_failure_when_drained_then_prefix_and_original_cause_survive() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (9)"))
                        .await?;
                    let result = attempt
                        .query_batch_open(
                            vec![
                                (statement("SELECT 11::bigint"), ReadProtection::Snapshot),
                                (statement("SELECT 1 / 0"), ReadProtection::Snapshot),
                                (statement("SELECT 13::bigint"), ReadProtection::Snapshot),
                            ],
                            budget(),
                        )
                        .await?;
                    assert_eq!(result.pages.len(), 1);
                    let failure = result.failure.unwrap();
                    assert_eq!(failure.statement_index, 1);
                    assert!(format!("{:?}", failure.error.as_ref()).contains("22012"));
                    Err::<(), _>(StorageError::internal("later callback error"))
                })
            },
        )
        .await
        .err()
        .unwrap();
    assert!(
        format!("{:?}", result.as_ref()).contains("22012"),
        "{result}"
    );
    let rows = fixture
        .admin
        .query(
            &format!("SELECT value FROM {}.native_commit_probe", fixture.schema),
            &[],
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_abandoned_write_observation_when_callback_catches_then_rollback_rejects_value() {
    let fixture = open_fixture().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (10)"))
                        .await?;
                    let canceled = tokio::time::timeout(
                        std::time::Duration::from_millis(25),
                        attempt.query_batch_open(
                            vec![(
                                statement("SELECT 1::bigint FROM pg_sleep(0.1)"),
                                ReadProtection::Snapshot,
                            )],
                            budget(),
                        ),
                    )
                    .await;
                    assert!(canceled.is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(result.is_err());
    let rows = fixture
        .admin
        .query(
            &format!("SELECT value FROM {}.native_commit_probe", fixture.schema),
            &[],
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
#[derive(Default)]
struct AbortProbe {
    events: std::sync::Mutex<Vec<crate::SqlNativeEvent>>,
    lose_abort: bool,
    abort_attempts: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for AbortProbe {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<StorageError> {
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
            if statements
                .iter()
                .any(|statement| statement.sql == "ROLLBACK")
            {
                self.abort_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if self.lose_abort {
                    return Err(StorageError::commit_unknown(
                        "suppressed abort acknowledgement",
                    ));
                }
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_failure_when_abort_settles_then_one_abort_and_no_commit_follow() {
    let fixture = open_fixture().await;
    for lose_abort in [false, true] {
        let probe = std::sync::Arc::new(AbortProbe {
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
                        let result = attempt
                            .query_batch_open(
                                vec![
                                    (statement("SELECT 11::bigint"), ReadProtection::Snapshot),
                                    (statement("SELECT 1 / 0"), ReadProtection::Snapshot),
                                ],
                                budget(),
                            )
                            .await;
                        if let Ok(result) = result {
                            assert_eq!(result.pages.len(), 1);
                        }
                        Ok(29)
                    })
                },
            )
            .await;
        // No COMMIT was sent: a lost ROLLBACK answer is still a known abort.
        assert!(result.is_err());
        assert!(!result.unwrap_err().is_commit_unknown());
        assert_eq!(backend.pool.status().size, usize::from(!lose_abort));
        if !lose_abort {
            let events = probe.events.lock().unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, crate::SqlNativeEvent::Rollback))
                    .count(),
                1
            );
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, crate::SqlNativeEvent::Commit))
            );
        }
        assert_eq!(
            probe
                .abort_attempts
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        drop(backend);
    }
    fixture.close().await;
}

#[cfg(feature = "fault-injection")]
struct ObservationStartProbe {
    cancel: bool,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for ObservationStartProbe {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }
    fn before_wave<'a>(
        &'a self,
        statements: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if statements
                .iter()
                .any(|statement| statement.sql == "SELECT 17::bigint")
            {
                if self.cancel {
                    futures::future::pending::<()>().await;
                }
                return Err(StorageError::internal("observation start rejected"));
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_observation_start_rejection_when_callback_catches_then_prior_write_rolls_back() {
    for cancel in [false, true] {
        let fixture = open_fixture().await;
        let backend = fixture
            .open_independent_backend()
            .await
            .with_fault_probe(std::sync::Arc::new(ObservationStartProbe { cancel }));
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt
                            .execute(statement("INSERT INTO native_commit_probe VALUES (17)"))
                            .await?;
                        let result = tokio::time::timeout(
                            std::time::Duration::from_millis(25),
                            attempt.query_batch_open(
                                vec![(statement("SELECT 17::bigint"), ReadProtection::Snapshot)],
                                budget(),
                            ),
                        )
                        .await;
                        if cancel {
                            assert!(result.is_err());
                        } else {
                            assert!(result.unwrap().is_err());
                        }
                        Ok(())
                    })
                },
            )
            .await;
        let rows = fixture
            .admin
            .query(
                &format!("SELECT value FROM {}.native_commit_probe", fixture.schema),
                &[],
            )
            .await
            .unwrap();
        let rejected = result.is_err();
        fixture.close().await;
        assert!(
            rejected,
            "caught wave start must reject settlement (cancel={cancel})"
        );
        assert!(
            rows.is_empty(),
            "prior write must roll back (cancel={cancel})"
        );
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_missing_names_and_optional_receipt_when_combined_write_observations_run_then_actual_stamps_and_empty_cardinality_are_preserved()
 {
    use storage_provider::DynamoTransactWriteAttempt as _;
    let fixture = open_fixture().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.tables (id text NOT NULL, catalog_revision bigint NOT NULL, \
             table_name text NOT NULL, table_status text NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    fixture
        .backend
        .with_schema_attempt(|attempt| {
            Box::pin(async move {
                crate::transact_write::initialize_transact_receipts(
                    attempt,
                    crate::SqlEngine::Postgres,
                )
                .await
            })
        })
        .await
        .unwrap();
    for has_present_table in [false, true] {
        if has_present_table {
            fixture
                .admin
                .execute(
                    &format!(
                        "INSERT INTO {}.tables VALUES ($1, 1, 'present', 'ACTIVE')",
                        fixture.schema
                    ),
                    &[&uuid::Uuid::new_v4().to_string()],
                )
                .await
                .unwrap();
        }
        let budgets = fixture.backend.attempt_deadlines();
        fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(budgets),
                move |attempt| {
                    Box::pin(async move {
                        let mut catalog = crate::catalog::SqlCatalogAttempt::new(
                            attempt,
                            crate::SqlEngine::Postgres,
                            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                                .start(budgets),
                        );
                        let missing = storage_types::TableName::new("missing");
                        let present = storage_types::TableName::new("present");
                        let tables = [(&missing, None), (&present, None)];
                        let result = catalog
                            .read_write_observations(storage_provider::WriteObservationBatch {
                                tables: &tables,
                                reads: &[],
                                receipt_key: Some(storage_provider::ReceiptSlot::Request(
                                    "opaque-receipt-key",
                                )),
                                prerequisites: storage_provider::ItemWritePrerequisites {
                                    needs_history: false,
                                },
                                cache: None,
                            })
                            .await?;
                        assert_eq!(result.receipt, Some(None));
                        let stamps = result.stamps.unwrap();
                        assert_eq!(stamps.len(), 2);
                        assert!(stamps[0].is_none());
                        assert_eq!(stamps[1].is_some(), has_present_table);
                        let observations = result.observations.map_err(|failure| failure.error)?;
                        if has_present_table {
                            assert!(observations.is_none());
                        } else {
                            assert!(observations.unwrap().is_empty());
                        }
                        Ok(())
                    })
                },
            )
            .await
            .unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_matching_candidate_and_other_changed_name_when_grouped_sql_runs_then_whole_batch_guard_skips_candidate_expression()
 {
    let fixture = open_fixture().await;
    let table = crate::test_support::table();
    let id = uuid::Uuid::from_bytes(*table.history_id.as_ref()).to_string();
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.tables (id text NOT NULL, catalog_revision bigint NOT NULL, \
             table_name text NOT NULL, table_status text NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    fixture
        .admin
        .execute(
            &format!(
                "INSERT INTO {}.tables VALUES ($1, 1, 'table', 'ACTIVE'), ($1, 1, 'changed', \
                 'ACTIVE')",
                fixture.schema
            ),
            &[&id],
        )
        .await
        .unwrap();
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let changed = storage_types::TableName::new("changed");
                    let tables = [
                        (
                            &table.info.table_name,
                            Some(storage_provider::TableReadCandidate {
                                table: &table,
                                stamp: storage_provider::CatalogStamp {
                                    history_id: table.history_id,
                                    revision: std::num::NonZeroU64::new(1).unwrap(),
                                },
                                catalog_version: None,
                            }),
                        ),
                        (&changed, None),
                    ];
                    let guarded = crate::item_observation::guard_observation_statement(
                        crate::SqlEngine::Postgres,
                        statement(
                            "SELECT 1 / (pg_backend_pid() - pg_backend_pid()) AS corrupt_candidate",
                        ),
                        &tables,
                    )?;
                    let result = attempt
                        .query_batch_open(vec![(guarded, ReadProtection::Snapshot)], budget())
                        .await?;
                    assert!(
                        result.failure.is_none(),
                        "a mismatch in another requested name must suppress this matching \
                         candidate's expression"
                    );
                    assert_eq!(result.pages.len(), 1);
                    assert!(result.pages[0].rows.is_empty());
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
async fn given_read_only_open_batch_when_continued_then_one_read_only_snapshot_is_retained() {
    let fixture = open_fixture().await;
    fixture.backend.with_attempt(AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(fixture.backend.attempt_deadlines()), |attempt| Box::pin(async move {
        let result = attempt.query_batch_open((0..261).map(|_| (statement("SELECT current_setting('transaction_read_only'), current_setting('transaction_isolation'), pg_current_snapshot()::text"), ReadProtection::Snapshot)).collect(), budget()).await?;
        assert!(result.failure.is_none());
        assert_eq!(result.pages.len(), 261);
        let snapshot = format!("{:?}", result.pages[0].rows[0].value(crate::ColumnIndex::new(2))?);
        for page in result.pages {
            let row = &page.rows[0];
            assert!(matches!(row.value(crate::ColumnIndex::new(0))?, crate::SqlValue::Text(value) if value == "on"));
            assert!(matches!(row.value(crate::ColumnIndex::new(1))?, crate::SqlValue::Text(value) if value == "repeatable read"));
            assert_eq!(format!("{:?}", row.value(crate::ColumnIndex::new(2))?), snapshot);
        }
        let later = attempt.query(statement("SELECT pg_current_snapshot()::text"), ReadProtection::Snapshot, budget()).await?;
        assert_eq!(format!("{:?}", later.rows[0].value(crate::ColumnIndex::new(0))?), snapshot);
        attempt.require_settlement()?;
        Ok(())
    })).await.unwrap();
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_read_only_open_batch_when_protection_or_mutation_is_requested_then_attempt_rejects()
{
    let fixture = open_fixture().await;
    let protection = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert!(
                        attempt
                            .query_batch_open(
                                vec![(statement("SELECT 1::bigint"), ReadProtection::Shared)],
                                budget()
                            )
                            .await
                            .is_err()
                    );
                    assert!(attempt.require_settlement().is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(protection.is_err());
    let mutation = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .query_batch_open(
                            vec![(
                                statement(
                                    "INSERT INTO native_commit_probe VALUES (81) RETURNING value",
                                ),
                                ReadProtection::Snapshot,
                            )],
                            budget(),
                        )
                        .await?;
                    assert!(result.pages.is_empty());
                    let failure = result.failure.unwrap();
                    assert_eq!(failure.statement_index, 0);
                    assert!(format!("{:?}", failure.error.as_ref()).contains("25006"));
                    assert!(attempt.require_settlement().is_err());
                    Ok(())
                })
            },
        )
        .await;
    assert!(mutation.is_err());
    assert!(!mutation.unwrap_err().is_commit_unknown());
    assert!(
        fixture
            .admin
            .query(
                &format!("SELECT value FROM {}.native_commit_probe", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .is_empty()
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_missing_relation_in_open_read_when_step_returns_ok_then_attempt_returns_fresh_validation()
 {
    let fixture = open_fixture().await;
    let error = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .query_batch_open(
                            vec![(
                                statement("SELECT value FROM native_missing_relation"),
                                ReadProtection::Snapshot,
                            )],
                            budget(),
                        )
                        .await?;
                    let failure = result.failure.unwrap();
                    assert_eq!(failure.statement_index, 0);
                    assert_eq!(
                        failure.error.attempt_kind(),
                        Some(storage_types::AttemptKind::FreshValidation)
                    );
                    Ok(23)
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

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_execution_time_missing_relation_then_follow_on_errors_do_not_outrank_it() {
    let fixture = open_fixture().await;
    // The statement parses and fails at execution, so the two statements after
    // it answer 25P02 (`Abandoned`, weaker) and must not outrank it. A
    // parse-time 42P01 would end the wave before they run.
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE FUNCTION {}.native_missing_relation_failure() RETURNS bigint LANGUAGE plpgsql \
             AS 'BEGIN RAISE EXCEPTION USING ERRCODE = ''42P01''; END'",
            fixture.schema
        ))
        .await
        .unwrap();
    let error = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let result = attempt
                        .query_batch_open(
                            vec![
                                (
                                    statement("SELECT native_missing_relation_failure()"),
                                    ReadProtection::Snapshot,
                                ),
                                (statement("SELECT 2::bigint"), ReadProtection::Snapshot),
                                (statement("SELECT 3::bigint"), ReadProtection::Snapshot),
                            ],
                            budget(),
                        )
                        .await?;
                    let failure = result.failure.unwrap();
                    assert_eq!(failure.statement_index, 0);
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
