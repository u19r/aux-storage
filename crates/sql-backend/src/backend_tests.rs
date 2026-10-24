use std::hint::black_box;

use alloc_counter::AllocationGuard;

use crate::SqlEngine;

#[test]
fn given_placeholder_boundaries_when_rendered_then_preserve_sql_syntax() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for index in [0, 1, 9, 10, 20, 21, 32, 100, usize::MAX] {
            assert_eq!(engine.placeholder(index), format!("{prefix}{index}"));
        }
        for count in [0, 1, 4, 20, 21, 32] {
            let expected = (1..=count)
                .map(|index| format!("{prefix}{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            assert_eq!(engine.placeholders(count), expected);
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_placeholder_shapes_when_rendered_then_report_cpu_and_allocations() {
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        for index in [1, 4, 20, 21, 32, 100] {
            report_rendering(&format!("{name}_single_{index}"), || {
                drop(black_box(engine.placeholder(black_box(index))))
            });
            report_rendering(&format!("{name}_list_{index}"), || {
                drop(black_box(engine.placeholders(black_box(index))))
            });
        }
    }
}

fn report_rendering(label: &str, mut render: impl FnMut()) {
    for _ in 0..100 {
        render();
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "sql_placeholders",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..100_000 {
        render();
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "sql_placeholders label={label} iterations=100000 cpu_ns={}",
        cpu.as_nanos()
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_fixed_sql_statement_when_rendered_then_report_cpu_and_allocations() {
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        report_rendering(&format!("{name}_sequence_statement"), || {
            drop(black_box(
                crate::stream_history::sequence_advance_statement(
                    black_box(engine),
                    black_box("shard-01"),
                    std::num::NonZeroU64::new(1).unwrap(),
                    std::num::NonZeroU64::new(2).unwrap(),
                )
                .unwrap(),
            ));
        });
    }
}

#[test]
fn given_numeric_placeholder_boundaries_when_rendered_then_preserve_cast_and_slot() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for index in [0, 1, 2, 4, 5, 20, 21, 32, usize::MAX] {
            assert_eq!(
                engine.numeric_placeholder(index),
                format!("CAST({prefix}{index} AS TEXT)::NUMERIC")
            );
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_numeric_placeholders_when_cached_then_report_cpu_and_allocations() {
    for index in [1, 2, 4, 5, 32] {
        report_rendering(&format!("numeric_baseline_{index}"), || {
            drop(black_box(format!(
                "CAST({} AS TEXT)::NUMERIC",
                SqlEngine::Postgres.placeholder(black_box(index))
            )));
        });
        report_rendering(&format!("numeric_optimized_{index}"), || {
            drop(black_box(
                SqlEngine::Postgres.numeric_placeholder(black_box(index)),
            ));
        });
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_supervised_attempt_when_caller_is_cancelled_then_settlement_continues() {
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (settled, completed) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(crate::backend::supervise_attempt(
        async move {
            started.send(()).unwrap();
            released.await.unwrap();
            settled.send(()).unwrap();
            Ok(())
        },
        "attempt stopped",
    ));
    ready.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), completed)
        .await
        .unwrap()
        .unwrap();
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_supervised_attempt_when_operation_panics_then_caller_receives_the_panic() {
    let caller = tokio::spawn(crate::backend::supervise_attempt::<()>(
        async { panic!("operation panic") },
        "attempt stopped",
    ));
    let panic = caller.await.unwrap_err();
    assert!(panic.is_panic());
    assert_eq!(
        *panic.into_panic().downcast::<&str>().unwrap(),
        "operation panic"
    );
}
