//! Safe native failure trace fields shared by all replacement providers.

use storage_common::diagnostics::{NativeEngine, NativeOperation, trace_native_failure};
use storage_observability_test_support::observation::observe;
use storage_types::{StorageEnum, StorageError};
use tracing::Level;

#[tokio::test]
async fn given_internal_failure_when_each_backend_logs_then_diagnostic_is_visible_without_payloads()
{
    for backend in [
        NativeEngine::Sqlite,
        NativeEngine::Postgres,
        NativeEngine::Turso,
        NativeEngine::RocksDb,
        NativeEngine::FoundationDb,
    ] {
        let observed = observe(async {
            trace_native_failure(
                backend,
                NativeOperation::SqlQuery,
                &StorageError::internal("catalog revision is missing"),
            );
        })
        .await;
        assert_eq!(observed.events.len(), 1);
        let event = &observed.events[0];
        assert_eq!(event.level, Level::WARN);
        assert_eq!(event.fields.get("backend").unwrap(), backend.as_str());
        assert_eq!(event.fields.get("operation").unwrap(), "sql.query");
        assert_eq!(
            event.fields.get("error_code").unwrap(),
            "InternalServerError"
        );
        assert_eq!(
            event.fields.get("diagnostic").unwrap(),
            "catalog revision is missing"
        );
        assert_eq!(
            event.fields.get("message").unwrap(),
            "native storage operation failed"
        );
        assert_eq!(event.fields.len(), 5, "no keys, binds, or payload fields");
    }
}

#[tokio::test]
async fn given_validation_and_conflict_when_native_logging_runs_then_codes_and_levels_are_distinct()
{
    for (error, code, level) in [
        (
            StorageError::validation("private request value"),
            "ValidationException",
            Level::WARN,
        ),
        (
            StorageError::from(StorageEnum::TransactionConflict {
                message: "native lock contention".to_owned(),
            }),
            "TransactionConflictException",
            Level::DEBUG,
        ),
    ] {
        let observed = observe(async {
            trace_native_failure(NativeEngine::Postgres, NativeOperation::SqlExecute, &error);
        })
        .await;
        assert_eq!(observed.events.len(), 1);
        let event = &observed.events[0];
        assert_eq!(event.level, level);
        assert_eq!(event.fields.get("error_code").unwrap(), code);
        assert!(
            !event
                .fields
                .values()
                .any(|value| value.contains("private request value"))
        );
    }
}
