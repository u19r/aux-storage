//! The sweep statement is index seeks on SQLite and Turso (W3S plan 5.4):
//! the plan never scans the state table.

use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, ReadProtection};

use crate::{SqlAttempt, SqlBackend, SqlStatement, SqlValue, pubsub_schedule::sweep_sql};

async fn query_plan<B: SqlBackend + 'static>(backend: B) -> Vec<String> {
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    let engine = store.backend.engine();
    let request = AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
        .start(store.backend.attempt_deadlines());
    store
        .backend
        .with_attempt(request, move |native| {
            Box::pin(async move {
                let sql = format!("EXPLAIN QUERY PLAN {}", sweep_sql(engine));
                let page = native
                    .query(
                        SqlStatement::new(
                            sql,
                            vec![SqlValue::Text(String::new()), SqlValue::Integer(256)],
                        ),
                        ReadProtection::Snapshot,
                        crate::constants::PUBSUB_CATALOG_READ_BYTES,
                    )
                    .await?;
                Ok(page
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_values()
                            .into_iter()
                            .map(|value| match value {
                                SqlValue::Text(text) => text,
                                SqlValue::Integer(number) => number.to_string(),
                                _ => String::new(),
                            })
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .collect())
            })
        })
        .await
        .unwrap()
}

fn assert_seeks(engine: &str, plan: &[String]) {
    eprintln!("{engine} sweep plan:\n{}", plan.join("\n"));
    assert!(!plan.is_empty(), "{engine}: no plan");
    // The CTE rows and the constant row are scanned; the state table (and
    // its aliases) is only searched through an index.
    for line in plan {
        let scanned = line.split("SCAN ").nth(1).unwrap_or_default();
        let table = scanned.split_whitespace().next().unwrap_or_default();
        assert!(
            !["sys_pubsub_delivery_state", "s", "h", "w"].contains(&table),
            "{engine}: a full scan: {line}"
        );
    }
    assert!(
        plan.iter()
            .any(|line| line.contains("sys_pubsub_delivery_partition")),
        "{engine}: the partition index is not used"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_when_sweep_planned_then_every_step_is_an_index_seek() {
    let backend = crate::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    assert_seeks("sqlite", &query_plan(backend).await);
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_when_sweep_planned_then_every_step_is_an_index_seek() {
    let path = std::env::temp_dir().join(format!("w3s-sweep-{}.db", uuid::Uuid::new_v4()));
    let backend = crate::turso::TursoBackend::initialize(
        path.to_str().unwrap(),
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let plan = query_plan(backend).await;
    let _ = std::fs::remove_file(&path);
    assert_seeks("turso", &plan);
}
