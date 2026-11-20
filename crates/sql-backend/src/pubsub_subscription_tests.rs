//! The pending-subscription page of the stale sweep seeks the token index on
//! SQLite and Turso (spec OWN-A-g 5.7): the plan never scans the table.

use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, ReadProtection};

use crate::{
    SqlAttempt, SqlBackend, SqlStatement, SqlValue, pubsub_subscription::pending_subscription_sql,
};

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
                let sql = format!("EXPLAIN QUERY PLAN {}", pending_subscription_sql(engine));
                let page = native
                    .query(
                        SqlStatement::new(
                            sql,
                            vec![SqlValue::Text(String::new()), SqlValue::Integer(101)],
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

fn assert_seeks_token_index(engine: &str, plan: &[String]) {
    eprintln!("{engine} pending page plan:\n{}", plan.join("\n"));
    assert!(!plan.is_empty(), "{engine}: no plan");
    for line in plan {
        assert!(
            !line.contains("SCAN sys_pubsub_subscriptions"),
            "{engine}: a full scan: {line}"
        );
    }
    assert!(
        plan.iter()
            .any(|line| line.contains("sys_pubsub_subscription_pending_token")),
        "{engine}: the pending token index is not used"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_when_pending_page_planned_then_it_seeks_the_token_index() {
    let backend = crate::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    assert_seeks_token_index("sqlite", &query_plan(backend).await);
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_when_pending_page_planned_then_it_seeks_the_token_index() {
    let path = std::env::temp_dir().join(format!("w3s-pending-{}.db", uuid::Uuid::new_v4()));
    let backend = crate::turso::TursoBackend::initialize(
        path.to_str().unwrap(),
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let plan = query_plan(backend).await;
    let _ = std::fs::remove_file(&path);
    assert_seeks_token_index("turso", &plan);
}
