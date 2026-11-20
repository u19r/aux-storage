use crate::{SqlEngine, data_schema::schema_statements};

fn regional_statements(statements: &[String]) -> Vec<&String> {
    statements
        .iter()
        .filter(|statement| {
            statement.contains("regional_markers") || statement.contains("regional_progress")
        })
        .collect()
}

#[tokio::test]
async fn given_regional_flag_when_schema_statements_then_regional_ddl_only_when_on() {
    for (label, engine) in [
        ("sqlite", SqlEngine::Sqlite),
        ("turso", SqlEngine::Turso),
        ("postgres", SqlEngine::Postgres),
    ] {
        let off = schema_statements(engine, false).await.unwrap();
        assert!(
            regional_statements(&off).is_empty(),
            "{label}: replication off creates no regional relation"
        );
        let on = schema_statements(engine, true).await.unwrap();
        assert_eq!(
            regional_statements(&on).len(),
            2,
            "{label}: the marker and progress relations"
        );
        assert_eq!(
            on.len(),
            off.len() + 2,
            "{label}: the flag adds only the two regional statements"
        );
    }
}
