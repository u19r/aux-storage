//! Test-only Postgres schema isolation. The shared fixture database keeps
//! older layouts in `public` (nothing migrates), so each test run initializes
//! its store in a schema of its own.

/// A DSN for `base_dsn` whose `search_path` is a new schema
/// `<prefix>_<uuid>`, and that schema's name. Call [`create_test_schema`]
/// before the first store opens on it.
pub fn isolated_test_dsn(base_dsn: &str, prefix: &str) -> (String, String) {
    let schema = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let separator = if base_dsn.contains('?') { '&' } else { '?' };
    (
        format!("{base_dsn}{separator}options=-csearch_path%3D{schema}"),
        schema,
    )
}

/// Creates `schema` when it does not exist yet; several stores of one test
/// may call it for the same schema.
pub async fn create_test_schema(dsn: &str, schema: &str) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connect Postgres test database: {error}"))?;
    let connection = tokio::spawn(connection);
    let created = client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .await
        .map_err(|error| format!("create Postgres test schema: {error}"));
    drop(client);
    let _ = connection.await;
    created
}
