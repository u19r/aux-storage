/// Kept from the queue fixture of these tests: concurrent queue store
/// initialization on one Postgres database is idempotent.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn postgres_queue_provider_initialization_is_concurrent_safe() {
    async fn initialize(dsn: &str) -> Result<(), String> {
        let driver = sql_backend::postgres::PostgresBackend::initialize(
            dsn,
            4,
            false,
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .map_err(|error| format!("open Postgres driver: {error:?}"))?;
        sql_backend::SqlStore::initialize_schema(
            driver,
            storage_types::InitialStreamLineages::FOUR,
            false,
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("initialize queue schema: {error:?}"))
    }

    let base_dsn = std::env::var("TEST_POSTGRES_DSN").expect("owned Postgres DSN is required");
    let (dsn, schema) =
        sql_backend::postgres::test_schema::isolated_test_dsn(&base_dsn, "pubsub_queue_init");
    sql_backend::postgres::test_schema::create_test_schema(&dsn, &schema)
        .await
        .expect("create Postgres test schema");
    let (first, second, third, fourth) = tokio::join!(
        initialize(&dsn),
        initialize(&dsn),
        initialize(&dsn),
        initialize(&dsn)
    );
    for result in [first, second, third, fourth] {
        result.expect("concurrent PostgreSQL queue initialization should be idempotent");
    }
}
