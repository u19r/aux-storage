use crate::{SqlStore, sqlite::SqliteBackend};

#[tokio::test]
async fn given_baseline_mutations_when_followed_by_ordinary_write_then_only_modify_is_published() {
    let store = SqlStore::initialize_schema(
        SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
            .await
            .unwrap(),
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    storage_provider::baseline_test_support::qualify_baseline_history(store).await;
}
