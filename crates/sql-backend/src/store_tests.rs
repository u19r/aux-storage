use crate::{SqlBackend as _, SqlEngine, SqlStore};

#[tokio::test]
async fn given_four_initial_roots_when_sqlite_reopens_with_one_then_scope_and_roots_are_preserved()
{
    use stream_provider::{
        StreamOperations as _, SystemStreamLineage, SystemStreamSplitChild,
        SystemStreamSplitOperationId, SystemStreamSplitRequest,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root
        .path()
        .join("lineages.db")
        .to_string_lossy()
        .into_owned();
    let driver = crate::sqlite::SqliteBackend::initialize(
        path.clone(),
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store =
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::FOUR, true)
            .await
            .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let roots = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    assert_eq!(roots.len(), 4);
    let parent = roots.into_iter().next().unwrap();
    let children = ["left", "right"].map(|side| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                format!("split-{side}"),
                format!("split-{side}-lineage"),
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    store
        .split_system_stream(
            SystemStreamSplitRequest::try_new(
                scope,
                parent,
                children,
                SystemStreamSplitOperationId::try_new("initial-config-reopen").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let before = store
        .backend
        .with_schema_attempt(|attempt| {
            Box::pin(crate::stream_history::capture_high(
                attempt,
                SqlEngine::Sqlite,
            ))
        })
        .await
        .unwrap();
    let before_catalog = store
        .system_stream_lineage_catalog(before.scope().clone())
        .await
        .unwrap();
    assert_eq!(before_catalog.len(), 6);
    assert_eq!(
        before_catalog
            .iter()
            .filter(|lineage| lineage.sealed())
            .count(),
        1
    );
    assert_eq!(before.frontier().len(), 5);
    drop(store);
    let driver =
        crate::sqlite::SqliteBackend::initialize(path, storage_types::AttemptDeadlines::default())
            .await
            .unwrap();
    let store =
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap();
    let after = store
        .backend
        .with_schema_attempt(|attempt| {
            Box::pin(crate::stream_history::capture_high(
                attempt,
                SqlEngine::Sqlite,
            ))
        })
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        store
            .system_stream_lineage_catalog(after.scope().clone())
            .await
            .unwrap(),
        before_catalog
    );
}
