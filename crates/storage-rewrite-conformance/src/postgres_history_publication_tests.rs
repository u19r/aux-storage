//! A native PostgreSQL allocation floor fences both snapshot readers and trim.

use std::{sync::Arc, time::Duration};

use anyhow::Context as _;
use serde_json::json;
use storage_provider::{
    BoundItemWrite, DynamoPort, PutItemShape, SystemStreamTrimExecutor, resolve_table_in_attempt,
};
use storage_types::{
    AttemptMode, Clock, GetStreamRecordsRequest, PutItemRequest, StorageError, TableName,
    TimestampMillis,
};
use stream_provider::SystemStreamPageRequest;

use super::{dynamodb_stream_history_tests::create_stream_table, test_support::TestStore};

#[derive(Clone, Copy)]
enum Settlement {
    Commit,
    Abort,
}

struct MaintenanceClock(TimestampMillis);

impl Clock for MaintenanceClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

#[tokio::test]
async fn given_earlier_uncommitted_history_when_later_write_commits_then_read_and_trim_do_not_skip_the_earlier_commit()
-> anyhow::Result<()> {
    qualify_publication_floor(Settlement::Commit).await
}

#[tokio::test]
async fn given_earlier_aborted_history_when_later_write_commits_then_read_and_trim_resume_without_a_phantom()
-> anyhow::Result<()> {
    qualify_publication_floor(Settlement::Abort).await
}

async fn qualify_publication_floor(settlement: Settlement) -> anyhow::Result<()> {
    let store = TestStore::new_postgres().await;
    let outcome = async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        let writer = sql_backend::SqlStore::initialize_schema(
            sql_backend::postgres::PostgresBackend::initialize(&store.postgres_dsn(), 2, false)
                .await?,
        )
        .await?;
        let cache = writer.metadata_cache();
        // Make the later committed row time-eligible for a separate maintenance
        // owner. Its publication floor must still prevent physical deletion.
        let trimmer = writer
            .clone()
            .with_clock(Arc::new(MaintenanceClock(store.now() + 25 * 3600 * 1000)));
        let name = table.clone();
        let created_at = store.now();
        let (staged_tx, staged_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn(async move {
            writer
                .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                    Box::pin(async move {
                        let (table, _) = resolve_table_in_attempt(attempt, &cache, &name).await?;
                        PutItemShape::try_from_request(history_put(&name, "earlier"))?
                            .bind_table(table, None)?
                            .execute(attempt, created_at)
                            .await?;
                        staged_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        match settlement {
                            Settlement::Commit => Ok(()),
                            Settlement::Abort => {
                                Err(StorageError::internal("injected publication abort"))
                            }
                        }
                    })
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), staged_rx).await??;
        tokio::time::timeout(
            Duration::from_secs(10),
            store.operations.put_item(history_put(&table, "later")),
        )
        .await?
        .map_err(|error| anyhow::anyhow!("later write: {:?}", error.as_ref()))?;
        let blocked = store
            .operations
            .get_stream_records(GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await
            .map_err(|error| anyhow::anyhow!("blocked table read: {:?}", error.as_ref()))?;
        anyhow::ensure!(
            blocked.records().is_empty(),
            "table cursor passed an open native allocation"
        );
        let scope = store.stream_operations.system_stream_scope().await?;
        let blocked_system = store
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 10)?)
            .await
            .map_err(|error| anyhow::anyhow!("blocked system read: {:?}", error.as_ref()))?;
        anyhow::ensure!(
            blocked_system.records().is_empty(),
            "system cursor passed an open native allocation"
        );
        let trimmed = SystemStreamTrimExecutor::new(&trimmer, 10)
            .run_page(scope.clone(), store.now() + 1, None)
            .await
            .map_err(|error| anyhow::anyhow!("trim held publication: {:?}", error.as_ref()))?;
        anyhow::ensure!(
            trimmed.scanned_records() > 0,
            "trim did not inspect the time-eligible later row"
        );
        anyhow::ensure!(
            trimmed.reclaimed_records() == 0,
            "trim deleted an unpublished later position"
        );
        release_tx.send(()).unwrap();
        let result = pending.await?;
        match settlement {
            Settlement::Commit => {
                result.map_err(|error| anyhow::anyhow!("earlier commit: {:?}", error.as_ref()))?
            }
            Settlement::Abort => anyhow::ensure!(result.is_err()),
        }
        let expected = match settlement {
            Settlement::Commit => vec!["earlier", "later"],
            Settlement::Abort => vec!["later"],
        };
        let request = GetStreamRecordsRequest::table(
            table,
            blocked.last_evaluated_key().map(str::to_owned),
            Some(10),
        );
        let visible = wait_for_published_records(
            || store.operations.get_stream_records(request.clone()),
            expected.len(),
            |page: &storage_types::GetStreamRecordsResponse| page.records().len(),
        )
        .await
        .context("settled table read")?;
        let keys = visible
            .records()
            .iter()
            .map(|record| {
                serde_json::to_value(&record.keys).unwrap()["pk"]["S"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            keys == expected,
            "table continuation skipped or invented a settled event"
        );
        let request = SystemStreamPageRequest::try_new(
            scope,
            Some(blocked_system.issued_checkpoint().token().clone()),
            10,
        )?;
        let visible_system = wait_for_published_records(
            || {
                store
                    .stream_operations
                    .get_system_stream_page(request.clone())
            },
            expected.len(),
            |page: &stream_provider::SystemStreamPage| page.records().len(),
        )
        .await
        .context("settled system read")?;
        let keys = visible_system
            .records()
            .iter()
            .map(|record| {
                serde_json::to_value(&record.keys).unwrap()["pk"]["S"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            keys == expected,
            "system continuation skipped or invented a settled event"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

fn history_put(table: &TableName, key: &str) -> PutItemRequest {
    serde_json::from_value(json!({
        "TableName": table, "Item": {"pk": {"S": key}, "value": {"S": key}}
    }))
    .unwrap()
}

// PostgreSQL's safe publication floor includes unrelated cluster transactions.
// Keep the exact original continuation until all settled fixture rows publish.
async fn wait_for_published_records<T, F, Fut, E>(
    mut read: F,
    expected: usize,
    count: impl Fn(&T) -> usize,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    Ok(tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let page = read().await?;
            if count(&page) >= expected {
                return Ok::<_, E>(page);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??)
}
