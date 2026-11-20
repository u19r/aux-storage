//! Native N3b concurrency, interrupted index recovery, and relation lock proof.

use std::sync::Arc;

use anyhow::Context as _;
use serde_json::json;
use storage_types::{BoundGsi, GsiPhase, StoredGsiBinding, TableHistoryId};

use super::{
    gsi_lifecycle_tests::{create_index, finish_backfill},
    gsi_workflow_tests::{build_request, group_keys, put_group, table_request},
    native_census_test_support::NativeCensus,
    test_support::TestStore,
};

async fn connect(store: &TestStore) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn load_index(
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<(TableHistoryId, BoundGsi)> {
    let row = client
        .query_one(
            "SELECT id, gsi_blob FROM tables WHERE table_name = $1",
            &[&table],
        )
        .await?;
    let id: String = row.get(0);
    let bytes: Vec<u8> = row.get(1);
    let binding: StoredGsiBinding = storage_types::storage_serde::from_bytes(&bytes)?;
    Ok((
        uuid::Uuid::parse_str(&id)?.into(),
        binding
            .gsis
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing GSI binding"))?,
    ))
}

async fn seed(store: &TestStore, table: &str) -> anyhow::Result<()> {
    let mut request = table_request(table);
    request.global_secondary_indexes = None;
    request.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store
        .operations
        .create_table(request)
        .await
        .map_err(|error| anyhow::anyhow!("seed create: {:?}", error.as_ref()))?;
    put_group(store, table, "A", "x").await.context("seed A")?;
    put_group(store, table, "B", "x").await.context("seed B")?;
    create_index(store, table).await.context("seed index")
}

async fn reach_building(
    store: &TestStore,
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<(TableHistoryId, BoundGsi)> {
    for _ in 0..8 {
        let index = load_index(client, table).await?;
        if index.1.phase == GsiPhase::Building {
            return Ok(index);
        }
        store
            .maintenance
            .advance_gsi_build(build_request(1))
            .await
            .map_err(|error| anyhow::anyhow!("fill page: {:?}", error.as_ref()))?;
    }
    anyhow::bail!("native GSI did not finish Filling")
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_row_changed_after_fill_read_when_conditional_update_fails_then_cursor_retries_row()
-> anyhow::Result<()> {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        seed(&store, "gsi_ctid_retry").await?;
        let held = census.hold(|wave| wave.sql_contains("ctid = CAST("));
        let repository = Arc::clone(&store.maintenance);
        let worker =
            tokio::spawn(async move { repository.advance_gsi_build(build_request(1)).await });
        held.reached().await;
        let observation = async {
            let client = connect(&store).await?;
            let rows = client
                .query(
                    "SELECT backend_xid IS NULL FROM pg_stat_activity WHERE application_name = \
                     current_setting('application_name') AND query LIKE 'SELECT \
                     item_value,%ctid::text%' AND state = 'idle in transaction'",
                    &[],
                )
                .await?;
            anyhow::ensure!(
                rows.len() == 1 && rows[0].get::<_, bool>(0),
                "fill read assigned an xid before its update wave"
            );
            put_group(&store, "gsi_ctid_retry", "A", "y").await
        }
        .await;
        held.release();
        worker.await??;
        observation?;
        let client = connect(&store).await?;
        let bytes: Vec<u8> = client
            .query_one(
                "SELECT state FROM sys_gsi_builds WHERE table_name = 'gsi_ctid_retry'",
                &[],
            )
            .await?
            .get(0);
        let state: storage_backfill::GsiState<Vec<u8>> = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            matches!(state, storage_backfill::GsiState::Filling(scan) if scan.after.is_none()),
            "failed conditional update advanced its cursor"
        );
        finish_backfill(&store, 1).await?;
        anyhow::ensure!(group_keys(&store, "gsi_ctid_retry", "x").await? == ["B"]);
        anyhow::ensure!(group_keys(&store, "gsi_ctid_retry", "y").await? == ["A"]);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_index_build_dispatch_when_writer_commits_then_no_transaction_pins_frontier()
-> anyhow::Result<()> {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        seed(&store, "gsi_cic_frontier").await?;
        let client = connect(&store).await?;
        reach_building(&store, &client, "gsi_cic_frontier").await?;
        let before = super::cdc_stream_tests::system_records(&store, 100)
            .await?
            .len();
        // This proves the post-commit dispatch boundary. A separate native
        // progress-view check must still prove an executing CIC's frontier.
        let held = census.hold(|wave| wave.sql_contains("CREATE INDEX CONCURRENTLY"));
        let repository = Arc::clone(&store.maintenance);
        let worker =
            tokio::spawn(async move { repository.advance_gsi_build(build_request(1)).await });
        held.reached().await;
        let observation = async {
            let active: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = \
                     current_setting('application_name') AND pid <> pg_backend_pid() AND \
                     xact_start IS NOT NULL",
                    &[],
                )
                .await?
                .get(0);
            anyhow::ensure!(active == 0, "index DDL retained a native transaction");
            put_group(&store, "gsi_cic_frontier", "C", "y").await?;
            anyhow::ensure!(
                super::cdc_stream_tests::system_records(&store, 100)
                    .await?
                    .len()
                    == before + 1,
                "a concurrent index build pinned the stream frontier"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        held.release();
        worker.await??;
        observation?;
        finish_backfill(&store, 1).await?;
        anyhow::ensure!(group_keys(&store, "gsi_cic_frontier", "y").await? == ["C"]);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_running_concurrent_index_when_writer_commits_then_stream_frontier_advances()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        seed(&store, "gsi_running_cic").await?;
        let client = connect(&store).await?;
        let (history, _) = reach_building(&store, &client, "gsi_running_cic").await?;
        let before = super::cdc_stream_tests::system_records(&store, 100)
            .await?
            .len();
        let blocker = connect(&store).await?;
        let relation = sql_backend::SqlEngine::Postgres.item_table_name(history);
        // A relation lock without an assigned xid keeps CIC in its native
        // writer-wait phase without itself pinning the stream frontier.
        blocker
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE \"{relation}\" IN ROW EXCLUSIVE MODE"
            ))
            .await?;
        let blocker_pid: i64 = blocker
            .query_one("SELECT pg_backend_pid()::bigint", &[])
            .await?
            .get(0);
        let repository = Arc::clone(&store.maintenance);
        let worker =
            tokio::spawn(async move { repository.advance_gsi_build(build_request(1)).await });
        let observation = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            let pid = loop {
                interval.tick().await;
                let rows = client
                    .query(
                        "SELECT p.pid FROM pg_stat_progress_create_index p WHERE p.relid = \
                         to_regclass($1) AND p.phase = 'waiting for writers before build' AND \
                         p.current_locker_pid = $2",
                        &[&relation, &blocker_pid],
                    )
                    .await?;
                if let Some(row) = rows.first() {
                    break row.get::<_, i32>(0);
                }
            };
            put_group(&store, "gsi_running_cic", "C", "y").await?;
            let records = super::cdc_stream_tests::system_records(&store, 100).await?;
            let row = client
                .query_one(
                    "SELECT p.phase, a.backend_xid::text FROM pg_stat_progress_create_index p \
                     JOIN pg_stat_activity a USING (pid) WHERE p.pid = $1",
                    &[&pid],
                )
                .await?;
            let phase: String = row.get(0);
            let xid: Option<String> = row.get(1);
            eprintln!(
                "N3b running CIC: phase={phase}, backend_xid={xid:?}, \
                 stream_records_before={before}, stream_records_after={}",
                records.len()
            );
            anyhow::ensure!(
                phase == "waiting for writers before build",
                "CIC escaped synchronization"
            );
            anyhow::ensure!(
                records.len() == before + 1,
                "running CIC pinned the stream frontier"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("running CIC/frontier synchronization deadlocked");
        blocker.batch_execute("ROLLBACK").await?;
        worker.await??;
        observation??;
        finish_backfill(&store, 1).await?;
        anyhow::ensure!(group_keys(&store, "gsi_running_cic", "y").await? == ["C"]);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_invalid_concurrent_index_when_job_resumes_then_new_build_replaces_it()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        seed(&store, "gsi_invalid_cic").await?;
        let client = connect(&store).await?;
        let (history, old) = reach_building(&store, &client, "gsi_invalid_cic").await?;
        let engine = sql_backend::SqlEngine::Postgres;
        let name = engine.gsi_index_name(history, &old);
        let column = old.columns.iter().find(|column| column.attribute == "gsi_pk").unwrap().physical_name();
        let sql = format!("CREATE UNIQUE INDEX CONCURRENTLY \"{name}\" ON \"{}\" (\"{column}\")", engine.item_table_name(history));
        anyhow::ensure!(client.batch_execute(&sql).await.is_err(), "duplicate keys must leave an interrupted index");
        let valid: bool = client.query_one("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", &[&name]).await?.get(0);
        anyhow::ensure!(!valid);
        finish_backfill(&store, 8).await?;
        let (_, rebuilt) = load_index(&client, "gsi_invalid_cic").await?;
        anyhow::ensure!(rebuilt.phase == GsiPhase::Active && rebuilt.build != old.build);
        let removed: bool = client.query_one("SELECT to_regclass($1) IS NULL", &[&name]).await?.get(0);
        anyhow::ensure!(removed, "invalid build was not removed");
        anyhow::ensure!(group_keys(&store, "gsi_invalid_cic", "x").await? == ["A", "B"]);
        store.operations.update_table(serde_json::from_value(json!({"TableName":"gsi_invalid_cic", "GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"by_group"}}]}))?).await?;
        for _ in 0..8 { store.maintenance.reclaim_table_page().await?; }
        let table = engine.item_table_name(history);
        let count: i64 = client.query_one("SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND NOT attisdropped", &[&table, &column]).await?.get(0);
        anyhow::ensure!(count == 0, "unused Postgres GSI column was retained");
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_twenty_partial_indexes_when_get_and_put_run_then_relation_locks_are_counted()
-> anyhow::Result<()> {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        let mut request = table_request("gsi_twenty_locks");
        let index = request.global_secondary_indexes.take().unwrap().remove(0);
        request.global_secondary_indexes = Some((0..20).map(|ordinal| {
            let mut index = index.clone();
            index.index_name = storage_types::IndexName::new(&format!("by_group_{ordinal}"));
            index
        }).collect());
        store.operations.create_table(request).await?;
        put_group(&store, "gsi_twenty_locks", "A", "x").await?;
        let client = connect(&store).await?;
        let (history, _) = load_index(&client, "gsi_twenty_locks").await?;
        let relation = sql_backend::SqlEngine::Postgres.item_table_name(history);
        for put in [false, true] {
            let held = census.hold(|wave| matches!(wave,
                super::native_census_test_support::CensusWave::Sql(sql)
                    if sql.iter().any(|(statement, _)| *statement == "COMMIT")));
            let operations = Arc::clone(&store.operations);
            let worker = tokio::spawn(async move {
                if put {
                    operations.put_item(storage_types::PutItemRequest::try_from(json!({"TableName":"gsi_twenty_locks", "Item":{"pk":{"S":"A"}, "gsi_pk":{"S":"y"}, "gsi_sk":{"S":"A"}}})).map_err(anyhow::Error::msg)?).await?;
                } else {
                    operations.get_item(storage_types::GetItemRequest::try_from(json!({"TableName":"gsi_twenty_locks", "Key":{"pk":{"S":"A"}}})).map_err(anyhow::Error::msg)?).await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            held.reached().await;
            let observed = client.query("SELECT mode, count(*)::bigint, count(*) FILTER (WHERE NOT fastpath)::bigint FROM pg_locks WHERE locktype = 'relation' AND pid <> pg_backend_pid() AND relation IN (SELECT indexrelid FROM pg_index WHERE indrelid = to_regclass($1) UNION SELECT to_regclass($1)) GROUP BY mode ORDER BY mode", &[&relation]).await;
            held.release();
            worker.await??;
            let observed = observed?;
            let mut total = 0;
            let mut nonfast = 0;
            for row in observed {
                let mode: String = row.get(0);
                anyhow::ensure!(matches!(mode.as_str(), "AccessShareLock" | "RowExclusiveLock"), "unexpected relation lock: {mode}");
                total += row.get::<_, i64>(1);
                nonfast += row.get::<_, i64>(2);
            }
            eprintln!("N3b 20 partial indexes {}: relation_locks={total} non_fast_path={nonfast}", if put { "PutItem" } else { "GetItem" });
            if put {
                anyhow::ensure!(total >= 22 && nonfast <= total, "missing table, primary, or GSI relation locks");
            } else {
                // GetItem sends BEGIN, its read and COMMIT in one wave, held
                // here before it is sent: no relation lock outlives that wave.
                anyhow::ensure!(total == 0, "a one-wave read held relation locks between waves");
            }
        }
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_invalid_index_with_held_build_lock_when_job_steps_then_build_is_kept()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        seed(&store, "gsi_held_cic").await?;
        let client = connect(&store).await?;
        let (history, old) = reach_building(&store, &client, "gsi_held_cic").await?;
        let engine = sql_backend::SqlEngine::Postgres;
        let name = engine.gsi_index_name(history, &old);
        let column = old
            .columns
            .iter()
            .find(|column| column.attribute == "gsi_pk")
            .unwrap()
            .physical_name();
        let sql = format!(
            "CREATE UNIQUE INDEX CONCURRENTLY \"{name}\" ON \"{}\" (\"{column}\")",
            engine.item_table_name(history)
        );
        anyhow::ensure!(
            client.batch_execute(&sql).await.is_err(),
            "duplicate keys must leave an INVALID index"
        );
        // Another node's DDL session holds the build lock of this index name.
        let builder = connect(&store).await?;
        builder
            .query_one("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&name])
            .await?;
        for _ in 0..3 {
            let step = store
                .maintenance
                .advance_gsi_build(build_request(1))
                .await
                .map_err(|error| anyhow::anyhow!("held step: {:?}", error.as_ref()))?;
            anyhow::ensure!(
                matches!(step, storage_types::maintenance::StepResult::Complete),
                "a running build must wait at the normal cadence"
            );
        }
        let kept: bool = client
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&name])
            .await?
            .get(0);
        anyhow::ensure!(kept, "a running build was dropped");
        let (_, current) = load_index(&client, "gsi_held_cic").await?;
        anyhow::ensure!(
            current.phase == GsiPhase::Building && current.build == old.build,
            "a running build was retired"
        );
        builder
            .query_one(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                &[&name],
            )
            .await?;
        finish_backfill(&store, 8).await?;
        let (_, rebuilt) = load_index(&client, "gsi_held_cic").await?;
        anyhow::ensure!(
            rebuilt.phase == GsiPhase::Active && rebuilt.build != old.build,
            "the failed build was not replaced after the lock was free"
        );
        anyhow::ensure!(group_keys(&store, "gsi_held_cic", "x").await? == ["A", "B"]);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_ttl_fill_page_when_writer_puts_then_page_takes_no_exclusive_catalog_lock()
-> anyhow::Result<()> {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome =
        async {
            let table = "ttl_fill_locks";
            let mut request = table_request(table);
            request.global_secondary_indexes = None;
            store.operations.create_table(request).await?;
            for key in ["a", "b", "c"] {
                store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": key}, "ttl": {"N": "4102444800"}}
            })).map_err(anyhow::Error::msg)?).await?;
            }
            store.operations.update_time_to_live(serde_json::from_value(json!({
            "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
        }))?).await?;
            census.reset();
            let held = census.hold(|wave| wave.sql_contains("SET __aux_ttl_epoch"));
            let repository = Arc::clone(&store.maintenance);
            let worker =
                tokio::spawn(async move { repository.advance_gsi_build(build_request(1)).await });
            held.reached().await;
            let write = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                store.operations.put_item(
                    storage_types::PutItemRequest::try_from(
                        json!({"TableName": table, "Item": {"pk": {"S": "z"}}}),
                    )
                    .map_err(anyhow::Error::msg)?,
                ),
            )
            .await;
            held.release();
            let page = worker
                .await?
                .map_err(|error| anyhow::anyhow!("fill page: {:?}", error.as_ref()))?;
            anyhow::ensure!(matches!(
                page,
                storage_types::maintenance::StepResult::Advanced(())
            ));
            write.context("an ordinary write waited for a TTL fill page")??;
            let fill = census.snapshot();
            eprintln!("RP2 TTL fill page and concurrent PutItem: {fill}");
            anyhow::ensure!(
                fill.locks.advisory_catalog_exclusive == 0 && fill.locks.for_update >= 1,
                "a fill page took the exclusive catalog lock"
            );
            let mut exclusive_steps = 0;
            for _ in 0..16 {
                census.reset();
                let step = store
                    .maintenance
                    .advance_gsi_build(build_request(1))
                    .await
                    .map_err(|error| anyhow::anyhow!("TTL step: {:?}", error.as_ref()))?;
                let counts = census.snapshot();
                eprintln!("RP2 TTL step: {counts}");
                if counts.locks.advisory_catalog_exclusive > 0 {
                    // The completing page still issues its FOR UPDATE scan, but
                    // it returns no rows, so no row lock precedes the exclusive
                    // lock.
                    anyhow::ensure!(
                        !counts.touched_written.contains("item"),
                        "a page with row locks took the exclusive catalog lock"
                    );
                    exclusive_steps += 1;
                }
                if matches!(step, storage_types::maintenance::StepResult::Complete) {
                    break;
                }
            }
            anyhow::ensure!(
                exclusive_steps == 2,
                "only the empty phase change and the activation take the exclusive lock: \
                 {exclusive_steps}"
            );
            let description = store
                .operations
                .describe_time_to_live(storage_types::DescribeTimeToLiveRequest {
                    table_name: storage_types::TableName::new(table),
                })
                .await?;
            anyhow::ensure!(
                description
                    .time_to_live_description
                    .unwrap()
                    .time_to_live_status
                    == storage_types::TimeToLiveStatus::Enabled
            );
            Ok(())
        }
        .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn given_reader_holds_table_when_update_table_adds_columns_then_alters_are_last_and_do_not_queue_readers()
-> anyhow::Result<()> {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        let table = "gsi_alter_batch";
        let mut request = table_request(table);
        request.global_secondary_indexes = None;
        store.operations.create_table(request).await?;
        put_group(&store, table, "A", "x").await?;
        super::table_stream_eligibility_tests::set_stream(&store, &storage_types::TableName::new(table), true).await?;
        census.reset();
        census.record();
        create_index(&store, table).await?;
        let counts = census.snapshot();
        let recorded = census.recorded();
        eprintln!("RP3 UpdateTable create GSI: {counts}");
        let first = recorded.iter().position(|sql| sql.starts_with("SET LOCAL lock_timeout"))
            .ok_or_else(|| anyhow::anyhow!("no bounded ALTER batch"))?;
        let alters = recorded[first + 1..].iter().take_while(|sql| sql.starts_with("ALTER TABLE")).count();
        anyhow::ensure!(alters >= 1, "the ALTER statements do not follow the lock timeout");
        anyhow::ensure!(recorded.get(first + 1 + alters).map(String::as_str) == Some("COMMIT"), "the ALTER batch is not the last before COMMIT: {:?}", &recorded[first..]);
        anyhow::ensure!(recorded.iter().filter(|sql| sql.starts_with("ALTER TABLE")).count() == alters, "ALTER statements outside the batch");
        // A reader holds ACCESS SHARE on another item table: its UpdateTable
        // gives up the ALTER after the lock timeout instead of queueing readers.
        let blocked = "gsi_alter_wait";
        let mut request = table_request(blocked);
        request.global_secondary_indexes = None;
        store.operations.create_table(request).await?;
        put_group(&store, blocked, "A", "x").await?;
        super::table_stream_eligibility_tests::set_stream(&store, &storage_types::TableName::new(blocked), true).await?;
        let client = connect(&store).await?;
        let id: String = client.query_one("SELECT id FROM tables WHERE table_name = $1", &[&blocked]).await?.get(0);
        let relation = sql_backend::SqlEngine::Postgres.item_table_name(TableHistoryId::from(uuid::Uuid::parse_str(&id)?));
        client.batch_execute(&format!("BEGIN; SELECT count(*) FROM \"{relation}\"")).await?;
        let reader = Arc::clone(&store.operations);
        let worker = tokio::spawn({
            let operations = Arc::clone(&store.operations);
            let indexes = table_request(blocked).global_secondary_indexes.unwrap();
            async move {
                let request: storage_types::UpdateTableRequest = serde_json::from_value(json!({
                    "TableName": blocked,
                    "GlobalSecondaryIndexUpdates": indexes.into_iter().map(|index| json!({"Create": index})).collect::<Vec<_>>()
                })).map_err(anyhow::Error::from)?;
                operations.update_table(request).await.map_err(|error| anyhow::anyhow!("blocked UpdateTable: {:?}", error.as_ref()))
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), reader.get_item(
            storage_types::GetItemRequest::try_from(json!({"TableName": blocked, "Key": {"pk": {"S": "A"}}})).map_err(anyhow::Error::msg)?,
        )).await;
        client.batch_execute("ROLLBACK").await?;
        let updated = worker.await?;
        read.context("a reader queued behind the waiting ALTER")??;
        updated?;
        Ok(())
    }.await;
    store.finish().await;
    outcome
}
