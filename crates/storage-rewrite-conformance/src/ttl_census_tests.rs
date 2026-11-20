//! Reusable native-count windows for the complete TTL maintenance path.

use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use scheduled_bg_jobs::{JobId, Namespace};
use serde_json::{Value, json};
use storage_common::{TtlSweepPageCommand, TtlSweepPageInput};
use storage_provider::maintenance::ExpirationRequest;
use storage_types::{GetItemRequest, PutItemRequest, TableName, maintenance::StepResult};

use super::{native_census_test_support::NativeCensus, test_support::TestStore};

fn begin_window(census: &NativeCensus) {
    census.reset();
    census.record();
}

fn report_window(census: &NativeCensus, name: &str) {
    let counts = census.snapshot();
    assert!(counts.begins > 0, "window must observe real native work");
    eprintln!(
        "ttl-census {} {name}: {counts} entry_blob_bytes={}",
        TestStore::selected_backend(),
        census.entry_blob_bytes()
    );
    for statement in census.recorded() {
        eprintln!("ttl-sql {name}: {statement}");
    }
}

async fn put_item(store: &TestStore, table: &TableName, key: &str, expiry: &str) {
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": key}, "ttl": {"N": expiry}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn read_item(store: &TestStore, table: &TableName, key: &str) -> Value {
    serde_json::to_value(
        store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": key}}
                }))
                .unwrap(),
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn given_ttl_workload_when_public_and_maintenance_operations_run_then_native_costs_are_recorded()
 {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = store.create_standard_table().await;
    // Setup: three pre-existing rows force actual bounded fill pages.
    for key in ["fill-a", "fill-b", "fill-c"] {
        put_item(&store, &table, key, "4102444800").await;
    }
    store.operations.update_time_to_live(serde_json::from_value(json!({
        "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
    })).unwrap()).await.unwrap();
    let build = super::gsi_workflow_tests::build_request(1);
    let mut complete = false;
    for page in 0..16 {
        begin_window(&census);
        let result = store
            .maintenance
            .advance_gsi_build(build.clone())
            .await
            .unwrap();
        report_window(&census, &format!("fill-page-{page}"));
        if matches!(result, StepResult::Complete) {
            complete = true;
            break;
        }
        assert!(matches!(result, StepResult::Advanced(())));
    }
    assert!(complete, "bounded fill must complete");
    let description = store
        .operations
        .describe_time_to_live(storage_types::DescribeTimeToLiveRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        description
            .time_to_live_description
            .unwrap()
            .time_to_live_status,
        storage_types::TimeToLiveStatus::Enabled
    );
    // Warm public statement shapes, then measure one whole attempt per
    // operation.
    put_item(&store, &table, "warm", "1").await;
    put_item(&store, &table, "warm", "4102444800").await;
    store
        .operations
        .delete_item(
            storage_types::DeleteItemRequest::try_from(
                json!({"TableName": table, "Key": {"pk": {"S": "warm"}}}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    begin_window(&census);
    put_item(&store, &table, "measured", "1").await;
    report_window(&census, "put");
    begin_window(&census);
    put_item(&store, &table, "measured", "4102444800").await;
    report_window(&census, "renew");
    assert_eq!(
        read_item(&store, &table, "measured").await["Item"]["ttl"]["N"],
        "4102444800"
    );
    begin_window(&census);
    store
        .operations
        .delete_item(
            storage_types::DeleteItemRequest::try_from(
                json!({"TableName": table, "Key": {"pk": {"S": "measured"}}}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    report_window(&census, "delete");
    assert_eq!(read_item(&store, &table, "measured").await, json!({}));
    for key in ["direct-a", "direct-b", "scheduled-a", "scheduled-b"] {
        put_item(&store, &table, key, "1").await;
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let first = TtlSweepPageCommand::try_new(
        policy.clone(),
        TtlSweepPageInput::try_new("ttl", now, 1, None).unwrap(),
        now,
    )
    .unwrap();
    begin_window(&census);
    let page = store
        .maintenance
        .execute_ttl_sweep_page(first)
        .await
        .unwrap();
    report_window(&census, "direct-first-page");
    assert_eq!(page.deleted_items, 1);
    let second = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 1, page.next_cursor).unwrap(),
        now,
    )
    .unwrap();
    begin_window(&census);
    let page = store
        .maintenance
        .execute_ttl_sweep_page(second)
        .await
        .unwrap();
    report_window(&census, "direct-next-page");
    assert_eq!(page.deleted_items, 1);
    assert_eq!(read_item(&store, &table, "direct-a").await, json!({}));
    assert_eq!(read_item(&store, &table, "direct-b").await, json!({}));
    let request = ExpirationRequest {
        destination: Namespace::try_new("ttl-census").unwrap(),
        job: JobId::from([91; 16]),
        fence: NonZeroU64::MIN,
        requested_cutoff: now,
        candidate_limit: NonZeroUsize::MIN,
    };
    let mut finished = false;
    for page in 0..16 {
        begin_window(&census);
        let result = store
            .maintenance
            .advance_expiration(request.clone())
            .await
            .unwrap();
        report_window(&census, &format!("scheduled-page-{page}"));
        if matches!(result, StepResult::Complete) {
            finished = true;
            break;
        }
        assert!(matches!(result, StepResult::Advanced(())));
    }
    assert!(finished);
    for key in ["scheduled-a", "scheduled-b"] {
        assert_eq!(read_item(&store, &table, key).await, json!({}));
    }
    // A real request-window receipt becomes eligible independently of TTL
    // pages.
    store.operations.transact_write_items(storage_types::TransactWriteItemsRequest::try_from(json!({
        "ClientRequestToken": "ttl-census-receipt", "TransactItems": [{"Put": {"TableName": table, "Item": {"pk": {"S": "receipt"}}}}]
    })).unwrap()).await.unwrap();
    store.advance_time(Duration::from_secs(3600));
    begin_window(&census);
    store
        .maintenance
        .trim_transact_receipts_page(None, NonZeroU32::new(10).unwrap())
        .await
        .unwrap();
    report_window(&census, "receipt-trim");
    assert!(
        census.snapshot().rows_written > 0,
        "trim must delete an expired receipt"
    );
    store.finish().await;
}

#[cfg(feature = "postgres")]
async fn read_native_primary_rows(store: &TestStore) -> Vec<tokio_postgres::Row> {
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let relation: String = client
        .query_one(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename ~ \
             '^table_[0-9a-f]{32}$'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    client
        .query(
            &format!(
                "SELECT pk, item_value, __aux_provenance, __aux_ttl_epoch, \"__aux-ttl-key\" FROM \
                 \"{relation}\" ORDER BY pk"
            ),
            &[],
        )
        .await
        .unwrap()
}

#[cfg(feature = "postgres")]
async fn count_native_history(store: &TestStore) -> i64 {
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query_one("SELECT count(*) FROM sys_stream_entries", &[])
        .await
        .unwrap()
        .get(0)
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_same_checkpoint_when_two_pages_overlap_then_losing_page_rolls_back_every_delete() {
    assert_eq!(TestStore::selected_backend(), "postgres");
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) =
        super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_AND_OLD_IMAGES")
            .await;
    store.operations.update_time_to_live(serde_json::from_value(json!({
        "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
    })).unwrap()).await.unwrap();
    for key in ["a", "b"] {
        put_item(&store, &table, key, "1").await;
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let command = |limit| {
        TtlSweepPageCommand::try_new(
            policy.clone(),
            TtlSweepPageInput::try_new("ttl", now, limit, None).unwrap(),
            now,
        )
        .unwrap()
    };
    let history_before = count_native_history(&store).await;
    let first = command(1);
    let second = command(2);
    // The first page already holds its item lock and has staged deletion.
    // Stop immediately before its conditional checkpoint UPDATE reaches
    // Postgres.
    let first_checkpoint =
        census.hold(|wave| wave.sql_contains("AND job = 'ttl_sweep' AND state ="));
    let repository = Arc::clone(&store.maintenance);
    let first_task = tokio::spawn(async move { repository.execute_ttl_sweep_page(first).await });
    first_checkpoint.reached().await;
    // Reaching this wave proves that page two observed the original policy and
    // selected both locators while page one remained uncommitted.
    let second_items = census.hold(|wave| wave.sql_contains("hashtextextended"));
    let repository = Arc::clone(&store.maintenance);
    let second_task = tokio::spawn(async move { repository.execute_ttl_sweep_page(second).await });
    second_items.reached().await;
    second_items.release();
    first_checkpoint.release();
    let first_page = tokio::time::timeout(Duration::from_secs(20), first_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first_page.deleted_items, 1);
    assert!(first_page.next_cursor.is_some());
    assert!(
        tokio::time::timeout(Duration::from_secs(20), second_task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(read_item(&store, &table, "a").await, json!({}));
    assert_eq!(
        read_item(&store, &table, "b").await["Item"]["ttl"]["N"],
        "1"
    );
    assert_eq!(
        count_native_history(&store).await,
        history_before + 1,
        "losing page history must roll back"
    );
    let next = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 1, first_page.next_cursor).unwrap(),
        now,
    )
    .unwrap();
    assert_eq!(
        store
            .maintenance
            .execute_ttl_sweep_page(next)
            .await
            .unwrap()
            .deleted_items,
        1
    );
    assert_eq!(read_item(&store, &table, "b").await, json!({}));
    assert_eq!(count_native_history(&store).await, history_before + 2);
    store.finish().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_selected_expired_locator_when_item_is_renewed_before_lock_then_cleanup_keeps_new_image()
 {
    assert_eq!(TestStore::selected_backend(), "postgres");
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) =
        super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_AND_OLD_IMAGES")
            .await;
    store.operations.update_time_to_live(serde_json::from_value(json!({
        "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
    })).unwrap()).await.unwrap();
    put_item(&store, &table, "renewed", "1").await;
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let command = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 1, None).unwrap(),
        now,
    )
    .unwrap();
    let held = census.hold(|wave| wave.sql_contains("hashtextextended"));
    let repository = Arc::clone(&store.maintenance);
    let cleanup = tokio::spawn(async move { repository.execute_ttl_sweep_page(command).await });
    held.reached().await;
    tokio::time::timeout(
        Duration::from_secs(20),
        put_item(&store, &table, "renewed", "4102444800"),
    )
    .await
    .unwrap();
    let history_after_renewal = count_native_history(&store).await;
    held.release();
    let page = tokio::time::timeout(Duration::from_secs(20), cleanup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(page.deleted_items, 0);
    assert_eq!(
        count_native_history(&store).await,
        history_after_renewal,
        "renewal must emit no cleanup history"
    );
    assert_eq!(
        read_item(&store, &table, "renewed").await["Item"]["ttl"]["N"],
        "4102444800"
    );
    let rows = read_native_primary_rows(&store).await;
    assert_eq!(rows[0].get::<_, Option<i64>>(3), Some(4_102_444_800));
    assert!(rows[0].get::<_, Option<Vec<u8>>>(4).is_some());
    store.finish().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_dense_equal_expiry_rows_when_reopened_then_native_plans_and_token_bytes_are_visible()
{
    assert_eq!(TestStore::selected_backend(), "postgres");
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = store.create_standard_table().await;
    for batch in 0..40 {
        let writes = (0..25)
            .map(|offset| {
                json!({"PutRequest": {"Item": {
                    "pk": {"S": format!("key-{:04}", batch * 25 + offset)}, "ttl": {"N": "1"},
                    "payload": {"S": "p".repeat(1024)}
                }}})
            })
            .collect::<Vec<_>>();
        store
            .operations
            .batch_write_item(
                storage_types::BatchWriteItemRequest::try_from(
                    json!({"RequestItems": {table.as_ref(): writes}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    store.operations.update_time_to_live(serde_json::from_value(json!({
        "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
    })).unwrap()).await.unwrap();
    let mut build = super::gsi_workflow_tests::build_request(1);
    build.page_limit = storage_backfill::GsiPageLimit::try_from(256usize).unwrap();
    let mut complete = false;
    for _ in 0..16 {
        if matches!(
            store
                .maintenance
                .advance_gsi_build(build.clone())
                .await
                .unwrap(),
            StepResult::Complete
        ) {
            complete = true;
            break;
        }
    }
    assert!(complete);
    // Capture the statements sent by real first and continued page operations.
    let original = read_native_primary_rows(&store).await;
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    census.record();
    let first = TtlSweepPageCommand::try_new(
        policy.clone(),
        TtlSweepPageInput::try_new("ttl", now, 1, None).unwrap(),
        now,
    )
    .unwrap();
    let page = store
        .maintenance
        .execute_ttl_sweep_page(first)
        .await
        .unwrap();
    let second = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 1, page.next_cursor).unwrap(),
        now,
    )
    .unwrap();
    store
        .maintenance
        .execute_ttl_sweep_page(second)
        .await
        .unwrap();
    let statements = census
        .recorded()
        .into_iter()
        .filter(|sql| sql.starts_with("SELECT \"__aux-ttl-key\" FROM"))
        .collect::<Vec<_>>();
    assert_eq!(statements.len(), 2);
    // Restore the exact representative population after exercising those pages.
    let remaining = read_native_primary_rows(&store)
        .await
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<std::collections::BTreeSet<_>>();
    for row in original {
        let key: String = row.get(0);
        if remaining.contains(&key) {
            continue;
        }
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({"TableName":table,"Item":{
                    "pk":{"S":key},"ttl":{"N":"1"},"payload":{"S":"p".repeat(1024)}
                }}))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let store = store.reopen().await;
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let relation: String = client
        .query_one(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename ~ \
             '^table_[0-9a-f]{32}$'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    client
        .batch_execute(&format!("ANALYZE \"{relation}\""))
        .await
        .unwrap();
    client
        .batch_execute(&format!("VACUUM \"{relation}\""))
        .await
        .unwrap();
    let selected: Vec<u8> = client
        .query_one(
            &format!(
                "SELECT \"__aux-ttl-key\" FROM \"{relation}\" ORDER BY \"__aux-ttl-key\" OFFSET \
                 899 LIMIT 1"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let upper = storage_common::TtlIndexKey::encode_ordered_seconds(2).to_vec();
    let empty_upper = storage_common::TtlIndexKey::encode_ordered_seconds(1).to_vec();
    for (name, statement, parameters) in [
        (
            "dense-first",
            &statements[0],
            vec![&upper as &(dyn tokio_postgres::types::ToSql + Sync), &2i64],
        ),
        (
            "selective-empty",
            &statements[0],
            vec![
                &empty_upper as &(dyn tokio_postgres::types::ToSql + Sync),
                &2i64,
            ],
        ),
        (
            "dense-continuation",
            &statements[1],
            vec![
                &upper as &(dyn tokio_postgres::types::ToSql + Sync),
                &selected,
                &2i64,
            ],
        ),
    ] {
        eprintln!("ttl-native-statement {name}: {statement}");
        let sql = format!("EXPLAIN (ANALYZE, BUFFERS) {statement}");
        let rows = client.query(&sql, &parameters).await.unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        eprintln!("ttl-native-plan {name}:\n{plan}");
        assert!(
            plan.contains("Index Only Scan"),
            "locator should use its covering partial index: {plan}"
        );
        assert!(!plan.contains("Sort"), "index must supply locator order");
        if name == "dense-continuation" {
            assert!(
                plan.contains("__aux-ttl-key\" >"),
                "continuation must use the scalar key lower bound: {plan}"
            );
            assert!(
                !plan.contains("Rows Removed by Filter"),
                "continuation must not rescan the prefix: {plan}"
            );
        }
    }
    let bytes = client
        .query_one(
            &format!(
                "SELECT count(*), sum(octet_length(\"__aux-ttl-key\")), \
                 min(octet_length(\"__aux-ttl-key\")), max(octet_length(\"__aux-ttl-key\")) FROM \
                 \"{relation}\" WHERE __aux_ttl_epoch IS NOT NULL"
            ),
            &[],
        )
        .await
        .unwrap();
    eprintln!(
        "ttl-native-token count={} total={} min={} max={}",
        bytes.get::<_, i64>(0),
        bytes.get::<_, i64>(1),
        bytes.get::<_, i32>(2),
        bytes.get::<_, i32>(3)
    );
    let index = format!("t_{}_ttl", relation.strip_prefix("table_").unwrap());
    let bytes: i64 = client
        .query_one("SELECT pg_relation_size($1::text::regclass)", &[&index])
        .await
        .unwrap()
        .get(0);
    eprintln!("ttl-native-index name={index} bytes={bytes}");
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_populated_policy_when_reenabled_on_another_attribute_then_restart_fill_preserves_images()
 {
    let mut store = TestStore::new_durable().await;
    let (table, _) =
        super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_AND_OLD_IMAGES")
            .await;
    let items = [
        json!({"pk":{"S":"a"},"ttl":{"N":"1"},"expires":{"N":"1"}}),
        json!({"pk":{"S":"b"},"ttl":{"N":"1"},"expires":{"N":"1"}}),
        json!({"pk":{"S":"c"},"ttl":{"N":"1"},"expires":{"S":"invalid"}}),
        json!({"pk":{"S":"d"},"ttl":{"N":"1"}}),
    ];
    for item in &items {
        store
            .operations
            .put_item(PutItemRequest::try_from(json!({"TableName":table,"Item":item})).unwrap())
            .await
            .unwrap();
    }
    #[cfg(feature = "postgres")]
    let before_fill = if TestStore::selected_backend() == "postgres" {
        Some((
            read_native_primary_rows(&store).await,
            count_native_history(&store).await,
        ))
    } else {
        None
    };
    for (attribute, enabled) in [("ttl", true), ("ttl", false), ("expires", true)] {
        store.operations.update_time_to_live(serde_json::from_value(json!({
            "TableName":table,"TimeToLiveSpecification":{"AttributeName":attribute,"Enabled":enabled}
        })).unwrap()).await.unwrap();
        if enabled {
            let first = store
                .maintenance
                .advance_gsi_build(super::gsi_workflow_tests::build_request(1))
                .await
                .unwrap();
            assert!(matches!(first, StepResult::Advanced(())));
            store = store.reopen().await;
            let mut complete = false;
            for _ in 0..32 {
                let description = store
                    .operations
                    .describe_time_to_live(storage_types::DescribeTimeToLiveRequest {
                        table_name: table.clone(),
                    })
                    .await
                    .unwrap()
                    .time_to_live_description
                    .unwrap();
                assert!(matches!(
                    description.time_to_live_status,
                    storage_types::TimeToLiveStatus::Enabling
                        | storage_types::TimeToLiveStatus::Enabled
                ));
                if matches!(
                    store
                        .maintenance
                        .advance_gsi_build(super::gsi_workflow_tests::build_request(1))
                        .await
                        .unwrap(),
                    StepResult::Complete
                ) {
                    complete = true;
                    break;
                }
            }
            assert!(complete);
            #[cfg(feature = "postgres")]
            if let Some((original_rows, history)) = &before_fill {
                let rows = read_native_primary_rows(&store).await;
                assert_eq!(rows.len(), 4);
                for (ordinal, (original, row)) in original_rows.iter().zip(&rows).enumerate() {
                    assert_eq!(
                        row.get::<_, Vec<u8>>(1),
                        original.get::<_, Vec<u8>>(1),
                        "fill changes no image bytes"
                    );
                    assert_eq!(
                        row.get::<_, Vec<u8>>(2),
                        original.get::<_, Vec<u8>>(2),
                        "fill changes no provenance"
                    );
                    let expected = if attribute == "ttl" || ordinal < 2 {
                        Some(1i64)
                    } else {
                        None
                    };
                    assert_eq!(row.get::<_, Option<i64>>(3), expected);
                    assert_eq!(
                        row.get::<_, Option<Vec<u8>>>(4).is_some(),
                        expected.is_some()
                    );
                }
                assert_eq!(
                    count_native_history(&store).await,
                    *history,
                    "fill emits no user history"
                );
            }
            assert_eq!(
                store
                    .operations
                    .describe_time_to_live(storage_types::DescribeTimeToLiveRequest {
                        table_name: table.clone()
                    })
                    .await
                    .unwrap()
                    .time_to_live_description
                    .unwrap()
                    .time_to_live_status,
                storage_types::TimeToLiveStatus::Enabled
            );
        }
    }
    for (key, item) in ["a", "b", "c", "d"].into_iter().zip(&items) {
        assert_eq!(read_item(&store, &table, key).await, json!({"Item":item}));
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let mut cursor = None;
    let mut deleted = 0;
    for _ in 0..4 {
        let command = TtlSweepPageCommand::try_new(
            policy.clone(),
            TtlSweepPageInput::try_new("expires", now, 1, cursor).unwrap(),
            now,
        )
        .unwrap();
        let page = store
            .maintenance
            .execute_ttl_sweep_page(command)
            .await
            .unwrap();
        deleted += page.deleted_items;
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(deleted, 2);
    for (key, item) in ["c", "d"].into_iter().zip(&items[2..]) {
        assert_eq!(read_item(&store, &table, key).await, json!({"Item":item}));
    }
    store.finish().await;
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
async fn read_sqlite_family_images(store: &TestStore) -> Vec<Vec<u8>> {
    async fn read_rows<B: sql_backend::SqlBackend>(backend: B) -> Vec<Vec<u8>> {
        use sql_backend::SqlAttempt as _;
        backend
            .with_schema_attempt(|attempt| {
                Box::pin(async move {
                    let tables = attempt
                        .query(
                            sql_backend::SqlStatement::new(
                                "SELECT name FROM sqlite_master WHERE type = 'table' AND name \
                                 LIKE 'table_%'",
                                vec![],
                            ),
                            sql_backend::ReadProtection::Snapshot,
                            NonZeroUsize::new(1 << 20).unwrap(),
                        )
                        .await?;
                    let relation = tables
                        .rows
                        .into_iter()
                        .flat_map(|row| row.into_values())
                        .find_map(|value| match value {
                            sql_backend::SqlValue::Text(name) if name.len() == 38 => Some(name),
                            _ => None,
                        })
                        .expect("one native item table");
                    let rows = attempt
                        .query(
                            sql_backend::SqlStatement::new(
                                format!(
                                    "SELECT item_value, __aux_provenance FROM \"{relation}\" \
                                     ORDER BY pk"
                                ),
                                vec![],
                            ),
                            sql_backend::ReadProtection::Snapshot,
                            NonZeroUsize::new(1 << 20).unwrap(),
                        )
                        .await?;
                    Ok(rows
                        .rows
                        .into_iter()
                        .flat_map(|row| row.into_values())
                        .map(|value| match value {
                            sql_backend::SqlValue::Blob(bytes) => bytes,
                            _ => panic!("image and provenance must be native blobs"),
                        })
                        .collect())
                })
            })
            .await
            .unwrap()
    }
    match TestStore::selected_backend().as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            read_rows(
                sql_backend::sqlite::SqliteBackend::initialize(
                    store.sqlite_path().unwrap(),
                    storage_types::AttemptDeadlines::default(),
                )
                .await
                .unwrap(),
            )
            .await
        }
        #[cfg(feature = "turso")]
        "turso" => {
            read_rows(
                sql_backend::turso::TursoBackend::initialize(
                    store.turso_path().unwrap(),
                    storage_types::AttemptDeadlines::default(),
                )
                .await
                .unwrap(),
            )
            .await
        }
        _ => Vec::new(),
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[derive(Default)]
struct ActivationReplyLoss {
    enabled: std::sync::atomic::AtomicBool,
    armed: std::sync::atomic::AtomicBool,
    activations: std::sync::atomic::AtomicUsize,
    lost: std::sync::atomic::AtomicUsize,
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl sql_backend::SqlFaultProbe for ActivationReplyLoss {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.enabled.load(SeqCst) && call.sql.starts_with("UPDATE tables SET ttl_blob") {
            self.activations.fetch_add(1, SeqCst);
            self.armed.store(true, SeqCst);
        }
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        use std::sync::atomic::Ordering::SeqCst;
        let lose = boundary == sql_backend::SqlCommitBoundary::AfterDurableAcknowledgement
            && self.armed.swap(false, SeqCst);
        Box::pin(async move {
            if lose {
                self.lost.fetch_add(1, SeqCst);
                Err(storage_types::StorageError::internal(
                    "lost TTL activation acknowledgement",
                ))
            } else {
                Ok(())
            }
        })
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
#[cfg_attr(
    feature = "postgres",
    ignore = "native SQL provider selection is explicit"
)]
async fn given_populated_fill_when_activation_reply_is_lost_then_reopen_keeps_enabled_without_replay()
 {
    use std::sync::atomic::Ordering::SeqCst;
    let probe = Arc::new(ActivationReplyLoss::default());
    let store = TestStore::new_durable_sql_with_fault_probe(probe.clone()).await;
    let (table, _) =
        super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_AND_OLD_IMAGES")
            .await;
    for key in ["a", "b", "c"] {
        put_item(&store, &table, key, "1").await;
    }
    store.wait_until_changes_are_readable().await;
    let history = super::image_retention_tests::canonical_history_reads(&store, &table)
        .await
        .unwrap();
    let before_history = (store.canonical_history_read)(history.clone())
        .await
        .unwrap();
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    let sqlite_family_before = read_sqlite_family_images(&store).await;
    #[cfg(feature = "postgres")]
    let native_before = if TestStore::selected_backend() == "postgres" {
        Some(read_native_primary_rows(&store).await)
    } else {
        None
    };
    store
        .operations
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName":table,"TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    // Setup policy commits are complete. Only the real activation UPDATE arms
    // loss.
    probe.enabled.store(true, SeqCst);
    let mut unknown = false;
    for _ in 0..32 {
        match store
            .maintenance
            .advance_gsi_build(super::gsi_workflow_tests::build_request(1))
            .await
        {
            Err(error) => {
                assert!(error.is_commit_unknown(), "{error}");
                unknown = true;
                break;
            }
            Ok(result) => assert!(
                !matches!(result, StepResult::Complete),
                "completion must expose lost acknowledgement"
            ),
        }
    }
    assert!(unknown);
    assert_eq!(
        probe.activations.load(SeqCst),
        1,
        "original call must not replay activation"
    );
    assert_eq!(probe.lost.load(SeqCst), 1);
    let store = store.reopen().await;
    assert_eq!(
        store
            .operations
            .describe_time_to_live(storage_types::DescribeTimeToLiveRequest {
                table_name: table.clone()
            })
            .await
            .unwrap()
            .time_to_live_description
            .unwrap()
            .time_to_live_status,
        storage_types::TimeToLiveStatus::Enabled
    );
    assert!(matches!(
        store
            .maintenance
            .advance_gsi_build(super::gsi_workflow_tests::build_request(1))
            .await
            .unwrap(),
        StepResult::Complete
    ));
    for key in ["a", "b", "c"] {
        assert_eq!(
            read_item(&store, &table, key).await["Item"]["ttl"]["N"],
            "1"
        );
    }
    #[cfg(any(feature = "sqlite", feature = "turso"))]
    assert_eq!(
        read_sqlite_family_images(&store).await,
        sqlite_family_before,
        "recovery preserves exact native image and provenance bytes"
    );
    let after_history = (store.canonical_history_read)(history).await.unwrap();
    assert_eq!(
        serde_json::to_value(
            after_history
                .into_iter()
                .flat_map(|page| page.entries)
                .map(|row| row.entry)
                .collect::<Vec<_>>()
        )
        .unwrap(),
        serde_json::to_value(
            before_history
                .into_iter()
                .flat_map(|page| page.entries)
                .map(|row| row.entry)
                .collect::<Vec<_>>()
        )
        .unwrap(),
        "activation and recovery emit no history"
    );
    #[cfg(feature = "postgres")]
    if let Some(original) = native_before {
        let rows = read_native_primary_rows(&store).await;
        for (before, after) in original.iter().zip(&rows) {
            assert_eq!(before.get::<_, Vec<u8>>(1), after.get::<_, Vec<u8>>(1));
            assert_eq!(before.get::<_, Vec<u8>>(2), after.get::<_, Vec<u8>>(2));
            assert_eq!(after.get::<_, Option<i64>>(3), Some(1));
            assert!(after.get::<_, Option<Vec<u8>>>(4).is_some());
        }
    }
    store.finish().await;
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "native SQLite family selection is explicit"
)]
async fn given_equal_expiry_rows_when_locator_sql_is_explained_then_scalar_index_supplies_range_and_order()
 {
    use sql_backend::{SqlAttempt as _, SqlBackend as _};
    if !matches!(TestStore::selected_backend().as_str(), "sqlite" | "turso") {
        eprintln!(
            "skipped: {} is not a SQLite-family backend",
            TestStore::selected_backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let table = store.create_standard_table().await;
    store
        .operations
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName":table,"TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    for key in ["a", "b", "c", "d"] {
        put_item(&store, &table, key, "1").await;
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    census.record();
    let mut cursor = None;
    for _ in 0..2 {
        let command = TtlSweepPageCommand::try_new(
            policy.clone(),
            TtlSweepPageInput::try_new("ttl", now, 1, cursor).unwrap(),
            now,
        )
        .unwrap();
        cursor = store
            .maintenance
            .execute_ttl_sweep_page(command)
            .await
            .unwrap()
            .next_cursor;
    }
    let statements = census
        .recorded()
        .into_iter()
        .filter(|sql| sql.starts_with("SELECT \"__aux-ttl-key\" FROM"))
        .collect::<Vec<_>>();
    assert_eq!(statements.len(), 2);
    let store = store.reopen().await;
    let mut plans = Vec::new();
    for (ordinal, statement) in statements.into_iter().enumerate() {
        let mut binds = vec![sql_backend::SqlValue::Blob(
            storage_common::TtlIndexKey::encode_ordered_seconds(2).to_vec(),
        )];
        if ordinal != 0 {
            binds.push(sql_backend::SqlValue::Blob(
                storage_common::TtlIndexKey::new(1, "a".to_owned().into())
                    .encode_ordered_bytes()
                    .unwrap(),
            ));
        }
        binds.push(sql_backend::SqlValue::Integer(2));
        eprintln!("ttl-native-locator {ordinal}: {statement}");
        let sql = format!("EXPLAIN QUERY PLAN {statement}");
        let page = match TestStore::selected_backend().as_str() {
            #[cfg(feature = "sqlite")]
            "sqlite" => sql_backend::sqlite::SqliteBackend::initialize(
                store.sqlite_path().unwrap(),
                storage_types::AttemptDeadlines::default(),
            )
            .await
            .unwrap()
            .with_schema_attempt(move |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            sql_backend::SqlStatement::new(sql, binds),
                            sql_backend::ReadProtection::Snapshot,
                            NonZeroUsize::new(1 << 20).unwrap(),
                        )
                        .await
                })
            })
            .await
            .unwrap(),
            #[cfg(feature = "turso")]
            "turso" => sql_backend::turso::TursoBackend::initialize(
                store.turso_path().unwrap(),
                storage_types::AttemptDeadlines::default(),
            )
            .await
            .unwrap()
            .with_schema_attempt(move |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            sql_backend::SqlStatement::new(sql, binds),
                            sql_backend::ReadProtection::Snapshot,
                            NonZeroUsize::new(1 << 20).unwrap(),
                        )
                        .await
                })
            })
            .await
            .unwrap(),
            _ => unreachable!(),
        };
        let plan = page
            .rows
            .into_iter()
            .flat_map(|row| row.into_values())
            .filter_map(|value| match value {
                sql_backend::SqlValue::Text(text) => Some(text),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        eprintln!("ttl-native-plan {ordinal}: {plan}");
        plans.push(plan);
    }
    store.finish().await;
    for (ordinal, plan) in plans.iter().enumerate() {
        assert!(plan.contains("ttl") && plan.contains("INDEX"), "{plan}");
        assert!(
            plan.contains("__aux-ttl-key") && plan.contains("<"),
            "{plan}"
        );
        assert!(
            !plan.contains("TEMP B-TREE") && !plan.contains("SORTER"),
            "index must supply order: {plan}"
        );
        if ordinal == 1 {
            assert!(plan.contains(">"), "scalar lower bound required: {plan}");
        }
    }
}

#[tokio::test]
async fn given_unpaired_stream_table_when_expiration_page_runs_then_no_regional_family_touched() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) =
        super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_AND_OLD_IMAGES")
            .await;
    store.operations.update_time_to_live(serde_json::from_value(json!({
        "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
    })).unwrap()).await.unwrap();
    for key in ["a", "b"] {
        put_item(&store, &table, key, "1").await;
    }
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let command = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 10, None).unwrap(),
        now,
    )
    .unwrap();
    begin_window(&census);
    let page = store
        .maintenance
        .execute_ttl_sweep_page(command)
        .await
        .unwrap();
    let counts = census.snapshot();
    assert_eq!(
        page.deleted_items, 2,
        "the window must observe real expiration: {counts}"
    );
    assert!(!counts.touched_written.is_empty(), "{counts}");
    for label in counts.touched_read.iter().chain(&counts.touched_written) {
        assert!(
            !label.contains("regional"),
            "unpaired expiration touched {label}: {counts}"
        );
    }
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    assert!(
        census
            .recorded()
            .iter()
            .all(|statement| !(statement.contains("regional_markers")
                || statement.contains("regional_progress"))),
        "unpaired expiration ran a regional statement"
    );
    assert_eq!(read_item(&store, &table, "a").await, json!({}));
    store.finish().await;
}
