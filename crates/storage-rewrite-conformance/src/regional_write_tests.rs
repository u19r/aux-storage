//! Paired local writes (REG-2/3/4): pair binding compare-and-set, gates, orders
//! on the stream, and the native cost of the delete marker.

use std::{sync::Arc, time::Duration};

use serde_json::json;
use storage_types::{
    DeleteItemRequest, DeleteTableRequest, GetRecordsRequest, PairId, PairState, PutItemRequest,
    RegionId, RegionalBinding, RegionalRole, StorageEnum, StorageError, TableName, TimestampMillis,
    UpdateTableRequest,
};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, find_populated_shard, iterator},
    test_support::TestStore,
};

pub(super) fn region(n: u16) -> RegionId {
    RegionId(std::num::NonZeroU16::new(n).unwrap())
}

pub(super) fn binding(state: PairState) -> RegionalBinding {
    RegionalBinding {
        pair: PairId::generate(),
        local: region(1),
        peer: region(2),
        peer_name: "peer".to_owned(),
        state,
    }
}

pub(super) async fn pair(
    store: &TestStore,
    table: &TableName,
    state: PairState,
) -> RegionalBinding {
    let binding = binding(state);
    let replaced = store
        .regional_bindings
        .replace_regional_binding(table, None, Some(binding.clone()))
        .await
        .unwrap();
    assert!(replaced);
    binding
}

pub(super) async fn put(
    store: &TestStore,
    table: &TableName,
    key: &str,
) -> Result<(), StorageError> {
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": {"pk": {"S": key}, "value": {"S": "v"}}
            }))
            .unwrap(),
        )
        .await
        .map(|_| ())
}

pub(super) async fn delete(
    store: &TestStore,
    table: &TableName,
    key: &str,
) -> Result<(), StorageError> {
    store
        .operations
        .delete_item(
            DeleteItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": key}}
            }))
            .unwrap(),
        )
        .await
        .map(|_| ())
}

async fn describe(store: &TestStore, table: &TableName) {
    store
        .operations
        .describe_table(storage_types::DescribeTableRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap();
}

pub(super) async fn delete_table(store: &TestStore, table: &TableName) -> Result<(), StorageError> {
    store
        .operations
        .delete_table(DeleteTableRequest {
            table_name: table.clone(),
        })
        .await
        .map(|_| ())
}

fn is_raw_validation(error: &StorageError) -> bool {
    matches!(error.base_error(), Some(StorageEnum::RawValidation { .. }))
}

#[tokio::test]
async fn given_table_when_binding_replaced_then_compare_and_set_on_pair() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let first = binding(PairState::Active);
    let second = binding(PairState::Active);
    let replace = |expected, next| {
        let store = &store;
        let table = &table;
        async move {
            store
                .regional_bindings
                .replace_regional_binding(table, expected, next)
                .await
                .unwrap()
        }
    };
    assert!(replace(None, Some(first.clone())).await);
    describe(&store, &table).await;
    assert!(!replace(None, Some(second)).await);
    describe(&store, &table).await;
    assert!(!replace(Some(PairId::generate()), None).await);
    describe(&store, &table).await;
    assert!(replace(Some(first.pair), None).await);
    describe(&store, &table).await;
}

#[tokio::test]
async fn given_paired_table_when_ttl_enabled_then_binding_kept() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    store
        .operations
        .update_time_to_live(storage_types::UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: storage_types::TimeToLiveSpecification {
                attribute_name: "ttl".into(),
                enabled: true,
            },
        })
        .await
        .unwrap();
    assert!(
        store
            .regional_bindings
            .replace_regional_binding(&table, Some(binding.pair), None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn given_paired_table_when_delete_absent_twice_put_delete_at_one_clock_then_orders_increase()
{
    let store = TestStore::new().await;
    let start = TimestampMillis::from_timestamp(1_700_000_000_000);
    store.set_time(start);
    let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    pair(&store, &table, PairState::Active).await;
    store.set_time(start);
    // The second delete runs over the existing marker (KV overwrite, SQL upsert
    // conflict branch).
    delete(&store, &table, "k").await.unwrap();
    delete(&store, &table, "k").await.unwrap();
    put(&store, &table, "k").await.unwrap();
    delete(&store, &table, "k").await.unwrap();
    store.wait_until_changes_are_readable().await;

    let reads = super::image_retention_tests::canonical_history_reads(&store, &table)
        .await
        .unwrap();
    let pages = (store.canonical_history_read)(reads).await.unwrap();
    let entries = pages
        .into_iter()
        .flat_map(|page| page.entries)
        .map(|row| row.entry)
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 4);
    let base = 1_700_000_000_000_u64 << 10;
    for (index, entry) in entries.iter().enumerate() {
        let mark = entry.regional().expect("paired entries carry a mark");
        assert_eq!(mark.order.stamp(), base + index as u64, "entry {index}");
        assert_eq!(mark.order.region(), region(1));
        assert_eq!(mark.role, RegionalRole::Origin);
        assert_eq!(mark.hidden, index < 2, "entry {index}");
        assert_eq!(entry.is_public(), index >= 2);
    }

    let shard = find_populated_shard(&store, &arn).await;
    let start_iterator = iterator(&store, &arn, &shard, "TRIM_HORIZON", None).await;
    let page = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let page = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: start_iterator.clone(),
                    limit: Some(10),
                })
                .await
                .unwrap();
            if page.records.len() >= 2 {
                return page;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("public records did not become published");
    let events = page
        .records
        .iter()
        .map(|record| record.event_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(events, ["INSERT", "REMOVE"]);

    // The system stream skips the hidden entries too: only the put and the
    // REMOVE of `k`.
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let mut checkpoint = None;
    let mut records = Vec::new();
    for _ in 0..200 {
        let page = store
            .stream_operations
            .get_system_stream_page(
                stream_provider::SystemStreamPageRequest::try_new(scope.clone(), checkpoint, 100)
                    .unwrap(),
            )
            .await
            .unwrap();
        records.extend(
            page.records()
                .iter()
                .filter(|record| record.source_table_name.as_ref() == Some(&table))
                .cloned(),
        );
        if page.terminal() {
            break;
        }
        checkpoint = Some(page.issued_checkpoint().token().clone());
    }
    assert_eq!(records.len(), 2);
    assert!(records[0].item.is_some());
    assert!(records[1].item.is_none());
}

#[tokio::test]
async fn given_paired_key_when_delete_put_delete_then_marker_stored_removed_stored() {
    let store = TestStore::new().await;
    let start = TimestampMillis::from_timestamp(1_700_000_000_000);
    store.set_time(start);
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (unpaired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    pair(&store, &paired, PairState::Active).await;
    store.set_time(start);
    let markers = |table: &TableName| {
        (store.regional_markers)(table.clone(), None, std::num::NonZeroU32::new(10).unwrap())
    };

    // Absent key: the shared P2 branch stores the marker.
    delete(&store, &paired, "k").await.unwrap();
    let first = markers(&paired).await.unwrap().0.markers;
    assert_eq!(first.len(), 1);
    let m1 = first[0].1;
    assert!(!m1.received);
    assert_eq!(m1.order.region(), region(1));
    assert_eq!(m1.order.stamp(), 1_700_000_000_000_u64 << 10);

    // Upsert clears the marker.
    put(&store, &paired, "k").await.unwrap();
    assert!(markers(&paired).await.unwrap().0.markers.is_empty());

    // Live delete stores a newer marker after the primary delete.
    delete(&store, &paired, "k").await.unwrap();
    let third = markers(&paired).await.unwrap().0.markers;
    assert_eq!(third.len(), 1);
    assert!(third[0].1.order > m1.order);
    assert!(!third[0].1.received);

    // Unpaired: no marker on any path.
    put(&store, &unpaired, "k").await.unwrap();
    delete(&store, &unpaired, "k").await.unwrap();
    delete(&store, &unpaired, "k").await.unwrap();
    assert!(markers(&unpaired).await.unwrap().0.markers.is_empty());
    store.finish().await;
}

#[tokio::test]
async fn given_paired_item_when_expired_then_expiry_child_marker_and_marked_remove() {
    use std::num::{NonZeroU64, NonZeroUsize};

    use scheduled_bg_jobs::{JobId, Namespace};
    use storage_provider::maintenance::ExpirationRequest;

    let store = TestStore::new().await;
    let start = TimestampMillis::from_timestamp(1_700_000_000_000);
    store.set_time(start);
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    pair(&store, &table, PairState::Active).await;
    store
        .operations
        .update_time_to_live(storage_types::UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: storage_types::TimeToLiveSpecification {
                attribute_name: "ttl".into(),
                enabled: true,
            },
        })
        .await
        .unwrap();
    store.set_time(start);
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": {"pk": {"S": "k"}, "ttl": {"N": "1"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let request = ExpirationRequest {
        destination: Namespace::try_new("conformance:regional-expiry").expect("namespace"),
        job: JobId::from([5; 16]),
        fence: NonZeroU64::new(1).expect("fence"),
        requested_cutoff: TimestampMillis::from_timestamp(2_000_000_000_000),
        candidate_limit: NonZeroUsize::new(100).unwrap(),
    };
    store.maintenance.advance_expiration(request).await.unwrap();
    store.wait_until_changes_are_readable().await;

    let base = 1_700_000_000_000_u64 << 10;
    let entries = super::regional_receive_tests::history(&store, &table).await;
    let last = entries.last().expect("history");
    assert!(last.entry.is_public());
    assert_eq!(last.entry.change(), stream_provider::StreamChange::Remove);
    let mark = last.entry.regional().unwrap();
    assert_eq!(
        (
            mark.order.stamp(),
            mark.order.region(),
            mark.order.is_expiry()
        ),
        (base, region(1), true)
    );
    assert_eq!(mark.role, RegionalRole::Origin);

    // A later put at the same clock observed the expiry marker: stamp + 1.
    store.set_time(start);
    put(&store, &table, "k").await.unwrap();
    let entries = super::regional_receive_tests::history(&store, &table).await;
    let newest = entries.last().unwrap().entry.regional().unwrap();
    assert_eq!(
        (newest.order.stamp(), newest.order.is_expiry()),
        (base + 1, false)
    );
}

#[tokio::test]
async fn given_paired_table_when_stream_disabled_then_validation_and_delete_table_rejected() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    let disable: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": table.as_ref(),
        "StreamSpecification": {"StreamEnabled": false}
    }))
    .unwrap();
    let error = store.operations.update_table(disable).await.unwrap_err();
    assert!(is_raw_validation(&error), "{error:?}");
    let error = delete_table(&store, &table).await.unwrap_err();
    assert!(is_raw_validation(&error), "{error:?}");
    assert!(
        store
            .regional_bindings
            .replace_regional_binding(&table, Some(binding.pair), None)
            .await
            .unwrap()
    );
    delete_table(&store, &table).await.unwrap();
}

#[tokio::test]
async fn given_joining_or_leaving_table_when_put_then_rejected() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let joining = pair(&store, &table, PairState::Joining).await;
    let error = put(&store, &table, "k").await.unwrap_err();
    assert!(
        matches!(
            error.base_error(),
            Some(StorageEnum::ResourceNotFound { .. })
        ),
        "{error:?}"
    );
    let leaving = RegionalBinding {
        state: PairState::Leaving,
        ..joining.clone()
    };
    assert!(
        store
            .regional_bindings
            .replace_regional_binding(&table, Some(joining.pair), Some(leaving))
            .await
            .unwrap()
    );
    let error = put(&store, &table, "k").await.unwrap_err();
    assert!(is_raw_validation(&error), "{error:?}");
}

/// Counts of the four measured steps of one table: put new, delete live, delete
/// absent, put over marker.
#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "postgres",
    feature = "rocksdb",
    feature = "foundationdb"
))]
async fn measure_marker_steps(
    store: &TestStore,
    census: &Arc<super::native_census_test_support::NativeCensus>,
    table: &TableName,
) -> [super::native_census_test_support::CensusCounts; 4] {
    // Warm statement and binding caches with every shape that is measured.
    put(store, table, "w").await.unwrap();
    delete(store, table, "w").await.unwrap();
    delete(store, table, "w2").await.unwrap();
    put(store, table, "w2").await.unwrap();
    let mut counts = Vec::new();
    census.reset();
    put(store, table, "a").await.unwrap();
    counts.push(census.snapshot());
    census.reset();
    delete(store, table, "a").await.unwrap();
    counts.push(census.snapshot());
    census.reset();
    delete(store, table, "b").await.unwrap();
    counts.push(census.snapshot());
    census.reset();
    put(store, table, "b").await.unwrap();
    counts.push(census.snapshot());
    counts.try_into().unwrap_or_else(|_| unreachable!())
}

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "postgres",
    feature = "rocksdb",
    feature = "foundationdb"
))]
async fn measure_unpaired_and_paired() -> [[super::native_census_test_support::CensusCounts; 4]; 2]
{
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (unpaired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    pair(&store, &paired, PairState::Active).await;
    let u = measure_marker_steps(&store, &census, &unpaired).await;
    let p = measure_marker_steps(&store, &census, &paired).await;
    for (step, (u, p)) in ["put new", "delete live", "delete absent", "put over marker"]
        .iter()
        .zip(u.iter().zip(&p))
    {
        println!("REG_MARKER_COST {step}: unpaired {u} | paired {p}");
    }
    [u, p]
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[tokio::test]
async fn given_paired_and_unpaired_writes_when_measured_then_marker_cost_only() {
    let [u, p] = measure_unpaired_and_paired().await;
    let marked = |counts: &super::native_census_test_support::CensusCounts| {
        counts
            .touched_written
            .iter()
            .any(|label| label.contains("regional_marker"))
    };
    // a) put new key: the marker key joins the same read wave.
    assert_eq!(p[0].waves, u[0].waves);
    assert_eq!(p[0].keys, u[0].keys + 1);
    assert_eq!(p[0].rows_written, u[0].rows_written);
    // b) delete live: one marker put.
    assert_eq!(p[1].waves, u[1].waves);
    assert_eq!(p[1].rows_written, u[1].rows_written + 1);
    assert!(marked(&p[1]));
    assert!(!marked(&u[1]));
    // c) delete absent: unpaired writes nothing; paired writes the marker and a
    // hidden entry.
    assert_eq!(u[2].rows_written, 0);
    assert!(p[2].rows_written >= 2);
    assert!(marked(&p[2]));
    // d) put over the marker: one marker delete.
    assert_eq!(p[3].rows_written, u[3].rows_written + 1);
    // Off costs nothing: no unpaired step reads or writes a marker.
    for (step, unpaired) in u.iter().take(4).enumerate() {
        assert!(
            !unpaired
                .touched_read
                .iter()
                .any(|label| label.contains("regional_marker")),
            "step {step}"
        );
        assert!(
            !unpaired
                .touched_written
                .iter()
                .any(|label| label.contains("regional_marker")),
            "step {step}"
        );
    }
    // The marker key is read in the item call, under conflict protection.
    if TestStore::selected_backend() == "foundationdb" {
        for step in [0, 1, 3] {
            assert_eq!(
                p[step].conflicts.read_keys,
                u[step].conflicts.read_keys + 1,
                "step {step}"
            );
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_paired_and_unpaired_sql_writes_when_measured_then_marker_cost_only() {
    let [u, p] = measure_unpaired_and_paired().await;
    let marked = |counts: &super::native_census_test_support::CensusCounts| {
        counts
            .touched_written
            .iter()
            .any(|label| label.contains("regional_markers"))
    };
    // Turso sends each statement as its own wave, so a marker statement there
    // is a wave.
    let own_wave = u64::from(TestStore::selected_backend() == "turso");
    // a) put new key: one marker SELECT in the same window.
    assert_eq!(p[0].waves, u[0].waves + own_wave);
    assert_eq!(p[0].statements, u[0].statements + 1);
    assert_eq!(p[0].rows_written, u[0].rows_written);
    // b) delete live: marker SELECT and marker UPSERT.
    assert_eq!(p[1].waves, u[1].waves + 2 * own_wave);
    assert_eq!(p[1].statements, u[1].statements + 2);
    assert_eq!(p[1].rows_written, u[1].rows_written + 1);
    assert!(marked(&p[1]));
    assert!(!marked(&u[1]));
    // c) delete absent.
    assert_eq!(u[2].rows_written, 0);
    assert!(marked(&p[2]));
    // d) put over the marker: marker SELECT and marker DELETE.
    assert_eq!(p[3].rows_written, u[3].rows_written + 1);
    // d) the marker DELETE is one statement after the marker SELECT, no more.
    assert_eq!(p[3].statements, u[3].statements + 2);
    // Off costs nothing: no unpaired step reads or writes a marker.
    for (step, unpaired) in u.iter().take(4).enumerate() {
        assert!(
            !unpaired
                .touched_read
                .iter()
                .any(|label| label.contains("regional_markers")),
            "step {step}"
        );
        assert!(
            !unpaired
                .touched_written
                .iter()
                .any(|label| label.contains("regional_markers")),
            "step {step}"
        );
    }
    if TestStore::selected_backend() == "postgres" {
        for step in 0..4 {
            assert_eq!(p[step].locks, u[step].locks, "step {step}");
        }
    }
}
