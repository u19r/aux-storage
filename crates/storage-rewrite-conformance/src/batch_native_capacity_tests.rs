//! Native byte capacity is settled at the public Batch/Transact boundary.

use std::{collections::BTreeMap, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use storage_types::{
    AttributeMap, AttributeValue, BatchWriteItemRequest, GetRecordsRequest, PutItemRequest,
    ReadVersionPolicy, ScanRequest, StorageEnum, TableName, TransactWriteItemsRequest, WireItem,
};

use super::{
    native_census_test_support::{CensusCounts, NativeCensus},
    test_support::TestStore,
};

// These batch fixtures await each writing group in order. SQLite and RocksDB
// durability barriers (only writing attempts sync) and Turso/Postgres commits
// of writing attempts distinguish writing groups from cold metadata ReadOnly
// attempts, whose commits RocksDB also counts. All preparation I/O stays in the
// complete census.
fn count_acknowledged_batch_groups(counts: &CensusCounts) -> u64 {
    match TestStore::selected_backend().as_str() {
        "sqlite" | "rocksdb" => counts.barriers,
        "turso" | "postgres" => counts.write_commits,
        _ => counts.commits,
    }
}

fn make_payload(ordinal: usize, generation: u64, bytes: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ ((ordinal as u64 + 1) << 32) ^ generation;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn make_image(ordinal: usize, generation: u64, payload: AttributeValue) -> AttributeMap {
    [
        ("pk".to_owned(), AttributeValue::S(format!("k{ordinal:02}"))),
        (
            "gsi_pk".to_owned(),
            AttributeValue::S(format!("generation-{generation}")),
        ),
        (
            "gsi_sk".to_owned(),
            AttributeValue::S(format!("k{ordinal:02}")),
        ),
        ("payload".to_owned(), payload),
    ]
    .into_iter()
    .collect()
}

fn make_binary_image(ordinal: usize, generation: u64, bytes: usize) -> AttributeMap {
    make_image(
        ordinal,
        generation,
        AttributeValue::B(STANDARD.encode(make_payload(ordinal, generation, bytes))),
    )
}

fn make_expanded_image(ordinal: usize, generation: u64) -> AttributeMap {
    // Printable random data preserves 390KB logical images while keeping the
    // old+new public JSON event below the independent 1MiB response budget.
    let payload = make_payload(ordinal, generation, 390_000)
        .into_iter()
        .map(|byte| char::from(32 + byte % 95))
        .collect::<String>();
    make_image(ordinal, generation, AttributeValue::S(payload))
}

async fn seed_images(
    store: &TestStore,
    table: &TableName,
    count: usize,
    bytes: usize,
    expanded: bool,
) -> anyhow::Result<Vec<AttributeMap>> {
    let mut create = super::gsi_workflow_tests::table_request(table.as_ref());
    if expanded {
        create.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
    } else {
        create.global_secondary_indexes = None;
        create
            .attribute_definitions
            .retain(|attribute| attribute.attribute_name == "pk");
    }
    store.operations.create_table(create).await?;
    let mut images = Vec::new();
    for ordinal in 0..count {
        let image = if expanded {
            make_expanded_image(ordinal, 0)
        } else {
            make_binary_image(ordinal, 0, bytes)
        };
        store
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&image)?,
            ))
            .await?;
        images.push(image);
    }
    Ok(images)
}

fn make_batch(table: &TableName, images: &[AttributeMap]) -> anyhow::Result<BatchWriteItemRequest> {
    let writes = images
        .iter()
        .map(|image| json!({"PutRequest": {"Item": image}}))
        .collect::<Vec<_>>();
    BatchWriteItemRequest::try_from(json!({"RequestItems": {table.as_ref(): writes}}))
        .map_err(anyhow::Error::msg)
}

async fn read_projection(
    store: &TestStore,
    table: &TableName,
    index: Option<&str>,
) -> anyhow::Result<BTreeMap<String, AttributeMap>> {
    let mut request: ScanRequest = serde_json::from_value(json!({
        "TableName": table, "IndexName": index, "Limit": 2
    }))?;
    let mut items = BTreeMap::new();
    loop {
        let page = store.operations.scan(request.clone()).await?;
        for item in page.items.unwrap_or_default() {
            let attributes = item.to_attribute_map()?;
            let Some(AttributeValue::S(key)) = attributes.get("pk") else {
                anyhow::bail!("missing primary key");
            };
            anyhow::ensure!(
                items.insert(key.clone(), attributes).is_none(),
                "duplicate scan member"
            );
        }
        request.exclusive_start_key = page
            .last_evaluated_key
            .map(storage_types::ExclusiveStartKey::Key);
        if request.exclusive_start_key.is_none() {
            return Ok(items);
        }
    }
}

async fn assert_images(
    store: &TestStore,
    table: &TableName,
    images: &[AttributeMap],
    expanded: bool,
) -> anyhow::Result<()> {
    let expected = images
        .iter()
        .enumerate()
        .map(|(ordinal, image)| (format!("k{ordinal:02}"), image.clone()))
        .collect::<BTreeMap<_, _>>();
    anyhow::ensure!(
        read_projection(store, table, None).await? == expected,
        "primary images differ"
    );
    if expanded {
        anyhow::ensure!(
            read_projection(store, table, Some("by_group")).await? == expected,
            "ALL projection images differ"
        );
    }
    Ok(())
}

async fn read_events(
    store: &TestStore,
    table: &TableName,
) -> anyhow::Result<Vec<storage_types::DynamoDbStreamsRecord>> {
    let arn = store
        .operations
        .describe_table(storage_types::DescribeTableRequest {
            table_name: table.clone(),
        })
        .await?
        .table
        .latest_stream_arn
        .ok_or_else(|| anyhow::anyhow!("missing stream ARN"))?;
    let mut events = Vec::new();
    for shard in super::dynamodb_stream_history_tests::describe_shards(store, &arn).await {
        let mut token = super::dynamodb_stream_history_tests::iterator(
            store,
            &arn,
            &shard.shard_id,
            "TRIM_HORIZON",
            None,
        )
        .await;
        loop {
            let page = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: token,
                    limit: Some(2),
                })
                .await?;
            let empty = page.records.is_empty();
            events.extend(page.records);
            if empty {
                break;
            }
            let Some(next) = page.next_shard_iterator else {
                break;
            };
            token = next;
        }
    }
    Ok(events)
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_expanded_wide_batch_when_native_capacity_is_reached_then_every_group_and_image_commits()
-> anyhow::Result<()> {
    verify_expanded_batch_capacity(ReadVersionPolicy::Fresh).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_expanded_wide_batch_when_native_capacity_is_reached_then_every_group_and_image_commits_with_cached_writes()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    verify_expanded_batch_capacity(ReadVersionPolicy::Cached).await
}

async fn verify_expanded_batch_capacity(policy: ReadVersionPolicy) -> anyhow::Result<()> {
    let census = Arc::new(NativeCensus::default());
    #[cfg(feature = "foundationdb")]
    let probe = Arc::new(BatchPrefixProbe {
        census: Arc::clone(&census),
        ..Default::default()
    });
    #[cfg(feature = "foundationdb")]
    let store = if TestStore::selected_backend() == "foundationdb" {
        new_prefix_store(&probe, policy).await
    } else {
        TestStore::new_with_census(Arc::clone(&census)).await
    };
    #[cfg(not(feature = "foundationdb"))]
    let store = {
        let _ = policy;
        TestStore::new_with_census(Arc::clone(&census)).await
    };
    let outcome = async {
        let table = TableName::new("batch_capacity_expanded");
        let old = seed_images(&store, &table, 25, 390_000, true).await?;
        let warm = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 1))
            .collect::<Vec<_>>();
        store
            .operations
            .batch_write_item(make_batch(&table, &warm)?)
            .await?;
        let new = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 2))
            .collect::<Vec<_>>();
        census.reset();
        #[cfg(feature = "foundationdb")]
        probe.reset_observations();
        let result = store
            .operations
            .batch_write_item(make_batch(&table, &new)?)
            .await?;
        let counts = census.snapshot();
        eprintln!(
            "expanded_batch_capacity provider={} {counts} durable={}",
            TestStore::selected_backend(),
            counts.durable_summary()
        );
        anyhow::ensure!(
            result
                .unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty)),
            "batch left unprocessed members"
        );
        if TestStore::selected_backend() == "foundationdb" {
            anyhow::ensure!(
                counts.commits >= 3,
                "expanded batch requires three acknowledged native groups, got {}",
                counts.commits
            );
        } else {
            anyhow::ensure!(
                count_acknowledged_batch_groups(&counts) == 1,
                "control provider must acknowledge one writing group"
            );
        }
        #[cfg(feature = "foundationdb")]
        if TestStore::selected_backend() == "foundationdb" {
            probe.assert_group_policies(policy)?;
            anyhow::ensure!(
                probe.byte_rejections() > 0,
                "expanded batch did not reach native code 2101"
            );
        }
        assert_images(&store, &table, &new, true).await?;
        let events = read_events(&store, &table).await?;
        anyhow::ensure!(
            events.len() == 75,
            "expected every seed/warm/measured event, got {}",
            events.len()
        );
        for ordinal in 0..25 {
            for (before, after) in [
                (None, &old[ordinal]),
                (Some(&old[ordinal]), &warm[ordinal]),
                (Some(&warm[ordinal]), &new[ordinal]),
            ] {
                anyhow::ensure!(
                    events
                        .iter()
                        .filter(|event| event.dynamodb.new_image.as_ref() == Some(after)
                            && event.dynamodb.old_image.as_ref() == before)
                        .count()
                        == 1,
                    "missing or repeated exact event for member {ordinal}"
                );
            }
        }
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_plain_wide_and_small_batches_when_written_then_each_uses_one_native_commit()
-> anyhow::Result<()> {
    let census = Arc::new(NativeCensus::default());
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        for (name, count, bytes) in [
            ("batch_capacity_plain", 25, 250_000),
            ("batch_capacity_small", 3, 32),
        ] {
            let table = TableName::new(name);
            seed_images(&store, &table, count, bytes, false).await?;
            let new = (0..count)
                .map(|ordinal| make_binary_image(ordinal, 1, bytes))
                .collect::<Vec<_>>();
            store
                .operations
                .batch_write_item(make_batch(&table, &new)?)
                .await?;
            let new = (0..count)
                .map(|ordinal| make_binary_image(ordinal, 2, bytes))
                .collect::<Vec<_>>();
            census.reset();
            let result = store
                .operations
                .batch_write_item(make_batch(&table, &new)?)
                .await?;
            let counts = census.snapshot();
            eprintln!(
                "plain_batch_capacity members={count} bytes={bytes} provider={} {counts} \
                 durable={}",
                TestStore::selected_backend(),
                counts.durable_summary()
            );
            anyhow::ensure!(
                count_acknowledged_batch_groups(&counts) == 1,
                "plain batch must acknowledge one writing group"
            );
            anyhow::ensure!(
                result
                    .unprocessed_items
                    .as_ref()
                    .is_none_or(|items| items.values().all(Vec::is_empty))
            );
            assert_images(&store, &table, &new, false).await?;
        }
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_expanded_native_oversize_transact_when_replayed_then_validation_has_no_durable_effects()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let census = Arc::new(NativeCensus::default());
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = async {
        let table = TableName::new("transact_capacity_expanded");
        let old = seed_images(&store, &table, 10, 390_000, true).await?;
        let before = read_events(&store, &table).await?;
        let writes = (0..10).map(|ordinal| json!({"Put": {"TableName": table, "Item": make_expanded_image(ordinal, 1)}})).collect::<Vec<_>>();
        let request: TransactWriteItemsRequest = serde_json::from_value(json!({"TransactItems": writes, "ClientRequestToken": "native-capacity-aborted-replay"}))?;
        for replay in 0..2 {
            census.reset();
            let error = store.operations.transact_write_items(request.clone()).await.err().ok_or_else(|| anyhow::anyhow!("oversized transaction unexpectedly committed"))?;
            let counts = census.snapshot();
            eprintln!("transact_capacity replay={replay} {counts} durable={}", counts.durable_summary());
            anyhow::ensure!(matches!(error.as_ref(), StorageEnum::Validation { message, .. } | StorageEnum::RawValidation { message } if message == "Transaction size exceeds the provider limit"), "expected provider capacity validation");
            anyhow::ensure!(counts.commits == 0 && counts.begins == 1, "known byte abort must settle once without commit");
            assert_images(&store, &table, &old, true).await?;
            let after = read_events(&store, &table).await?;
            anyhow::ensure!(serde_json::to_value(&after)? == serde_json::to_value(&before)?, "aborted transaction changed stream effects");
        }
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_duplicate_in_later_byte_group_when_batch_is_validated_then_no_prefix_commits()
-> anyhow::Result<()> {
    verify_duplicate_before_prefix(ReadVersionPolicy::Fresh).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_duplicate_in_later_byte_group_when_batch_is_validated_then_no_prefix_commits_with_cached_writes()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    verify_duplicate_before_prefix(ReadVersionPolicy::Cached).await
}

async fn verify_duplicate_before_prefix(policy: ReadVersionPolicy) -> anyhow::Result<()> {
    let census = Arc::new(NativeCensus::default());
    #[cfg(feature = "foundationdb")]
    let probe = Arc::new(BatchPrefixProbe {
        census: Arc::clone(&census),
        ..Default::default()
    });
    #[cfg(feature = "foundationdb")]
    let store = if TestStore::selected_backend() == "foundationdb" {
        new_prefix_store(&probe, policy).await
    } else {
        TestStore::new_with_census(Arc::clone(&census)).await
    };
    #[cfg(not(feature = "foundationdb"))]
    let store = {
        let _ = policy;
        TestStore::new_with_census(Arc::clone(&census)).await
    };
    let outcome = async {
        let table = TableName::new("batch_capacity_duplicate");
        let old = seed_images(&store, &table, 24, 390_000, true).await?;
        let before = read_events(&store, &table).await?;
        let mut new = (0..24).map(|ordinal| make_expanded_image(ordinal, 1)).collect::<Vec<_>>();
        new.push(new[0].clone());
        census.reset();
        #[cfg(feature = "foundationdb")]
        probe.reset_observations();
        let error = store.operations.batch_write_item(make_batch(&table, &new)?).await.err()
            .ok_or_else(|| anyhow::anyhow!("duplicate batch unexpectedly succeeded"))?;
        let counts = census.snapshot();
        eprintln!("duplicate_later_group provider={} {counts}", TestStore::selected_backend());
        anyhow::ensure!(matches!(error.as_ref(), StorageEnum::Validation { message, .. } | StorageEnum::RawValidation { message }
            if message == "Provided list of item keys contains duplicates"), "duplicate must retain its public validation failure: {error}");
        anyhow::ensure!(counts.rows_written == 0, "invalid inventory staged native mutations before complete validation");
        #[cfg(feature = "foundationdb")]
        if TestStore::selected_backend() == "foundationdb" {
            anyhow::ensure!(probe.acknowledged_mutation_groups() == 0, "duplicate was discovered after a writing prefix committed");
        }
        #[cfg(feature = "foundationdb")]
        if TestStore::selected_backend() == "foundationdb" { probe.assert_group_policies(policy)?; }
        #[cfg(feature = "foundationdb")]
        if TestStore::selected_backend() == "foundationdb" && policy == ReadVersionPolicy::Cached {
            anyhow::ensure!(probe.writer_policies().last() == Some(&ReadVersionPolicy::Fresh), "cached duplicate rejection must settle Fresh");
        }
        assert_images(&store, &table, &old, true).await?;
        anyhow::ensure!(serde_json::to_value(read_events(&store, &table).await?)? == serde_json::to_value(before)?, "invalid batch changed stream effects");
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy, Default)]
enum BatchPrefixFault {
    #[default]
    Conflict,
    Unknown,
    Capacity,
    Deadline,
    DeadlineUnknown,
}

#[cfg(feature = "foundationdb")]
#[derive(Default)]
struct BatchPrefixProbe {
    census: Arc<NativeCensus>,
    observations: std::sync::Mutex<Vec<PrefixObservation>>,
    independent: std::sync::atomic::AtomicBool,
    armed: std::sync::atomic::AtomicBool,
    acknowledgements: std::sync::atomic::AtomicUsize,
    fault: BatchPrefixFault,
    capacity_aborts: std::sync::atomic::AtomicUsize,
    held_group_rejections: std::sync::atomic::AtomicUsize,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy, Debug)]
enum PrefixEvent {
    Native(kv_backend::KvNativeEvent),
    Boundary(kv_backend::KvCommitBoundary),
}

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy, Debug)]
struct PrefixObservation {
    independent: bool,
    event: PrefixEvent,
}

#[cfg(feature = "foundationdb")]
async fn new_prefix_store(probe: &Arc<BatchPrefixProbe>, policy: ReadVersionPolicy) -> TestStore {
    if policy == ReadVersionPolicy::Cached {
        TestStore::new_with_cached_write_probes(
            #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
            probe.census.clone(),
            probe.clone(),
        )
        .await
    } else {
        TestStore::new_with_native_probes(
            #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
            probe.census.clone(),
            probe.clone(),
        )
        .await
    }
}

#[cfg(feature = "foundationdb")]
impl BatchPrefixProbe {
    fn reset_observations(&self) {
        self.observations.lock().unwrap().clear();
    }

    fn writer_policies(&self) -> Vec<ReadVersionPolicy> {
        self.observations
            .lock()
            .unwrap()
            .iter()
            .filter_map(|observation| match observation {
                PrefixObservation {
                    independent: false,
                    event:
                        PrefixEvent::Native(kv_backend::KvNativeEvent::AttemptRequested {
                            mode: storage_types::AttemptMode::ReadWriteItems,
                            read_version,
                        }),
                } => Some(*read_version),
                _ => None,
            })
            .collect()
    }

    fn acknowledged_mutation_groups(&self) -> usize {
        let mut has_mutations = false;
        let mut groups = 0;
        for observation in self
            .observations
            .lock()
            .unwrap()
            .iter()
            .filter(|observation| !observation.independent)
        {
            match observation.event {
                PrefixEvent::Native(kv_backend::KvNativeEvent::AttemptRequested { .. }) => {
                    has_mutations = false
                }
                PrefixEvent::Native(kv_backend::KvNativeEvent::Mutations {
                    puts,
                    deletes,
                    range_deletes,
                }) => {
                    has_mutations |= puts + deletes + range_deletes > 0;
                }
                PrefixEvent::Boundary(
                    kv_backend::KvCommitBoundary::AfterDurableAcknowledgement,
                ) if has_mutations => groups += 1,
                _ => {}
            }
        }
        groups
    }

    fn byte_rejections(&self) -> usize {
        self.observations
            .lock()
            .unwrap()
            .iter()
            .filter(|observation| {
                matches!(
                    observation,
                    PrefixObservation {
                        independent: false,
                        event: PrefixEvent::Native(kv_backend::KvNativeEvent::CommitFailed {
                            native_code: 2101,
                            ..
                        })
                    }
                )
            })
            .count()
    }

    fn assert_group_policies(&self, initial: ReadVersionPolicy) -> anyhow::Result<()> {
        let observations = self.observations.lock().unwrap();
        let mut expected = initial;
        let mut writers = 0;
        let mut has_mutations = false;
        for observation in observations
            .iter()
            .filter(|observation| !observation.independent)
        {
            match observation.event {
                PrefixEvent::Native(kv_backend::KvNativeEvent::AttemptRequested {
                    mode: storage_types::AttemptMode::ReadWriteItems,
                    read_version,
                }) => {
                    anyhow::ensure!(
                        read_version == expected,
                        "writer {writers} requested {read_version:?}, expected {expected:?}; \
                         observations={observations:?}"
                    );
                    writers += 1;
                    has_mutations = false;
                    // Only a retry of this group can follow without a commit or
                    // native size rejection.
                    expected = ReadVersionPolicy::Fresh;
                }
                PrefixEvent::Native(kv_backend::KvNativeEvent::Mutations {
                    puts,
                    deletes,
                    range_deletes,
                }) => {
                    has_mutations |= puts + deletes + range_deletes > 0;
                }
                PrefixEvent::Native(kv_backend::KvNativeEvent::CommitFailed {
                    native_code: 2101,
                    ..
                }) => expected = initial,
                PrefixEvent::Boundary(
                    kv_backend::KvCommitBoundary::AfterDurableAcknowledgement,
                ) => {
                    // A mutation-free Cached acknowledgement asks the same
                    // group to decide Fresh.
                    expected = if has_mutations {
                        initial
                    } else {
                        ReadVersionPolicy::Fresh
                    };
                }
                _ => {}
            }
        }
        anyhow::ensure!(writers > 0, "no requested batch writers observed");
        drop(observations);
        eprintln!(
            "batch_prefix_policy initial={initial:?} writer_policies={:?} byte_rejections={} \
             requested_only=true",
            self.writer_policies(),
            self.byte_rejections()
        );
        Ok(())
    }
}

#[cfg(feature = "foundationdb")]
impl kv_backend::KvFaultProbe for BatchPrefixProbe {
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        use std::sync::atomic::Ordering::SeqCst;
        if matches!(self.fault, BatchPrefixFault::Capacity)
            && self.acknowledgements.load(SeqCst) == 1
            && matches!(
                call.operation,
                kv_backend::KvFaultOperation::Stage
                    | kv_backend::KvFaultOperation::StageVersionstamped
            )
            && self.armed.swap(false, SeqCst)
        {
            self.capacity_aborts.fetch_add(1, SeqCst);
            return Some(storage_types::AttemptFailure::CAPACITY.into());
        }
        None
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        kv_backend::KvFaultProbe::observe(self.census.as_ref(), event);
        let independent = self.independent.load(std::sync::atomic::Ordering::SeqCst);
        self.observations.lock().unwrap().push(PrefixObservation {
            independent,
            event: PrefixEvent::Native(event),
        });
    }

    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        kv_backend::KvFaultProbe::observe_access(self.census.as_ref(), access);
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        use std::sync::atomic::Ordering::SeqCst;
        Box::pin(async move {
            self.observations.lock().unwrap().push(PrefixObservation {
                independent: self.independent.load(SeqCst),
                event: PrefixEvent::Boundary(boundary),
            });
            // Metadata preparation is outside the injected writing-group
            // schedule.
            let is_writer = self
                .observations
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|observation| match observation.event {
                    PrefixEvent::Native(kv_backend::KvNativeEvent::AttemptRequested {
                        mode,
                        ..
                    }) => Some(mode == storage_types::AttemptMode::ReadWriteItems),
                    _ => None,
                })
                .unwrap_or(false);
            if !is_writer || !self.armed.load(SeqCst) {
                return Ok(());
            }
            match boundary {
                kv_backend::KvCommitBoundary::AfterDurableAcknowledgement => {
                    let count = self.acknowledgements.fetch_add(1, SeqCst) + 1;
                    if (matches!(self.fault, BatchPrefixFault::Deadline) && count == 1)
                        || (matches!(self.fault, BatchPrefixFault::DeadlineUnknown) && count == 2)
                    {
                        tokio::time::pause();
                        tokio::time::advance(std::time::Duration::from_secs(121)).await;
                        tokio::time::resume();
                    }
                    if matches!(
                        self.fault,
                        BatchPrefixFault::Unknown | BatchPrefixFault::DeadlineUnknown
                    ) && count == 2
                    {
                        self.armed.store(false, SeqCst);
                        return Err(storage_types::StorageError::internal(
                            "suppress second group acknowledgement",
                        ));
                    }
                }
                kv_backend::KvCommitBoundary::BeforeCommit => {
                    if matches!(self.fault, BatchPrefixFault::Conflict)
                        && self.acknowledgements.load(SeqCst) == 1
                    {
                        // Disarm before the independent writer can enter the
                        // same seam.
                        self.armed.store(false, SeqCst);
                        self.reached.notify_one();
                        self.release.notified().await;
                        self.held_group_rejections.fetch_add(1, SeqCst);
                        // KvCommitBoundary maps this pre-submission rejection
                        // to known TransactionConflict.
                        return Err(storage_types::StorageError::internal(
                            "reject held middle group",
                        ));
                    }
                }
            }
            Ok(())
        })
    }
}

#[cfg(feature = "foundationdb")]
fn assert_exact_events(
    events: &[storage_types::DynamoDbStreamsRecord],
    pairs: &[(Option<&AttributeMap>, &AttributeMap)],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        events.len() == pairs.len(),
        "unexpected event count: {} versus {}",
        events.len(),
        pairs.len()
    );
    for (before, after) in pairs {
        anyhow::ensure!(
            events
                .iter()
                .filter(|event| event.dynamodb.new_image.as_ref() == Some(*after)
                    && event.dynamodb.old_image.as_ref() == *before)
                .count()
                == 1,
            "missing or repeated exact old/new image pair"
        );
    }
    Ok(())
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_middle_group_conflicts_then_fresh_retry_preserves_prefix_and_external_image()
-> anyhow::Result<()> {
    verify_conflict_after_prefix(ReadVersionPolicy::Fresh).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_middle_group_conflicts_then_fresh_retry_preserves_prefix_and_external_image_with_cached_writes()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    verify_conflict_after_prefix(ReadVersionPolicy::Cached).await
}

#[cfg(feature = "foundationdb")]
async fn verify_conflict_after_prefix(policy: ReadVersionPolicy) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let probe = Arc::new(BatchPrefixProbe::default());
    let store = new_prefix_store(&probe, policy).await;
    let outcome = async {
        let table = TableName::new("batch_prefix_conflict");
        let old = seed_images(&store, &table, 25, 390_000, true).await?;
        let warm = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 1))
            .collect::<Vec<_>>();
        store
            .operations
            .batch_write_item(make_batch(&table, &warm)?)
            .await?;
        let new = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 2))
            .collect::<Vec<_>>();
        // The fixture commits four members per group: k05 belongs to the held
        // second group.
        let external = make_expanded_image(5, 3);
        let request = make_batch(&table, &new)?;
        probe.census.reset();
        probe.reset_observations();
        probe.armed.store(true, SeqCst);
        let (batch, writer) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(store.operations.batch_write_item(request), async {
                probe.reached.notified().await;
                probe.independent.store(true, SeqCst);
                let result = store
                    .independent_operations
                    .as_ref()
                    .unwrap()
                    .put_item(PutItemRequest::new(
                        table.clone(),
                        WireItem::try_from(&external)?,
                    ))
                    .await;
                probe.independent.store(false, SeqCst);
                probe.release.notify_one();
                result.map(|_| ()).map_err(anyhow::Error::from)
            })
        })
        .await?;
        writer?;
        anyhow::ensure!(
            batch?
                .unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty))
        );
        probe.assert_group_policies(policy)?;
        let counts = probe.census.snapshot();
        eprintln!(
            "batch_prefix_conflict {counts} staged={}",
            counts.durable_summary()
        );
        anyhow::ensure!(
            probe.acknowledgements.load(SeqCst) == 1,
            "fault did not hold immediately after first acknowledged group"
        );
        anyhow::ensure!(
            probe.byte_rejections() > 0,
            "fixture did not reach native transaction byte rejection"
        );
        anyhow::ensure!(
            probe.held_group_rejections.load(SeqCst) == 1,
            "expected one known pre-submission group conflict"
        );
        assert_images(&store, &table, &new, true).await?;
        let mut pairs = Vec::new();
        for ordinal in 0..25 {
            pairs.push((None, &old[ordinal]));
            pairs.push((Some(&old[ordinal]), &warm[ordinal]));
            pairs.push((
                Some(if ordinal == 5 {
                    &external
                } else {
                    &warm[ordinal]
                }),
                &new[ordinal],
            ));
        }
        pairs.push((Some(&warm[5]), &external));
        assert_exact_events(&read_events(&store, &table).await?, &pairs)?;
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_second_group_ack_is_unknown_then_no_replay_or_later_group_commits()
-> anyhow::Result<()> {
    verify_unknown_after_prefix(ReadVersionPolicy::Fresh, BatchPrefixFault::Unknown).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_second_group_ack_is_unknown_then_no_replay_or_later_group_commits_with_cached_writes()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    verify_unknown_after_prefix(ReadVersionPolicy::Cached, BatchPrefixFault::Unknown).await
}

#[cfg(feature = "foundationdb")]
async fn verify_unknown_after_prefix(
    policy: ReadVersionPolicy,
    fault: BatchPrefixFault,
) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let probe = Arc::new(BatchPrefixProbe {
        fault,
        ..Default::default()
    });
    let store = new_prefix_store(&probe, policy).await;
    let outcome = async {
        let table = TableName::new("batch_prefix_unknown");
        let old = seed_images(&store, &table, 25, 390_000, true).await?;
        let warm = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 1))
            .collect::<Vec<_>>();
        store
            .operations
            .batch_write_item(make_batch(&table, &warm)?)
            .await?;
        let new = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 2))
            .collect::<Vec<_>>();
        probe.census.reset();
        probe.reset_observations();
        probe.armed.store(true, SeqCst);
        let error = store
            .operations
            .batch_write_item(make_batch(&table, &new)?)
            .await
            .err()
            .ok_or_else(|| {
                anyhow::anyhow!("unknown group unexpectedly returned a batch response")
            })?;
        anyhow::ensure!(
            error.is_commit_unknown(),
            "original unknown outcome was lost: {error}"
        );
        probe.assert_group_policies(policy)?;
        let counts = probe.census.snapshot();
        eprintln!(
            "batch_prefix_unknown {counts} staged={}",
            counts.durable_summary()
        );
        anyhow::ensure!(
            probe.acknowledgements.load(SeqCst) == 2 && counts.commits == 2,
            "unknown group replayed or later group committed"
        );
        let actual = read_projection(&store, &table, None).await?;
        let committed = (0..25)
            .take_while(|ordinal| actual.get(&format!("k{ordinal:02}")) == Some(&new[*ordinal]))
            .count();
        anyhow::ensure!(
            committed > 0 && committed < 25,
            "expected a proper committed prefix"
        );
        let expected = (0..25)
            .map(|ordinal| {
                if ordinal < committed {
                    new[ordinal].clone()
                } else {
                    warm[ordinal].clone()
                }
            })
            .collect::<Vec<_>>();
        assert_images(&store, &table, &expected, true).await?;
        let mut pairs = Vec::new();
        for ordinal in 0..25 {
            pairs.push((None, &old[ordinal]));
            pairs.push((Some(&old[ordinal]), &warm[ordinal]));
            if ordinal < committed {
                pairs.push((Some(&warm[ordinal]), &new[ordinal]));
            }
        }
        assert_exact_events(&read_events(&store, &table).await?, &pairs)?;
        eprintln!(
            "batch_prefix_unknown committed_members={committed} exact_events={}",
            pairs.len()
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_next_group_capacity_aborts_then_exact_original_tail_is_unprocessed()
-> anyhow::Result<()> {
    verify_capacity_after_prefix(ReadVersionPolicy::Fresh, BatchPrefixFault::Capacity).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_batch_prefix_when_next_group_capacity_aborts_then_exact_original_tail_is_unprocessed_with_cached_writes()
-> anyhow::Result<()> {
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    verify_capacity_after_prefix(ReadVersionPolicy::Cached, BatchPrefixFault::Capacity).await
}

#[cfg(feature = "foundationdb")]
async fn verify_capacity_after_prefix(
    policy: ReadVersionPolicy,
    fault: BatchPrefixFault,
) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let probe = Arc::new(BatchPrefixProbe {
        fault,
        ..Default::default()
    });
    let store = new_prefix_store(&probe, policy).await;
    let outcome = async {
        let table = TableName::new("batch_prefix_capacity_abort");
        let old = seed_images(&store, &table, 25, 390_000, true).await?;
        let warm = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 1))
            .collect::<Vec<_>>();
        store
            .operations
            .batch_write_item(make_batch(&table, &warm)?)
            .await?;
        let new = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 2))
            .collect::<Vec<_>>();
        let request = make_batch(&table, &new)?;
        let original = request.request_items.get(&table).unwrap().clone();
        probe.census.reset();
        probe.reset_observations();
        probe.armed.store(true, SeqCst);
        let result = store.operations.batch_write_item(request).await?;
        probe.assert_group_policies(policy)?;
        let counts = probe.census.snapshot();
        anyhow::ensure!(
            counts.commits == 1 && probe.acknowledgements.load(SeqCst) == 1,
            "known capacity abort retried or committed a later group"
        );
        anyhow::ensure!(
            probe.capacity_aborts.load(SeqCst)
                == usize::from(matches!(fault, BatchPrefixFault::Capacity)),
            "unexpected capacity abort count"
        );
        let unprocessed = result
            .unprocessed_items
            .ok_or_else(|| anyhow::anyhow!("missing unprocessed tail"))?;
        anyhow::ensure!(
            unprocessed.len() == 1,
            "unexpected unprocessed table inventory"
        );
        let tail = unprocessed
            .get(&table)
            .ok_or_else(|| anyhow::anyhow!("missing original table"))?;
        anyhow::ensure!(
            !tail.is_empty() && tail.len() < original.len(),
            "expected a proper committed prefix and unprocessed suffix"
        );
        let committed = original.len() - tail.len();
        anyhow::ensure!(
            serde_json::to_value(tail)? == serde_json::to_value(&original[committed..])?,
            "unprocessed members differ from exact original contiguous suffix"
        );
        let expected = (0..25)
            .map(|ordinal| {
                if ordinal < committed {
                    new[ordinal].clone()
                } else {
                    warm[ordinal].clone()
                }
            })
            .collect::<Vec<_>>();
        assert_images(&store, &table, &expected, true).await?;
        let mut pairs = Vec::new();
        for ordinal in 0..25 {
            pairs.push((None, &old[ordinal]));
            pairs.push((Some(&old[ordinal]), &warm[ordinal]));
            if ordinal < committed {
                pairs.push((Some(&warm[ordinal]), &new[ordinal]));
            }
        }
        assert_exact_events(&read_events(&store, &table).await?, &pairs)?;
        eprintln!(
            "batch_prefix_capacity_abort committed_members={committed} unprocessed_members={} \
             exact_events={} known_capacity_aborts=1 {counts} staged={}",
            tail.len(),
            pairs.len(),
            counts.durable_summary()
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_cold_batch_metadata_when_table_recreated_after_prefix_then_fresh_retry_cannot_retarget()
-> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let probe = Arc::new(BatchPrefixProbe::default());
    let store = new_prefix_store(&probe, ReadVersionPolicy::Cached).await;
    let outcome = async {
        let table = TableName::new("batch_prefix_cold_recreation");
        let independent = store.independent_operations.as_ref().unwrap();
        let mut create = super::gsi_workflow_tests::table_request(table.as_ref());
        create.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        // Leave the request cache cold: only this independent cache owner
        // creates and seeds.
        independent.create_table(create.clone()).await?;
        for ordinal in 0..25 {
            independent
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&make_expanded_image(ordinal, 0))?,
                ))
                .await?;
        }
        let new = (0..25)
            .map(|ordinal| make_expanded_image(ordinal, 1))
            .collect::<Vec<_>>();
        let request = make_batch(&table, &new)?;
        let sentinel: AttributeMap = [("pk".to_owned(), AttributeValue::S("sentinel".to_owned()))]
            .into_iter()
            .collect();
        probe.census.reset();
        probe.reset_observations();
        probe.armed.store(true, SeqCst);
        let (batch, recreation) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(store.operations.batch_write_item(request), async {
                probe.reached.notified().await;
                let observations = probe.observations.lock().unwrap().clone();
                let attempts = observations
                    .iter()
                    .filter_map(|observation| match observation.event {
                        PrefixEvent::Native(kv_backend::KvNativeEvent::AttemptRequested {
                            mode,
                            read_version,
                        }) => Some((mode, read_version)),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    attempts.first()
                        == Some(&(
                            storage_types::AttemptMode::ReadOnly,
                            ReadVersionPolicy::Fresh
                        )),
                    "cold request did not pin through genuine Fresh preparation: {attempts:?}"
                );
                anyhow::ensure!(
                    attempts.get(1)
                        == Some(&(
                            storage_types::AttemptMode::ReadWriteItems,
                            ReadVersionPolicy::Cached
                        )),
                    "cold preparation did not precede Cached writing: {attempts:?}"
                );
                // The hook already disarmed itself; neither the mutex nor the
                // batch writer runs during recreation.
                probe.independent.store(true, SeqCst);
                let result = async {
                    independent
                        .delete_table(storage_types::DeleteTableRequest {
                            table_name: table.clone(),
                        })
                        .await?;
                    independent.create_table(create).await?;
                    independent
                        .put_item(PutItemRequest::new(
                            table.clone(),
                            WireItem::try_from(&sentinel)?,
                        ))
                        .await?;
                    anyhow::Ok(())
                }
                .await;
                probe.independent.store(false, SeqCst);
                probe.release.notify_one();
                result
            })
        })
        .await?;
        recreation?;
        let error = batch
            .err()
            .ok_or_else(|| anyhow::anyhow!("recreated table accepted the remaining batch"))?;
        anyhow::ensure!(
            matches!(error.as_ref(), StorageEnum::TableNotFound { .. }),
            "request retargeted or lost table-identity rejection: {error}"
        );
        probe.assert_group_policies(ReadVersionPolicy::Cached)?;
        anyhow::ensure!(
            probe.acknowledgements.load(SeqCst) == 1,
            "did not hold the group after exactly one acknowledged prefix"
        );
        anyhow::ensure!(
            probe.held_group_rejections.load(SeqCst) == 1,
            "held group was not rejected before native submission"
        );
        anyhow::ensure!(
            probe.writer_policies().last() == Some(&ReadVersionPolicy::Fresh),
            "held group did not retry Fresh"
        );
        let observations = probe.observations.lock().unwrap().clone();
        let acknowledged = observations
            .iter()
            .filter(|observation| {
                matches!(
                    observation,
                    PrefixObservation {
                        independent: false,
                        event: PrefixEvent::Boundary(
                            kv_backend::KvCommitBoundary::AfterDurableAcknowledgement
                        )
                    }
                )
            })
            .count();
        // The prefix commits, then the Fresh table-identity rejection
        // acknowledges without mutations.
        anyhow::ensure!(
            acknowledged == 2,
            "unexpected original-request settlements after recreation: {acknowledged}"
        );
        anyhow::ensure!(
            probe.acknowledged_mutation_groups() == 1,
            "a later writing group acknowledged after recreation"
        );
        let expected = [("sentinel".to_owned(), sentinel)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            read_projection(&store, &table, None).await? == expected,
            "replacement contains batch keys or lost the sentinel"
        );
        anyhow::ensure!(
            read_projection(&store, &table, Some("by_group"))
                .await?
                .is_empty(),
            "replacement index contains old batch members"
        );
        eprintln!(
            "cold_batch_prefix_recreation fresh_preparation=true acknowledged_prefix_groups=1 \
             replacement_members=1 {counts}",
            counts = probe.census.snapshot()
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_batch_committed_prefix_when_original_deadline_expires_then_exact_remaining_tail_is_unprocessed()
-> anyhow::Result<()> {
    verify_capacity_after_prefix(ReadVersionPolicy::Fresh, BatchPrefixFault::Deadline).await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_batch_committed_prefix_when_current_group_ack_is_unknown_after_expiry_then_unknown_wins_without_replay()
-> anyhow::Result<()> {
    verify_unknown_after_prefix(ReadVersionPolicy::Fresh, BatchPrefixFault::DeadlineUnknown).await
}
