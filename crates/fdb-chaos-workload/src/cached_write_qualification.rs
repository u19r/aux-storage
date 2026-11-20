//! Requested writer policies and a separate no-mutation authority control.
use std::sync::Mutex;

use kv_backend::{KvFaultCall, KvFaultProbe, KvNativeEvent};
use storage_types::{AttemptMode, ReadVersionPolicy, StorageError, StorageResult};

use crate::{imports::*, table_atomicity::TableAtomicityWorkload};

type Store = kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>;

#[derive(Default)]
pub(crate) struct WriteProbe {
    // Fixed event cap bounds diagnostic memory; totals continue after the cap.
    state: Mutex<WriteCounts>,
}

#[derive(Default)]
struct WriteCounts {
    cached_writers: u64,
    fresh_item_writers: u64,
    fresh_other_attempts: u64,
    acknowledgements: u64,
    native_failures: u64,
    events: Vec<serde_json::Value>,
}

impl WriteProbe {
    pub(crate) fn report(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        serde_json::json!({"requested_cached_writers": state.cached_writers,
            "fresh_item_writers": state.fresh_item_writers,
            "fresh_preparation_jobs_and_reads": state.fresh_other_attempts,
            "acknowledgements": state.acknowledgements, "native_failures": state.native_failures,
            "events": state.events, "event_limit": 4096,
            "actual_cache_hit_observed": false})
    }
}

impl KvFaultProbe for WriteProbe {
    fn fail(&self, _: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: KvNativeEvent) {
        let mut state = self.state.lock().unwrap();
        let record = match event {
            KvNativeEvent::AttemptRequested { mode, read_version } => {
                match (mode, read_version) {
                    (AttemptMode::ReadWriteItems, ReadVersionPolicy::Cached) => {
                        state.cached_writers += 1
                    }
                    (AttemptMode::ReadWriteItems, ReadVersionPolicy::Fresh) => {
                        state.fresh_item_writers += 1
                    }
                    _ => state.fresh_other_attempts += 1,
                }
                Some(
                    serde_json::json!({"event": "attempt", "mode": format!("{mode:?}"), "policy": format!("{read_version:?}")}),
                )
            }
            KvNativeEvent::Commit => {
                state.acknowledgements += 1;
                Some(serde_json::json!({"event": "acknowledged"}))
            }
            KvNativeEvent::CommitFailed { native_code, kind } => {
                state.native_failures += 1;
                Some(
                    serde_json::json!({"event": "native_commit_failure", "code": native_code, "rejection": format!("{kind:?}")}),
                )
            }
            KvNativeEvent::Abort => Some(serde_json::json!({"event": "abort"})),
            _ => None,
        };
        if state.events.len() < 4096
            && let Some(record) = record
        {
            state.events.push(record);
        }
    }
}

impl TableAtomicityWorkload {
    pub(crate) async fn qualify_cached_write(&self, store: &Store) -> StorageResult<()> {
        // This absent key is outside client histories. A rejected Put must
        // leave both the item and all seed stream records exactly unchanged.
        let key = "cached-write-control/absent";
        let read = || {
            store.get_item(GetItemRequest::new(
                self.table_name(),
                self.key_attributes(key),
            ))
        };
        let before = read().await?;
        let history_before = read_seed_history(store, self.table_name()).await?;
        let start = self.write_probe.report();
        let result = store
            .put_item(
                storage_types::PutItemRequest::builder()
                    .table_name(self.table_name())
                    .item(
                        WireItem::try_from(self.item(key, "control")).expect("valid control item"),
                    )
                    .condition_expression(Some("attribute_exists(pk)".into()))
                    .build(),
            )
            .await;
        let observed = self.write_probe.report();
        let error = result
            .err()
            .ok_or_else(|| StorageError::internal("control condition accepted"))?;
        let after = read().await?;
        let history_after = read_seed_history(store, self.table_name()).await?;
        let first = start["events"].as_array().unwrap().len();
        let events = &observed["events"].as_array().unwrap()[first..];
        let writers: Vec<_> = events
            .iter()
            .filter(|event| event["mode"] == "ReadWriteItems")
            .collect();
        let cached_index = events
            .iter()
            .position(|event| event["mode"] == "ReadWriteItems" && event["policy"] == "Cached");
        let fresh_index = cached_index.and_then(|cached| {
            events
                .iter()
                .enumerate()
                .skip(cached + 1)
                .find(|(_, event)| event["event"] == "attempt")
                .map(|(index, _)| index)
        });
        let acknowledged_cached = cached_index
            .zip(fresh_index)
            .is_some_and(|(cached, fresh)| {
                let segment = &events[cached + 1..fresh];
                segment.iter().any(|event| event["event"] == "acknowledged")
                    && !segment.iter().any(|event| {
                        event["event"] == "native_commit_failure" || event["event"] == "abort"
                    })
            });
        let passed = acknowledged_cached
            && crate::common::is_condition_failure(&error)
            && before.item.is_none()
            && after.item.is_none()
            && history_before == history_after
            && writers.len() >= 2
            && writers[0]["policy"] == "Cached"
            && writers[1..].iter().all(|event| event["policy"] == "Fresh");
        let report = serde_json::json!({"passed": passed, "cached_no_mutation_acknowledged": acknowledged_cached, "response": "ConditionalCheckFailedException",
            "item_unchanged": before.item.is_none() && after.item.is_none(), "history_unchanged": history_before == history_after,
            "events": events, "native_error": format!("{error:?}")});
        let root = self.client_artifact_root();
        fs::create_dir_all(&root).map_err(|error| StorageError::internal(&error))?;
        fs::write(root.join("cached-write-control.json"), report.to_string())
            .map_err(|error| StorageError::internal(&error))?;
        if passed {
            Ok(())
        } else {
            Err(StorageError::internal(&format!(
                "cached write control failed: {report}"
            )))
        }
    }
}

async fn read_seed_history(
    store: &Store,
    table_name: TableName,
) -> StorageResult<serde_json::Value> {
    let table = store
        .describe_table(storage_types::DescribeTableRequest { table_name })
        .await?;
    let arn = table.table.latest_stream_arn.expect("seed stream ARN");
    let shards = store
        .describe_stream(storage_types::DescribeStreamRequest {
            stream_arn: arn.clone(),
            exclusive_start_shard_id: None,
            limit: None,
            shard_filter: None,
        })
        .await?
        .stream_description
        .shards;
    let mut records = Vec::new();
    for shard in shards {
        let iterator = store
            .get_shard_iterator(storage_types::GetShardIteratorRequest {
                stream_arn: arn.clone(),
                shard_id: shard.shard_id,
                shard_iterator_type: "TRIM_HORIZON".into(),
                sequence_number: None,
            })
            .await?
            .shard_iterator;
        let page = store
            .get_records(storage_types::GetRecordsRequest {
                shard_iterator: iterator,
                limit: Some(1000),
            })
            .await?;
        // Setup has exactly sixteen inserts and no concurrent writer. The page
        // limit exceeds that complete history; the final total prevents empty
        // or truncated observations from satisfying the equality check.
        if page.records.len() >= 1000 {
            return Err(StorageError::internal("seed history page reached limit"));
        }
        records.extend(page.records);
    }
    if records.len() != 16 {
        return Err(StorageError::internal(
            "seed history must contain exactly sixteen inserts",
        ));
    }
    Ok(serde_json::to_value(records).expect("serializable records"))
}
