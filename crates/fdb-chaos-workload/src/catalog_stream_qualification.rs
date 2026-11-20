//! Public stream replay after real catalog publication and native read expiry.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use kv_backend::{KvAccess, KvFaultCall, KvFaultProbe, KvNativeEvent, KvWave, KvWaveKind};
use storage_provider::DynamoOperations;
use storage_types::{Clock, StorageError, StorageResult};

use crate::{
    constants::SIMULATION_METADATA_CACHE_TTL, imports::*, table_atomicity::TableAtomicityWorkload,
};

// A measured fault stimulus, not a universal native retention bound.
const NATIVE_READ_HOLD: Duration = Duration::from_secs(30);

type Store = kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>;

#[derive(Default)]
struct ReadCounts {
    begins: u64,
    aborts: u64,
    completed_reads: u64,
    name_reads: u64,
    delays: u64,
    should_delay: bool,
    events: Vec<&'static str>,
    publication: Option<storage_types::CreateTableRequest>,
    overlapping_publications: u64,
}

struct ReadExpiryProbe {
    counts: Mutex<ReadCounts>,
    clock: Arc<crate::simulation_clock::SimulationClock>,
    name_key: Vec<u8>,
    publisher: Store,
}

impl ReadExpiryProbe {
    fn arm(&self, should_delay: bool, publication: Option<storage_types::CreateTableRequest>) {
        *self.counts.lock().unwrap() = ReadCounts {
            should_delay,
            publication,
            ..ReadCounts::default()
        };
    }

    fn snapshot(&self) -> serde_json::Value {
        let counts = self.counts.lock().unwrap();
        serde_json::json!({"begins": counts.begins, "aborts": counts.aborts,
            "completed_reads": counts.completed_reads, "name_reads": counts.name_reads,
            "delays": counts.delays, "events": counts.events,
            "overlapping_publications": counts.overlapping_publications,
            "ordered_expiry_retry": has_expiry_retry(&counts.events)})
    }
}

impl KvFaultProbe for ReadExpiryProbe {
    fn fail(&self, _: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: KvNativeEvent) {
        let mut counts = self.counts.lock().unwrap();
        match event {
            KvNativeEvent::Begin => {
                counts.begins += 1;
                counts.completed_reads = 0;
                counts.events.push("begin");
            }
            KvNativeEvent::Abort => {
                counts.aborts += 1;
                counts.events.push("abort");
            }
            KvNativeEvent::Wave {
                kind: KvWaveKind::GetBatch | KvWaveKind::Scan | KvWaveKind::FirstKey,
                ..
            } => {
                counts.completed_reads += 1;
                counts.events.push("read_complete");
            }
            _ => {}
        }
    }

    fn observe_access(&self, access: KvAccess<'_>) {
        if access.key == self.name_key {
            self.counts.lock().unwrap().name_reads += 1;
        }
    }

    fn before_wave(&self, wave: KvWave) -> BoxFuture<'_, StorageResult<()>> {
        let (should_delay, publication) = {
            let mut counts = self.counts.lock().unwrap();
            let should_delay = counts.should_delay
                && counts.completed_reads > 0
                && matches!(
                    wave.kind,
                    KvWaveKind::GetBatch | KvWaveKind::Scan | KvWaveKind::FirstKey
                );
            if should_delay {
                counts.should_delay = false;
                counts.delays += 1;
                counts.events.push("pause_after_read");
            }
            let publication = if should_delay {
                counts.publication.take()
            } else {
                None
            };
            (should_delay, publication)
        };
        Box::pin(async move {
            if let Some(publication) = publication {
                // This is a separate real customer operation while the reader's
                // native attempt is paused after its metadata observation.
                self.publisher.create_table(publication).await?;
                let mut counts = self.counts.lock().unwrap();
                counts.overlapping_publications += 1;
                counts.events.push("publication_after_read");
            }
            if should_delay {
                self.counts.lock().unwrap().events.push("delay_after_read");
                self.clock.sleep(NATIVE_READ_HOLD).await?;
                self.counts.lock().unwrap().events.push("hold_complete");
            }
            Ok(())
        })
    }
}

impl TableAtomicityWorkload {
    pub(crate) async fn qualify_catalog_stream_reads(
        &self,
        db: SimDatabase,
        publisher: &Store,
    ) -> StorageResult<()> {
        let clock = self.clock.as_ref().expect("setup clock");
        // This separate direct-native control attributes the expiry to an
        // actual FoundationDB error code. Public replay below records native
        // lifecycle.
        // Simulated proxies can reject a read version transiently (for
        // example 1042); only a retryable rejection is retried.
        let mut transaction = db
            .create_trx()
            .map_err(|error| StorageError::internal(&error))?;
        while let Err(error) = transaction.get_read_version().await {
            transaction = transaction
                .on_error(error)
                .await
                .map_err(|error| StorageError::internal(&error))?;
        }
        clock.sleep(NATIVE_READ_HOLD).await?;
        let error = transaction
            .get(b"aux-storage/fdb-chaos/expiry-control", false)
            .await
            .err()
            .ok_or_else(|| invalid("old native read unexpectedly succeeded"))?;
        if error.code() != 1007 {
            return Err(invalid(&format!(
                "old native read code {} != 1007",
                error.code()
            )));
        }
        drop(transaction);

        let mut name_key = vec![b'M'];
        name_key.extend_from_slice(self.table_name().as_ref().as_bytes());
        let probe = Arc::new(ReadExpiryProbe {
            counts: Mutex::default(),
            clock: clock.clone(),
            name_key,
            publisher: publisher.clone(),
        });
        let reader = kv_backend::KvStore::initialize_format_with_clock(
            self.driver(db)
                .map_err(|error| invalid(&error))?
                // The hold must end in native read expiry, not in the
                // attempt budget or the native timeout derived from it.
                .with_attempt_deadlines(storage_types::AttemptDeadlines::try_new(
                    [NATIVE_READ_HOLD * 4; storage_types::DeadlineClass::COUNT],
                    storage_types::AttemptDeadlines::default().commit_grace(),
                )?)
                .with_fault_probe(probe.clone()),
            storage_provider::StorageBackend::FoundationDb.default_initial_stream_lineages(),
            clock.clone(),
        )
        .await?
        .with_metadata_cache_ttl(SIMULATION_METADATA_CACHE_TTL);
        let table = publisher
            .describe_table(storage_types::DescribeTableRequest {
                table_name: self.table_name(),
            })
            .await?;
        let arn = table
            .table
            .latest_stream_arn
            .ok_or_else(|| invalid("seed table has no stream ARN"))?;
        let shards = reader
            .describe_stream(storage_types::DescribeStreamRequest {
                stream_arn: arn.clone(),
                exclusive_start_shard_id: None,
                limit: None,
                shard_filter: None,
            })
            .await?
            .stream_description
            .shards;
        let mut selected = None;
        for shard in shards {
            let request = storage_types::GetShardIteratorRequest {
                stream_arn: arn.clone(),
                shard_id: shard.shard_id,
                shard_iterator_type: "TRIM_HORIZON".into(),
                sequence_number: None,
            };
            let iterator = reader
                .get_shard_iterator(request.clone())
                .await?
                .shard_iterator;
            let page = reader
                .get_records(storage_types::GetRecordsRequest {
                    shard_iterator: iterator.clone(),
                    limit: Some(1),
                })
                .await?;
            if !page.records.is_empty() {
                selected = Some((
                    request,
                    iterator,
                    serde_json::to_value(page.records).unwrap(),
                ));
                break;
            }
        }
        let (request, iterator, expected) =
            selected.ok_or_else(|| invalid("seed stream has no public record"))?;
        if expected.as_array().is_none_or(|records| records.len() != 1)
            || expected[0]["dynamodb"]["NewImage"].is_null()
        {
            return Err(invalid("public record oracle lacks complete image"));
        }
        let mut windows = Vec::new();
        let mut publications = 0;
        for records in [false, true] {
            // Warm this exact operation before comparing its equal-version
            // path.
            read_public(&reader, &probe, &request, &iterator, records, &expected).await?;
            probe.arm(false, None);
            let equal =
                read_public(&reader, &probe, &request, &iterator, records, &expected).await?;
            if equal["name_reads"] != 0 {
                return Err(invalid("equal catalog path loaded table name"));
            }
            for should_delay in [false, true] {
                let mut create = self.create_table_request();
                create.table_name = storage_types::TableName::new(&format!(
                    "FdbChaosCatalogPublication{publications}"
                ));
                create.global_secondary_indexes = None;
                create
                    .attribute_definitions
                    .retain(|definition| definition.attribute_name == "pk");
                // CreateTable keeps the catalog version (no node caches an
                // absent name); DeleteTable changes it.
                let name = create.table_name.clone();
                publisher.create_table(create).await?;
                publisher
                    .delete_table(storage_types::DeleteTableRequest { table_name: name })
                    .await?;
                publications += 1;
                let publication = if should_delay {
                    let mut create = self.create_table_request();
                    create.table_name = storage_types::TableName::new(&format!(
                        "FdbChaosDuringReadPublication{publications}"
                    ));
                    create.global_secondary_indexes = None;
                    create
                        .attribute_definitions
                        .retain(|definition| definition.attribute_name == "pk");
                    Some(create)
                } else {
                    None
                };
                probe.arm(should_delay, publication);
                let observed =
                    read_public(&reader, &probe, &request, &iterator, records, &expected).await?;
                if observed["name_reads"].as_u64().unwrap() == 0 {
                    return Err(invalid(&format!(
                        "changed catalog path did not load table name (records={records}, \
                         delayed={should_delay}): {observed}"
                    )));
                }
                if should_delay
                    && (observed["delays"] != 1
                        || observed["aborts"].as_u64().unwrap() == 0
                        || observed["begins"].as_u64().unwrap() < 2
                        || observed["ordered_expiry_retry"] != true
                        || observed["overlapping_publications"] != 1)
                {
                    return Err(invalid(&format!(
                        "native expiry replay lacks witnesses (records={records}): {observed}"
                    )));
                }
                windows.push(serde_json::json!({"records": records, "delayed": should_delay, "equal": equal, "observed": observed}));
            }
        }
        let report = serde_json::json!({"qualification_option": "qualifyCatalogStreamReads", "hold_seconds": NATIVE_READ_HOLD.as_secs(), "native_expiry_code": error.code(), "between_operation_publications": publications, "overlapping_publications": 2,
            "checked_record": expected, "windows": windows});
        let root = self.client_artifact_root();
        std::fs::create_dir_all(&root).map_err(|error| StorageError::internal(&error))?;
        std::fs::write(
            root.join("catalog-stream-reads.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .map_err(|error| StorageError::internal(&error))?;
        self.context.trace(Severity::Info, "AuxStorageCatalogStreamReadQualification", details![
            "NativeExpiryCode" => error.code(), "BetweenOperationPublications" => publications, "OverlappingPublications" => 2_u64, "CheckedRecordCount" => 4_u64,
            "NativeExpiryReplayCount" => 2_u64, "ClientId" => self.client_id,
        ]);
        Ok(())
    }
}

async fn read_public(
    reader: &Store,
    probe: &ReadExpiryProbe,
    request: &storage_types::GetShardIteratorRequest,
    iterator: &str,
    records: bool,
    expected: &serde_json::Value,
) -> StorageResult<serde_json::Value> {
    let iterator = if records {
        iterator.to_owned()
    } else {
        reader
            .get_shard_iterator(request.clone())
            .await?
            .shard_iterator
    };
    let iterator_counts = probe.snapshot();
    let page = reader
        .get_records(storage_types::GetRecordsRequest {
            shard_iterator: iterator,
            limit: Some(1),
        })
        .await?;
    if serde_json::to_value(page.records).unwrap() != *expected {
        return Err(invalid(
            "public stream record changed, disappeared or duplicated",
        ));
    }
    Ok(if records {
        probe.snapshot()
    } else {
        iterator_counts
    })
}

fn invalid(message: &str) -> StorageError {
    StorageError::internal(message)
}

// Counts alone can mistake a natural earlier abort for the held attempt's
// abort. Cached reads may finish after the hold; require the later native
// rejection and a completed read in its fresh attempt without forbidding those
// completions.
fn has_expiry_retry(events: &[&str]) -> bool {
    let expected = [
        "publication_after_read",
        "delay_after_read",
        "hold_complete",
        "abort",
        "begin",
        "read_complete",
    ];
    let mut next = 0;
    for event in events {
        if *event == expected[next] {
            next += 1;
            if next == expected.len() {
                return true;
            }
        }
    }
    false
}
