use std::sync::{Arc, Mutex};

use storage_backfill::{
    LogicalBackfillActivationGate, LogicalBackfillCaller, LogicalBackfillChecksum,
    LogicalBackfillChunk, LogicalBackfillDomain, LogicalBackfillExport, LogicalBackfillId,
    LogicalBackfillImport, LogicalBackfillManifest, LogicalBackfillRecord, LogicalBackfillResult,
    LogicalExportPage, LogicalExportRequest,
};
use storage_types::{ItemStreamVersion, StorageError};

use crate::{
    SyncBackendPairDecision,
    catchup::{
        SyncLearnerCatchupConfig, SyncLearnerCatchupExecutor, SyncLearnerCatchupRequirement,
    },
    plan_sync_backend_pair,
};

#[test]
fn sync_learner_catchup_requirement_uses_storage_backfill_policy() {
    let requirement = SyncLearnerCatchupRequirement::default();

    assert_eq!(
        requirement.caller(),
        LogicalBackfillCaller::SyncLearnerCatchup
    );
    assert_eq!(
        requirement.activation_gate(),
        LogicalBackfillActivationGate::RaftPromotionReadiness
    );
}

#[tokio::test]
async fn sync_learner_catchup_uses_shared_logical_backfill_traits() {
    let source = RecordingExport::new(vec![
        LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records: vec![LogicalBackfillRecord::PresentItem {
                table_name: "users".to_string(),
                key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
                item_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
                item_stream_version: ItemStreamVersion::new(7),
            }],
            next_cursor: Some("next".to_string()),
            checksum: LogicalBackfillChecksum::new("page-1").unwrap(),
        },
        LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records: Vec::new(),
            next_cursor: None,
            checksum: LogicalBackfillChecksum::new("page-2").unwrap(),
        },
    ]);
    let destination = RecordingImport::default();
    let executor = SyncLearnerCatchupExecutor::new(
        SyncLearnerCatchupRequirement::default(),
        SyncLearnerCatchupConfig {
            page_limit: 1,
            domains: vec![LogicalBackfillDomain::ItemRecords],
        },
    );

    let report = executor
        .run(
            &source,
            &destination,
            LogicalBackfillId::new("manifest-1").unwrap(),
            "sqlite",
            "sqlite",
        )
        .await
        .expect("catchup");

    assert_eq!(
        report.manifest.caller,
        LogicalBackfillCaller::SyncLearnerCatchup
    );
    assert_eq!(
        report.manifest.activation_gate,
        LogicalBackfillActivationGate::RaftPromotionReadiness
    );
    assert_eq!(report.chunks_imported, 2);
    assert_eq!(report.records_imported, 1);
    assert_eq!(source.requests().len(), 2);
    assert_eq!(source.requests()[0].cursor, None);
    assert_eq!(source.requests()[1].cursor.as_deref(), Some("next"));
    assert_eq!(destination.imported_chunks().len(), 2);
}

#[tokio::test]
async fn sync_learner_catchup_transfers_one_chunk_at_a_time_and_resumes() {
    let source = RecordingExport::new(vec![
        LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records: vec![LogicalBackfillRecord::PresentItem {
                table_name: "users".to_string(),
                key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
                item_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
                item_stream_version: ItemStreamVersion::new(9),
            }],
            next_cursor: Some("resume-after-first".to_string()),
            checksum: LogicalBackfillChecksum::new("page-1").unwrap(),
        },
        LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records: Vec::new(),
            next_cursor: None,
            checksum: LogicalBackfillChecksum::new("page-2").unwrap(),
        },
    ]);
    let destination = RecordingImport::default();
    let executor = SyncLearnerCatchupExecutor::new(
        SyncLearnerCatchupRequirement::default(),
        SyncLearnerCatchupConfig {
            page_limit: 1,
            domains: vec![LogicalBackfillDomain::ItemRecords],
        },
    );
    let checkpoint = executor.start_checkpoint(
        LogicalBackfillId::new("manifest-resume").unwrap(),
        "sqlite",
        "sqlite",
    );

    let first = executor
        .transfer_next_chunk(&source, &destination, checkpoint)
        .await
        .expect("first chunk");

    assert!(!first.complete);
    assert_eq!(
        first.checkpoint.cursor.as_deref(),
        Some("resume-after-first")
    );
    assert_eq!(first.checkpoint.page, 1);
    assert_eq!(destination.imported_chunks().len(), 1);

    let second = executor
        .transfer_next_chunk(&source, &destination, first.checkpoint)
        .await
        .expect("second chunk");

    assert!(second.complete);
    assert_eq!(second.checkpoint.cursor, None);
    assert_eq!(second.checkpoint.domain_index, 1);
    assert_eq!(second.checkpoint.chunks_imported, 2);
    assert_eq!(second.checkpoint.records_imported, 1);
    assert_eq!(
        source.requests()[1].cursor.as_deref(),
        Some("resume-after-first")
    );
    assert_eq!(destination.imported_chunks().len(), 2);
}

#[tokio::test]
async fn sync_learner_catchup_resumes_after_source_accepts_continuing_writes() {
    let source = RecordingExport::new(vec![LogicalExportPage {
        domain: LogicalBackfillDomain::ItemRecords,
        records: vec![LogicalBackfillRecord::PresentItem {
            table_name: "users".to_string(),
            key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
            item_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
            item_stream_version: ItemStreamVersion::new(10),
        }],
        next_cursor: Some("after-first-page".to_string()),
        checksum: LogicalBackfillChecksum::new("page-1").unwrap(),
    }]);
    let destination = RecordingImport::default();
    let executor = SyncLearnerCatchupExecutor::new(
        SyncLearnerCatchupRequirement::default(),
        SyncLearnerCatchupConfig {
            page_limit: 1,
            domains: vec![LogicalBackfillDomain::ItemRecords],
        },
    );
    let checkpoint = executor.start_checkpoint(
        LogicalBackfillId::new("manifest-continuing-writes").unwrap(),
        "sqlite",
        "sqlite",
    );

    let first = executor
        .transfer_next_chunk(&source, &destination, checkpoint)
        .await
        .expect("first chunk");
    source.push_page(LogicalExportPage {
        domain: LogicalBackfillDomain::ItemRecords,
        records: vec![LogicalBackfillRecord::PresentItem {
            table_name: "users".to_string(),
            key_json: r#"{"pk":{"S":"u#2"}}"#.to_string(),
            item_json: r#"{"pk":{"S":"u#2"}}"#.to_string(),
            item_stream_version: ItemStreamVersion::new(11),
        }],
        next_cursor: None,
        checksum: LogicalBackfillChecksum::new("page-2-after-write").unwrap(),
    });

    let second = executor
        .transfer_next_chunk(&source, &destination, first.checkpoint)
        .await
        .expect("second chunk after continuing write");

    assert!(second.complete);
    assert_eq!(second.checkpoint.chunks_imported, 2);
    assert_eq!(second.checkpoint.records_imported, 2);
    assert_eq!(
        source.requests()[1].cursor.as_deref(),
        Some("after-first-page")
    );
    assert_eq!(destination.imported_chunks().len(), 2);
}

#[tokio::test]
async fn sync_learner_catchup_resumes_after_learner_restart_during_import() {
    let source = RecordingExport::new(vec![
        item_page("u#1", 20, Some("restart-cursor"), "restart-page-1"),
        item_page("u#2", 21, None, "restart-page-2"),
    ]);
    let destination = RecordingImport::default();
    let executor = item_catchup_executor();
    let checkpoint = executor.start_checkpoint(
        LogicalBackfillId::new("manifest-restart").unwrap(),
        "sqlite",
        "sqlite",
    );

    let first = executor
        .transfer_next_chunk(&source, &destination, checkpoint)
        .await
        .expect("first chunk");
    let persisted_checkpoint = serde_json::to_vec(&first.checkpoint).expect("serialize checkpoint");
    let restored_checkpoint = serde_json::from_slice(&persisted_checkpoint).expect("checkpoint");
    let restarted_executor = item_catchup_executor();

    let second = restarted_executor
        .transfer_next_chunk(&source, &destination, restored_checkpoint)
        .await
        .expect("second chunk after restart");

    assert!(second.complete);
    assert_eq!(second.checkpoint.chunks_imported, 2);
    assert_eq!(second.checkpoint.records_imported, 2);
    assert_eq!(
        source.requests()[1].cursor.as_deref(),
        Some("restart-cursor")
    );
}

#[tokio::test]
async fn sync_learner_catchup_resumes_with_new_source_after_leader_failover_during_import() {
    let old_leader = RecordingExport::new(vec![item_page(
        "u#1",
        30,
        Some("failover-cursor"),
        "old-leader-page",
    )]);
    let new_leader = RecordingExport::new(vec![item_page("u#2", 31, None, "new-leader-page")]);
    let destination = RecordingImport::default();
    let executor = item_catchup_executor();
    let checkpoint = executor.start_checkpoint(
        LogicalBackfillId::new("manifest-failover").unwrap(),
        "sqlite",
        "sqlite",
    );

    let first = executor
        .transfer_next_chunk(&old_leader, &destination, checkpoint)
        .await
        .expect("first chunk from old leader");
    let second = executor
        .transfer_next_chunk(&new_leader, &destination, first.checkpoint)
        .await
        .expect("second chunk from new leader");

    assert!(second.complete);
    assert_eq!(
        new_leader.requests()[0].cursor.as_deref(),
        Some("failover-cursor")
    );
    assert_eq!(destination.imported_chunks().len(), 2);
}

#[tokio::test]
async fn sync_learner_catchup_records_sqlite_to_rocksdb_validation_pair() {
    assert_mixed_backend_catchup_manifest("sqlite", "rocksdb").await;
}

#[tokio::test]
async fn sync_learner_catchup_records_sqlite_to_foundationdb_validation_pair() {
    assert_mixed_backend_catchup_manifest("sqlite", "foundationdb").await;
}

#[tokio::test]
async fn sync_learner_catchup_records_foundationdb_to_sqlite_validation_pair() {
    assert_mixed_backend_catchup_manifest("foundationdb", "sqlite").await;
}

#[derive(Clone)]
struct RecordingExport {
    pages: Arc<Mutex<Vec<LogicalExportPage>>>,
    requests: Arc<Mutex<Vec<LogicalExportRequest>>>,
}

async fn assert_mixed_backend_catchup_manifest(source_backend: &str, destination_backend: &str) {
    assert_eq!(
        plan_sync_backend_pair(source_backend, destination_backend),
        SyncBackendPairDecision::ValidationOnly
    );
    let source = RecordingExport::new(vec![LogicalExportPage {
        domain: LogicalBackfillDomain::ItemRecords,
        records: Vec::new(),
        next_cursor: None,
        checksum: LogicalBackfillChecksum::new("empty").unwrap(),
    }]);
    let destination = RecordingImport::default();
    let report = item_catchup_executor()
        .run(
            &source,
            &destination,
            LogicalBackfillId::new(format!("{source_backend}-to-{destination_backend}")).unwrap(),
            source_backend,
            destination_backend,
        )
        .await
        .expect("mixed backend logical catchup");

    assert_eq!(report.manifest.source_backend, source_backend);
    assert_eq!(report.manifest.destination_backend, destination_backend);
    assert_eq!(report.chunks_imported, 1);
}

fn item_catchup_executor() -> SyncLearnerCatchupExecutor {
    SyncLearnerCatchupExecutor::new(
        SyncLearnerCatchupRequirement::default(),
        SyncLearnerCatchupConfig {
            page_limit: 1,
            domains: vec![LogicalBackfillDomain::ItemRecords],
        },
    )
}

fn item_page(
    key: &str,
    version: u64,
    next_cursor: Option<&str>,
    checksum: &str,
) -> LogicalExportPage {
    LogicalExportPage {
        domain: LogicalBackfillDomain::ItemRecords,
        records: vec![LogicalBackfillRecord::PresentItem {
            table_name: "users".to_string(),
            key_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
            item_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
            item_stream_version: ItemStreamVersion::new(version),
        }],
        next_cursor: next_cursor.map(ToString::to_string),
        checksum: LogicalBackfillChecksum::new(checksum).unwrap(),
    }
}

impl RecordingExport {
    fn new(pages: Vec<LogicalExportPage>) -> Self {
        Self {
            pages: Arc::new(Mutex::new(pages.into_iter().rev().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<LogicalExportRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn push_page(&self, page: LogicalExportPage) {
        self.pages.lock().unwrap().push(page);
    }
}

#[async_trait::async_trait]
impl LogicalBackfillExport for RecordingExport {
    async fn export_logical_page(
        &self,
        request: LogicalExportRequest,
    ) -> Result<LogicalExportPage, StorageError> {
        self.requests.lock().unwrap().push(request);
        self.pages
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| StorageError::internal("missing export page"))
    }
}

#[derive(Default)]
struct RecordingImport {
    chunks: Mutex<Vec<LogicalBackfillChunk>>,
}

impl RecordingImport {
    fn imported_chunks(&self) -> Vec<LogicalBackfillChunk> {
        self.chunks.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl LogicalBackfillImport for RecordingImport {
    async fn import_logical_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> Result<LogicalBackfillResult, StorageError> {
        assert!(manifest.domains.contains(&chunk.summary.domain));
        self.chunks.lock().unwrap().push(chunk);
        Ok(LogicalBackfillResult::ChunkImported)
    }
}
