//! Customer control identity survives native abort and public-name recreation.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use futures::future::BoxFuture;
use sql_backend::{SqlCommitBoundary, SqlFaultCall, SqlFaultProbe};
use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{Clock, StorageEnum, StorageError, StorageResult, TableName, TimestampMillis};

use crate::test_support as fixture;

type RetryHook = Box<dyn FnOnce() -> BoxFuture<'static, StorageResult<()>> + Send>;
#[derive(Default)]
struct RetryClock {
    hook: Mutex<Option<RetryHook>>,
}
impl Clock for RetryClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::now()
    }
    fn sleep(&self, duration: std::time::Duration) -> BoxFuture<'_, StorageResult<()>> {
        let hook = self.hook.lock().unwrap().take();
        Box::pin(async move {
            if let Some(hook) = hook {
                hook().await?;
            }
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }
}
#[derive(Default)]
struct ControlAbort {
    point: AtomicU8,
    hits: AtomicUsize,
    commits: AtomicUsize,
}
impl SqlFaultProbe for ControlAbort {
    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError> {
        if call.sql.starts_with("SELECT id, table_name, table_status,")
            && self
                .point
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Some(aborted())
        } else {
            None
        }
    }
    fn at_commit_boundary(&self, boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        let point = match boundary {
            SqlCommitBoundary::BeforeCommit => {
                self.commits.fetch_add(1, Ordering::SeqCst);
                2
            }
            SqlCommitBoundary::AfterDurableAcknowledgement => 3,
        };
        let should_abort = self
            .point
            .compare_exchange(point, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        if should_abort {
            self.hits.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move { if should_abort { Err(aborted()) } else { Ok(()) } })
    }
}
fn aborted() -> StorageError {
    StorageEnum::TransactionConflict {
        message: "injected customer control abort".into(),
    }
    .into()
}
#[derive(Clone, Copy)]
enum ControlKind {
    Enable,
    Add,
    Remove,
    Transfer,
}
impl ControlKind {
    fn request(self) -> storage_types::UpdateTableRequest {
        serde_json::from_value(match self{
            Self::Enable=>serde_json::json!({"TableName":fixture::NAME,"ReplicaUpdates":[{"Create":{"RegionName":"region-2"}}]}),
            Self::Add=>serde_json::json!({"TableName":fixture::NAME,"ReplicaUpdates":[{"Create":{"RegionName":"region-3"}}]}),
            Self::Remove=>serde_json::json!({"TableName":fixture::NAME,"ReplicaUpdates":[{"Delete":{"RegionName":"region-2"}}]}),
            Self::Transfer=>serde_json::json!({"TableName":fixture::NAME,"AuxConfigurationPrimaryRegion":"region-2"}),
        }).unwrap()
    }
}
struct ControlFixture {
    _directory: tempfile::TempDir,
    source: fixture::Store,
    peer: fixture::Store,
    cluster: Arc<fixture::NativeCluster>,
    clock: Arc<RetryClock>,
    probe: Arc<ControlAbort>,
    original_id: String,
}
impl ControlFixture {
    async fn new(kind: ControlKind) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let probe = Arc::new(ControlAbort::default());
        let clock = Arc::new(RetryClock::default());
        let source =
            fixture::open_with_probe(&directory.path().join("source.sqlite"), Some(probe.clone()))
                .await
                .with_clock(clock.clone());
        let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
        let third = if matches!(kind, ControlKind::Add) {
            Some(fixture::open(&directory.path().join("third.sqlite")).await)
        } else {
            None
        };
        let mut stores = vec![&source, &peer];
        if let Some(third) = third.as_ref() {
            stores.push(third);
        }
        let cluster = Arc::new(fixture::NativeCluster::start(&stores).await);
        match kind {
            ControlKind::Enable => {
                fixture::create_table(&source, &TableName::new(fixture::NAME)).await
            }
            _ => cluster.install(&source, fixture::NAME, &[2]).await,
        }
        let original_id = cluster.operations[0]
            .describe_table(storage_types::DescribeTableRequest {
                table_name: TableName::new(fixture::NAME),
            })
            .await
            .unwrap()
            .table
            .table_id;
        Self {
            _directory: directory,
            source,
            peer,
            cluster,
            clock,
            probe,
            original_id,
        }
    }
    fn arm_abort(&self, point: u8) {
        if point == 1 {
            self.source
                .metadata_cache()
                .invalidate(&TableName::new(fixture::NAME));
        }
        self.probe.point.store(point, Ordering::SeqCst);
    }
    fn arm_replacement(&self, kind: ControlKind) {
        let cluster = Arc::clone(&self.cluster);
        let source = self.source.clone();
        let peer = self.peer.clone();
        *self.clock.hook.lock().unwrap() = Some(Box::new(move || {
            Box::pin(async move {
                // Native incarnation retirement is used by replica lifecycle;
                // public DeleteTable retains its local-only metadata
                // restriction.
                source
                    .delete_table(storage_types::DeleteTableRequest {
                        table_name: TableName::new(fixture::NAME),
                    })
                    .await?;
                match kind {
                    ControlKind::Enable => {
                        fixture::create_table(&source, &TableName::new(fixture::NAME)).await
                    }
                    _ => {
                        peer.delete_table(storage_types::DeleteTableRequest {
                            table_name: TableName::new(fixture::NAME),
                        })
                        .await?;
                        cluster.install(&source, fixture::NAME, &[2]).await;
                    }
                }
                Ok(())
            })
        }));
    }
    async fn finish(self) {
        Arc::try_unwrap(self.cluster).ok().unwrap().shutdown().await;
    }
}

#[tokio::test]
async fn given_replica_controls_abort_when_name_is_recreated_then_retry_cannot_enable_or_add_to_the_replacement()
 {
    for kind in [ControlKind::Enable, ControlKind::Add] {
        for point in [1, 2] {
            assert_control_retry_is_bound(kind, point).await;
        }
    }
}
#[tokio::test]
async fn given_lifecycle_controls_abort_when_name_is_recreated_then_retry_cannot_remove_or_transfer_the_replacement()
 {
    for kind in [ControlKind::Remove, ControlKind::Transfer] {
        for point in [1, 2] {
            assert_control_retry_is_bound(kind, point).await;
        }
    }
}

async fn assert_control_retry_is_bound(kind: ControlKind, point: u8) {
    let fixture = ControlFixture::new(kind).await;
    fixture.arm_replacement(kind);
    fixture.arm_abort(point);
    let result = fixture.cluster.operations[0]
        .update_table(kind.request())
        .await;
    assert_eq!(
        fixture.probe.hits.load(Ordering::SeqCst),
        1,
        "the intended native abort must occur"
    );
    assert!(
        fixture.clock.hook.lock().unwrap().is_none(),
        "replacement must occur between native attempts"
    );
    assert!(
        matches!(
            result.as_ref().err().map(|error| error.as_ref()),
            Some(StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. })
        ),
        "control invocation followed a recreated name: {result:?}"
    );
    let current = fixture.cluster.operations[0]
        .describe_table(storage_types::DescribeTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap()
        .table;
    assert_ne!(current.table_id, fixture.original_id);
    assert_eq!(current.table_status, storage_types::TableStatus::Active);
    match kind {
        ControlKind::Enable => {
            assert!(current.aux_configuration_primary_region.is_none());
            assert!(current.replicas.is_none_or(|replicas| replicas.is_empty()));
        }
        _ => {
            assert_eq!(
                current.aux_configuration_primary_region.as_deref(),
                Some("region-1")
            );
            let replicas = current.replicas.unwrap();
            assert_eq!(replicas.len(), 2);
            assert!(
                replicas
                    .iter()
                    .all(|replica| replica.replica_status == storage_types::ReplicaStatus::Active)
            );
        }
    }
    let journal = Arc::clone(&fixture.cluster.journals[0]);
    fixture
        .source
        .with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                assert!(journal.select_pending_enable(attempt).await?.is_none());
                assert!(journal.select_pending_work(attempt).await?.is_none());
                assert!(journal.select_pending_addition(attempt).await?.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn given_control_abort_when_identity_is_unchanged_then_retry_still_succeeds() {
    for kind in [ControlKind::Enable, ControlKind::Transfer] {
        for point in [1, 2] {
            let fixture = ControlFixture::new(kind).await;
            fixture.arm_abort(point);
            let result = fixture.cluster.operations[0]
                .update_table(kind.request())
                .await
                .unwrap();
            assert_eq!(fixture.probe.hits.load(Ordering::SeqCst), 1);
            assert_eq!(result.table_description.table_id, fixture.original_id);
            fixture.finish().await;
        }
    }
}
#[tokio::test]
async fn given_control_commit_acknowledgment_is_lost_when_outcome_is_unknown_then_no_retry_occurs()
{
    for kind in [ControlKind::Enable, ControlKind::Transfer] {
        let fixture = ControlFixture::new(kind).await;
        let commits = fixture.probe.commits.load(Ordering::SeqCst);
        fixture.arm_abort(3);
        let error = fixture.cluster.operations[0]
            .update_table(kind.request())
            .await
            .unwrap_err();
        assert!(
            error.is_commit_unknown(),
            "unknown durable outcome was remapped: {error:?}"
        );
        assert_eq!(fixture.probe.hits.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.probe.commits.load(Ordering::SeqCst) - commits, 1);
        fixture.finish().await;
    }
}
