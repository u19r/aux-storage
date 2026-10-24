//! Actual scheduler claims and native maintenance exclusion. This does not
//! produce an accepted-dispatch receipt or prove an external dispatch aborted.

use std::{sync::Arc, time::Duration};

use bg_job_store::ScheduledJobStoreSource;
use scheduled_bg_jobs::{
    JobHandler, JobOutcome, Namespace, runtime::MonotonicDeadline, test_support::raw::ClaimedJob,
};
use storage_provider::{
    DynamoPort,
    maintenance::{ExpirationHandler, ExpirationPort, ExpirationProgress, MaintenancePort},
};
use storage_types::{
    AttemptMode,
    maintenance::{MaintenanceKind, TaskProgress},
};
use tokio::sync::oneshot;

use super::{
    commit_fault::{NativeCommitBoundary, NativeCommitFault},
    commit_hold::BeforeCommitHold,
    fixture,
    job_claim::{prepare_claim, take_over},
};
use crate::HarnessResult;

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_held_destination_when_actual_job_is_reclaimed_then_only_destination_ack_excludes_stale_progress()
-> HarnessResult<()> {
    let scheduler = fixture::FileSqlite::try_new().await?;
    let destination = fixture::FileSqlite::try_new().await?;
    let (hold, release) = BeforeCommitHold::new();
    let destination = Arc::new(
        fixture::FileSqlite::open_store_with_fault_probe(&destination.path, hold.clone()).await?,
    );
    verify_destination_exclusion(&scheduler.store, destination, hold, release).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_held_destination_when_actual_job_is_reclaimed_then_only_destination_ack_excludes_stale_progress()
-> HarnessResult<()> {
    let scheduler = fixture::Postgres::try_new().await?;
    let destination = fixture::Postgres::try_new().await?;
    let result = async {
        let (hold, release) = BeforeCommitHold::new();
        let store = Arc::new(
            destination
                .open_independent_store_with_fault_probe(hold.clone())
                .await?,
        );
        verify_destination_exclusion(&scheduler.store, store, hold, release).await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_held_destination_when_actual_job_is_reclaimed_then_only_destination_ack_excludes_stale_progress()
-> HarnessResult<()> {
    let scheduler = super::turso::FileTurso::try_new().await?;
    let destination = super::turso::FileTurso::try_new().await?;
    drop(destination.store);
    let (hold, release) = BeforeCommitHold::new();
    let store = Arc::new(
        super::turso::FileTurso::open_store_with_fault_probe(&destination.path, hold.clone())
            .await?,
    );
    verify_destination_exclusion(&scheduler.store, store, hold, release).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_held_destination_when_actual_job_is_reclaimed_then_only_destination_ack_excludes_stale_progress()
-> HarnessResult<()> {
    let scheduler = super::rocksdb::RocksDb::try_new().await?;
    let destination = super::rocksdb::RocksDb::try_new().await?;
    drop(destination.store);
    let (hold, release) = BeforeCommitHold::new();
    let store = Arc::new(
        super::rocksdb::RocksDb::open_store_with_fault_probe(&destination.path, hold.clone())
            .await?,
    );
    verify_destination_exclusion(&scheduler.store, store, hold, release).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_held_destination_when_actual_job_is_reclaimed_then_only_destination_ack_excludes_stale_progress()
-> HarnessResult<()> {
    let scheduler = super::foundationdb::FoundationDb::try_new().await?;
    let destination = super::foundationdb::FoundationDb::try_new().await?;
    let result = async {
        let (hold, release) = BeforeCommitHold::new();
        let store = Arc::new(
            super::foundationdb::FoundationDb::open_store(
                &destination.cluster_file,
                &destination.prefix,
                Some(hold.clone()),
            )
            .await?,
        );
        verify_destination_exclusion(&scheduler.store, store, hold, release).await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}

async fn verify_destination_exclusion<S, P>(
    scheduler: &S,
    destination: Arc<P>,
    hold: Arc<BeforeCommitHold>,
    release: oneshot::Sender<()>,
) -> HarnessResult<()>
where
    S: ScheduledJobStoreSource,
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let (original, successor, first) = prepare_claim(
        scheduler,
        destination.clone(),
        destination.clone(),
        Namespace::try_new("native-destination-expiration")?,
    )
    .await?;
    hold.arm();
    let earlier = original.execute_claimed(
        &first,
        first.context().ok_or("claim context missing")?.wall_now(),
        || MonotonicDeadline::ZERO,
    );
    let reclaim = async {
        tokio::time::timeout(Duration::from_secs(10), hold.wait_ready()).await?;
        let claim = take_over(&successor, &first).await?;
        // The scheduler acknowledged takeover while the earlier native
        // destination write is still held. That claim is no abort proof.
        release
            .send(())
            .map_err(|_| "earlier native write stopped before release")?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(claim)
    };
    let (settled, claim) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(earlier, reclaim)
    })
    .await?;
    // Durable destination progress below is the oracle across scheduler
    // takeover.
    let _outcome = settled?;
    let claim = claim?;
    let before_barrier = load_progress(destination.as_ref()).await?;
    let old = first.context().ok_or("first context missing")?;
    assert_eq!(before_barrier.authority.scope.destination, *old.namespace());
    assert_eq!(before_barrier.authority.job, old.job_id());
    assert_eq!(before_barrier.authority.fence, old.fence());

    let _outcome = successor
        .execute_claimed(
            &claim,
            claim.context().ok_or("claim context missing")?.wall_now(),
            || MonotonicDeadline::ZERO,
        )
        .await?;
    let acknowledged = load_progress(destination.as_ref()).await?;
    assert!(acknowledged.authority.scope == before_barrier.authority.scope);
    assert_eq!(acknowledged.authority.job, first.record().job_id);
    assert_eq!(
        acknowledged.authority.fence,
        claim.context().ok_or("successor context missing")?.fence()
    );
    assert!(acknowledged.revision > before_barrier.revision);

    verify_stale_progress(destination, &first, &acknowledged).await
}

async fn verify_stale_progress<P>(
    destination: Arc<P>,
    first: &ClaimedJob,
    acknowledged: &TaskProgress<ExpirationProgress>,
) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let handler = ExpirationHandler::try_new(destination.clone(), Duration::from_secs(60))?;
    let stale = first.context().ok_or("stale context missing")?;
    assert!(
        !stale.is_cancelled(),
        "destination fence must reject an uncancelled stale invocation"
    );
    let stale_now = stale.wall_now();
    let outcome = handler.execute(stale, &[]).await?;
    assert!(matches!(outcome, JobOutcome::Continue { not_before, .. } if not_before == stale_now));
    let unchanged = load_progress(destination.as_ref()).await?;
    assert!(
        &unchanged == acknowledged,
        "stale authority changed destination progress"
    );
    Ok(())
}

async fn load_progress<P>(destination: &P) -> HarnessResult<TaskProgress<ExpirationProgress>>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: MaintenancePort,
{
    let progress = destination
        .with_dynamo_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                let (_, progress, _) = attempt
                    .load_task_admission::<ExpirationProgress>(MaintenanceKind::TtlSweep)
                    .await?;
                Ok(progress)
            })
        })
        .await?;
    progress.ok_or_else(|| "native expiration progress missing".into())
}

async fn lose_destination_ack<S, P>(
    scheduler: &S,
    destination: Arc<P>,
    fault: Arc<NativeCommitFault>,
) -> HarnessResult<(ClaimedJob, TaskProgress<ExpirationProgress>)>
where
    S: ScheduledJobStoreSource,
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let (original, successor, first) = prepare_claim(
        scheduler,
        destination.clone(),
        destination.clone(),
        Namespace::try_new("native-destination-expiration")?,
    )
    .await?;
    let _outcome = original
        .execute_claimed(
            &first,
            first.context().ok_or("claim context missing")?.wall_now(),
            || MonotonicDeadline::ZERO,
        )
        .await?;
    let prior = load_progress(destination.as_ref()).await?;
    let claim = take_over(&successor, &first).await?;
    fault.arm();
    let outcome = successor
        .execute_claimed(
            &claim,
            claim.context().ok_or("claim context missing")?.wall_now(),
            || MonotonicDeadline::ZERO,
        )
        .await?;
    assert!(matches!(outcome, JobOutcome::Retry { code, .. } if code.as_str() == "handler_failed"));
    assert!(
        fault.was_observed(),
        "native durable acknowledgement was not interrupted"
    );
    let durable = load_progress(destination.as_ref()).await?;
    assert!(durable.authority.scope == prior.authority.scope);
    assert_eq!(durable.authority.job, prior.authority.job);
    assert_eq!(
        durable.authority.fence,
        claim.context().ok_or("successor context missing")?.fence()
    );
    assert!(durable.revision > prior.revision);
    Ok((first, durable))
}

async fn verify_reopened_progress<P>(
    destination: P,
    first: &ClaimedJob,
    durable: &TaskProgress<ExpirationProgress>,
) -> HarnessResult<()>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let destination = Arc::new(destination);
    assert!(
        &load_progress(destination.as_ref()).await? == durable,
        "reopen lost acknowledged progress"
    );
    verify_stale_progress(destination, first, durable).await
}

#[cfg(any(feature = "native-postgres", feature = "native-foundationdb"))]
enum NativeOverlap {
    #[cfg(feature = "native-postgres")]
    Blocking(tokio_postgres::Client),
    #[cfg(feature = "native-foundationdb")]
    Optimistic,
}

#[cfg(any(feature = "native-postgres", feature = "native-foundationdb"))]
async fn verify_overlapping_exclusion<S, P>(
    scheduler: &S,
    destinations: (Arc<P>, Arc<P>),
    hold: (Arc<BeforeCommitHold>, oneshot::Sender<()>),
    overlap: NativeOverlap,
) -> HarnessResult<()>
where
    S: ScheduledJobStoreSource,
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let (destination, successor_destination) = destinations;
    let (original, successor, first) = prepare_claim(
        scheduler,
        destination.clone(),
        successor_destination.clone(),
        Namespace::try_new("native-destination-expiration")?,
    )
    .await?;
    let (hold, release) = hold;
    hold.allow_independent_commits();
    hold.arm();
    let earlier = original.execute_claimed(
        &first,
        first.context().ok_or("claim context missing")?.wall_now(),
        || MonotonicDeadline::ZERO,
    );
    let reclaim = async {
        tokio::time::timeout(Duration::from_secs(10), hold.wait_ready()).await?;
        let claim = take_over(&successor, &first).await?;
        let later = successor.execute_claimed(
            &claim,
            claim.context().ok_or("claim context missing")?.wall_now(),
            || MonotonicDeadline::ZERO,
        );
        let acknowledged_before_release = match overlap {
            #[cfg(feature = "native-foundationdb")]
            NativeOverlap::Optimistic => {
                // Higher progress commits before the old native commit is
                // released.
                let _outcome = later.await?;
                let acknowledged = load_progress(successor_destination.as_ref()).await?;
                assert_eq!(
                    acknowledged.authority.fence,
                    claim.context().ok_or("successor context missing")?.fence()
                );
                release
                    .send(())
                    .map_err(|_| "older transaction stopped before successor acknowledgement")?;
                Some(acknowledged)
            }
            #[cfg(feature = "native-postgres")]
            NativeOverlap::Blocking(observer) => {
                tokio::pin!(later);
                tokio::select! {
                    _result = &mut later => return Err("successor settled before its native block".into()),
                    result = super::protected_read_tests::wait_for_native_block(&observer) => result?,
                }
                // Native blocking is observed, rather than inferred from a
                // delay.
                release
                    .send(())
                    .map_err(|_| "older transaction stopped before native block observation")?;
                let _outcome = later.await?;
                None
            }
        };
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((claim, acknowledged_before_release))
    };
    let (earlier, claim) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(earlier, reclaim)
    })
    .await?;
    let outcome = earlier?;
    let (claim, acknowledged_before_release) = claim?;
    let acknowledged = load_progress(successor_destination.as_ref()).await?;
    let old = first.context().ok_or("first context missing")?;
    assert_eq!(acknowledged.authority.scope.destination, *old.namespace());
    assert_eq!(acknowledged.authority.job, old.job_id());
    assert_eq!(
        acknowledged.authority.fence,
        claim.context().ok_or("successor context missing")?.fence()
    );
    if let Some(before_release) = acknowledged_before_release {
        assert!(
            acknowledged == before_release,
            "old conflicting attempt altered successor progress"
        );
        assert!(
            matches!(outcome, JobOutcome::Continue { not_before, .. } if not_before == old.wall_now())
        );
    } else {
        let completed = old.wall_now().checked_add(Duration::from_secs(60))?;
        assert!(
            matches!(outcome, JobOutcome::Continue { not_before, .. } if not_before == completed)
        );
    }
    verify_stale_progress(destination, &first, &acknowledged).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_destination_write_is_held_when_successor_runs_then_native_block_precedes_acknowledged_exclusion()
-> HarnessResult<()> {
    let scheduler = fixture::Postgres::try_new().await?;
    let destination = fixture::Postgres::try_new().await?;
    let result = async {
        let hold = BeforeCommitHold::new();
        let old = Arc::new(
            destination
                .open_independent_store_with_fault_probe(hold.0.clone())
                .await?,
        );
        let successor = Arc::new(destination.open_independent_store().await?);
        verify_overlapping_exclusion(
            &scheduler.store,
            (old, successor),
            hold,
            NativeOverlap::Blocking(destination.connect().await?),
        )
        .await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_destination_write_is_held_when_successor_commits_then_conflicting_old_write_cannot_replace_progress()
-> HarnessResult<()> {
    let scheduler = super::foundationdb::FoundationDb::try_new().await?;
    let destination = super::foundationdb::FoundationDb::try_new().await?;
    let result = async {
        let hold = BeforeCommitHold::new();
        let old = Arc::new(
            super::foundationdb::FoundationDb::open_store(
                &destination.cluster_file,
                &destination.prefix,
                Some(hold.0.clone()),
            )
            .await?,
        );
        let successor = Arc::new(destination.open_independent_store().await?);
        verify_overlapping_exclusion(
            &scheduler.store,
            (old, successor),
            hold,
            NativeOverlap::Optimistic,
        )
        .await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_destination_ack_is_lost_when_reopened_then_durable_progress_excludes_actual_stale_claim()
-> HarnessResult<()> {
    let scheduler = fixture::FileSqlite::try_new().await?;
    let destination = fixture::FileSqlite::try_new().await?;
    drop(destination.store);
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let store = Arc::new(
        fixture::FileSqlite::open_store_with_fault_probe(&destination.path, fault.clone()).await?,
    );
    let (first, durable) = lose_destination_ack(&scheduler.store, store, fault).await?;
    verify_reopened_progress(
        fixture::FileSqlite::open_store(&destination.path).await?,
        &first,
        &durable,
    )
    .await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_destination_ack_is_lost_when_reopened_then_durable_progress_excludes_actual_stale_claim()
-> HarnessResult<()> {
    let scheduler = fixture::Postgres::try_new().await?;
    let destination = fixture::Postgres::try_new().await?;
    let result = async {
        let fault = Arc::new(NativeCommitFault::new(
            NativeCommitBoundary::AfterDurableAcknowledgement,
        ));
        let store = Arc::new(
            destination
                .open_independent_store_with_fault_probe(fault.clone())
                .await?,
        );
        let (first, durable) = lose_destination_ack(&scheduler.store, store, fault).await?;
        verify_reopened_progress(
            destination.open_independent_store().await?,
            &first,
            &durable,
        )
        .await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_destination_ack_is_lost_when_reopened_then_durable_progress_excludes_actual_stale_claim()
-> HarnessResult<()> {
    let scheduler = super::turso::FileTurso::try_new().await?;
    let destination = super::turso::FileTurso::try_new().await?;
    drop(destination.store);
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let store = Arc::new(
        super::turso::FileTurso::open_store_with_fault_probe(&destination.path, fault.clone())
            .await?,
    );
    let (first, durable) = lose_destination_ack(&scheduler.store, store, fault).await?;
    verify_reopened_progress(
        super::turso::FileTurso::open_store(&destination.path).await?,
        &first,
        &durable,
    )
    .await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_destination_ack_is_lost_when_reopened_then_durable_progress_excludes_actual_stale_claim()
-> HarnessResult<()> {
    let scheduler = super::rocksdb::RocksDb::try_new().await?;
    let destination = super::rocksdb::RocksDb::try_new().await?;
    drop(destination.store);
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let store = Arc::new(
        super::rocksdb::RocksDb::open_store_with_fault_probe(&destination.path, fault.clone())
            .await?,
    );
    let (first, durable) = lose_destination_ack(&scheduler.store, store, fault).await?;
    verify_reopened_progress(
        super::rocksdb::RocksDb::open_store(&destination.path).await?,
        &first,
        &durable,
    )
    .await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_destination_ack_is_lost_when_reopened_then_durable_progress_excludes_actual_stale_claim()
-> HarnessResult<()> {
    let scheduler = super::foundationdb::FoundationDb::try_new().await?;
    let destination = super::foundationdb::FoundationDb::try_new().await?;
    let result = async {
        let fault = Arc::new(NativeCommitFault::new(
            NativeCommitBoundary::AfterDurableAcknowledgement,
        ));
        let store = Arc::new(
            super::foundationdb::FoundationDb::open_store(
                &destination.cluster_file,
                &destination.prefix,
                Some(fault.clone()),
            )
            .await?,
        );
        let (first, durable) = lose_destination_ack(&scheduler.store, store, fault).await?;
        verify_reopened_progress(
            destination.open_independent_store().await?,
            &first,
            &durable,
        )
        .await
    }
    .await;
    destination.finish().await?;
    scheduler.finish().await?;
    result
}
