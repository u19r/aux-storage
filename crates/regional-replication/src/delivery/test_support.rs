use std::{sync::Arc, time::Duration};

use bg_job_store::{ScheduledJobStore, ScheduledJobStoreSource};
use bg_jobs_tokio::{
    JobRuntime,
    test_support::{TestClock, runtime_builder},
};
use replication_provider::runtime_configuration::deployment::QualifiedConfiguration;
use scheduled_bg_jobs::{
    DriveBudget, DriveBudgetInput, JobCatalog, Namespace, OwnerId, QueueSelection,
};

use super::{DeliveryJob, DeliveryScope};
use crate::test_support as fixture;

pub(crate) fn runtime(
    source: Arc<fixture::Store>,
    configuration: Arc<QualifiedConfiguration>,
    worker: (Arc<TestClock>, u8),
) -> JobRuntime<ScheduledJobStore> {
    let now = bg_jobs_tokio::test_support::JobClock::wall_now(worker.0.as_ref());
    let handler = DeliveryJob::try_new(
        Arc::clone(&source),
        fixture::journal(1),
        fixture::codec(),
        crate::transport::TcpTransport::try_new(configuration).unwrap(),
    )
    .unwrap();
    let mut catalog = JobCatalog::new();
    handler.register(&mut catalog, now).unwrap();
    let budget = DriveBudget::try_from(DriveBudgetInput {
        queues: QueueSelection::all(),
        max_claims: 1,
        max_concurrency: 1,
        max_store_operations: 128,
        stop_claiming_after: Duration::from_secs(1),
        finish_after: Duration::from_secs(2),
    })
    .unwrap();
    runtime_builder(
        source.scheduled_job_store(Default::default()).unwrap(),
        catalog,
        budget,
    )
    .namespace(Namespace::try_new("regional-delivery-test").unwrap())
    .owner(OwnerId::from([worker.1; 16]))
    .clock(worker.0)
    .build()
    .unwrap()
}

pub(crate) async fn drive_through(
    runtimes: &[&JobRuntime<ScheduledJobStore>],
    source: &fixture::Store,
    scope: &DeliveryScope,
    through: u64,
    clock: &TestClock,
) {
    let through = fixture::source_position(source, 1, through).await;
    let mut observed = 0;
    let mut last_counts = None;
    for _ in 0..256 {
        // A worker must not advance the shared clock while another worker is
        // still delivering: that would consume its execution and lease budget.
        last_counts = Some(
            futures::future::try_join_all(runtimes.iter().map(|runtime| runtime.drive_once()))
                .await
                .unwrap(),
        );
        let progress = super::load_delivery_progress(source, fixture::journal(1), scope.clone())
            .await
            .unwrap();
        if u64::from(progress.through()) == through {
            return;
        }
        observed = u64::from(progress.through());
        clock.advance(Duration::from_millis(10));
    }
    panic!(
        "scheduled delivery did not finish its bounded inventory: expected={through}, \
         observed={observed}, last drive={last_counts:?}"
    );
}
