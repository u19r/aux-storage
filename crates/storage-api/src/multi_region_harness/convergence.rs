use std::{collections::HashSet, sync::Arc, time::Duration};

use storage_types::{StorageResult, TableName, TimestampMillis};
use tokio::{
    sync::{Semaphore, mpsc},
    time::Instant,
};

use super::simulation::SimulationHarness;

#[derive(Clone)]
pub(super) struct ConvergenceSampler {
    permits: Arc<Semaphore>,
    tx: mpsc::UnboundedSender<StorageResult<Option<f64>>>,
}

impl ConvergenceSampler {
    pub(super) fn new(
        max_in_flight_convergence_checks: usize,
    ) -> (Self, mpsc::UnboundedReceiver<StorageResult<Option<f64>>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                permits: Arc::new(Semaphore::new(max_in_flight_convergence_checks)),
                tx,
            },
            rx,
        )
    }

    pub(super) fn spawn<F>(&self, future: F)
    where F: std::future::Future<Output = StorageResult<Option<f64>>> + Send + 'static {
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            return;
        };
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tx.send(future.await);
        });
    }
}

pub(super) async fn wait_for_value_convergence(
    harness: Arc<SimulationHarness>,
    table_name: TableName,
    pk: String,
    sk: String,
    expected_value: String,
) -> StorageResult<Option<f64>> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    while Instant::now() < deadline {
        if harness
            .all_regions_match_value(&table_name, &pk, &sk, Some(expected_value.as_str()))
            .await?
        {
            return Ok(Some(started.elapsed().as_secs_f64() * 1_000.0));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(None)
}

pub(super) async fn wait_for_commit_watermark_convergence(
    harness: Arc<SimulationHarness>,
    source_region: String,
    source_commit_ts: TimestampMillis,
) -> StorageResult<Option<f64>> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    while Instant::now() < deadline {
        if harness
            .all_replica_regions_applied_commit(&source_region, source_commit_ts)
            .await?
        {
            return Ok(Some(started.elapsed().as_secs_f64() * 1_000.0));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(None)
}

pub(super) async fn collect_divergent_keys(
    harness: &SimulationHarness,
    table_name: &TableName,
    touched_keys: &HashSet<String>,
) -> StorageResult<Vec<String>> {
    let mut divergent = Vec::new();
    for key in touched_keys {
        let Some((pk, sk)) = key.split_once('/') else {
            continue;
        };
        let baseline = harness
            .get_item_value(&harness.region_names()[0], table_name, pk, sk)
            .await?;
        for region in harness.region_names().iter().skip(1) {
            let candidate = harness.get_item_value(region, table_name, pk, sk).await?;
            if candidate != baseline {
                divergent.push(key.clone());
                break;
            }
        }
    }
    divergent.sort();
    Ok(divergent)
}
