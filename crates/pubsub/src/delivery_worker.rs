//! Host-owned polling lifetime; accepted batches settle before worker drainage.
use std::{sync::Arc, time::Duration};

use pubsub_provider::{DeliveryClaimLimit, PubsubError, PubsubResult};
use tokio::{sync::watch, task::JoinHandle};

use super::manager::PubsubManager;

pub struct PubsubDeliveryWorker {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

struct DeliveryWorkerSettings {
    limit: usize,
    idle_interval: Duration,
    error_interval: Duration,
}

impl PubsubDeliveryWorker {
    pub fn start(
        manager: Arc<PubsubManager>,
        limit: usize,
        idle_interval: Duration,
        error_interval: Duration,
    ) -> PubsubResult<Self> {
        DeliveryClaimLimit::try_from(limit)?;
        if idle_interval.is_zero() || error_interval.is_zero() {
            return Err(PubsubError::storage(
                "pubsub worker polling intervals must be nonzero",
            ));
        }
        let settings = DeliveryWorkerSettings {
            limit,
            idle_interval,
            error_interval,
        };
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(run_worker(manager, settings, receiver));
        Ok(Self {
            stop,
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) -> PubsubResult<()> {
        self.stop.send_replace(true);
        let task = self
            .task
            .take()
            .ok_or_else(|| PubsubError::storage("pubsub worker drainage lost ownership"))?;
        task.await
            .map_err(|_| PubsubError::storage("pubsub delivery worker failed during drainage"))
    }
}

impl Drop for PubsubDeliveryWorker {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

async fn run_worker(
    manager: Arc<PubsubManager>,
    settings: DeliveryWorkerSettings,
    mut stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        let interval = match manager.process_due_deliveries(settings.limit).await {
            Ok(0) => settings.idle_interval,
            Ok(_) => Duration::ZERO,
            Err(_) => {
                tracing::warn!("pubsub delivery worker failed; retrying");
                settings.error_interval
            }
        };
        if *stop.borrow() {
            return;
        }
        tokio::select! {
            _ = stop.changed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}
