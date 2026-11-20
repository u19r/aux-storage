use std::sync::Arc;

use pubsub::{DeliveryRunner, DeliveryRunnerConfig, DeliveryWake, PubsubManager};
use queue::{QueueManager, QueueWakeHandle};
use storage_types::{MessagingIdentity, StorageError, StorageResult};

use super::settings::{MessagingConfig, PubsubServiceConfig};
use crate::DatabaseManager;

/// The SQS and SNS services of one process, over the default connection.
pub struct MessagingServices {
    /// The SQS route manager; `Some` when the queue service is configured.
    pub queue: Option<Arc<QueueManager>>,
    /// The SNS route manager; `Some` when the pubsub service is configured.
    pub pubsub: Option<Arc<PubsubManager>>,
    identity: MessagingIdentity,
    delivery: Option<DeliveryService>,
}

/// The manager the delivery runner drives and the runner limits.
struct DeliveryService {
    manager: Arc<PubsubManager>,
    runner: DeliveryRunnerConfig,
}

impl MessagingServices {
    /// Build the configured managers. Starts no task.
    pub fn build(database: &DatabaseManager, config: MessagingConfig) -> StorageResult<Self> {
        let MessagingConfig {
            queue,
            pubsub,
            identity,
        } = config;
        let stores = database.default_messaging_stores();
        let queue = match queue {
            None => None,
            Some(endpoint) => {
                let stores = stores.ok_or_else(|| {
                    StorageError::validation(
                        "queue.enabled is true but the configured backend does not expose a queue \
                         provider",
                    )
                })?;
                // Customer queue requests take one admission permit per
                // storage attempt, never for the time a receive waits.
                Some(Arc::new(
                    QueueManager::new(Arc::clone(&stores.customer.queue))
                        .with_messaging_identity(identity.clone())
                        .with_endpoint(endpoint),
                ))
            }
        };
        let Some(settings) = pubsub else {
            return Ok(Self {
                queue,
                pubsub: None,
                identity,
                delivery: None,
            });
        };
        let stores = stores.ok_or_else(|| {
            StorageError::validation(
                "pubsub.enabled is true but the configured backend does not expose a pubsub \
                 provider",
            )
        })?;
        // One wake for the route manager and the delivery manager: a publish
        // on this node starts its runner at once (W3 design 4.4). Delivery
        // commits wake the waiting receives of this process's queue manager.
        let wake = DeliveryWake::default();
        let queue_wake = queue.as_ref().map(|queue| queue.wake_handle());
        let route = pubsub_manager(
            Arc::clone(&stores.customer.pubsub),
            &identity,
            &settings,
            &wake,
            queue_wake.clone(),
        )?;
        let delivery = pubsub_manager(
            Arc::clone(&stores.delivery),
            &identity,
            &settings,
            &wake,
            queue_wake,
        )?;
        Ok(Self {
            queue,
            pubsub: Some(route),
            identity,
            delivery: Some(DeliveryService {
                manager: delivery,
                runner: settings.runner,
            }),
        })
    }

    /// Start the SNS delivery runner of this process and hand it to the
    /// background services of `database`, which stop it first in
    /// `shutdown_background_services`. Does nothing when pubsub is not
    /// configured. Call it once per process, when the host is ready.
    pub async fn start_delivery(&self, database: &DatabaseManager) -> StorageResult<()> {
        let Some(delivery) = &self.delivery else {
            return Ok(());
        };
        let runner = DeliveryRunner::start(Arc::clone(&delivery.manager), delivery.runner)
            .map_err(|_| StorageError::internal("failed to start pubsub delivery runner"))?;
        database.publish_delivery_runner(runner).await
    }

    /// The partition, region and account of queue and topic ARNs.
    #[must_use]
    pub fn identity(&self) -> &MessagingIdentity {
        &self.identity
    }
}

fn pubsub_manager(
    provider: Arc<dyn pubsub_provider::PubsubProvider>,
    identity: &MessagingIdentity,
    settings: &PubsubServiceConfig,
    wake: &DeliveryWake,
    queue_wake: Option<QueueWakeHandle>,
) -> StorageResult<Arc<PubsubManager>> {
    let builder = PubsubManager::builder()
        .provider(provider)
        .messaging_identity(identity.clone())
        .delivery_config(settings.delivery.clone())
        .delivery_wake(wake.clone());
    let builder = match queue_wake {
        Some(queue_wake) => builder.queue_wake(queue_wake),
        None => builder,
    };
    builder
        .build()
        .map(Arc::new)
        .map_err(|_| StorageError::internal("failed to initialize pubsub manager"))
}
