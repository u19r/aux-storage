//! W3 test 15 (design 9.1) on the provider that `STORAGE_TEST_BACKEND`
//! selects: two runners with their own worker IDs drain one database.
//! Both runners use the one store handle of the fixture (a second handle on
//! one database is not available for every provider); their claims are
//! separate native attempts, so the claim fence decides every race.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use pubsub_provider::{PublishRequest, SubscribeRequest, SubscriptionProtocol};

use super::{pubsub_contract_tests::queue_arn, test_support::TestStore};

const PUBLISHES: usize = 100;

fn node(
    store: &TestStore,
    queues: &Arc<queue::QueueManager>,
    name: &str,
) -> Arc<pubsub::PubsubManager> {
    Arc::new(
        pubsub::PubsubManager::builder()
            .provider(store.pubsub_provider.clone())
            .queue_wake(queues.wake_handle())
            .delivery_config(pubsub::PubsubDeliveryConfig {
                worker_id: format!("{name}/{}", uuid::Uuid::now_v7()),
                ..pubsub::PubsubDeliveryConfig::default()
            })
            .build()
            .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_two_runners_when_200_deliveries_drain_then_each_message_arrives_once() {
    let store = TestStore::new().await;
    store.use_system_time();
    let queues = Arc::new(queue::QueueManager::new(store.queue_operations.clone()));
    let label = uuid::Uuid::new_v4().simple().to_string();
    let first = node(&store, &queues, "node-a");
    let second = node(&store, &queues, "node-b");
    let topic = first
        .create_topic(format!("runners-{label}"))
        .await
        .unwrap();
    let mut urls = Vec::new();
    for index in 0..2 {
        let name = format!("runners-{label}-{index}");
        urls.push(
            queues
                .create_queue(queue_provider::CreateQueueRequest {
                    queue_name: name.clone(),
                    attributes: None,
                    ..Default::default()
                })
                .await
                .unwrap()
                .queue_url,
        );
        first
            .subscribe(SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_arn(&name),
                attributes: HashMap::from([("RawMessageDelivery".to_owned(), "true".to_owned())]),
                extra_json: serde_json::Value::Null,
            })
            .await
            .unwrap();
    }
    let config = pubsub::DeliveryRunnerConfig {
        max_in_flight: NonZeroUsize::new(16).unwrap(),
        idle_probe: Duration::from_millis(50),
        sweep_interval: Duration::from_millis(50),
        ..pubsub::DeliveryRunnerConfig::default()
    };
    let runners = [
        pubsub::DeliveryRunner::start(first.clone(), config).unwrap(),
        pubsub::DeliveryRunner::start(second.clone(), config).unwrap(),
    ];
    for index in 0..PUBLISHES {
        let manager = if index % 2 == 0 { &first } else { &second };
        manager
            .publish(PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: format!("m-{index}"),
                subject: None,
                message_attributes: HashMap::new(),
            })
            .await
            .unwrap();
    }

    let mut bodies: Vec<Vec<String>> = vec![Vec::new(), Vec::new()];
    let drained = tokio::time::timeout(Duration::from_secs(60), async {
        while bodies.iter().map(Vec::len).sum::<usize>() < 2 * PUBLISHES {
            for (url, seen) in urls.iter().zip(bodies.iter_mut()) {
                let messages = store
                    .queue_operations
                    .receive_messages(
                        url,
                        10,
                        Some(storage_types::DurationSeconds::from(300)),
                        None,
                    )
                    .await
                    .unwrap()
                    .messages;
                seen.extend(messages.into_iter().map(|message| message.body));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let mut delivered = 0;
    for runner in runners {
        let stats = runner.stats();
        runner.shutdown().await.unwrap();
        delivered += stats.delivered.load(std::sync::atomic::Ordering::SeqCst);
    }
    drained.expect("both runners drained 200 deliveries");
    for seen in &bodies {
        let distinct: HashSet<&String> = seen.iter().collect();
        assert_eq!(seen.len(), PUBLISHES, "no duplicate in a queue");
        assert_eq!(distinct.len(), PUBLISHES);
    }
    assert_eq!(delivered, 2 * PUBLISHES as u64, "each record settled once");
    let heads = store
        .pubsub_provider
        .sweep_destination_heads(
            pubsub_provider::SweepCursor::from_start(None),
            std::num::NonZeroU32::new(256).unwrap(),
        )
        .await
        .unwrap()
        .heads;
    assert!(heads.is_empty(), "no delivery record is left");
    store.finish().await;
}
