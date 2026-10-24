use std::{collections::HashMap, time::Instant};

use queue_provider::{
    CreateQueueRequest, GetQueueAttributesRequest, QueueMessageCounts, SendMessageRequest,
};

use crate::{
    manager::selected_queue_attributes, queue_test_support_tests::create_selected_test_service,
};

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
fn given_stored_attributes_when_attribute_projection_then_defaults_counts_and_custom_values_are_preserved()
 {
    let stored = HashMap::from([
        ("VisibilityTimeout".to_string(), "77".to_string()),
        ("Custom".to_string(), "stored".to_string()),
        (
            "ApproximateNumberOfMessages".to_string(),
            "stale".to_string(),
        ),
    ]);
    let counts = QueueMessageCounts {
        visible: 3,
        not_visible: 5,
        delayed: 7,
    };
    let all = selected_queue_attributes(stored.clone(), counts.clone(), None);
    assert_eq!(all.len(), 9);
    assert_eq!(all["VisibilityTimeout"], "77");
    assert_eq!(all["DelaySeconds"], "0");
    assert_eq!(all["ApproximateNumberOfMessages"], "3");
    assert_eq!(all["ApproximateNumberOfMessagesNotVisible"], "5");
    assert_eq!(all["ApproximateNumberOfMessagesDelayed"], "7");
    for requested in [
        vec![],
        names(&["Unknown"]),
        names(&["VisibilityTimeout", "VisibilityTimeout"]),
        names(&[
            "Custom",
            "DelaySeconds",
            "ApproximateNumberOfMessagesDelayed",
            "Unknown",
        ]),
        names(&["All"]),
        names(&["Unknown", "All", "DelaySeconds"]),
    ] {
        let expected = if requested.iter().any(|name| name == "All") {
            all.clone()
        } else {
            all.iter()
                .filter(|(name, _)| requested.contains(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect()
        };
        assert_eq!(
            selected_queue_attributes(stored.clone(), counts.clone(), Some(&requested)),
            expected
        );
    }
}

#[tokio::test]
async fn given_missing_queue_when_attribute_projection_then_every_projection_returns_not_found() {
    let manager = create_selected_test_service("attribute-projection-missing").await;
    for requested in [
        None,
        Some(vec![]),
        Some(names(&["Unknown"])),
        Some(names(&["VisibilityTimeout"])),
        Some(names(&["All"])),
        Some(names(&["ApproximateNumberOfMessages"])),
    ] {
        let result = manager
            .get_queue_attributes(GetQueueAttributesRequest {
                queue_url: "http://localhost/missing".to_string(),
                attribute_names: requested,
            })
            .await;
        assert!(
            matches!(
                result,
                Err(queue_provider::QueueError::ResourceNotFound { .. })
            ),
            "{result:?}"
        );
    }
}

#[tokio::test]
#[ignore = "operation measurement; run exclusively with --ignored --nocapture"]
async fn given_queue_depths_when_attribute_projection_then_measure_full_manager_operation() {
    let cases = [
        ("static", Some(names(&["VisibilityTimeout"]))),
        ("empty", Some(vec![])),
        ("unknown", Some(names(&["Unknown"]))),
        (
            "mixed",
            Some(names(&["DelaySeconds", "ApproximateNumberOfMessages"])),
        ),
        ("count", Some(names(&["ApproximateNumberOfMessages"]))),
        ("all", Some(names(&["All"]))),
        ("omitted", None),
    ];
    for depth in [0, 100, 10_000] {
        let manager = create_selected_test_service("attribute-projection-measure").await;
        let queue_url = manager
            .create_queue(CreateQueueRequest {
                queue_name: format!("attributes-{depth}"),
                attributes: None,
            })
            .await
            .unwrap()
            .queue_url;
        for _ in 0..depth {
            manager
                .send_message(SendMessageRequest {
                    queue_url: queue_url.clone(),
                    message_body: "x".repeat(1150),
                    delay_seconds: None,
                    message_attributes: None,
                })
                .await
                .unwrap();
        }
        for round in 0..7 {
            for offset in 0..cases.len() {
                let (label, requested) = &cases[(round + offset) % cases.len()];
                let request = GetQueueAttributesRequest {
                    queue_url: queue_url.clone(),
                    attribute_names: requested.clone(),
                };
                let first = manager.get_queue_attributes(request.clone()).await.unwrap();
                if first.attributes.contains_key("ApproximateNumberOfMessages") {
                    assert_eq!(
                        first.attributes["ApproximateNumberOfMessages"],
                        depth.to_string()
                    );
                }
                let start = Instant::now();
                for _ in 0..100 {
                    std::hint::black_box(
                        manager.get_queue_attributes(request.clone()).await.unwrap(),
                    );
                }
                println!(
                    "attribute_projection depth={depth} case={label} round={round} iterations=100 \
                     ns_per_op={}",
                    start.elapsed().as_nanos() / 100
                );
            }
        }
    }
}

#[derive(Default)]
struct AttributeProvider {
    should_fail_counts: bool,
    metadata_reads: std::sync::atomic::AtomicUsize,
    snapshot_reads: std::sync::atomic::AtomicUsize,
}

fn fixture_queue(queue_url: &str, visibility: &str) -> queue_provider::Queue {
    queue_provider::Queue {
        queue_name: "attributes".to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::from([("VisibilityTimeout".to_string(), visibility.to_string())]),
        created_at: storage_types::TimestampMillis::now(),
    }
}

#[async_trait::async_trait]
impl queue_provider::QueueOperations for AttributeProvider {
    async fn create_queue(
        &self,
        _: queue_provider::Queue,
    ) -> queue_provider::QueueResult<queue_provider::Queue> {
        unreachable!()
    }
    async fn get_queue(
        &self,
        url: &str,
    ) -> queue_provider::QueueResult<Option<queue_provider::Queue>> {
        self.metadata_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(fixture_queue(url, "11")))
    }
    async fn get_queue_with_message_counts(
        &self,
        url: &str,
    ) -> queue_provider::QueueResult<Option<(queue_provider::Queue, QueueMessageCounts)>> {
        self.snapshot_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.should_fail_counts {
            return Err(queue_provider::QueueError::internal(
                queue_provider::QueueInternalKind::MissingQueuePartitionState,
            ));
        }
        Ok(Some((
            fixture_queue(url, "22"),
            QueueMessageCounts {
                visible: 3,
                not_visible: 5,
                delayed: 7,
            },
        )))
    }
    async fn get_queue_by_name(
        &self,
        _: &str,
    ) -> queue_provider::QueueResult<Option<queue_provider::Queue>> {
        unreachable!()
    }
    async fn list_queues(
        &self,
        _: Option<&str>,
    ) -> queue_provider::QueueResult<Vec<queue_provider::Queue>> {
        unreachable!()
    }
    async fn delete_queue(&self, _: &str) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn purge_queue(&self, _: &str) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn set_queue_attributes(
        &self,
        _: &str,
        _: HashMap<String, String>,
    ) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn publish_message(
        &self,
        _: queue_provider::QueueMessage,
    ) -> queue_provider::QueueResult<queue_provider::MessageId> {
        unreachable!("attribute fixture does not publish messages")
    }

    async fn send_message(
        &self,
        _: queue_provider::QueueMessage,
    ) -> queue_provider::QueueResult<queue_provider::MessageId> {
        unreachable!()
    }
    async fn send_messages(
        &self,
        _: Vec<queue_provider::QueueMessage>,
    ) -> queue_provider::QueueResult<Vec<queue_provider::QueueResult<queue_provider::MessageId>>>
    {
        unreachable!()
    }
    async fn delete_messages(
        &self,
        _: &str,
        _: Vec<queue_provider::ReceiptHandle>,
    ) -> queue_provider::QueueResult<Vec<queue_provider::QueueResult<()>>> {
        unreachable!()
    }
    async fn change_message_visibilities(
        &self,
        _: &str,
        _: Vec<(
            queue_provider::ReceiptHandle,
            storage_types::DurationSeconds,
        )>,
    ) -> queue_provider::QueueResult<Vec<queue_provider::QueueResult<()>>> {
        unreachable!()
    }
    async fn receive_messages(
        &self,
        _: &str,
        _: u32,
        _: storage_types::DurationSeconds,
        _: storage_types::DurationSeconds,
    ) -> queue_provider::QueueResult<Vec<queue_provider::MessageResponse>> {
        unreachable!()
    }
    async fn delete_message(
        &self,
        _: &str,
        _: queue_provider::ReceiptHandle,
    ) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn change_message_visibility(
        &self,
        _: &str,
        _: queue_provider::ReceiptHandle,
        _: storage_types::DurationSeconds,
    ) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn update_message_snapshot_checkpoint(
        &self,
        _: &str,
        _: queue_provider::ReceiptHandle,
        _: String,
    ) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn given_attribute_projection_when_reading_attributes_then_only_demanded_counts_use_combined_snapshot()
 {
    for (requested, needs_counts) in [
        (Some(vec![]), false),
        (Some(names(&["Unknown"])), false),
        (
            Some(names(&["VisibilityTimeout", "VisibilityTimeout"])),
            false,
        ),
        (Some(names(&["All"])), true),
        (None, true),
        (Some(names(&["Unknown", "All"])), true),
        (Some(names(&["ApproximateNumberOfMessages"])), true),
        (
            Some(names(&["ApproximateNumberOfMessagesNotVisible"])),
            true,
        ),
        (
            Some(names(&[
                "ApproximateNumberOfMessagesDelayed",
                "VisibilityTimeout",
            ])),
            true,
        ),
    ] {
        let provider = std::sync::Arc::new(AttributeProvider::default());
        let manager = crate::QueueManager::new(provider.clone());
        let response = manager
            .get_queue_attributes(GetQueueAttributesRequest {
                queue_url: "http://localhost/attributes".to_string(),
                attribute_names: requested.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            provider
                .snapshot_reads
                .load(std::sync::atomic::Ordering::Relaxed),
            usize::from(needs_counts),
            "{requested:?}"
        );
        assert_eq!(
            provider
                .metadata_reads
                .load(std::sync::atomic::Ordering::Relaxed),
            usize::from(!needs_counts),
            "{requested:?}"
        );
        if let Some(value) = response.attributes.get("VisibilityTimeout") {
            assert_eq!(value, if needs_counts { "22" } else { "11" });
        }
        if let Some(value) = response
            .attributes
            .get("ApproximateNumberOfMessagesDelayed")
        {
            assert_eq!(value, "7");
        }
    }
}

#[tokio::test]
async fn given_failed_count_storage_when_attribute_projection_then_only_demanded_counts_fail() {
    for (requested, needs_counts) in [
        (Some(names(&["VisibilityTimeout"])), false),
        (Some(vec![]), false),
        (Some(names(&["Unknown"])), false),
        (None, true),
        (Some(names(&["All"])), true),
        (
            Some(names(&["ApproximateNumberOfMessages", "DelaySeconds"])),
            true,
        ),
    ] {
        let provider = std::sync::Arc::new(AttributeProvider {
            should_fail_counts: true,
            ..AttributeProvider::default()
        });
        let manager = crate::QueueManager::new(provider);
        let result = manager
            .get_queue_attributes(GetQueueAttributesRequest {
                queue_url: "http://localhost/attributes".to_string(),
                attribute_names: requested.clone(),
            })
            .await;
        assert_eq!(result.is_err(), needs_counts, "{requested:?}: {result:?}");
    }
}
