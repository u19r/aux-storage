use std::collections::HashMap;

use queue_provider::{GetQueueAttributesRequest, QueueMessageCounts};

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
    let all = selected_queue_attributes(stored.clone(), counts.clone(), &names(&["All"]));
    assert_eq!(all.len(), 10);
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
            selected_queue_attributes(stored.clone(), counts.clone(), &requested),
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
        ..Default::default()
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
    async fn list_queue_page(
        &self,
        _: Option<&str>,
        _: Option<&str>,
        _: std::num::NonZeroU32,
    ) -> queue_provider::QueueResult<(Vec<queue_provider::Queue>, Option<String>)> {
        unreachable!()
    }
    async fn delete_queue(&self, _: &str) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn change_queue_tags(
        &self,
        _: &str,
        _: std::collections::BTreeMap<String, String>,
        _: Vec<String>,
    ) -> queue_provider::QueueResult<()> {
        unreachable!()
    }
    async fn list_queue_tags(
        &self,
        _: &str,
    ) -> queue_provider::QueueResult<Option<std::collections::BTreeMap<String, String>>> {
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
    async fn list_dead_letter_source_queues(
        &self,
        _: &str,
        _: Option<&str>,
        _: std::num::NonZeroU32,
    ) -> queue_provider::QueueResult<Option<queue_provider::ListDeadLetterSourceQueuesResponse>>
    {
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
        _: Option<storage_types::DurationSeconds>,
        _: Option<storage_types::DurationSeconds>,
    ) -> queue_provider::QueueResult<queue_provider::ReceivedMessages> {
        unreachable!()
    }
    async fn probe_receive(
        &self,
        _: &str,
    ) -> queue_provider::QueueResult<Option<queue_provider::ReceiveProbe>> {
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
        (
            Some(names(&["VisibilityTimeout", "VisibilityTimeout"])),
            false,
        ),
        (Some(names(&["All"])), true),
        // Probe 7: no names select nothing and need no count scan.
        (None, false),
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
        (None, false),
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
