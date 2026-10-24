use std::{collections::HashMap, sync::Arc};

use queue_provider::{
    ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageRequest, MessageId,
    PublishMessageRequest, PurgeQueueRequest, QueueOperations, ReceiveMessageRequest,
    SendMessageRequest,
};
use storage_types::TimestampMillis;

use crate::{
    QueueManager, create_queue_provider, queue_test_support_tests::selected_native_backend_case,
};

struct PublicationFixture {
    provider: Arc<dyn QueueOperations>,
    manager: Arc<QueueManager>,
    queue_url: String,
    id: MessageId,
}

impl PublicationFixture {
    async fn new() -> Self {
        let case = selected_native_backend_case("queue-publication");
        let provider: Arc<dyn QueueOperations> =
            Arc::from(create_queue_provider(case.config).await.unwrap());
        let manager = Arc::new(QueueManager::new(provider.clone()));
        let queue_url = manager
            .create_queue(CreateQueueRequest {
                queue_name: crate::queue_test_support_tests::unique_suffix("stable-publication"),
                attributes: Some(HashMap::from([(
                    "MessageRetentionPeriod".into(),
                    "60".into(),
                )])),
            })
            .await
            .unwrap()
            .queue_url;
        Self {
            provider,
            manager,
            queue_url,
            id: MessageId::from_bytes([7; queue_provider::MESSAGE_ID_VERSIONSTAMP_LEN]),
        }
    }
    fn request(&self, body: &str) -> PublishMessageRequest {
        PublishMessageRequest {
            message_id: self.id,
            message: SendMessageRequest {
                queue_url: self.queue_url.clone(),
                message_body: body.into(),
                delay_seconds: None,
                message_attributes: None,
            },
        }
    }
    async fn receive(&self) -> Vec<queue_provider::MessageResponse> {
        self.manager
            .receive_message(ReceiveMessageRequest {
                queue_url: self.queue_url.clone(),
                max_number_of_messages: Some(10),
                visibility_timeout: Some(60),
                wait_time_seconds: Some(0),
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .unwrap()
            .messages
    }
}

#[tokio::test]
async fn given_one_stable_publication_when_many_callers_publish_then_one_pending_message_is_retained_without_resetting_its_receipt()
 {
    let fixture = PublicationFixture::new().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let manager = fixture.manager.clone();
        let request = fixture.request("immutable work");
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            manager.publish_message(request).await.unwrap().message_id
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), fixture.id);
    }
    let messages = fixture.receive().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, fixture.id.to_string());
    for _ in 0..8 {
        fixture
            .manager
            .publish_message(fixture.request("immutable work"))
            .await
            .unwrap();
    }
    assert!(
        fixture.receive().await.is_empty(),
        "publication reset a pending visibility deadline"
    );
    fixture
        .manager
        .delete_message(DeleteMessageRequest {
            queue_url: fixture.queue_url.clone(),
            receipt_handle: messages[0].receipt_handle.as_str().into(),
        })
        .await
        .unwrap();
    fixture
        .manager
        .publish_message(fixture.request("immutable work"))
        .await
        .unwrap();
    assert_eq!(
        fixture.receive().await.len(),
        1,
        "retained source could not repair a missing queue message"
    );
}

#[tokio::test]
async fn given_pending_publication_when_body_or_attributes_change_then_identity_conflicts_without_replacing_original_work()
 {
    let fixture = PublicationFixture::new().await;
    fixture
        .manager
        .publish_message(fixture.request("original"))
        .await
        .unwrap();
    let error = fixture
        .manager
        .publish_message(fixture.request("changed"))
        .await
        .unwrap_err();
    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
    let mut changed = fixture.request("original");
    changed.message.message_attributes = Some(HashMap::from([(
        "kind".into(),
        queue_provider::MessageAttributeValue {
            string_value: Some("changed".into()),
            binary_value: None,
            data_type: "String".into(),
        },
    )]));
    assert!(fixture.manager.publish_message(changed).await.is_err());
    let messages = fixture.receive().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "original");
    assert!(messages[0].message_attributes.is_none());
}

#[tokio::test]
async fn given_expired_native_message_when_its_source_publishes_again_then_one_fresh_delivery_replaces_the_expired_record()
 {
    let fixture = PublicationFixture::new().await;
    let mut expired = queue_provider::queue_message_for_send(
        fixture.request("work").message,
        TimestampMillis::from_timestamp(TimestampMillis::now().timestamp_millis() - 61_000),
    );
    expired.message_id = fixture.id;
    fixture.provider.send_message(expired).await.unwrap();
    fixture
        .manager
        .publish_message(fixture.request("work"))
        .await
        .unwrap();
    let messages = fixture.receive().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, fixture.id.to_string());
}

#[tokio::test]
async fn given_purged_queue_when_retained_source_publishes_then_its_new_incarnation_accepts_one_message()
 {
    let fixture = PublicationFixture::new().await;
    fixture
        .manager
        .publish_message(fixture.request("work"))
        .await
        .unwrap();
    fixture
        .manager
        .purge_queue(PurgeQueueRequest {
            queue_url: fixture.queue_url.clone(),
        })
        .await
        .unwrap();
    fixture
        .manager
        .publish_message(fixture.request("work"))
        .await
        .unwrap();
    assert_eq!(fixture.receive().await.len(), 1);
}

#[tokio::test]
async fn given_existing_visibility_extension_when_relay_publishes_then_the_original_receipt_remains_valid()
 {
    let fixture = PublicationFixture::new().await;
    fixture
        .manager
        .publish_message(fixture.request("work"))
        .await
        .unwrap();
    let message = fixture.receive().await.pop().unwrap();
    fixture
        .manager
        .change_message_visibility(ChangeMessageVisibilityRequest {
            queue_url: fixture.queue_url.clone(),
            receipt_handle: message.receipt_handle.as_str().into(),
            visibility_timeout: 600,
        })
        .await
        .unwrap();
    fixture
        .manager
        .publish_message(fixture.request("work"))
        .await
        .unwrap();
    assert!(fixture.receive().await.is_empty());
    fixture
        .manager
        .delete_message(DeleteMessageRequest {
            queue_url: fixture.queue_url.clone(),
            receipt_handle: message.receipt_handle.as_str().into(),
        })
        .await
        .unwrap();
    assert!(fixture.receive().await.is_empty());
}

#[cfg(feature = "sqlite")]
async fn read_publication_record(
    store: &sql_backend::SqlStore<sql_backend::sqlite::SqliteBackend>,
    queue_url: String,
    id: MessageId,
) -> queue_provider::QueueMessageRecord {
    use queue_provider::{QueueAttempt, QueuePort};
    store
        // Protected point reads require a write-capable native attempt.
        .with_queue_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let incarnation = queue_provider::QueueOperation::load(attempt, &queue_url)
                    .await?
                    .unwrap()
                    .incarnation();
                Ok(attempt
                    .read_messages_by_id(incarnation, vec![id])
                    .await?
                    .pop()
                    .flatten()
                    .unwrap())
            })
        })
        .await
        .unwrap()
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_pending_message_with_checkpoint_when_published_again_then_native_receipt_expiry_timestamp_and_checkpoint_are_unchanged()
 {
    let backend = sql_backend::sqlite::SqliteBackend::initialize(":memory:")
        .await
        .unwrap();
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(backend)
            .await
            .unwrap(),
    );
    let manager = QueueManager::new(store.clone());
    let queue_url = manager
        .create_queue(CreateQueueRequest {
            queue_name: "checkpoint-publication".into(),
            attributes: None,
        })
        .await
        .unwrap()
        .queue_url;
    let id = MessageId::from_bytes([8; queue_provider::MESSAGE_ID_VERSIONSTAMP_LEN]);
    let request = || PublishMessageRequest {
        message_id: id,
        message: SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body: "checkpoint work".into(),
            delay_seconds: None,
            message_attributes: None,
        },
    };
    manager.publish_message(request()).await.unwrap();
    let message = manager
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(60),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap()
        .messages
        .pop()
        .unwrap();
    manager
        .update_message_snapshot(queue_provider::UpdateMessageSnapshotRequest {
            queue_url: queue_url.clone(),
            receipt_handle: message.receipt_handle,
            checkpoint_data: "durable progress".into(),
        })
        .await
        .unwrap();
    let before = read_publication_record(&store, queue_url.clone(), id).await;
    manager.publish_message(request()).await.unwrap();
    let after = read_publication_record(&store, queue_url, id).await;
    assert_eq!(before.checkpoint_data.as_deref(), Some("durable progress"));
    assert_eq!(after.checkpoint_data, before.checkpoint_data);
    assert_eq!(after.expires_at, before.expires_at);
    assert_eq!(
        after
            .message
            .receipt_handle
            .as_ref()
            .map(|handle| &handle.0),
        before
            .message
            .receipt_handle
            .as_ref()
            .map(|handle| &handle.0)
    );
    assert_eq!(
        after.message.visibility_timestamp,
        before.message.visibility_timestamp
    );
    assert_eq!(after.message.created_at, before.message.created_at);
}

#[tokio::test]
async fn given_equal_repeated_source_work_when_stable_publication_replaces_random_sends_then_pending_rows_drop_without_losing_identities()
 {
    let baseline = PublicationFixture::new().await;
    let candidate = PublicationFixture::new().await;
    for fixture in [&baseline, &candidate] {
        fixture
            .manager
            .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
                queue_url: fixture.queue_url.clone(),
                attributes: HashMap::from([("MessageRetentionPeriod".into(), "345600".into())]),
            })
            .await
            .unwrap();
    }
    for _ in 0..8 {
        for index in 1..=100u8 {
            let body = format!("source-{index}");
            baseline
                .manager
                .send_message(baseline.request(&body).message)
                .await
                .unwrap();
            let mut request = candidate.request(&body);
            let mut identity = [7; queue_provider::MESSAGE_ID_VERSIONSTAMP_LEN];
            identity[0] = index;
            request.message_id = MessageId::from_bytes(identity);
            candidate.manager.publish_message(request).await.unwrap();
        }
    }
    let mut pending = Vec::new();
    for fixture in [&baseline, &candidate] {
        let attributes = fixture
            .manager
            .get_queue_attributes(queue_provider::GetQueueAttributesRequest {
                queue_url: fixture.queue_url.clone(),
                attribute_names: Some(vec!["ApproximateNumberOfMessages".into()]),
            })
            .await
            .unwrap()
            .attributes;
        pending.push(
            attributes["ApproximateNumberOfMessages"]
                .parse::<u64>()
                .unwrap(),
        );
    }
    assert_eq!(pending, [800, 100]);
    let mut delivered = std::collections::BTreeSet::new();
    for _ in 0..10 {
        delivered.extend(
            candidate
                .receive()
                .await
                .into_iter()
                .map(|message| message.body),
        );
    }
    assert_eq!(
        delivered,
        (1..=100u8).map(|index| format!("source-{index}")).collect()
    );

    println!(
        "queue_publication_equal_work={}",
        serde_json::json!({"source_intents":100,"publication_attempts_per_variant":800,"baseline_pending":pending[0],"candidate_pending":pending[1]})
    );
}
