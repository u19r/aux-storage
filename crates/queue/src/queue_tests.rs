#[cfg(test)]
use std::{collections::HashMap, time::Instant};

use queue_provider::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
    CreateQueueRequest, DeleteMessageRequest, GetQueueAttributesRequest, ReceiveMessageRequest,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageRequest,
};

use crate::{QueueManager, queue_test_support_tests::create_selected_test_service};

async fn create_test_service() -> QueueManager {
    create_selected_test_service("queue-tests").await
}

/// A shared database (Postgres, FoundationDB) keeps queues between runs, so
/// each run gets its own queue.
async fn create_test_queue(service: &QueueManager, queue_name: &str) -> String {
    service
        .create_queue(CreateQueueRequest {
            queue_name: crate::queue_test_support_tests::unique_suffix(queue_name),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url
}

#[tokio::test]
async fn manager_rejects_invalid_queue_name_before_storage() {
    let service = create_test_service().await;

    let error = service
        .create_queue(CreateQueueRequest {
            queue_name: "invalid.name".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
    assert!(
        error
            .aws_query_message()
            .contains("Can only include alphanumeric characters")
    );
}

#[tokio::test]
async fn manager_rejects_unsupported_queue_attributes_before_storage() {
    let service = create_test_service().await;
    let queue_url = create_test_queue(&service, "attribute-validation").await;

    let error = service
        .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
            queue_url,
            attributes: HashMap::from([("RedriveAllowPolicy".to_string(), "{}".to_string())]),
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
    assert_eq!(
        error.aws_query_message(),
        "RedriveAllowPolicy is not supported"
    );
}

#[tokio::test]
async fn manager_rejects_empty_message_body_before_storage() {
    let service = create_test_service().await;
    let queue_url = create_test_queue(&service, "message-validation").await;

    let error = service
        .send_message(SendMessageRequest {
            queue_url,
            message_body: String::new(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_message(),
        "The request must contain the parameter MessageBody."
    );
}

#[tokio::test]
async fn manager_rejects_duplicate_batch_entry_ids_before_partial_send() {
    let service = create_test_service().await;
    let queue_url = create_test_queue(&service, "batch-validation").await;

    let error = service
        .send_message_batch(SendMessageBatchRequest {
            queue_url: queue_url.clone(),
            entries: vec![
                SendMessageBatchRequestEntry {
                    id: "dup".to_string(),
                    message_body: "first".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                    ..Default::default()
                },
                SendMessageBatchRequestEntry {
                    id: "dup".to_string(),
                    message_body: "second".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                    ..Default::default()
                },
            ],
        })
        .await
        .unwrap_err();

    assert_eq!(error.aws_query_message(), "Id dup repeated.");
    let receive = service
        .receive_message(ReceiveMessageRequest {
            queue_url,
            max_number_of_messages: Some(10),
            visibility_timeout: None,
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert!(receive.messages.is_empty());
}

#[tokio::test]
async fn change_message_visibility_batch_returns_successes() {
    let service = create_test_service().await;
    let queue_url = create_test_queue(&service, "change-batch-queue").await;
    service
        .send_message(SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body: "change me".to_string(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let receive_response = service
        .receive_message(ReceiveMessageRequest {
            queue_url: queue_url.clone(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();

    let response = service
        .change_message_visibility_batch(ChangeMessageVisibilityBatchRequest {
            queue_url,
            entries: vec![ChangeMessageVisibilityBatchRequestEntry {
                id: "ok".to_string(),
                receipt_handle: receive_response.messages[0].receipt_handle.as_str().into(),
                visibility_timeout: 60,
            }],
        })
        .await
        .unwrap();

    assert_eq!(response.successful.len(), 1);
    assert!(response.failed.is_empty());
}

#[tokio::test]
async fn sqlite_send_receive_delete_handles_representative_local_volume() {
    let service = create_test_service().await;
    let queue_url = service
        .create_queue(CreateQueueRequest {
            queue_name: "volume-check-queue".to_string(),
            attributes: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;

    for index in 0..200usize {
        service
            .send_message(SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: format!("volume-check-{index}"),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let mut received_bodies = std::collections::HashSet::new();
    while received_bodies.len() < 200 {
        let response = service
            .receive_message(ReceiveMessageRequest {
                queue_url: queue_url.clone(),
                max_number_of_messages: Some(10),
                visibility_timeout: Some(30),
                wait_time_seconds: None,
                attribute_names: None,
                message_attribute_names: None,
            })
            .await
            .unwrap();
        assert!(
            !response.messages.is_empty(),
            "volume check should drain all sent messages"
        );

        for message in response.messages {
            assert!(
                received_bodies.insert(message.body),
                "volume check should not duplicate messages before timeout"
            );
            service
                .delete_message(DeleteMessageRequest {
                    queue_url: queue_url.clone(),
                    receipt_handle: message.receipt_handle.as_str().into(),
                })
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn given_private_queue_and_body_when_messages_are_sent_then_traces_omit_identifiers_and_payload()
 {
    use storage_observability_test_support::observation::observe;
    let service = create_test_service().await;
    let marker = "private-queue-log-marker";
    let body = "private-message-body-marker";
    let observed = observe(async {
        let queue_url = create_test_queue(&service, marker).await;
        let response = service
            .send_message(SendMessageRequest {
                queue_url,
                message_body: body.to_owned(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            })
            .await
            .unwrap();
        let failure = service
            .send_message(SendMessageRequest {
                queue_url: "private-missing-queue-marker".to_owned(),
                message_body: body.to_owned(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            })
            .await;
        assert!(failure.is_err());
        response.message_id
    })
    .await;
    assert!(!observed.spans.is_empty());
    let traces = format!("{:?}{:?}", observed.events, observed.spans);
    let message_id = observed.result.to_string();
    for private in [
        marker,
        body,
        "private-missing-queue-marker",
        message_id.as_str(),
    ] {
        assert!(
            !traces.contains(private),
            "private queue data entered tracing"
        );
    }
}

/// W12 through the manager: `QueueArn`, a policy set with a string count
/// reads back in normal form, the move, the source ARN on receive, and
/// `ListDeadLetterSourceQueues` with paging.
#[tokio::test]
async fn manager_moves_exhausted_message_to_dead_letter_queue_and_lists_its_source() {
    let service = create_test_service().await;
    let dlq = create_test_queue(&service, "dlq").await;
    let source = create_test_queue(&service, "dlq-source").await;
    let attributes = |queue_url: &str, names: &[&str]| GetQueueAttributesRequest {
        queue_url: queue_url.to_owned(),
        attribute_names: Some(names.iter().map(|name| (*name).to_owned()).collect()),
    };
    let dlq_arn = service
        .get_queue_attributes(attributes(&dlq, &["QueueArn"]))
        .await
        .unwrap()
        .attributes["QueueArn"]
        .clone();
    let source_arn = service
        .get_queue_attributes(attributes(&source, &["All"]))
        .await
        .unwrap()
        .attributes["QueueArn"]
        .clone();
    service
        .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
            queue_url: source.clone(),
            attributes: HashMap::from([(
                "RedrivePolicy".to_owned(),
                format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":\"1\"}}"),
            )]),
        })
        .await
        .unwrap();
    assert_eq!(
        service
            .get_queue_attributes(attributes(&source, &["RedrivePolicy"]))
            .await
            .unwrap()
            .attributes["RedrivePolicy"],
        format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":1}}")
    );
    service
        .send_message(SendMessageRequest {
            queue_url: source.clone(),
            message_body: "poison".to_owned(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let receive = |queue_url: &str| ReceiveMessageRequest {
        queue_url: queue_url.to_owned(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(0),
        wait_time_seconds: Some(0),
        attribute_names: Some(vec!["All".to_owned()]),
        message_attribute_names: None,
    };
    assert_eq!(
        service
            .receive_message(receive(&source))
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    assert!(
        service
            .receive_message(receive(&source))
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let moved = service
        .receive_message(receive(&dlq))
        .await
        .unwrap()
        .messages;
    assert_eq!(moved.len(), 1);
    let moved_attributes = moved[0].attributes.as_ref().unwrap();
    assert_eq!(moved_attributes["DeadLetterQueueSourceArn"], source_arn);
    assert_eq!(moved_attributes["ApproximateReceiveCount"], "2");

    let listed = service
        .list_dead_letter_source_queues(queue_provider::ListDeadLetterSourceQueuesRequest {
            queue_url: dlq.clone(),
            max_results: Some(1),
            next_token: None,
        })
        .await
        .unwrap();
    assert_eq!(listed.queue_urls, vec![source.clone()]);
    assert_eq!(listed.next_token, None);
    let error = service
        .list_dead_letter_source_queues(queue_provider::ListDeadLetterSourceQueuesRequest {
            queue_url: dlq,
            max_results: Some(0),
            next_token: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
}

/// Review A1 through the manager: two queues that are each other's
/// dead-letter queue with `maxReceiveCount = 1`. A long poll on the second
/// queue is woken by the move and receives the message; it does not move the
/// message back and wait on.
#[tokio::test]
async fn manager_long_poll_on_a_redrive_cycle_receives_the_moved_message() {
    let service = create_test_service().await;
    let first = create_test_queue(&service, "cycle-a").await;
    let second = create_test_queue(&service, "cycle-b").await;
    let arn = |queue_url: &str| {
        let service = &service;
        let queue_url = queue_url.to_owned();
        async move {
            service
                .get_queue_attributes(GetQueueAttributesRequest {
                    queue_url,
                    attribute_names: Some(vec!["QueueArn".to_owned()]),
                })
                .await
                .unwrap()
                .attributes["QueueArn"]
                .clone()
        }
    };
    for (source, target) in [(&first, &second), (&second, &first)] {
        let target_arn = arn(target).await;
        service
            .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
                queue_url: source.clone(),
                attributes: HashMap::from([(
                    "RedrivePolicy".to_owned(),
                    format!("{{\"deadLetterTargetArn\":\"{target_arn}\",\"maxReceiveCount\":1}}"),
                )]),
            })
            .await
            .unwrap();
    }
    service
        .send_message(SendMessageRequest {
            queue_url: first.clone(),
            message_body: "cycle".to_owned(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        })
        .await
        .unwrap();
    let receive = |queue_url: &str, wait: u32| ReceiveMessageRequest {
        queue_url: queue_url.to_owned(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(0),
        wait_time_seconds: Some(wait.into()),
        attribute_names: Some(vec!["All".to_owned()]),
        message_attribute_names: None,
    };
    let started = Instant::now();
    let (polled, ()) = tokio::join!(service.receive_message(receive(&second, 5)), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let delivered = service.receive_message(receive(&first, 0)).await.unwrap();
        assert_eq!(delivered.messages.len(), 1);
        let moved = service.receive_message(receive(&first, 0)).await.unwrap();
        assert!(moved.messages.is_empty(), "the second receive moves");
    });
    let polled = polled.unwrap().messages;
    assert_eq!(polled.len(), 1, "the long poll receives the moved message");
    assert_eq!(polled[0].body, "cycle");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(4),
        "the long poll ended on the move, not on its timeout"
    );
}
