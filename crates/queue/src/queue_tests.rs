#[cfg(test)]
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use queue_provider::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
    ChangeMessageVisibilityRequest, CreateQueueRequest, DeleteMessageBatchRequest,
    DeleteMessageBatchRequestEntry, DeleteMessageRequest, GetQueueAttributesRequest,
    MessageAttributeValue, QueueBackend, QueueConfig, ReceiveMessageRequest,
    SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageRequest,
};

use crate::{QueueManager, create_queue_provider};

async fn create_test_service() -> QueueManager {
    create_sqlite_service(":memory:").await
}

async fn create_sqlite_service(path: impl AsRef<Path>) -> QueueManager {
    let config = QueueConfig {
        backend_type: QueueBackend::SQLite,
        connection_string: Some(path.as_ref().to_string_lossy().to_string()),
        file_path: None,
        postgres: None,
        foundationdb: None,
        remote: None,
    };

    let storage = create_queue_provider(config).await.unwrap();
    storage.initialize().await.unwrap();

    QueueManager::new(Arc::from(storage))
}

fn local_sqlite_test_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    workspace_root
        .join("target")
        .join("queue-test-data")
        .join(format!("{label}-{nanos}-{}.sqlite3", std::process::id()))
}

#[tokio::test]
async fn create_queue() {
    let service = create_test_service().await;

    let request = CreateQueueRequest {
        queue_name: "test-queue".to_string(),
        attributes: None,
    };

    let response = service.create_queue(request).await.unwrap();
    assert_eq!(response.queue_url, "test-queue");
}

#[tokio::test]
async fn manager_rejects_invalid_queue_name_before_storage() {
    let service = create_test_service().await;

    let error = service
        .create_queue(CreateQueueRequest {
            queue_name: "invalid.name".to_string(),
            attributes: None,
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
    service
        .create_queue(CreateQueueRequest {
            queue_name: "attribute-validation".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let error = service
        .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
            queue_url: "attribute-validation".to_string(),
            attributes: HashMap::from([("RedrivePolicy".to_string(), "{}".to_string())]),
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_error_type(),
        queue_provider::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE
    );
    assert_eq!(
        error.aws_query_message(),
        "Unsupported queue attribute: RedrivePolicy"
    );
}

#[tokio::test]
async fn manager_rejects_empty_message_body_before_storage() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "message-validation".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let error = service
        .send_message(SendMessageRequest {
            queue_url: "message-validation".to_string(),
            message_body: String::new(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_message(),
        "The request must contain the parameter MessageBody."
    );
}

#[tokio::test]
async fn manager_rejects_receive_count_above_sqs_limit() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "receive-validation".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let error = service
        .receive_message(ReceiveMessageRequest {
            queue_url: "receive-validation".to_string(),
            max_number_of_messages: Some(11),
            visibility_timeout: None,
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap_err();

    assert_eq!(
        error.aws_query_message(),
        "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, \
         if provided."
    );
}

#[tokio::test]
async fn manager_rejects_duplicate_batch_entry_ids_before_partial_send() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "batch-validation".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let error = service
        .send_message_batch(SendMessageBatchRequest {
            queue_url: "batch-validation".to_string(),
            entries: vec![
                SendMessageBatchRequestEntry {
                    id: "dup".to_string(),
                    message_body: "first".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                },
                SendMessageBatchRequestEntry {
                    id: "dup".to_string(),
                    message_body: "second".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                },
            ],
        })
        .await
        .unwrap_err();

    assert_eq!(error.aws_query_message(), "Id dup repeated.");
    let receive = service
        .receive_message(ReceiveMessageRequest {
            queue_url: "batch-validation".to_string(),
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
async fn create_queue_is_idempotent_when_attributes_match() {
    let service = create_test_service().await;
    let mut attributes = HashMap::new();
    attributes.insert("VisibilityTimeout".to_string(), "30".to_string());

    service
        .create_queue(CreateQueueRequest {
            queue_name: "idempotent-queue".to_string(),
            attributes: Some(attributes.clone()),
        })
        .await
        .unwrap();
    let response = service
        .create_queue(CreateQueueRequest {
            queue_name: "idempotent-queue".to_string(),
            attributes: Some(attributes),
        })
        .await
        .unwrap();

    assert_eq!(response.queue_url, "idempotent-queue");
}

#[tokio::test]
async fn create_queue_rejects_existing_queue_with_different_attributes() {
    let service = create_test_service().await;

    service
        .create_queue(CreateQueueRequest {
            queue_name: "duplicate-queue".to_string(),
            attributes: Some(HashMap::from([(
                "VisibilityTimeout".to_string(),
                "30".to_string(),
            )])),
        })
        .await
        .unwrap();
    let result = service
        .create_queue(CreateQueueRequest {
            queue_name: "duplicate-queue".to_string(),
            attributes: Some(HashMap::from([(
                "VisibilityTimeout".to_string(),
                "60".to_string(),
            )])),
        })
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn send_message() {
    let service = create_test_service().await;
    let create_request = CreateQueueRequest {
        queue_name: "send-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    // Send a message
    let send_request = SendMessageRequest {
        queue_url: "send-queue".to_string(),
        message_body: "Hello, World!".to_string(),
        delay_seconds: None,
        message_attributes: None,
    };

    let response = service.send_message(send_request).await.unwrap();
    assert!(!response.message_id.to_string().is_empty());
    assert!(!response.md5_of_body.is_empty());
    assert_eq!(response.md5_of_body.len(), 32); // MD5 hash is 32 hex chars
}

#[tokio::test]
async fn receive_message() {
    let service = create_test_service().await;
    let create_request = CreateQueueRequest {
        queue_name: "receive-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    let send_request = SendMessageRequest {
        queue_url: "receive-queue".to_string(),
        message_body: "Test message".to_string(),
        delay_seconds: None,
        message_attributes: None,
    };
    service.send_message(send_request).await.unwrap();

    // Receive the message
    let receive_request = ReceiveMessageRequest {
        queue_url: "receive-queue".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: None,
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    };

    let response = service.receive_message(receive_request).await.unwrap();

    assert_eq!(response.messages.len(), 1);
    let message = &response.messages[0];
    assert_eq!(message.body, "Test message");
    assert!(!message.receipt_handle.is_empty());
    assert!(!message.message_id.clone().is_empty());
}

#[tokio::test]
async fn delete_message() {
    let service = create_test_service().await;
    let create_request = CreateQueueRequest {
        queue_name: "delete-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    let send_request = SendMessageRequest {
        queue_url: "delete-queue".to_string(),
        message_body: "Delete me".to_string(),
        delay_seconds: None,
        message_attributes: None,
    };
    service.send_message(send_request).await.unwrap();

    let receive_request = ReceiveMessageRequest {
        queue_url: "delete-queue".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: None,
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    };
    let receive_response = service.receive_message(receive_request).await.unwrap();
    let receipt_handle = &receive_response.messages[0].receipt_handle;
    let delete_request = DeleteMessageRequest {
        queue_url: "delete-queue".to_string(),
        receipt_handle: receipt_handle.as_str().into(),
    };

    let result = service.delete_message(delete_request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn change_message_visibility() {
    let service = create_test_service().await;
    let create_request = CreateQueueRequest {
        queue_name: "visibility-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    let send_request = SendMessageRequest {
        queue_url: "visibility-queue".to_string(),
        message_body: "Visibility test".to_string(),
        delay_seconds: None,
        message_attributes: None,
    };
    service.send_message(send_request).await.unwrap();

    let receive_request = ReceiveMessageRequest {
        queue_url: "visibility-queue".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(30),
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    };
    let receive_response = service.receive_message(receive_request).await.unwrap();
    let receipt_handle = &receive_response.messages[0].receipt_handle;

    // Change visibility timeout
    let change_request = ChangeMessageVisibilityRequest {
        queue_url: "visibility-queue".to_string(),
        receipt_handle: receipt_handle.as_str().into(),
        visibility_timeout: 120,
    };

    let result = service.change_message_visibility(change_request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_message_with_attributes() {
    let service = create_test_service().await;

    let create_request = CreateQueueRequest {
        queue_name: "attr-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    let mut message_attributes = HashMap::new();
    message_attributes.insert(
        "priority".to_string(),
        MessageAttributeValue {
            string_value: Some("high".to_string()),
            binary_value: None,
            data_type: "String".to_string(),
        },
    );

    let send_request = SendMessageRequest {
        queue_url: "attr-queue".to_string(),
        message_body: "Message with attributes".to_string(),
        delay_seconds: None,
        message_attributes: Some(message_attributes),
    };

    let response = service.send_message(send_request).await.unwrap();
    assert!(!response.message_id.to_string().is_empty());
}

#[tokio::test]
async fn send_message_batch_returns_per_entry_successes() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "send-batch-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let response = service
        .send_message_batch(SendMessageBatchRequest {
            queue_url: "send-batch-queue".to_string(),
            entries: vec![
                SendMessageBatchRequestEntry {
                    id: "first".to_string(),
                    message_body: "one".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                },
                SendMessageBatchRequestEntry {
                    id: "second".to_string(),
                    message_body: "two".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(response.successful.len(), 2);
    assert!(response.failed.is_empty());
    assert_eq!(response.successful[0].id, "first");
    assert_eq!(response.successful[1].id, "second");
}

#[tokio::test]
async fn delete_message_batch_returns_mixed_success_and_failure_entries() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "delete-batch-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    service
        .send_message(SendMessageRequest {
            queue_url: "delete-batch-queue".to_string(),
            message_body: "delete me".to_string(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    let receive_response = service
        .receive_message(ReceiveMessageRequest {
            queue_url: "delete-batch-queue".to_string(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(30),
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    let receipt_handle = receive_response.messages[0].receipt_handle.as_str().into();

    let response = service
        .delete_message_batch(DeleteMessageBatchRequest {
            queue_url: "delete-batch-queue".to_string(),
            entries: vec![
                DeleteMessageBatchRequestEntry {
                    id: "ok".to_string(),
                    receipt_handle,
                },
                DeleteMessageBatchRequestEntry {
                    id: "missing".to_string(),
                    receipt_handle: "missing-handle".into(),
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(response.successful.len(), 1);
    assert_eq!(response.successful[0].id, "ok");
    assert_eq!(response.failed.len(), 1);
    assert_eq!(response.failed[0].id, "missing");
}

#[tokio::test]
async fn change_message_visibility_batch_returns_successes() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "change-batch-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    service
        .send_message(SendMessageRequest {
            queue_url: "change-batch-queue".to_string(),
            message_body: "change me".to_string(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    let receive_response = service
        .receive_message(ReceiveMessageRequest {
            queue_url: "change-batch-queue".to_string(),
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
            queue_url: "change-batch-queue".to_string(),
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
async fn empty_queue_receive() {
    let service = create_test_service().await;

    let create_request = CreateQueueRequest {
        queue_name: "empty-queue".to_string(),
        attributes: None,
    };
    service.create_queue(create_request).await.unwrap();

    let receive_request = ReceiveMessageRequest {
        queue_url: "empty-queue".to_string(),
        max_number_of_messages: Some(5),
        visibility_timeout: None,
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    };

    let response = service.receive_message(receive_request).await.unwrap();
    assert_eq!(response.messages.len(), 0);
}

#[tokio::test]
async fn get_queue_attributes_returns_defaults_and_approximate_counts() {
    let service = create_test_service().await;
    service
        .create_queue(CreateQueueRequest {
            queue_name: "attributes-count-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    service
        .send_message(SendMessageRequest {
            queue_url: "attributes-count-queue".to_string(),
            message_body: "visible".to_string(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();
    service
        .send_message(SendMessageRequest {
            queue_url: "attributes-count-queue".to_string(),
            message_body: "delayed".to_string(),
            delay_seconds: Some(60),
            message_attributes: None,
        })
        .await
        .unwrap();
    let received = service
        .receive_message(ReceiveMessageRequest {
            queue_url: "attributes-count-queue".to_string(),
            max_number_of_messages: Some(1),
            visibility_timeout: Some(60),
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap();
    assert_eq!(received.messages.len(), 1);

    let response = service
        .get_queue_attributes(GetQueueAttributesRequest {
            queue_url: "attributes-count-queue".to_string(),
            attribute_names: Some(vec!["All".to_string()]),
        })
        .await
        .unwrap();

    assert_eq!(response.attributes["DelaySeconds"], "0");
    assert_eq!(response.attributes["MaximumMessageSize"], "1048576");
    assert_eq!(response.attributes["ApproximateNumberOfMessages"], "0");
    assert_eq!(
        response.attributes["ApproximateNumberOfMessagesNotVisible"],
        "1"
    );
    assert_eq!(
        response.attributes["ApproximateNumberOfMessagesDelayed"],
        "1"
    );
}

#[tokio::test]
async fn shared_sqlite_backend_prevents_duplicate_concurrent_claims() {
    let db_path = local_sqlite_test_path("shared-queue");
    let first_manager = create_sqlite_service(&db_path).await;
    let second_manager = create_sqlite_service(&db_path).await;

    first_manager
        .create_queue(CreateQueueRequest {
            queue_name: "shared-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();
    first_manager
        .send_message(SendMessageRequest {
            queue_url: "shared-queue".to_string(),
            message_body: "shared".to_string(),
            delay_seconds: None,
            message_attributes: None,
        })
        .await
        .unwrap();

    let first_receive = first_manager.receive_message(ReceiveMessageRequest {
        queue_url: "shared-queue".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(30),
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    });
    let second_receive = second_manager.receive_message(ReceiveMessageRequest {
        queue_url: "shared-queue".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(30),
        wait_time_seconds: None,
        attribute_names: None,
        message_attribute_names: None,
    });

    let (first_response, second_response) = tokio::join!(first_receive, second_receive);
    let received_count =
        first_response.unwrap().messages.len() + second_response.unwrap().messages.len();

    assert_eq!(received_count, 1);
}

#[tokio::test]
async fn sqlite_send_receive_delete_handles_representative_local_volume() {
    let service = create_test_service().await;
    let queue_url = service
        .create_queue(CreateQueueRequest {
            queue_name: "volume-check-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap()
        .queue_url;

    let started = Instant::now();
    for index in 0..200usize {
        service
            .send_message(SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: format!("volume-check-{index}"),
                delay_seconds: None,
                message_attributes: None,
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

    assert!(
        started.elapsed() < Duration::from_secs(30),
        "local send/receive/delete check should stay below the broad production-readiness \
         guardrail"
    );
}

#[tokio::test]
async fn empty_queue_receive_waits_for_deadline() {
    let service = Arc::new(create_test_service().await);

    service
        .create_queue(CreateQueueRequest {
            queue_name: "waiting-empty-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let receive_task = tokio::spawn({
        let service = Arc::clone(&service);
        async move {
            service
                .receive_message(ReceiveMessageRequest {
                    queue_url: "waiting-empty-queue".to_string(),
                    max_number_of_messages: Some(1),
                    visibility_timeout: None,
                    wait_time_seconds: Some(1),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
                .unwrap()
        }
    });

    let start = Instant::now();
    let response = receive_task.await.unwrap();

    assert!(start.elapsed() >= Duration::from_secs(1));
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(response.messages.is_empty());
}

#[tokio::test]
async fn receive_wait_returns_message_before_deadline() {
    let service = Arc::new(create_test_service().await);

    service
        .create_queue(CreateQueueRequest {
            queue_name: "waiting-message-queue".to_string(),
            attributes: None,
        })
        .await
        .unwrap();

    let receive_task = tokio::spawn({
        let service = Arc::clone(&service);
        async move {
            service
                .receive_message(ReceiveMessageRequest {
                    queue_url: "waiting-message-queue".to_string(),
                    max_number_of_messages: Some(1),
                    visibility_timeout: None,
                    wait_time_seconds: Some(2),
                    attribute_names: None,
                    message_attribute_names: None,
                })
                .await
                .unwrap()
        }
    });

    let send_task = tokio::spawn({
        let service = Arc::clone(&service);
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            service
                .send_message(SendMessageRequest {
                    queue_url: "waiting-message-queue".to_string(),
                    message_body: "arrived while waiting".to_string(),
                    delay_seconds: None,
                    message_attributes: None,
                })
                .await
                .unwrap();
        }
    });

    let start = Instant::now();
    let response = receive_task.await.unwrap();
    send_task.await.unwrap();

    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].body, "arrived while waiting");
}
