use std::collections::HashMap;

use httpmock::prelude::*;
use queue_provider::{
    MessageId, Queue, QueueError, QueueMessage, QueueProvider, ReceiptHandle,
    RemoteCredentialStrategy, RemoteQueueSettings, RemoteSigv4Settings,
};

use super::{
    RemoteQueueProvider,
    implementation::{classify_remote_error, queue_attribute_updates},
};

fn settings(endpoint: String) -> RemoteQueueSettings {
    RemoteQueueSettings {
        endpoint_urls: vec![endpoint],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
        sigv4: RemoteSigv4Settings::default(),
    }
}

#[tokio::test]
async fn remote_send_batch_maps_ten_results_in_request_order_with_one_call() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/batch", server.base_url());
    let successful = (0..10)
        .rev()
        .filter(|index| *index != 3)
        .map(|index| {
            serde_json::json!({
                "Id": index.to_string(),
                "MessageId": format!("{index:024x}"),
                "MD5OfMessageBody": "body"
            })
        })
        .collect::<Vec<_>>();
    let send = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.SendMessageBatch");
        then.status(200).json_body_obj(&serde_json::json!({
            "Successful": successful,
            "Failed": [{
                "Id": "3",
                "SenderFault": false,
                "Code": "ServiceUnavailable",
                "Message": "retry later"
            }]
        }));
    });
    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");
    let messages = (0..10)
        .map(|index| QueueMessage {
            receive_count: 0,
            message_id: MessageId::default(),
            queue_url: queue_url.clone(),
            body: format!("message-{index}"),
            message_attributes: None,
            receipt_handle: None,
            created_at: storage_types::TimestampMillis::now(),
            visibility_timestamp: None,
        })
        .collect();

    let results = provider
        .send_messages(messages)
        .await
        .expect("batch response");

    assert_eq!(send.calls(), 1);
    assert_eq!(results.len(), 10);
    for (index, result) in results.iter().enumerate() {
        if index == 3 {
            assert!(matches!(
                result,
                Err(QueueError::BatchEntry {
                    sender_fault: false,
                    code,
                    message,
                }) if code == "ServiceUnavailable" && message == "retry later"
            ));
        } else {
            assert_eq!(
                result.as_ref().expect("successful message").to_string(),
                format!("{index:024x}")
            );
        }
    }
}

#[tokio::test]
async fn remote_delete_and_visibility_batches_preserve_partial_result_order() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/batch", server.base_url());
    let delete = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.DeleteMessageBatch");
        then.status(200).json_body_obj(&serde_json::json!({
            "Successful": [{"Id": "1"}],
            "Failed": [{
                "Id": "0",
                "SenderFault": true,
                "Code": "ReceiptHandleIsInvalid",
                "Message": "bad receipt"
            }]
        }));
    });
    let visibility = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.ChangeMessageVisibilityBatch");
        then.status(200).json_body_obj(&serde_json::json!({
            "Successful": [{"Id": "1"}, {"Id": "0"}],
            "Failed": []
        }));
    });
    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");

    let delete_results = provider
        .delete_messages(
            &queue_url,
            vec![ReceiptHandle::from("bad"), ReceiptHandle::from("good")],
        )
        .await
        .expect("delete batch response");
    let visibility_results = provider
        .change_message_visibilities(
            &queue_url,
            vec![
                (ReceiptHandle::from("first"), 30_u32.into()),
                (ReceiptHandle::from("second"), 60_u32.into()),
            ],
        )
        .await
        .expect("visibility batch response");

    assert_eq!(delete.calls(), 1);
    assert_eq!(visibility.calls(), 1);
    assert!(matches!(
        &delete_results[0],
        Err(QueueError::BatchEntry {
            sender_fault: true,
            code,
            message,
        }) if code == "ReceiptHandleIsInvalid" && message == "bad receipt"
    ));
    assert!(delete_results[1].is_ok());
    assert!(visibility_results.iter().all(Result::is_ok));
}

#[tokio::test]
async fn empty_remote_batches_do_not_issue_requests() {
    let server = MockServer::start();
    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");

    assert!(
        provider
            .send_messages(Vec::new())
            .await
            .expect("send")
            .is_empty()
    );
    assert!(
        provider
            .delete_messages("unused", Vec::new())
            .await
            .expect("delete")
            .is_empty()
    );
    assert!(
        provider
            .change_message_visibilities("unused", Vec::new())
            .await
            .expect("visibility")
            .is_empty()
    );
}

#[tokio::test]
async fn create_queue_updates_existing_queue_attributes() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/notifications-delivery", server.base_url());

    let create_queue = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.CreateQueue");
        then.status(400).json_body_obj(&serde_json::json!({
            "__type": "AWS.SimpleQueueService.QueueNameExists",
            "message": "queue already exists"
        }));
    });
    let get_queue_url = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueUrl");
        then.status(200).json_body_obj(&serde_json::json!({
            "QueueUrl": queue_url
        }));
    });
    let get_queue_attributes = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueAttributes");
        then.status(200).json_body_obj(&serde_json::json!({
            "Attributes": {
                "VisibilityTimeout": "30",
                "DelaySeconds": "0"
            }
        }));
    });
    let set_queue_attributes = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.SetQueueAttributes")
            .json_body_obj(&serde_json::json!({
                "QueueUrl": queue_url,
                "Attributes": {
                    "VisibilityTimeout": "45"
                }
            }));
        then.status(200).json_body_obj(&serde_json::json!({}));
    });

    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");
    provider
        .create_queue(Queue {
            queue_name: "notifications-delivery".to_string(),
            queue_url: queue_url.clone(),
            attributes: HashMap::from([
                ("VisibilityTimeout".to_string(), "45".to_string()),
                ("DelaySeconds".to_string(), "0".to_string()),
            ]),
            created_at: storage_types::TimestampMillis::now(),
        })
        .await
        .expect("ensure queue exists");

    create_queue.assert();
    get_queue_url.assert();
    get_queue_attributes.assert();
    set_queue_attributes.assert();
}

#[tokio::test]
async fn create_queue_skips_attribute_update_when_queue_matches() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/notifications-delivery", server.base_url());

    let create_queue = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.CreateQueue");
        then.status(400).json_body_obj(&serde_json::json!({
            "__type": "AWS.SimpleQueueService.QueueNameExists",
            "message": "queue already exists"
        }));
    });
    let get_queue_url = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueUrl");
        then.status(200).json_body_obj(&serde_json::json!({
            "QueueUrl": queue_url
        }));
    });
    let get_queue_attributes = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueAttributes");
        then.status(200).json_body_obj(&serde_json::json!({
            "Attributes": {
                "VisibilityTimeout": "45"
            }
        }));
    });
    let set_queue_attributes = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.SetQueueAttributes");
        then.status(200).json_body_obj(&serde_json::json!({}));
    });

    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");
    provider
        .create_queue(Queue {
            queue_name: "notifications-delivery".to_string(),
            queue_url: queue_url.clone(),
            attributes: HashMap::from([("VisibilityTimeout".to_string(), "45".to_string())]),
            created_at: storage_types::TimestampMillis::now(),
        })
        .await
        .expect("ensure queue exists");

    create_queue.assert();
    get_queue_url.assert();
    get_queue_attributes.assert();
    assert_eq!(set_queue_attributes.calls(), 0);
}

#[tokio::test]
async fn remote_queue_url_lookup_does_not_fetch_attributes() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/url-only", server.base_url());
    let get_queue_url = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueUrl");
        then.status(200).json_body_obj(&serde_json::json!({
            "QueueUrl": queue_url
        }));
    });
    let get_queue_attributes = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueAttributes");
        then.status(500);
    });

    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");
    let resolved = provider
        .get_queue_url_by_name("url-only")
        .await
        .expect("queue URL");

    assert_eq!(resolved.as_deref(), Some(queue_url.as_str()));
    assert_eq!(get_queue_url.calls(), 1);
    assert_eq!(get_queue_attributes.calls(), 0);
}

#[tokio::test]
async fn remote_queue_url_projection_removes_the_attribute_round_trip() {
    let baseline_server = MockServer::start();
    let baseline_queue_url = format!("{}/123456789012/baseline", baseline_server.base_url());
    let baseline_get_queue_url = baseline_server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueUrl");
        then.status(200).json_body_obj(&serde_json::json!({
            "QueueUrl": baseline_queue_url
        }));
    });
    let baseline_get_queue_attributes = baseline_server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueAttributes");
        then.status(200).json_body_obj(&serde_json::json!({
            "Attributes": {
                "VisibilityTimeout": "30",
                "DelaySeconds": "0",
                "MaximumMessageSize": "262144",
                "MessageRetentionPeriod": "345600",
                "ReceiveMessageWaitTimeSeconds": "0"
            }
        }));
    });
    let baseline_provider = RemoteQueueProvider::new(settings(baseline_server.base_url()))
        .await
        .expect("baseline provider");
    let baseline = baseline_provider
        .get_queue_by_name("baseline")
        .await
        .expect("baseline queue lookup")
        .expect("baseline queue");

    let candidate_server = MockServer::start();
    let candidate_queue_url = format!("{}/123456789012/candidate", candidate_server.base_url());
    let candidate_get_queue_url = candidate_server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueUrl");
        then.status(200).json_body_obj(&serde_json::json!({
            "QueueUrl": candidate_queue_url
        }));
    });
    let candidate_get_queue_attributes = candidate_server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.GetQueueAttributes");
        then.status(500);
    });
    let candidate_provider = RemoteQueueProvider::new(settings(candidate_server.base_url()))
        .await
        .expect("candidate provider");
    let candidate = candidate_provider
        .get_queue_url_by_name("candidate")
        .await
        .expect("candidate queue URL")
        .expect("candidate URL");

    assert_eq!(baseline.queue_url, baseline_queue_url);
    assert_eq!(baseline_get_queue_url.calls(), 1);
    assert_eq!(baseline_get_queue_attributes.calls(), 1);
    assert_eq!(candidate, candidate_queue_url);
    assert_eq!(candidate_get_queue_url.calls(), 1);
    assert_eq!(candidate_get_queue_attributes.calls(), 0);
}

#[test]
fn classify_queue_name_exists_as_resource_exists() {
    let error = classify_remote_error(
        400,
        br#"{"__type":"AWS.SimpleQueueService.QueueNameExists","message":"exists"}"#,
    );

    assert!(matches!(error, QueueError::ResourceExists { .. }));
}

#[test]
fn classify_json_queue_does_not_exist_as_resource_not_found() {
    let error = classify_remote_error(
        400,
        br#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}"#,
    );

    assert!(matches!(
        error,
        QueueError::ResourceNotFound {
            resource_type: "queue",
            ..
        }
    ));
}

#[test]
fn classify_query_queue_does_not_exist_as_resource_not_found() {
    let error = classify_remote_error(
        400,
        br#"{"__type":"AWS.SimpleQueueService.NonExistentQueue","message":"missing"}"#,
    );

    assert!(matches!(
        error,
        QueueError::ResourceNotFound {
            resource_type: "queue",
            ..
        }
    ));
}

#[test]
fn queue_attribute_updates_only_returns_changed_keys() {
    let updates = queue_attribute_updates(
        &HashMap::from([
            ("VisibilityTimeout".to_string(), "30".to_string()),
            ("DelaySeconds".to_string(), "0".to_string()),
        ]),
        &HashMap::from([
            ("VisibilityTimeout".to_string(), "45".to_string()),
            ("DelaySeconds".to_string(), "0".to_string()),
        ]),
    );

    assert_eq!(
        updates,
        HashMap::from([("VisibilityTimeout".to_string(), "45".to_string())])
    );
}

#[tokio::test]
async fn remote_send_message_clamps_expired_visibility_to_zero_delay() {
    let server = MockServer::start();
    let queue_url = format!("{}/123456789012/expired", server.base_url());
    let send = server.mock(|when, then| {
        when.method(POST)
            .header("x-amz-target", "AmazonSQS.SendMessage")
            .json_body_obj(&serde_json::json!({
                "QueueUrl": queue_url,
                "MessageBody": "body",
                "DelaySeconds": 0,
            }));
        then.status(200).json_body_obj(&serde_json::json!({
            "MessageId": "018f1f612a6f7ac3b9b67f65",
            "MD5OfMessageBody": "841a2d689ad86bd1611447453c22c6fc",
        }));
    });

    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .expect("provider");
    let message_id = provider
        .send_message(QueueMessage {
            receive_count: 0,
            message_id: MessageId::default(),
            queue_url,
            body: "body".to_string(),
            message_attributes: None,
            receipt_handle: None,
            created_at: storage_types::TimestampMillis::now(),
            visibility_timestamp: Some(storage_types::TimestampMillis::now() - 1),
        })
        .await
        .expect("send message");

    assert_eq!(message_id.to_string(), "018f1f612a6f7ac3b9b67f65");
    send.assert();
}

#[tokio::test]
async fn given_sqs_delivery_when_receiving_then_durable_attempt_and_original_age_are_requested() {
    let server = MockServer::start();
    let receive = server.mock(|when, then| {
        when.method(POST).header("x-amz-target", "AmazonSQS.ReceiveMessage")
            .json_body_includes(serde_json::json!({"AttributeNames": ["SentTimestamp", "ApproximateReceiveCount"], "MessageAttributeNames": ["All"]}).to_string());
        then.status(200).json_body_obj(&serde_json::json!({"Messages": [{
            "MessageId": "message", "ReceiptHandle": "receipt", "MD5OfBody": "digest", "Body": "work",
            "Attributes": {"SentTimestamp": "1000", "ApproximateReceiveCount": "180"}
        }]}));
    });
    let provider = RemoteQueueProvider::new(settings(server.base_url()))
        .await
        .unwrap();
    let messages = queue_provider::QueueProvider::receive_messages(
        &provider,
        "queue",
        1,
        30u32.into(),
        0u32.into(),
    )
    .await
    .unwrap();
    assert_eq!(
        messages[0].attributes.as_ref().unwrap()["ApproximateReceiveCount"],
        "180"
    );
    assert_eq!(
        messages[0].attributes.as_ref().unwrap()["SentTimestamp"],
        "1000"
    );
    receive.assert_calls(1);
}
