use std::collections::HashMap;

use httpmock::prelude::*;
use queue_provider::{
    Queue, QueueError, QueueProvider, RemoteCredentialStrategy, RemoteQueueSettings,
    RemoteSigv4Settings,
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

#[test]
fn classify_queue_name_exists_as_resource_exists() {
    let error = classify_remote_error(
        400,
        br#"{"__type":"AWS.SimpleQueueService.QueueNameExists","message":"exists"}"#,
    );

    assert!(matches!(error, QueueError::ResourceExists { .. }));
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
