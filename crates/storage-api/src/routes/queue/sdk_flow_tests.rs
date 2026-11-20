//! An unchanged `aws-sdk-sqs` client against the assembled `/queue` route.

use aws_sdk_sqs::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    error::ProvideErrorMetadata as _,
    types::{DeleteMessageBatchRequestEntry, QueueAttributeName, SendMessageBatchRequestEntry},
};

use super::test_support::QueueRoute;

async fn sdk_client() -> Client {
    let endpoint = QueueRoute::start().await.serve().await;
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(config)
}

async fn create_queue(client: &Client, name: &str) -> String {
    client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_sdk_client_when_core_flow_runs_then_message_round_trips() {
    let client = sdk_client().await;
    let queue_url = create_queue(&client, "sdk-core").await;

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("hello")
        .send()
        .await
        .unwrap();
    let listed = client.list_queues().send().await.unwrap();
    assert_eq!(listed.queue_urls(), std::slice::from_ref(&queue_url));
    let received = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .visibility_timeout(30)
        .send()
        .await
        .unwrap();
    let message = &received.messages()[0];
    assert_eq!(message.body(), Some("hello"));
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(message.receipt_handle().unwrap())
        .send()
        .await
        .unwrap();
    let attributes = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attributes
            .attributes()
            .and_then(|map| map.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .map(String::as_str),
        Some("0")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_missing_queue_when_sdk_gets_url_then_queue_does_not_exist() {
    let client = sdk_client().await;

    let error = client
        .get_queue_url()
        .queue_name("missing-queue")
        .send()
        .await
        .unwrap_err()
        .into_service_error();

    assert!(error.is_queue_does_not_exist(), "{error:?}");
    assert_eq!(error.message(), Some("The specified queue does not exist."));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_invalid_or_used_receipt_handle_when_sdk_deletes_then_only_the_invalid_one_fails() {
    let client = sdk_client().await;
    let queue_url = create_queue(&client, "sdk-receipts").await;

    let invalid = client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle("invalid")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(invalid.is_receipt_handle_is_invalid(), "{invalid:?}");
    assert_eq!(
        invalid.message(),
        Some("The input receipt handle \"invalid\" is not a valid receipt handle.")
    );

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("stale-handle")
        .send()
        .await
        .unwrap();
    let received = client
        .receive_message()
        .queue_url(&queue_url)
        .visibility_timeout(30)
        .send()
        .await
        .unwrap();
    let handle = received.messages()[0].receipt_handle().unwrap().to_owned();
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(&handle)
        .send()
        .await
        .unwrap();
    // Probe 1: a used handle is a no-op success, not `ReceiptHandleIsInvalid`.
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(&handle)
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_invalid_requests_when_sdk_sends_them_then_error_codes_and_texts_match() {
    let client = sdk_client().await;

    let invalid_name = client
        .create_queue()
        .queue_name("invalid.name")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(invalid_name.code(), Some("InvalidParameterValue"));
    assert_eq!(
        invalid_name.message(),
        Some(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
        )
    );

    let queue_url = create_queue(&client, "sdk-validation").await;
    let duplicate = client
        .send_message_batch()
        .queue_url(&queue_url)
        .entries(batch_entry("dup", "one"))
        .entries(batch_entry("dup", "two"))
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(duplicate.is_batch_entry_ids_not_distinct(), "{duplicate:?}");
    assert_eq!(duplicate.message(), Some("Id dup repeated."));

    let fifo = client
        .create_queue()
        .queue_name("sdk-fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert_eq!(fifo.code(), Some("InvalidParameterValue"));
    assert_eq!(
        fifo.message(),
        Some("FIFO queue attributes are not supported")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_sdk_client_when_batches_send_and_delete_then_every_entry_succeeds() {
    let client = sdk_client().await;
    let queue_url = create_queue(&client, "sdk-batch").await;

    let sent = client
        .send_message_batch()
        .queue_url(&queue_url)
        .entries(batch_entry("first", "one"))
        .entries(batch_entry("second", "two"))
        .send()
        .await
        .unwrap();
    assert_eq!(sent.successful().len(), 2);
    assert!(sent.failed().is_empty());
    let received = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(2)
        .visibility_timeout(30)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 2);
    let mut delete = client.delete_message_batch().queue_url(&queue_url);
    for (index, message) in received.messages().iter().enumerate() {
        delete = delete.entries(
            DeleteMessageBatchRequestEntry::builder()
                .id(format!("entry_{index}"))
                .receipt_handle(message.receipt_handle().unwrap())
                .build()
                .unwrap(),
        );
    }
    let deleted = delete.send().await.unwrap();
    assert_eq!(deleted.successful().len(), 2);
    assert!(deleted.failed().is_empty());
}

fn batch_entry(id: &str, body: &str) -> SendMessageBatchRequestEntry {
    SendMessageBatchRequestEntry::builder()
        .id(id)
        .message_body(body)
        .build()
        .unwrap()
}
