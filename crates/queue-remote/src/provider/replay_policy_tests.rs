use std::time::Duration;

use http_request::HttpRequestDispatchStatus::{self, NotDispatched, PossiblyDispatched};
use queue_provider::{MessageId, QueueError, QueueMessage, QueueOperations as _};
use storage_types::ErrorClass;
use tokio::{io::AsyncReadExt as _, net::TcpListener};

use super::{RemoteQueueProvider, replay_policy::transport_failure};
use crate::{
    RemoteCredentialStrategy, RemoteQueueSettings, RemoteSigv4Settings, RemoteTimeoutOverrides,
};

/// Upper bound for one call; a hang fails here.
const TEST_CALL_BOUND: Duration = Duration::from_secs(10);

const WRITE_TARGETS: [&str; 4] = [
    "AmazonSQS.SendMessage",
    "AmazonSQS.ReceiveMessage",
    "AmazonSQS.DeleteMessage",
    "AmazonSQS.CreateQueue",
];

const READ_ONLY_TARGETS: [&str; 5] = [
    "AmazonSQS.GetQueueAttributes",
    "AmazonSQS.GetQueueUrl",
    "AmazonSQS.ListDeadLetterSourceQueues",
    "AmazonSQS.ListQueueTags",
    "AmazonSQS.ListQueues",
];

fn class_of(error: QueueError) -> ErrorClass {
    match error {
        QueueError::StorageError(error) => error.class(),
        other => panic!("expected a storage error, got {other}"),
    }
}

fn class_for(target: &str, dispatch: HttpRequestDispatchStatus) -> ErrorClass {
    class_of(transport_failure(target, dispatch))
}

#[test]
fn given_write_target_when_possibly_dispatched_then_commit_unknown() {
    for target in WRITE_TARGETS {
        assert_eq!(
            class_for(target, PossiblyDispatched),
            ErrorClass::CommitUnknown,
            "{target}"
        );
    }
    assert_eq!(
        class_for("AmazonSQS.UnlistedTarget", PossiblyDispatched),
        ErrorClass::CommitUnknown
    );
}

#[test]
fn given_any_target_when_not_dispatched_then_retryable() {
    for target in WRITE_TARGETS.into_iter().chain(READ_ONLY_TARGETS) {
        assert_eq!(
            class_for(target, NotDispatched),
            ErrorClass::Retryable,
            "{target}"
        );
    }
}

#[test]
fn given_read_only_target_when_possibly_dispatched_then_retryable() {
    for target in READ_ONLY_TARGETS {
        assert_eq!(
            class_for(target, PossiblyDispatched),
            ErrorClass::Retryable,
            "{target}"
        );
    }
}

fn settings(endpoint: String) -> RemoteQueueSettings {
    RemoteQueueSettings {
        endpoint_urls: vec![endpoint],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: Some(RemoteTimeoutOverrides {
            connect_timeout_ms: None,
            request_timeout_ms: Some(5_000),
        }),
        sigv4: RemoteSigv4Settings::default(),
    }
}

/// A server that reads one whole request head, counts it, then drops the
/// socket without answering. Returns the endpoint and the request counter.
async fn cutting_server() -> (String, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let (requests, received) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let requests = requests.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0_u8; 1024];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => head.extend_from_slice(&chunk[..read]),
                    }
                }
                let _ = requests.send(());
                drop(socket);
            });
        }
    });
    (endpoint, received)
}

fn message(queue_url: String) -> QueueMessage {
    QueueMessage {
        receive_count: 0,
        message_id: MessageId::default(),
        queue_url,
        body: "body".to_string(),
        message_attributes: None,
        receipt_handle: None,
        created_at: storage_types::TimestampMillis::now(),
        visibility_timestamp: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn given_send_message_when_connection_cut_after_request_read_then_commit_unknown_after_one_send()
 {
    let (endpoint, mut received) = cutting_server().await;
    let queue_url = format!("{endpoint}/123456789012/cut");
    let provider = RemoteQueueProvider::new(settings(endpoint))
        .await
        .expect("provider");

    let error = tokio::time::timeout(TEST_CALL_BOUND, provider.send_message(message(queue_url)))
        .await
        .expect("send_message finishes within the bound")
        .expect_err("a cut write must not succeed");

    assert_eq!(class_of(error), ErrorClass::CommitUnknown);
    received.recv().await.expect("the server read the request");
    assert!(received.try_recv().is_err(), "the request is sent once");
}

#[tokio::test]
async fn given_get_queue_url_when_connection_cut_after_request_read_then_retryable() {
    let (endpoint, mut received) = cutting_server().await;
    let provider = RemoteQueueProvider::new(settings(endpoint))
        .await
        .expect("provider");

    let error = tokio::time::timeout(TEST_CALL_BOUND, provider.get_queue_url_by_name("cut"))
        .await
        .expect("get_queue_url_by_name finishes within the bound")
        .expect_err("a cut lookup must not succeed");

    assert_eq!(class_of(error), ErrorClass::Retryable);
    received.recv().await.expect("the server read the request");
}

#[tokio::test]
async fn given_send_message_when_connect_refused_then_retryable() {
    // Bind, note the port, then close: nothing listens there.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    drop(listener);
    let queue_url = format!("{endpoint}/123456789012/closed");
    let provider = RemoteQueueProvider::new(settings(endpoint))
        .await
        .expect("provider");

    let error = tokio::time::timeout(TEST_CALL_BOUND, provider.send_message(message(queue_url)))
        .await
        .expect("send_message finishes within the bound")
        .expect_err("a refused connect must not succeed");

    assert_eq!(class_of(error), ErrorClass::Retryable);
}
