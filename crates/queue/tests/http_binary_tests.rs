use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

use reqwest::Client;
use serde_json::{Value, json};

fn reserve_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct QueueServer {
    child: Child,
    client: Client,
    base_url: String,
}

impl QueueServer {
    async fn start() -> Self {
        let port = reserve_port();
        let base_url = format!("http://127.0.0.1:{port}");
        let child = Command::new(env!("CARGO_BIN_EXE_queue"))
            .arg("--port")
            .arg(port.to_string())
            .arg("--db-path")
            .arg(":memory:")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn queue server");
        let client = Client::new();

        let server = Self {
            child,
            client,
            base_url,
        };
        server.wait_until_ready().await;
        server
    }

    async fn wait_until_ready(&self) {
        for _ in 0..80 {
            let response = self
                .client
                .get(format!("{}/health", self.base_url))
                .send()
                .await;
            if response.is_ok_and(|resp| resp.status().is_success()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("queue server did not become ready");
    }

    async fn post(&self, target: &str, payload: Value) -> reqwest::Response {
        self.client
            .post(format!("{}/", self.base_url))
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", target)
            .body(payload.to_string())
            .send()
            .await
            .expect("send request")
    }

    async fn post_query(&self, payload: &[(&str, &str)]) -> reqwest::Response {
        self.client
            .post(format!("{}/", self.base_url))
            .header("content-type", "application/x-www-form-urlencoded")
            .form(payload)
            .send()
            .await
            .expect("send query request")
    }
}

impl Drop for QueueServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    xml.split_once(&start_tag)?
        .1
        .split_once(&end_tag)
        .map(|(value, _)| value.to_string())
}

#[tokio::test]
async fn queue_binary_supports_core_sqs_flow() {
    let server = QueueServer::start().await;

    let create = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({
                "QueueName": "binary-test-queue"
            }),
        )
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body: Value = create.json().await.expect("create json");
    let queue_url = create_body["QueueUrl"]
        .as_str()
        .expect("queue url")
        .to_string();

    let send = server
        .post(
            "AmazonSQS.SendMessage",
            json!({
                "QueueUrl": queue_url,
                "MessageBody": "hello"
            }),
        )
        .await;
    assert_eq!(send.status(), reqwest::StatusCode::OK);

    let list = server.post("AmazonSQS.ListQueues", json!({})).await;
    assert_eq!(list.status(), reqwest::StatusCode::OK);
    let list_body: Value = list.json().await.expect("list json");
    assert_eq!(
        list_body["QueueUrls"].as_array().expect("queue urls").len(),
        1
    );

    let receive = server
        .post(
            "AmazonSQS.ReceiveMessage",
            json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 1,
                "VisibilityTimeout": 30
            }),
        )
        .await;
    assert_eq!(receive.status(), reqwest::StatusCode::OK);
    let receive_body: Value = receive.json().await.expect("receive json");
    let receipt_handle = receive_body["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("receipt handle")
        .to_string();
    assert_eq!(receive_body["Messages"][0]["Body"], "hello");

    let delete = server
        .post(
            "AmazonSQS.DeleteMessage",
            json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": receipt_handle
            }),
        )
        .await;
    assert_eq!(delete.status(), reqwest::StatusCode::OK);
    assert_eq!(
        delete.json::<Value>().await.expect("delete json"),
        json!({})
    );
}

#[tokio::test]
async fn queue_binary_returns_sqs_style_errors() {
    let server = QueueServer::start().await;

    let response = server
        .post(
            "AmazonSQS.GetQueueUrl",
            json!({
                "QueueName": "missing-queue"
            }),
        )
        .await;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get("x-amzn-query-error")
            .and_then(|value| value.to_str().ok()),
        Some("AWS.SimpleQueueService.NonExistentQueue;Sender")
    );
    let body: Value = response.json().await.expect("error json");
    assert_eq!(
        body["__type"].as_str(),
        Some("com.amazonaws.sqs#QueueDoesNotExist")
    );
    assert_eq!(
        body["message"].as_str(),
        Some("The specified queue does not exist.")
    );
    assert!(body.get("Message").is_none());
}

#[tokio::test]
async fn queue_binary_returns_aws_style_invalid_receipt_handle_errors() {
    let server = QueueServer::start().await;

    let create = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({ "QueueName": "invalid-receipt-queue" }),
        )
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body: Value = create.json().await.expect("create json");
    let queue_url = create_body["QueueUrl"].as_str().expect("queue url");

    let response = server
        .post(
            "AmazonSQS.DeleteMessage",
            json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": "invalid"
            }),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get("x-amzn-query-error")
            .and_then(|value| value.to_str().ok()),
        Some("ReceiptHandleIsInvalid;Sender")
    );
    let body: Value = response.json().await.expect("error json");
    assert_eq!(
        body["__type"].as_str(),
        Some("com.amazonaws.sqs#ReceiptHandleIsInvalid")
    );
    assert_eq!(
        body["message"].as_str(),
        Some("The input receipt handle \"invalid\" is not a valid receipt handle.")
    );

    let send = server
        .post(
            "AmazonSQS.SendMessage",
            json!({
                "QueueUrl": queue_url,
                "MessageBody": "stale-handle"
            }),
        )
        .await;
    assert_eq!(send.status(), reqwest::StatusCode::OK);
    let receive = server
        .post(
            "AmazonSQS.ReceiveMessage",
            json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 1,
                "VisibilityTimeout": 30
            }),
        )
        .await;
    assert_eq!(receive.status(), reqwest::StatusCode::OK);
    let receive_body: Value = receive.json().await.expect("receive json");
    let stale_receipt_handle = receive_body["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("receipt handle");
    let delete = server
        .post(
            "AmazonSQS.DeleteMessage",
            json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": stale_receipt_handle
            }),
        )
        .await;
    assert_eq!(delete.status(), reqwest::StatusCode::OK);

    let stale = server
        .post(
            "AmazonSQS.DeleteMessage",
            json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": stale_receipt_handle
            }),
        )
        .await;
    assert_eq!(stale.status(), reqwest::StatusCode::NOT_FOUND);
    let stale_body: Value = stale.json().await.expect("stale json");
    assert_eq!(
        stale_body["__type"].as_str(),
        Some("com.amazonaws.sqs#ReceiptHandleIsInvalid")
    );
    let expected_stale_message = format!(
        "The input receipt handle \"{stale_receipt_handle}\" is not a valid receipt handle."
    );
    assert_eq!(
        stale_body["message"].as_str(),
        Some(expected_stale_message.as_str())
    );
}

#[tokio::test]
async fn queue_binary_fixture_locks_validation_error_text() {
    let server = QueueServer::start().await;

    let invalid_name = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({ "QueueName": "invalid.name" }),
        )
        .await;
    assert_eq!(invalid_name.status(), reqwest::StatusCode::BAD_REQUEST);
    let invalid_name_body: Value = invalid_name.json().await.expect("invalid name json");
    assert_eq!(
        invalid_name_body["__type"].as_str(),
        Some("com.amazon.coral.service#InvalidParameterValueException")
    );
    assert_eq!(
        invalid_name_body["message"].as_str(),
        Some(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
        )
    );

    let create = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({ "QueueName": "duplicate-batch-id-queue" }),
        )
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body: Value = create.json().await.expect("create json");
    let queue_url = create_body["QueueUrl"].as_str().expect("queue url");

    let duplicate_batch = server
        .post(
            "AmazonSQS.SendMessageBatch",
            json!({
                "QueueUrl": queue_url,
                "Entries": [
                    {
                        "Id": "dup",
                        "MessageBody": "one"
                    },
                    {
                        "Id": "dup",
                        "MessageBody": "two"
                    }
                ]
            }),
        )
        .await;
    assert_eq!(duplicate_batch.status(), reqwest::StatusCode::BAD_REQUEST);
    let duplicate_body: Value = duplicate_batch.json().await.expect("duplicate json");
    assert_eq!(
        duplicate_body["__type"].as_str(),
        Some("com.amazonaws.sqs#BatchEntryIdsNotDistinct")
    );
    assert_eq!(duplicate_body["message"].as_str(), Some("Id dup repeated."));

    let fifo = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({
                "QueueName": "unsupported-fifo-queue",
                "Attributes": {
                    "FifoQueue": "true"
                }
            }),
        )
        .await;
    assert_eq!(fifo.status(), reqwest::StatusCode::BAD_REQUEST);
    let fifo_body: Value = fifo.json().await.expect("fifo json");
    assert_eq!(
        fifo_body["__type"].as_str(),
        Some("com.amazon.coral.service#InvalidParameterValueException")
    );
    assert_eq!(
        fifo_body["message"].as_str(),
        Some("FIFO queue attributes are not supported")
    );
}

#[tokio::test]
async fn queue_binary_fixture_locks_protocol_error_text() {
    let server = QueueServer::start().await;

    let unsupported_action = server.post("AmazonSQS.Unsupported", json!({})).await;
    assert_eq!(
        unsupported_action.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let unsupported_body: Value = unsupported_action
        .json()
        .await
        .expect("unsupported action json");
    assert_eq!(
        unsupported_body["__type"].as_str(),
        Some("com.amazonaws.sqs#InvalidAction")
    );
    assert_eq!(
        unsupported_body["message"].as_str(),
        Some("unsupported_action")
    );

    let malformed = server
        .client
        .post(format!("{}/", server.base_url))
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "AmazonSQS.CreateQueue")
        .body("{")
        .send()
        .await
        .expect("malformed request");
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let malformed_body: Value = malformed.json().await.expect("malformed json");
    assert_eq!(
        malformed_body["__type"].as_str(),
        Some("com.amazon.coral.service#InvalidParameterValueException")
    );
    assert!(
        malformed_body["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("invalid_json:")),
        "malformed error body should preserve JSON parse detail: {malformed_body}"
    );

    let wrong_content_type = server
        .client
        .post(format!("{}/", server.base_url))
        .header("content-type", "application/json")
        .header("x-amz-target", "AmazonSQS.CreateQueue")
        .body(r#"{"QueueName":"wrong-content-type"}"#)
        .send()
        .await
        .expect("wrong content type request");
    assert_eq!(
        wrong_content_type.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let wrong_content_type_body: Value = wrong_content_type
        .json()
        .await
        .expect("wrong content type json");
    assert_eq!(
        wrong_content_type_body["__type"].as_str(),
        Some("com.amazon.coral.service#InvalidParameterValueException")
    );
    assert_eq!(
        wrong_content_type_body["message"].as_str(),
        Some("unsupported_content_type")
    );
}

#[tokio::test]
async fn queue_binary_accepts_query_protocol_for_basic_lifecycle() {
    let server = QueueServer::start().await;

    let create = server
        .post_query(&[
            ("Action", "CreateQueue"),
            ("Version", "2012-11-05"),
            ("QueueName", "query-test-queue"),
        ])
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body = create.text().await.expect("create xml");
    let queue_url = xml_text(&create_body, "QueueUrl").expect("queue url");

    let send = server
        .post_query(&[
            ("Action", "SendMessage"),
            ("Version", "2012-11-05"),
            ("QueueUrl", queue_url.as_str()),
            ("MessageBody", "hello-query"),
            ("DelaySeconds", "0"),
        ])
        .await;
    assert_eq!(send.status(), reqwest::StatusCode::OK);

    let receive = server
        .post_query(&[
            ("Action", "ReceiveMessage"),
            ("Version", "2012-11-05"),
            ("QueueUrl", queue_url.as_str()),
            ("MaxNumberOfMessages", "1"),
            ("VisibilityTimeout", "30"),
        ])
        .await;
    assert_eq!(receive.status(), reqwest::StatusCode::OK);
    let receive_body = receive.text().await.expect("receive xml");
    assert_eq!(
        xml_text(&receive_body, "Body").as_deref(),
        Some("hello-query")
    );
}

#[tokio::test]
async fn queue_binary_renders_query_xml_headers_and_error_shape() {
    let server = QueueServer::start().await;

    let create = server
        .post_query(&[
            ("Action", "CreateQueue"),
            ("Version", "2012-11-05"),
            ("QueueName", "query-render-queue"),
        ])
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    assert_eq!(
        create
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/xml; charset=utf-8")
    );
    assert!(create.headers().get("x-amzn-requestid").is_some());
    let create_body = create.text().await.expect("create xml");
    assert!(create_body.starts_with("<?xml version=\"1.0\"?>"));
    assert!(
        create_body
            .contains("<CreateQueueResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">")
    );
    assert!(create_body.contains("<CreateQueueResult><QueueUrl>"));
    assert!(create_body.contains("<ResponseMetadata><RequestId>"));

    let missing = server
        .post_query(&[
            ("Action", "GetQueueUrl"),
            ("Version", "2012-11-05"),
            ("QueueName", "missing-query-render-queue"),
        ])
        .await;
    assert_eq!(missing.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        missing
            .headers()
            .get("x-amzn-query-error")
            .and_then(|value| value.to_str().ok()),
        Some("AWS.SimpleQueueService.NonExistentQueue;Sender")
    );
    let missing_body = missing.text().await.expect("missing xml");
    assert!(missing_body.contains("<ErrorResponse><Error><Type>Sender</Type>"));
    assert!(missing_body.contains("<Code>NonExistentQueue</Code>"));
    assert!(missing_body.contains("<RequestId>"));
}

#[tokio::test]
async fn queue_binary_renders_query_xml_for_protocol_and_validation_errors() {
    let server = QueueServer::start().await;

    let missing_action = server.post_query(&[("Version", "2012-11-05")]).await;
    assert_query_error(
        missing_action,
        reqwest::StatusCode::BAD_REQUEST,
        "InvalidAction;Sender",
        "InvalidAction",
        "missing_action",
    )
    .await;

    let unsupported_action = server
        .post_query(&[("Action", "Unsupported"), ("Version", "2012-11-05")])
        .await;
    assert_query_error(
        unsupported_action,
        reqwest::StatusCode::BAD_REQUEST,
        "InvalidAction;Sender",
        "InvalidAction",
        "unsupported_action",
    )
    .await;

    let invalid_name = server
        .post_query(&[
            ("Action", "CreateQueue"),
            ("Version", "2012-11-05"),
            ("QueueName", "invalid.name"),
        ])
        .await;
    assert_query_error(
        invalid_name,
        reqwest::StatusCode::BAD_REQUEST,
        "InvalidParameterValue;Sender",
        "InvalidParameterValue",
        "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
    )
    .await;

    let create = server
        .post_query(&[
            ("Action", "CreateQueue"),
            ("Version", "2012-11-05"),
            ("QueueName", "query-error-queue"),
        ])
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body = create.text().await.expect("create xml");
    let queue_url = xml_text(&create_body, "QueueUrl").expect("queue url");

    let duplicate_batch_id = server
        .post_query(&[
            ("Action", "SendMessageBatch"),
            ("Version", "2012-11-05"),
            ("QueueUrl", queue_url.as_str()),
            ("SendMessageBatchRequestEntry.1.Id", "dup"),
            ("SendMessageBatchRequestEntry.1.MessageBody", "one"),
            ("SendMessageBatchRequestEntry.2.Id", "dup"),
            ("SendMessageBatchRequestEntry.2.MessageBody", "two"),
        ])
        .await;
    assert_query_error(
        duplicate_batch_id,
        reqwest::StatusCode::BAD_REQUEST,
        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct;Sender",
        "BatchEntryIdsNotDistinct",
        "Id dup repeated.",
    )
    .await;

    let invalid_receipt = server
        .post_query(&[
            ("Action", "DeleteMessage"),
            ("Version", "2012-11-05"),
            ("QueueUrl", queue_url.as_str()),
            ("ReceiptHandle", "invalid"),
        ])
        .await;
    assert_query_error(
        invalid_receipt,
        reqwest::StatusCode::NOT_FOUND,
        "ReceiptHandleIsInvalid;Sender",
        "ReceiptHandleIsInvalid",
        "The input receipt handle &quot;invalid&quot; is not a valid receipt handle.",
    )
    .await;
}

#[tokio::test]
async fn queue_binary_supports_json_batch_send_receive_delete() {
    let server = QueueServer::start().await;

    let create = server
        .post(
            "AmazonSQS.CreateQueue",
            json!({ "QueueName": "batch-queue" }),
        )
        .await;
    assert_eq!(create.status(), reqwest::StatusCode::OK);
    let create_body: Value = create.json().await.expect("create json");
    let queue_url = create_body["QueueUrl"].as_str().expect("queue url");

    let send = server
        .post(
            "AmazonSQS.SendMessageBatch",
            json!({
                "QueueUrl": queue_url,
                "Entries": [
                    {
                        "Id": "first",
                        "MessageBody": "one"
                    },
                    {
                        "Id": "second",
                        "MessageBody": "two"
                    }
                ]
            }),
        )
        .await;
    assert_eq!(send.status(), reqwest::StatusCode::OK);
    let send_body: Value = send.json().await.expect("send batch json");
    assert_eq!(
        send_body["Successful"]
            .as_array()
            .expect("successful")
            .len(),
        2
    );
    assert_eq!(send_body["Failed"].as_array().expect("failed").len(), 0);

    let receive = server
        .post(
            "AmazonSQS.ReceiveMessage",
            json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 2,
                "VisibilityTimeout": 30
            }),
        )
        .await;
    assert_eq!(receive.status(), reqwest::StatusCode::OK);
    let receive_body: Value = receive.json().await.expect("receive json");
    let messages = receive_body["Messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 2);

    let delete_entries: Vec<Value> = messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            json!({
                "Id": format!("entry_{index}"),
                "ReceiptHandle": message["ReceiptHandle"].as_str().expect("receipt handle")
            })
        })
        .collect();
    let delete = server
        .post(
            "AmazonSQS.DeleteMessageBatch",
            json!({
                "QueueUrl": queue_url,
                "Entries": delete_entries
            }),
        )
        .await;
    assert_eq!(delete.status(), reqwest::StatusCode::OK);
    let delete_body: Value = delete.json().await.expect("delete batch json");
    assert_eq!(
        delete_body["Successful"]
            .as_array()
            .expect("successful")
            .len(),
        2
    );
    assert_eq!(delete_body["Failed"].as_array().expect("failed").len(), 0);
}

async fn assert_query_error(
    response: reqwest::Response,
    expected_status: reqwest::StatusCode,
    expected_header: &str,
    expected_code: &str,
    expected_message: &str,
) {
    assert_eq!(response.status(), expected_status);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/xml; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get("x-amzn-query-error")
            .and_then(|value| value.to_str().ok()),
        Some(expected_header)
    );
    let body = response.text().await.expect("query error xml");
    assert!(body.contains("<ErrorResponse><Error><Type>Sender</Type>"));
    assert!(
        body.contains(&format!("<Code>{expected_code}</Code>")),
        "error body should include expected code {expected_code}: {body}"
    );
    assert!(
        body.contains(&format!("<Message>{expected_message}</Message>")),
        "error body should include expected message {expected_message}: {body}"
    );
    assert!(body.contains("<RequestId>"));
}
