//! AWS emulator behavioural suite ports (JSON protocol).
//!
//! Only wire-level assertions live here: the HTTP status, the XML error
//! envelope, the `x-amzn-query-error` header, and the state shared by the
//! two protocols. Error codes and messages of single actions are asserted
//! for every provider in `storage-rewrite-conformance`
//! (`aws_behavioral_queue_api_tests.rs`).

use axum::http::StatusCode;
use serde_json::{Value, json};

use super::test_support::QueueRoute;

const NO_SUCH_QUEUE: &str = "AWS.SimpleQueueService.NonExistentQueue";

async fn create_queue(route: &QueueRoute, name: &str, attributes: Value) -> String {
    let created = route
        .post_json(
            "CreateQueue",
            &json!({"QueueName": name, "Attributes": attributes}),
        )
        .await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.body);
    created.json()["QueueUrl"].as_str().unwrap().to_owned()
}

/// AWS behavioural: check_query_and_json_protocol_clients_share_one_queue.py
#[tokio::test]
async fn given_query_and_json_clients_when_they_share_a_queue_then_each_reads_the_others_writes() {
    let route = QueueRoute::start().await;
    let url = create_queue(&route, "protocol-share", json!({"VisibilityTimeout": "30"})).await;

    // Query -> JSON
    let sent = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &url),
                ("MessageBody", "via-query"),
                ("MessageAttribute.1.Name", "proto"),
                ("MessageAttribute.1.Value.DataType", "String"),
                ("MessageAttribute.1.Value.StringValue", "query"),
            ],
        )
        .await;
    assert_eq!(sent.status, StatusCode::OK, "{}", sent.body);
    let result = "SendMessageResponse/SendMessageResult";
    let query_id = sent.xml(&format!("{result}/MessageId")).expect("MessageId");
    let query_body_md5 = sent
        .xml(&format!("{result}/MD5OfMessageBody"))
        .expect("MD5OfMessageBody");
    // md5("via-query")
    assert_eq!(query_body_md5, "99b1d58021dd2730e9ffc17c84debeb6");
    let query_attributes_md5 = sent
        .xml(&format!("{result}/MD5OfMessageAttributes"))
        .expect("MD5OfMessageAttributes");

    let received = route
        .post_json(
            "ReceiveMessage",
            &json!({"QueueUrl": url, "MaxNumberOfMessages": 10,
                "MessageAttributeNames": ["All"]}),
        )
        .await;
    assert_eq!(received.status, StatusCode::OK, "{}", received.body);
    let messages = received.json()["Messages"].as_array().unwrap().clone();
    assert_eq!(messages.len(), 1, "{}", received.body);
    let message = &messages[0];
    assert_eq!(message["Body"], "via-query");
    assert_eq!(message["MessageId"], query_id.as_str());
    assert_eq!(message["MD5OfBody"], query_body_md5.as_str());
    assert_eq!(
        message["MD5OfMessageAttributes"],
        query_attributes_md5.as_str()
    );
    assert_eq!(
        message["MessageAttributes"],
        json!({"proto": {"DataType": "String", "StringValue": "query"}})
    );
    let deleted = route
        .post_json(
            "DeleteMessage",
            &json!({"QueueUrl": url, "ReceiptHandle": message["ReceiptHandle"]}),
        )
        .await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);

    // JSON -> Query
    let json_sent = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": url, "MessageBody": "via-json",
                "MessageAttributes": {"proto": {"DataType": "Number", "StringValue": "42"}}}),
        )
        .await;
    assert_eq!(json_sent.status, StatusCode::OK, "{}", json_sent.body);
    let json_sent = json_sent.json();
    let query_received = route
        .post_form(
            "/queue",
            &[
                ("Action", "ReceiveMessage"),
                ("QueueUrl", &url),
                ("MaxNumberOfMessages", "10"),
                ("MessageAttributeName.1", "All"),
            ],
        )
        .await;
    assert_eq!(
        query_received.status,
        StatusCode::OK,
        "{}",
        query_received.body
    );
    let message = "ReceiveMessageResponse/ReceiveMessageResult/Message";
    assert_eq!(
        query_received.xml_all(&format!("{message}/Body")),
        ["via-json"]
    );
    assert_eq!(
        query_received
            .xml(&format!("{message}/MessageId"))
            .as_deref(),
        json_sent["MessageId"].as_str()
    );
    assert_eq!(
        query_received
            .xml(&format!("{message}/MD5OfBody"))
            .as_deref(),
        json_sent["MD5OfMessageBody"].as_str()
    );
    assert_eq!(
        query_received
            .xml(&format!("{message}/MD5OfMessageAttributes"))
            .as_deref(),
        json_sent["MD5OfMessageAttributes"].as_str()
    );
    assert_eq!(
        query_received.xml_all(&format!("{message}/MessageAttribute/Name")),
        ["proto"]
    );
    assert_eq!(
        query_received
            .xml(&format!("{message}/MessageAttribute/Value/DataType"))
            .as_deref(),
        Some("Number")
    );
    assert_eq!(
        query_received
            .xml(&format!("{message}/MessageAttribute/Value/StringValue"))
            .as_deref(),
        Some("42")
    );
    let handle = query_received
        .xml(&format!("{message}/ReceiptHandle"))
        .expect("ReceiptHandle");
    let query_deleted = route
        .post_form(
            "/queue",
            &[
                ("Action", "DeleteMessage"),
                ("QueueUrl", &url),
                ("ReceiptHandle", &handle),
            ],
        )
        .await;
    assert_eq!(
        query_deleted.status,
        StatusCode::OK,
        "{}",
        query_deleted.body
    );

    // A Query write is a JSON read.
    let set = route
        .post_form(
            "/queue",
            &[
                ("Action", "SetQueueAttributes"),
                ("QueueUrl", &url),
                ("Attribute.1.Name", "VisibilityTimeout"),
                ("Attribute.1.Value", "45"),
            ],
        )
        .await;
    assert_eq!(set.status, StatusCode::OK, "{}", set.body);
    let read = route
        .post_json(
            "GetQueueAttributes",
            &json!({"QueueUrl": url, "AttributeNames": [
                "VisibilityTimeout",
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible"]}),
        )
        .await;
    assert_eq!(
        read.json()["Attributes"],
        json!({"VisibilityTimeout": "45",
            "ApproximateNumberOfMessages": "0",
            "ApproximateNumberOfMessagesNotVisible": "0"})
    );
}

/// AWS behavioural:
/// check_query_protocol_refusals_are_xml_error_responses_rejected.py
#[tokio::test]
async fn given_query_refusals_when_posted_then_each_is_an_xml_error_response_and_only_the_valid_send_lands()
 {
    let route = QueueRoute::start().await;
    let url = create_queue(&route, "protocol-refusals", json!({})).await;

    let absent = route
        .post_form(
            "/queue",
            &[("Action", "GetQueueUrl"), ("QueueName", "protocol-absent")],
        )
        .await;
    assert_eq!(absent.status, StatusCode::BAD_REQUEST, "{}", absent.body);
    assert_eq!(
        absent.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    assert_eq!(
        absent.xml("ErrorResponse/Error/Code").as_deref(),
        Some(NO_SUCH_QUEUE)
    );
    assert_eq!(
        absent.xml("ErrorResponse/Error/Message").as_deref(),
        Some("The specified queue does not exist for this wsdl version.")
    );
    let json_absent = route
        .post_json("GetQueueUrl", &json!({"QueueName": "protocol-absent"}))
        .await;
    assert_eq!(json_absent.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        json_absent.header("x-amzn-query-error"),
        Some("AWS.SimpleQueueService.NonExistentQueue;Sender")
    );
    assert_eq!(
        json_absent.json()["message"],
        "The specified queue does not exist."
    );

    let delay = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &url),
                ("MessageBody", "too-late"),
                ("DelaySeconds", "901"),
            ],
        )
        .await;
    assert_eq!(delay.status, StatusCode::BAD_REQUEST, "{}", delay.body);
    assert_eq!(
        delay.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    assert_eq!(
        delay.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidParameterValue")
    );
    assert_eq!(
        delay.xml("ErrorResponse/Error/Message").as_deref(),
        Some(
            "Value 901 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 \
             and <= 900."
        )
    );
    let json_delay = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": url, "MessageBody": "too-late", "DelaySeconds": 901}),
        )
        .await;
    assert_eq!(
        json_delay.header("x-amzn-query-error"),
        Some("InvalidParameterValue;Sender")
    );
    assert_eq!(
        json_delay.json()["message"],
        "Value 901 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and \
         <= 900."
    );

    let entries = (1..=11)
        .flat_map(|index| {
            [
                (
                    format!("DeleteMessageBatchRequestEntry.{index}.Id"),
                    format!("i{index}"),
                ),
                (
                    format!("DeleteMessageBatchRequestEntry.{index}.ReceiptHandle"),
                    "not-a-handle".to_owned(),
                ),
            ]
        })
        .collect::<Vec<_>>();
    let mut fields = vec![("Action", "DeleteMessageBatch"), ("QueueUrl", url.as_str())];
    fields.extend(
        entries
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    );
    let batch = route.post_form("/queue", &fields).await;
    assert_eq!(batch.status, StatusCode::BAD_REQUEST, "{}", batch.body);
    assert_eq!(
        batch.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    assert_eq!(
        batch.xml("ErrorResponse/Error/Code").as_deref(),
        Some("AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
    );
    assert_eq!(
        batch.xml("ErrorResponse/Error/Message").as_deref(),
        Some("Maximum number of entries per request are 10. You have sent 11.")
    );

    let unknown = route
        .post_form("/queue", &[("Action", "NoSuchAction"), ("QueueUrl", &url)])
        .await;
    assert_eq!(unknown.status, StatusCode::BAD_REQUEST, "{}", unknown.body);
    assert_eq!(
        unknown.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    assert_eq!(
        unknown.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidAction")
    );
    assert_eq!(
        unknown.xml("ErrorResponse/Error/Message").as_deref(),
        Some("The action NoSuchAction is not valid for this endpoint.")
    );

    let ok = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &url),
                ("MessageBody", "the-only-message"),
            ],
        )
        .await;
    assert_eq!(ok.status, StatusCode::OK, "{}", ok.body);
    assert!(ok.has_xml_element("SendMessageResult"));
    let received = route
        .post_json(
            "ReceiveMessage",
            &json!({"QueueUrl": url, "MaxNumberOfMessages": 10}),
        )
        .await;
    let bodies = received.json()["Messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["Body"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(bodies, ["the-only-message"]);
}
