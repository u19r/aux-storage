//! SQS wire protocol scenarios (W14-P) through the assembled `/queue` route.

use axum::http::StatusCode;
use serde_json::json;

use super::test_support::QueueRoute;

#[tokio::test]
async fn given_json_protocol_errors_when_posted_then_error_codes_match() {
    let route = QueueRoute::start().await;

    let unsupported = route.post_json("Unsupported", &json!({})).await;
    assert_eq!(unsupported.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unsupported.json()["__type"],
        "com.amazonaws.sqs#InvalidAction"
    );
    assert_eq!(
        unsupported.header("x-amzn-query-error"),
        Some("InvalidAction;Sender")
    );

    let malformed = route
        .post(
            "/queue",
            "application/x-amz-json-1.0",
            Some("AmazonSQS.CreateQueue"),
            "{".to_owned(),
        )
        .await;
    assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        malformed.json()["__type"],
        "com.amazon.coral.service#InvalidParameterValueException"
    );

    let wrong_content_type = route
        .post(
            "/queue",
            "application/json",
            Some("AmazonSQS.CreateQueue"),
            r#"{"QueueName":"wrong-content-type"}"#.to_owned(),
        )
        .await;
    assert_eq!(wrong_content_type.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        wrong_content_type.json()["__type"],
        "com.amazon.coral.service#InvalidParameterValueException"
    );
    assert_eq!(
        wrong_content_type.json()["message"],
        "The content type application/json is not supported."
    );
}

#[tokio::test]
async fn given_unknown_action_when_posted_in_either_protocol_then_invalid_action() {
    let route = QueueRoute::start().await;

    let query = route.post_form("/queue", &[("Action", "Bogus")]).await;
    assert_eq!(query.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        query.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidAction")
    );
    assert_eq!(
        query.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    let missing = route
        .post_form("/queue", &[("Version", "2012-11-05")])
        .await;
    assert_eq!(
        missing.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidAction")
    );
    let publish = route
        .post_form("/queue", &[("Action", "PublishMessage")])
        .await;
    assert_eq!(
        publish.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidAction")
    );
    let json = route.post_json("Bogus", &json!({})).await;
    assert_eq!(json.status, StatusCode::BAD_REQUEST);
    assert_eq!(json.json()["__type"], "com.amazonaws.sqs#InvalidAction");
    let no_target = route
        .post(
            "/queue",
            "application/x-amz-json-1.0",
            None,
            "{}".to_owned(),
        )
        .await;
    assert_eq!(
        no_target.json()["__type"],
        "com.amazonaws.sqs#InvalidAction"
    );
}

#[tokio::test]
async fn given_queue_with_other_attributes_when_created_again_then_name_clash_codes_match() {
    let route = QueueRoute::start().await;
    let created = route
        .post_json(
            "CreateQueue",
            &json!({"QueueName": "orders", "Attributes": {"VisibilityTimeout": "30"}}),
        )
        .await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.body);

    let query = route
        .post_form(
            "/queue",
            &[
                ("Action", "CreateQueue"),
                ("QueueName", "orders"),
                ("Attribute.1.Name", "VisibilityTimeout"),
                ("Attribute.1.Value", "60"),
            ],
        )
        .await;
    assert_eq!(query.status, StatusCode::BAD_REQUEST, "{}", query.body);
    assert_eq!(
        query.xml("ErrorResponse/Error/Code").as_deref(),
        Some("QueueAlreadyExists")
    );
    assert_eq!(
        query.header("x-amzn-query-error"),
        Some("QueueAlreadyExists;Sender")
    );
    let json = route
        .post_json(
            "CreateQueue",
            &json!({"QueueName": "orders", "Attributes": {"VisibilityTimeout": "60"}}),
        )
        .await;
    assert_eq!(json.json()["__type"], "com.amazonaws.sqs#QueueNameExists");
}

async fn create_queue(route: &QueueRoute, name: &str) -> String {
    let created = route
        .post_json("CreateQueue", &json!({"QueueName": name}))
        .await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.body);
    created.json()["QueueUrl"].as_str().unwrap().to_owned()
}

/// D3 and probe 12: an unknown top-level JSON member is ignored.
#[tokio::test]
async fn given_unknown_json_member_when_queue_is_created_then_it_is_ignored() {
    let route = QueueRoute::start().await;

    let created = route
        .post_json(
            "CreateQueue",
            &json!({"QueueName": "x", "Tags": {"env": "dev"}, "WaitTimeSecond": 5}),
        )
        .await;

    assert_eq!(created.status, StatusCode::OK, "{}", created.body);
}

/// W14-Q1: entry message attributes survive the Query protocol (C12).
#[tokio::test]
async fn given_query_batch_entry_attributes_when_sent_then_receive_returns_them() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "orders").await;

    let sent = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessageBatch"),
                ("QueueUrl", &queue_url),
                ("SendMessageBatchRequestEntry.1.Id", "a"),
                ("SendMessageBatchRequestEntry.1.MessageBody", "body"),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name",
                    "trace",
                ),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType",
                    "String",
                ),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue",
                    "abc",
                ),
            ],
        )
        .await;
    assert_eq!(sent.status, StatusCode::OK, "{}", sent.body);

    let received = route
        .post_json(
            "ReceiveMessage",
            &json!({"QueueUrl": queue_url, "MessageAttributeNames": ["All"]}),
        )
        .await;
    assert_eq!(
        received.json()["Messages"][0]["MessageAttributes"]["trace"]["StringValue"],
        "abc"
    );
}

#[tokio::test]
async fn given_indexed_query_members_when_decoded_then_lists_maps_and_entries_follow_the_index() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "ordered").await;

    let sent = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessageBatch"),
                ("QueueUrl", &queue_url),
                ("SendMessageBatchRequestEntry.2.Id", "second"),
                ("SendMessageBatchRequestEntry.2.MessageBody", "two"),
                ("SendMessageBatchRequestEntry.1.Id", "first"),
                ("SendMessageBatchRequestEntry.1.MessageBody", "one"),
                ("SendMessageBatchRequestEntry.1.DelaySeconds", "0"),
                ("UnknownParameter", "ignored"),
            ],
        )
        .await;
    assert_eq!(sent.status, StatusCode::OK, "{}", sent.body);

    let set = route
        .post_form(
            "/queue",
            &[
                ("Action", "SetQueueAttributes"),
                ("QueueUrl", &queue_url),
                ("Attribute.2.Name", "DelaySeconds"),
                ("Attribute.2.Value", "5"),
                ("Attribute.1.Name", "VisibilityTimeout"),
                ("Attribute.1.Value", "40"),
            ],
        )
        .await;
    assert_eq!(set.status, StatusCode::OK, "{}", set.body);
    let attributes = route
        .post_json(
            "GetQueueAttributes",
            &json!({"QueueUrl": queue_url, "AttributeNames": ["DelaySeconds", "VisibilityTimeout"]}),
        )
        .await;
    assert_eq!(attributes.json()["Attributes"]["DelaySeconds"], "5");
    assert_eq!(attributes.json()["Attributes"]["VisibilityTimeout"], "40");
}

#[tokio::test]
async fn given_malformed_query_members_when_decoded_then_missing_or_invalid_parameter() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "malformed").await;

    let missing_value = route
        .post_form(
            "/queue",
            &[
                ("Action", "SetQueueAttributes"),
                ("QueueUrl", &queue_url),
                ("Attribute.1.Name", "DelaySeconds"),
            ],
        )
        .await;
    assert_eq!(missing_value.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing_value.xml("ErrorResponse/Error/Code").as_deref(),
        Some("MissingParameter")
    );

    let not_integer = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &queue_url),
                ("MessageBody", "x"),
                ("DelaySeconds", "soon"),
            ],
        )
        .await;
    assert_eq!(not_integer.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        not_integer.xml("ErrorResponse/Error/Message").as_deref(),
        Some("Value soon for parameter DelaySeconds is invalid.")
    );

    let negative = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &queue_url),
                ("MessageBody", "x"),
                ("DelaySeconds", "-1"),
            ],
        )
        .await;
    assert_eq!(
        negative.xml("ErrorResponse/Error/Message").as_deref(),
        Some(
            "Value -1 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 \
             and <= 900."
        )
    );
}

/// W14-P1 and probe 1: a Query response is structured XML.
#[tokio::test]
async fn given_queue_when_query_gets_its_url_then_response_is_structured_xml() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "orders").await;

    let response = route
        .post_form(
            "/queue",
            &[
                ("Action", "GetQueueUrl"),
                ("Version", "2012-11-05"),
                ("QueueName", "orders"),
            ],
        )
        .await;

    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(
        response.header("content-type"),
        Some("text/xml; charset=utf-8")
    );
    assert!(response.header("x-amzn-requestid").is_some());
    assert_eq!(
        response
            .xml("GetQueueUrlResponse/GetQueueUrlResult/QueueUrl")
            .as_deref(),
        Some(queue_url.as_str())
    );
    assert!(
        response
            .xml("GetQueueUrlResponse/ResponseMetadata/RequestId")
            .is_some()
    );
    let deleted = route
        .post_form(
            "/queue",
            &[("Action", "DeleteQueue"), ("QueueUrl", &queue_url)],
        )
        .await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    assert!(!deleted.has_xml_element("DeleteQueueResult"));
}

/// W14-P2: Query results use AWS element names, never `member`. The shapes
/// are documentation-derived, not SDK-verified.
#[tokio::test]
async fn given_messages_when_query_batches_and_receives_then_elements_use_aws_names() {
    let route = QueueRoute::start().await;
    let created = route
        .post_json(
            "CreateQueue",
            &json!({"QueueName": "orders", "Attributes": {"MaximumMessageSize": "1024"}}),
        )
        .await;
    let queue_url = created.json()["QueueUrl"].as_str().unwrap().to_owned();
    let too_long = "x".repeat(2_000);

    let batch = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessageBatch"),
                ("QueueUrl", &queue_url),
                ("SendMessageBatchRequestEntry.1.Id", "a"),
                ("SendMessageBatchRequestEntry.1.MessageBody", "hello"),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name",
                    "trace",
                ),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType",
                    "String",
                ),
                (
                    "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue",
                    "abc",
                ),
                ("SendMessageBatchRequestEntry.2.Id", "b"),
                ("SendMessageBatchRequestEntry.2.MessageBody", &too_long),
            ],
        )
        .await;
    assert_eq!(batch.status, StatusCode::OK, "{}", batch.body);
    let result = "SendMessageBatchResponse/SendMessageBatchResult";
    assert_eq!(
        batch.xml_all(&format!("{result}/SendMessageBatchResultEntry/Id")),
        ["a"]
    );
    assert!(
        batch
            .xml(&format!(
                "{result}/SendMessageBatchResultEntry/MD5OfMessageAttributes"
            ))
            .is_some()
    );
    assert_eq!(
        batch.xml_all(&format!("{result}/BatchResultErrorEntry/Id")),
        ["b"]
    );
    assert!(!batch.has_xml_element("member"));

    let received = route
        .post_form(
            "/queue",
            &[
                ("Action", "ReceiveMessage"),
                ("QueueUrl", &queue_url),
                ("MessageAttributeName.1", "All"),
                ("AttributeName.1", "All"),
            ],
        )
        .await;
    assert_eq!(received.status, StatusCode::OK, "{}", received.body);
    let message = "ReceiveMessageResponse/ReceiveMessageResult/Message";
    assert_eq!(
        received.xml(&format!("{message}/Body")).as_deref(),
        Some("hello")
    );
    assert_eq!(
        received
            .xml(&format!("{message}/MessageAttribute/Name"))
            .as_deref(),
        Some("trace")
    );
    assert_eq!(
        received
            .xml(&format!("{message}/MessageAttribute/Value/StringValue"))
            .as_deref(),
        Some("abc")
    );
    assert!(
        received
            .xml_all(&format!("{message}/Attribute/Name"))
            .contains(&"SentTimestamp".to_owned())
    );
    assert!(!received.has_xml_element("member"));
}

/// W14-P3 and probe 6: the Query code keeps the AWS prefix and the Query text.
#[tokio::test]
async fn given_missing_queue_when_query_deletes_it_then_error_keeps_aws_prefix() {
    let route = QueueRoute::start().await;

    let response = route
        .post_form(
            "/queue",
            &[
                ("Action", "DeleteQueue"),
                ("QueueUrl", "http://localhost/queue/000000000000/ghost"),
            ],
        )
        .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.xml("ErrorResponse/Error/Code").as_deref(),
        Some("AWS.SimpleQueueService.NonExistentQueue")
    );
    assert_eq!(
        response.xml("ErrorResponse/Error/Message").as_deref(),
        Some("The specified queue does not exist for this wsdl version.")
    );
    assert_eq!(
        response.xml("ErrorResponse/Error/Type").as_deref(),
        Some("Sender")
    );
    assert_eq!(
        response.header("x-amzn-query-error"),
        Some("AWS.SimpleQueueService.NonExistentQueue;Sender")
    );
}

/// W14-P4: queue URL forms (D7).
#[tokio::test]
async fn given_queue_when_sent_through_each_url_form_then_only_the_configured_account_works() {
    let route = QueueRoute::start_with_base("http://aux:3000/queue").await;
    create_queue(&route, "orders").await;

    let by_path = route
        .post_form(
            "/queue/000000000000/orders",
            &[("Action", "SendMessage"), ("MessageBody", "one")],
        )
        .await;
    assert_eq!(by_path.status, StatusCode::OK, "{}", by_path.body);
    let other_host = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": "http://localhost:4566/000000000000/orders", "MessageBody": "two"}),
        )
        .await;
    assert_eq!(other_host.status, StatusCode::OK, "{}", other_host.body);
    let other_account = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": "http://aux:3000/queue/111111111111/orders", "MessageBody": "three"}),
        )
        .await;
    assert_eq!(other_account.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        other_account.json()["__type"],
        "com.amazonaws.sqs#QueueDoesNotExist"
    );
    let path_other_account = route
        .post_form(
            "/queue/111111111111/orders",
            &[("Action", "SendMessage"), ("MessageBody", "four")],
        )
        .await;
    assert_eq!(
        path_other_account
            .xml("ErrorResponse/Error/Code")
            .as_deref(),
        Some("AWS.SimpleQueueService.NonExistentQueue")
    );

    let attributes = route
        .post_json(
            "GetQueueAttributes",
            &json!({
                "QueueUrl": "http://aux:3000/queue/000000000000/orders",
                "AttributeNames": ["ApproximateNumberOfMessages"]
            }),
        )
        .await;
    assert_eq!(
        attributes.json()["Attributes"]["ApproximateNumberOfMessages"],
        "2"
    );
}

/// W14-L3 Query shape and probe 7: `ListQueuesResult` holds `QueueUrl`
/// elements and a `NextToken`.
#[tokio::test]
async fn given_queues_when_query_lists_one_page_then_result_has_urls_and_token() {
    let route = QueueRoute::start().await;
    let first = create_queue(&route, "p-a").await;
    create_queue(&route, "p-b").await;

    let page = route
        .post_form(
            "/queue",
            &[
                ("Action", "ListQueues"),
                ("QueueNamePrefix", "p-"),
                ("MaxResults", "1"),
            ],
        )
        .await;

    assert_eq!(page.status, StatusCode::OK, "{}", page.body);
    let result = "ListQueuesResponse/ListQueuesResult";
    assert_eq!(page.xml_all(&format!("{result}/QueueUrl")), [first]);
    assert!(page.xml(&format!("{result}/NextToken")).is_some());
}

/// Tags and the trace header through the Query protocol; the shapes are
/// documentation-derived, not SDK-verified.
#[tokio::test]
async fn given_query_tags_and_trace_header_when_sent_then_results_use_aws_elements() {
    let route = QueueRoute::start().await;
    let created = route
        .post_form(
            "/queue",
            &[
                ("Action", "CreateQueue"),
                ("QueueName", "tagged"),
                ("Tag.1.Key", "env"),
                ("Tag.1.Value", "dev"),
            ],
        )
        .await;
    let queue_url = created
        .xml("CreateQueueResponse/CreateQueueResult/QueueUrl")
        .unwrap();
    let tags = route
        .post_form(
            "/queue",
            &[("Action", "ListQueueTags"), ("QueueUrl", &queue_url)],
        )
        .await;
    let result = "ListQueueTagsResponse/ListQueueTagsResult/Tag";
    assert_eq!(tags.xml(&format!("{result}/Key")).as_deref(), Some("env"));
    assert_eq!(tags.xml(&format!("{result}/Value")).as_deref(), Some("dev"));

    let sent = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &queue_url),
                ("MessageBody", "x"),
                ("MessageSystemAttribute.1.Name", "AWSTraceHeader"),
                ("MessageSystemAttribute.1.Value.DataType", "String"),
                ("MessageSystemAttribute.1.Value.StringValue", "Root=1-abc"),
            ],
        )
        .await;
    assert!(
        sent.xml("SendMessageResponse/SendMessageResult/MD5OfMessageSystemAttributes")
            .is_some(),
        "{}",
        sent.body
    );
}

/// A member that AWS marks required and the request omits is 400
/// `MissingParameter` with the AWS text.
#[tokio::test]
async fn given_required_member_missing_when_decoded_then_missing_parameter() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "required").await;

    for (action, body, member) in [
        ("CreateQueue", json!({}), "QueueName"),
        ("GetQueueUrl", json!({}), "QueueName"),
        (
            "SetQueueAttributes",
            json!({"QueueUrl": queue_url}),
            "Attributes",
        ),
        ("SendMessage", json!({"QueueUrl": queue_url}), "MessageBody"),
        (
            "SendMessageBatch",
            json!({"QueueUrl": queue_url}),
            "Entries",
        ),
        (
            "DeleteMessage",
            json!({"QueueUrl": queue_url}),
            "ReceiptHandle",
        ),
        (
            "DeleteMessageBatch",
            json!({"QueueUrl": queue_url}),
            "Entries",
        ),
        (
            "ChangeMessageVisibility",
            json!({"QueueUrl": queue_url, "ReceiptHandle": "h"}),
            "VisibilityTimeout",
        ),
        (
            "ChangeMessageVisibilityBatch",
            json!({"QueueUrl": queue_url}),
            "Entries",
        ),
        ("TagQueue", json!({"QueueUrl": queue_url}), "Tags"),
        ("UntagQueue", json!({"QueueUrl": queue_url}), "TagKeys"),
        (
            "SendMessageBatch",
            json!({"QueueUrl": queue_url, "Entries": [{"MessageBody": "x"}]}),
            "SendMessageBatchRequestEntry.1.Id",
        ),
    ] {
        let response = route.post_json(action, &body).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST, "{action}");
        assert_eq!(
            response.header("x-amzn-query-error"),
            Some("MissingParameter;Sender"),
            "{action}: {}",
            response.body
        );
        assert_eq!(
            response.json()["message"],
            format!("The request must contain the parameter {member}."),
            "{action}"
        );
    }
}

/// Review R1: a deep Query parameter name is rejected before any recursion.
#[tokio::test]
async fn given_query_name_with_100_kb_of_segments_when_posted_then_invalid_parameter_value() {
    let route = QueueRoute::start().await;
    let name = "a.".repeat(50_000);

    let response = route
        .post_form("/queue", &[("Action", "ListQueues"), (&name, "1")])
        .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidParameterValue")
    );
}

/// Review R3, R4, L3, L7: XML text rules, `PublishMessage` on a queue URL,
/// Query indexes, and batch count codes.
#[tokio::test]
async fn given_review_cases_when_posted_then_aws_shapes_are_kept() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "review").await;

    let sent = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": queue_url, "MessageBody": "a\r\nb"}),
        )
        .await;
    assert_eq!(sent.status, StatusCode::OK, "{}", sent.body);
    let received = route
        .post_form(
            "/queue",
            &[("Action", "ReceiveMessage"), ("QueueUrl", &queue_url)],
        )
        .await;
    assert!(received.body.contains("a&#xD;\nb"), "{}", received.body);
    assert_eq!(
        received
            .xml("ReceiveMessageResponse/ReceiveMessageResult/Message/Body")
            .as_deref(),
        Some("a\r\nb")
    );

    let control = json!({"QueueUrl": queue_url, "MessageBody": "x", "MessageAttributes":
        {"k": {"DataType": "String", "StringValue": "\u{1}"}}});
    assert_eq!(
        route.post_json("SendMessage", &control).await.status,
        StatusCode::BAD_REQUEST
    );

    let published = route
        .post(
            "/queue/000000000000/review",
            "application/x-amz-json-1.0",
            Some("AmazonSQS.PublishMessage"),
            json!({"MessageId": "07070707-0707-0707-0707-070707070707", "Message": {"MessageBody": "p"}})
                .to_string(),
        )
        .await;
    assert_eq!(published.status, StatusCode::OK, "{}", published.body);

    for index in ["0", "01", "x"] {
        let name = format!("Attribute.{index}.Name");
        let value = format!("Attribute.{index}.Value");
        let response = route
            .post_form(
                "/queue",
                &[
                    ("Action", "SetQueueAttributes"),
                    ("QueueUrl", &queue_url),
                    (&name, "DelaySeconds"),
                    (&value, "0"),
                ],
            )
            .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST, "{index}");
    }

    let empty = route
        .post_json(
            "SendMessageBatch",
            &json!({"QueueUrl": queue_url, "Entries": []}),
        )
        .await;
    assert_eq!(
        empty.header("x-amzn-query-error"),
        Some("AWS.SimpleQueueService.EmptyBatchRequest;Sender")
    );
    let entries = (0..11)
        .map(|index| json!({"Id": format!("e{index}"), "MessageBody": "x"}))
        .collect::<Vec<_>>();
    let many = route
        .post_json(
            "SendMessageBatch",
            &json!({"QueueUrl": queue_url, "Entries": entries}),
        )
        .await;
    assert_eq!(
        many.json()["message"],
        "Maximum number of entries per request are 10. You have sent 11."
    );
}

/// Review R5 and R6: size bounds (not probed) and the probe 9 redrive texts.
#[tokio::test]
async fn given_large_values_or_bad_redrive_policies_when_set_then_aws_errors() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "limits").await;
    let set =
        |name: &str, value: String| json!({"QueueUrl": queue_url, "Attributes": {name: value}});

    for body in [
        set("KmsMasterKeyId", "k".repeat(2_049)),
        set("Policy", format!(r#"{{"a":"{}"}}"#, "p".repeat(95_000))),
    ] {
        let response = route.post_json("SetQueueAttributes", &body).await;
        assert_eq!(
            response.status,
            StatusCode::BAD_REQUEST,
            "{}",
            response.body
        );
    }
    let arn = "arn:aws:sqs:us-east-1:000000000000:limits";
    for (policy, code, reason) in [
        ("{not json".to_owned(), "InvalidAttributeValue", None),
        (
            "{}".to_owned(),
            "InvalidParameterValue",
            Some("Redrive policy does not contain mandatory attribute: maxReceiveCount."),
        ),
        (
            format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":1,"foo":1}}"#),
            "InvalidParameterValue",
            Some(
                "Only following attributes are supported: [maxReceiveCount, deadLetterTargetArn].",
            ),
        ),
        (
            format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"abc"}}"#),
            "InvalidParameterValue",
            Some(
                "Invalid value for maxReceiveCount: abc, valid values are from 1 to 1000 both \
                 inclusive.",
            ),
        ),
    ] {
        let response = route
            .post_json("SetQueueAttributes", &set("RedrivePolicy", policy.clone()))
            .await;
        assert_eq!(
            response.header("x-amzn-query-error"),
            Some(format!("{code};Sender").as_str()),
            "{policy}"
        );
        let message = response.json()["message"].as_str().unwrap().to_owned();
        match reason {
            Some(reason) => assert_eq!(
                message,
                format!("Value {policy} for parameter RedrivePolicy is invalid. Reason: {reason}")
            ),
            None => assert_eq!(
                message,
                "Invalid value for the parameter RedrivePolicy. Reason: Redrive policy is not a \
                 valid JSON map."
            ),
        }
    }
}

/// Review L10: a queue URL path with a trailing `/` is the queue's route.
#[tokio::test]
async fn given_queue_url_path_with_trailing_slash_when_posted_then_it_is_routed() {
    let route = QueueRoute::start().await;
    create_queue(&route, "slash").await;
    let response = route
        .post_form(
            "/queue/000000000000/slash/",
            &[("Action", "SendMessage"), ("MessageBody", "x")],
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert!(response.has_xml_element("MessageId"));
}

/// Review L10: a refused ingress slot and an oversized body are answered in
/// the SQS shape of the request's protocol.
#[tokio::test]
async fn given_refused_slot_or_large_body_when_posted_then_the_error_has_the_sqs_shape() {
    let refused = QueueRoute::start_without_ingress().await;
    let query = refused
        .post_form("/queue", &[("Action", "ListQueues")])
        .await;
    assert_eq!(query.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        query.xml("ErrorResponse/Error/Code").as_deref(),
        Some("ServiceUnavailable")
    );
    assert_eq!(query.header("retry-after"), Some("1"));
    let json = refused
        .post_json("ListQueues", &serde_json::json!({}))
        .await;
    assert_eq!(json.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json.json()["__type"],
        "com.amazon.coral.service#ServiceUnavailableException"
    );

    let route = QueueRoute::start().await;
    let large = format!("Action=ListQueues&Pad={}", "x".repeat(5 * 1024 * 1024));
    let response = route
        .post("/queue", "application/x-www-form-urlencoded", None, large)
        .await;
    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response.xml("ErrorResponse/Error/Code").as_deref(),
        Some("InvalidParameterValue")
    );
}

/// Review L1, L2, L4, L5: a required member sent as `null` is missing; a
/// batch entry member is named with its entry; another action's entry
/// prefix is ignored; no internal code reaches the client.
#[tokio::test]
async fn given_null_member_or_bad_entry_when_decoded_then_the_error_names_the_parameter() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "members").await;
    let null = route
        .post_json(
            "SendMessage",
            &serde_json::json!({"QueueUrl": queue_url, "MessageBody": null}),
        )
        .await;
    assert_eq!(null.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        null.json()["message"],
        "The request must contain the parameter MessageBody."
    );

    let entry = route
        .post_form(
            "/queue",
            &[
                ("Action", "DeleteMessageBatch"),
                ("QueueUrl", &queue_url),
                ("DeleteMessageBatchRequestEntry.1.Id", "a"),
                ("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "h"),
                ("DeleteMessageBatchRequestEntry.2.ReceiptHandle", "h"),
            ],
        )
        .await;
    assert_eq!(
        entry.xml("ErrorResponse/Error/Message").as_deref(),
        Some("The request must contain the parameter DeleteMessageBatchRequestEntry.2.Id.")
    );

    let foreign = route
        .post_form(
            "/queue",
            &[
                ("Action", "SendMessage"),
                ("QueueUrl", &queue_url),
                ("MessageBody", "x"),
                ("DeleteMessageBatchRequestEntry.1.Id", "a"),
            ],
        )
        .await;
    assert_eq!(foreign.status, StatusCode::OK, "{}", foreign.body);

    let content_type = route
        .post("/queue", "application/json", None, "{}".to_owned())
        .await;
    assert!(!content_type.body.contains("unsupported_content_type"));
}

/// OWN-A-a: the SQS `MessageId` is a hyphenated UUIDv7; the old 24-hex form is
/// refused on `PublishMessage`.
#[tokio::test]
async fn given_send_message_when_answered_then_message_id_is_a_hyphenated_uuid() {
    let route = QueueRoute::start().await;
    let queue_url = create_queue(&route, "uuid-id").await;

    let sent = route
        .post_json(
            "SendMessage",
            &json!({"QueueUrl": queue_url, "MessageBody": "x"}),
        )
        .await;
    assert_eq!(sent.status, StatusCode::OK, "{}", sent.body);
    let response = sent.json();
    let message_id = response["MessageId"].as_str().expect("MessageId string");
    assert_eq!(message_id.len(), 36, "{message_id}");
    let uuid = uuid::Uuid::try_parse(message_id).expect("MessageId should be a UUID");
    assert_eq!(uuid.get_version_num(), 7);

    let published = route
        .post(
            "/queue/000000000000/uuid-id",
            "application/x-amz-json-1.0",
            Some("AmazonSQS.PublishMessage"),
            json!({"MessageId": "070707070707070707070707", "Message": {"MessageBody": "p"}})
                .to_string(),
        )
        .await;
    assert_eq!(
        published.status,
        StatusCode::BAD_REQUEST,
        "{}",
        published.body
    );
}
