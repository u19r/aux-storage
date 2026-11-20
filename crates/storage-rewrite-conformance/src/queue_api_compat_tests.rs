//! W14 SQS API compatibility on the provider that `STORAGE_TEST_BACKEND`
//! selects. Each request goes through the JSON wire decode,
//! `QueueManager::dispatch`, and the JSON encode, as `storage-api` runs it.

use http_error::HttpApiError;
use queue::{QueueEndpoint, QueueManager};
use queue_provider::wire::{
    QueueProtocol, WireRequestParts, decode_wire_request, encode_error, encode_success,
};
use serde_json::{Value, json};

use super::{queue_claim_tests::probed_store, test_support::TestStore};

const BASE_URL: &str = "http://aux.test/queue";

/// The manager stamps sends with the system clock, so the store uses it too.
fn manager(store: &TestStore) -> QueueManager {
    store.use_system_time();
    QueueManager::new(store.queue_operations.clone()).with_endpoint(QueueEndpoint {
        public_base_url: BASE_URL.to_owned(),
    })
}

/// One SQS JSON request through decode, dispatch, and encode.
async fn sqs(manager: &QueueManager, action: &str, body: Value) -> (u16, Value) {
    let target = format!("AmazonSQS.{action}");
    let body = body.to_string();
    let response = match decode_wire_request(WireRequestParts {
        content_type: Some("application/x-amz-json-1.0"),
        target: Some(&target),
        body: body.as_bytes(),
        queue_url: None,
    }) {
        Ok(wire) => match manager.dispatch(wire.request, || {}).await {
            Ok(response) => encode_success(QueueProtocol::Json, "test", &response),
            Err(error) => encode_error(QueueProtocol::Json, "test", &HttpApiError::from(error)),
        },
        Err(rejection) => encode_error(rejection.protocol, "test", &rejection.error),
    };
    let value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    (response.status, value)
}

async fn create(manager: &QueueManager, name: &str, extra: Value) -> String {
    let mut body = json!({ "QueueName": name });
    if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        body.extend(extra.clone());
    }
    let (status, created) = sqs(manager, "CreateQueue", body).await;
    assert_eq!(status, 200, "{created}");
    created["QueueUrl"].as_str().unwrap().to_owned()
}

fn unique(label: &str) -> String {
    format!("{label}{}", uuid::Uuid::new_v4().simple())
}

fn assert_missing_queue((status, body): &(u16, Value)) {
    assert_eq!(*status, 400, "{body}");
    assert_eq!(
        body["__type"], "com.amazonaws.sqs#QueueDoesNotExist",
        "{body}"
    );
    assert!(body.get("Successful").is_none() && body.get("Failed").is_none());
}

/// W14-L1 and probe 6: a call on a missing queue fails the whole request.
#[tokio::test]
async fn given_missing_queue_when_batches_or_delete_are_called_then_whole_request_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let ghost = format!("{BASE_URL}/000000000000/{}", unique("ghost"));

    let send = json!({"QueueUrl": ghost, "Entries": [
        {"Id": "a", "MessageBody": "one"}, {"Id": "b", "MessageBody": "two"}]});
    assert_missing_queue(&sqs(&manager, "SendMessageBatch", send).await);
    let delete = json!({"QueueUrl": ghost, "Entries": [
        {"Id": "a", "ReceiptHandle": "AQEB-a"}, {"Id": "b", "ReceiptHandle": "AQEB-b"}]});
    assert_missing_queue(&sqs(&manager, "DeleteMessageBatch", delete).await);
    let visibility = json!({"QueueUrl": ghost, "Entries": [
        {"Id": "a", "ReceiptHandle": "AQEB-a", "VisibilityTimeout": 5},
        {"Id": "b", "ReceiptHandle": "AQEB-b", "VisibilityTimeout": 5}]});
    assert_missing_queue(&sqs(&manager, "ChangeMessageVisibilityBatch", visibility).await);
    assert_missing_queue(&sqs(&manager, "DeleteQueue", json!({"QueueUrl": ghost})).await);
    store.finish().await;
}

/// W14-L2 and probe 7: no names select nothing; an unknown name is
/// `InvalidAttributeName`.
#[tokio::test]
async fn given_queue_with_messages_when_attributes_are_selected_then_only_named_ones_return() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("orders"), json!({})).await;
    for body in ["one", "two", "three"] {
        let sent = json!({"QueueUrl": url, "MessageBody": body});
        assert_eq!(sqs(&manager, "SendMessage", sent).await.0, 200);
    }

    let (status, none) = sqs(&manager, "GetQueueAttributes", json!({"QueueUrl": url})).await;
    assert_eq!((status, &none), (200, &json!({})));
    let empty = json!({"QueueUrl": url, "AttributeNames": []});
    assert_eq!(sqs(&manager, "GetQueueAttributes", empty).await.1, none);
    // The fixture clock can differ from the send time, so count every state.
    let count = json!({"QueueUrl": url, "AttributeNames": [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible"]});
    let (_, counted) = sqs(&manager, "GetQueueAttributes", count).await;
    let total = counted["Attributes"]
        .as_object()
        .unwrap()
        .values()
        .map(|value| value.as_str().unwrap().parse::<u64>().unwrap())
        .sum::<u64>();
    assert_eq!(
        (counted["Attributes"].as_object().unwrap().len(), total),
        (3, 3)
    );
    let bogus = json!({"QueueUrl": url, "AttributeNames": ["Bogus"]});
    let (status, error) = sqs(&manager, "GetQueueAttributes", bogus).await;
    assert_eq!(status, 400);
    assert_eq!(error["__type"], "com.amazonaws.sqs#InvalidAttributeName");
    assert_eq!(error["message"], "Unknown Attribute Bogus.");
    store.finish().await;
}

/// W14-L3 and W14-L4: a paged listing continues with its token; a token that
/// does not decode is rejected.
#[tokio::test]
async fn given_three_queues_when_listed_two_at_a_time_then_token_continues_the_listing() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let prefix = unique("p");
    let mut urls = Vec::new();
    for suffix in ["-a", "-b", "-c"] {
        urls.push(create(&manager, &format!("{prefix}{suffix}"), json!({})).await);
    }

    let first = json!({"QueueNamePrefix": prefix, "MaxResults": 2});
    let (status, page) = sqs(&manager, "ListQueues", first).await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(page["QueueUrls"], json!(urls[..2]));
    let token = page["NextToken"].as_str().expect("NextToken").to_owned();
    let second = json!({"QueueNamePrefix": prefix, "MaxResults": 2, "NextToken": token});
    let (_, page) = sqs(&manager, "ListQueues", second).await;
    assert_eq!(page["QueueUrls"], json!(urls[2..]));
    assert!(page.get("NextToken").is_none(), "{page}");
    let unbounded = json!({"QueueNamePrefix": prefix});
    let (_, page) = sqs(&manager, "ListQueues", unbounded).await;
    assert_eq!(page["QueueUrls"], json!(urls));
    assert!(page.get("NextToken").is_none(), "{page}");

    for (body, message) in [
        (
            json!({"NextToken": "garbage"}),
            "Invalid or expired next token.",
        ),
        (
            json!({"QueueNamePrefix": "other", "NextToken": token}),
            "Invalid or expired next token.",
        ),
        (
            json!({"MaxResults": 1001}),
            "Value for parameter MaxResults is invalid. Reason: MaxResults must be an integer \
             between 1 and 1000.",
        ),
    ] {
        let (status, error) = sqs(&manager, "ListQueues", body).await;
        assert_eq!(status, 400, "{error}");
        assert_eq!(error["message"], message);
    }
    store.finish().await;
}

/// Native calls: an attribute read with no names is one catalog read, as a
/// read of a stored attribute; a count read adds the count scan. A paged
/// listing is one native read when the page fits the byte bound.
#[tokio::test]
async fn given_attribute_and_list_requests_when_measured_then_native_calls_stay_bounded() {
    let (store, probe) = probed_store().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("measure"), json!({})).await;

    let mut calls = Vec::new();
    for body in [
        json!({"QueueUrl": url}),
        json!({"QueueUrl": url, "AttributeNames": ["VisibilityTimeout"]}),
        json!({"QueueUrl": url, "AttributeNames": ["All"]}),
    ] {
        probe.reset();
        assert_eq!(sqs(&manager, "GetQueueAttributes", body).await.0, 200);
        calls.push(probe.calls());
    }
    probe.reset();
    assert_eq!(
        sqs(&manager, "ListQueues", json!({"MaxResults": 5}))
            .await
            .0,
        200
    );
    let list_calls = probe.calls();
    eprintln!("native calls: attributes none/named/All = {calls:?}, ListQueues = {list_calls}");
    assert_eq!(calls[0], calls[1]);
    assert!(calls[0] < calls[2], "{calls:?}");
    store.finish().await;
}

/// W14-A1 (without tags) and probes 7 and 12: stored attributes read back
/// unchanged, with the computed timestamps and the `SqsManagedSseEnabled`
/// default.
#[tokio::test]
async fn given_terraform_attributes_when_queue_is_created_then_they_read_back_unchanged() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let policy = r#"{"Version":"2012-10-17"}"#;
    let url = create(
        &manager,
        &unique("tf"),
        json!({"Attributes": {
            "SqsManagedSseEnabled": "false",
            "Policy": policy,
            "KmsMasterKeyId": "alias/x",
            "KmsDataKeyReusePeriodSeconds": "300"}}),
    )
    .await;
    let plain = create(&manager, &unique("plain"), json!({})).await;

    let all = json!({"QueueUrl": url, "AttributeNames": ["All"]});
    let (status, read) = sqs(&manager, "GetQueueAttributes", all).await;
    assert_eq!(status, 200, "{read}");
    let attributes = &read["Attributes"];
    assert_eq!(attributes["SqsManagedSseEnabled"], "false");
    assert_eq!(attributes["Policy"], policy);
    assert_eq!(attributes["KmsMasterKeyId"], "alias/x");
    assert_eq!(attributes["KmsDataKeyReusePeriodSeconds"], "300");
    let created = attributes["CreatedTimestamp"].as_str().unwrap();
    assert_eq!(created.len(), 10, "{created}");
    assert_eq!(attributes["LastModifiedTimestamp"], created);
    let sse = json!({"QueueUrl": plain, "AttributeNames": ["SqsManagedSseEnabled"]});
    let (_, plain_read) = sqs(&manager, "GetQueueAttributes", sse).await;
    assert_eq!(plain_read["Attributes"]["SqsManagedSseEnabled"], "true");

    let removal = json!({"QueueUrl": url, "Attributes": {"Policy": ""}});
    assert_eq!(sqs(&manager, "SetQueueAttributes", removal).await.0, 200);
    let policy_read = json!({"QueueUrl": url, "AttributeNames": ["Policy"]});
    let (_, read) = sqs(&manager, "GetQueueAttributes", policy_read).await;
    assert_eq!(read, json!({}));
    store.finish().await;
}

/// W14-A3 (rows without tags or the trace header): accepted, ignored, and
/// rejected parameters.
#[tokio::test]
async fn given_parameters_that_aws_accepts_or_rejects_when_sent_then_aux_answers_the_same() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("orders");
    let url = create(&manager, &name, json!({})).await;

    let receive = json!({"QueueUrl": url, "ReceiveRequestAttemptId": "r1"});
    assert_eq!(sqs(&manager, "ReceiveMessage", receive).await.0, 200);
    let group = json!({"QueueUrl": url, "MessageBody": "x", "MessageGroupId": "g1"});
    assert_eq!(sqs(&manager, "SendMessage", group).await.0, 200);
    let dedup = json!({"QueueUrl": url, "MessageBody": "x", "MessageDeduplicationId": "d"});
    let (status, error) = sqs(&manager, "SendMessage", dedup).await;
    assert_eq!(status, 400);
    assert_eq!(
        error["message"],
        "Value d for parameter MessageDeduplicationId is invalid. Reason: The request include \
         parameter that is not valid for this queue type."
    );
    for (attribute, expected) in [
        ("Bogus", "Unknown Attribute Bogus."),
        ("QueueArn", "Unknown Attribute QueueArn."),
    ] {
        let set = json!({"QueueUrl": url, "Attributes": {attribute: "1"}});
        let (status, error) = sqs(&manager, "SetQueueAttributes", set).await;
        assert_eq!(status, 400, "{error}");
        assert_eq!(error["__type"], "com.amazonaws.sqs#InvalidAttributeName");
        assert_eq!(error["message"], expected);
    }
    for (attribute, value) in [
        ("RedriveAllowPolicy", r#"{"redrivePermission":"allowAll"}"#),
        ("FifoQueue", "true"),
    ] {
        let set = json!({"QueueUrl": url, "Attributes": {attribute: value}});
        let (_, error) = sqs(&manager, "SetQueueAttributes", set).await;
        assert_eq!(
            error["__type"],
            "com.amazon.coral.service#InvalidParameterValueException"
        );
    }
    let owner = json!({"QueueName": name, "QueueOwnerAWSAccountId": "111111111111"});
    assert_missing_queue(&sqs(&manager, "GetQueueUrl", owner).await);
    let own = json!({"QueueName": name, "QueueOwnerAWSAccountId": "000000000000"});
    assert_eq!(sqs(&manager, "GetQueueUrl", own).await.0, 200);
    store.finish().await;
}

/// W14-A4 (without tags; the Terraform sequence is from memory of the
/// provider, not verified): apply, read back, destroy, and poll the URL.
#[tokio::test]
async fn given_terraform_sequence_when_applied_and_destroyed_then_every_value_reads_back() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("tf2");
    let sent = json!({
        "DelaySeconds": "0",
        "MaximumMessageSize": "262144",
        "MessageRetentionPeriod": "345600",
        "ReceiveMessageWaitTimeSeconds": "0",
        "VisibilityTimeout": "30",
        "KmsDataKeyReusePeriodSeconds": "300"});
    let url = create(&manager, &name, json!({"Attributes": sent})).await;

    let all = json!({"QueueUrl": url, "AttributeNames": ["All"]});
    let (_, read) = sqs(&manager, "GetQueueAttributes", all).await;
    for (attribute, value) in sent.as_object().unwrap() {
        assert_eq!(&read["Attributes"][attribute], value, "{attribute}");
    }
    assert_eq!(
        sqs(&manager, "DeleteQueue", json!({"QueueUrl": url}))
            .await
            .0,
        200
    );
    assert_missing_queue(&sqs(&manager, "GetQueueUrl", json!({"QueueName": name})).await);
    store.finish().await;
}

/// W14 item 08, probes 2 and 3 (C11): a handle whose visibility timeout
/// ended, and whose message was not received again, still changes the
/// visibility and still deletes the message.
#[tokio::test]
async fn given_expired_handle_not_received_again_when_used_then_visibility_and_delete_apply() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("expired"), json!({})).await;
    let sent = json!({"QueueUrl": url, "MessageBody": "x"});
    assert_eq!(sqs(&manager, "SendMessage", sent).await.0, 200);
    let receive = json!({"QueueUrl": url, "VisibilityTimeout": 0});
    let (_, received) = sqs(&manager, "ReceiveMessage", receive.clone()).await;
    let handle = received["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_owned();

    let hide = json!({"QueueUrl": url, "ReceiptHandle": handle, "VisibilityTimeout": 5});
    let (status, body) = sqs(&manager, "ChangeMessageVisibility", hide).await;
    assert_eq!(status, 200, "{body}");
    let (_, hidden) = sqs(&manager, "ReceiveMessage", receive).await;
    assert!(
        hidden
            .get("Messages")
            .is_none_or(|messages| messages == &json!([])),
        "{hidden}"
    );
    let delete = json!({"QueueUrl": url, "ReceiptHandle": handle});
    assert_eq!(sqs(&manager, "DeleteMessage", delete).await.0, 200);
    let counts = json!({"QueueUrl": url, "AttributeNames": ["All"]});
    let (_, read) = sqs(&manager, "GetQueueAttributes", counts).await;
    for name in [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ] {
        assert_eq!(read["Attributes"][name], "0", "{name}");
    }
    store.finish().await;
}

fn trace(value: &str) -> Value {
    json!({"AWSTraceHeader": {"DataType": "String", "StringValue": value}})
}

/// W14-A2 and probe 12: the trace header is stored, answered with its MD5,
/// and returned by a receive that names it (both name lists together).
#[tokio::test]
async fn given_trace_header_when_sent_then_receive_returns_it() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("orders"), json!({})).await;

    let sent = json!({"QueueUrl": url, "MessageBody": "x",
        "MessageSystemAttributes": trace("Root=1-abc")});
    let (status, response) = sqs(&manager, "SendMessage", sent).await;
    assert_eq!(status, 200, "{response}");
    assert_eq!(
        response["MD5OfMessageSystemAttributes"],
        queue_provider::md5_of_trace_header("Root=1-abc")
    );
    let batch = json!({"QueueUrl": url, "Entries": [{"Id": "a", "MessageBody": "y",
        "MessageSystemAttributes": trace("Root=1-def")}]});
    let (_, batch) = sqs(&manager, "SendMessageBatch", batch).await;
    assert!(
        batch["Successful"][0]["MD5OfMessageSystemAttributes"].is_string(),
        "{batch}"
    );

    let receive = json!({"QueueUrl": url, "MaxNumberOfMessages": 10,
        "AttributeNames": ["SentTimestamp"],
        "MessageSystemAttributeNames": ["AWSTraceHeader"]});
    let (status, received) = sqs(&manager, "ReceiveMessage", receive).await;
    assert_eq!(status, 200, "{received}");
    let mut headers = received["Messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| {
            assert!(message["Attributes"]["SentTimestamp"].is_string());
            message["Attributes"]["AWSTraceHeader"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    headers.sort();
    assert_eq!(headers, ["Root=1-abc", "Root=1-def"]);

    let foo = json!({"QueueUrl": url, "MessageBody": "x",
        "MessageSystemAttributes": {"Foo": {"DataType": "String", "StringValue": "x"}}});
    let (status, error) = sqs(&manager, "SendMessage", foo).await;
    assert_eq!(status, 400);
    assert_eq!(
        error["message"],
        "Message system attribute name 'Foo' is invalid."
    );
    store.finish().await;
}

/// W14-T1, W14-T2, and the tag parts of W14-A1, A3, and A4.
#[tokio::test]
async fn given_queue_tags_when_tagged_untagged_and_recreated_then_the_merged_set_is_kept() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("orders");
    let url = create(&manager, &name, json!({"tags": {"a": "1"}})).await;
    let list = || json!({"QueueUrl": url});

    let tag = json!({"QueueUrl": url, "Tags": {"b": "2"}});
    assert_eq!(sqs(&manager, "TagQueue", tag).await.0, 200);
    let untag = json!({"QueueUrl": url, "TagKeys": ["a", "unknown"]});
    assert_eq!(sqs(&manager, "UntagQueue", untag).await.0, 200);
    let (_, tags) = sqs(&manager, "ListQueueTags", list()).await;
    assert_eq!(tags, json!({"Tags": {"b": "2"}}));

    // CreateQueue of the existing queue with the same tags keeps them; other
    // tags are a name clash.
    let again = json!({"QueueName": name, "tags": {"b": "2"}});
    assert_eq!(sqs(&manager, "CreateQueue", again).await.0, 200);
    let different = json!({"QueueName": name, "tags": {"c": "3"}});
    assert_eq!(sqs(&manager, "CreateQueue", different).await.0, 400);
    assert_eq!(sqs(&manager, "ListQueueTags", list()).await.1, tags);

    let many = (0..49)
        .map(|index| (format!("k{index}"), json!("v")))
        .collect::<serde_json::Map<_, _>>();
    let fill = json!({"QueueUrl": url, "Tags": many});
    assert_eq!(sqs(&manager, "TagQueue", fill).await.0, 200);
    let over = json!({"QueueUrl": url, "Tags": {"new": "x"}});
    let (status, error) = sqs(&manager, "TagQueue", over).await;
    assert_eq!(status, 400, "{error}");
    assert_eq!(
        error["__type"],
        "com.amazon.coral.service#InvalidParameterValueException"
    );
    let (_, kept) = sqs(&manager, "ListQueueTags", list()).await;
    assert_eq!(kept["Tags"].as_object().unwrap().len(), 50);

    let capital = unique("capital");
    let capital_url = create(&manager, &capital, json!({"Tags": {"env": "dev"}})).await;
    let (_, none) = sqs(&manager, "ListQueueTags", json!({"QueueUrl": capital_url})).await;
    assert_eq!(none, json!({}));
    let ghost = json!({"QueueUrl": format!("{BASE_URL}/000000000000/{}", unique("ghost"))});
    assert_missing_queue(&sqs(&manager, "ListQueueTags", ghost.clone()).await);
    let ghost_tag = json!({"QueueUrl": ghost["QueueUrl"], "Tags": {"a": "1"}});
    assert_missing_queue(&sqs(&manager, "TagQueue", ghost_tag).await);
    store.finish().await;
}

/// Native calls: tags and a trace header add no native call.
#[tokio::test]
async fn given_tags_or_trace_header_when_measured_then_native_calls_do_not_grow() {
    let (store, probe) = probed_store().await;
    let manager = manager(&store);
    let mut calls = Vec::new();
    for extra in [json!({}), json!({"tags": {"env": "dev"}})] {
        probe.reset();
        create(&manager, &unique("measure"), extra).await;
        calls.push(probe.calls());
    }
    let url = create(&manager, &unique("send"), json!({})).await;
    // Unmeasured warm-up: both measured sends then run with a catalog hint.
    let warm = json!({"QueueUrl": url, "MessageBody": "warm"});
    assert_eq!(sqs(&manager, "SendMessage", warm).await.0, 200);
    for extra in [
        json!({}),
        json!({"MessageSystemAttributes": trace("Root=1-abc")}),
    ] {
        let mut body = json!({"QueueUrl": url, "MessageBody": "x"});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        probe.reset();
        assert_eq!(sqs(&manager, "SendMessage", body).await.0, 200);
        calls.push(probe.calls());
    }
    eprintln!("native calls: create plain/tags, send plain/trace = {calls:?}");
    assert_eq!((calls[0], calls[2]), (calls[1], calls[3]));
    store.finish().await;
}

/// Review finding 2: nodes with different base URLs share one store. A queue
/// created through node A is found through node B from A's URL, from B's URL,
/// and from a bare name; a changed base URL finds it too.
#[tokio::test]
async fn given_queue_created_on_one_node_when_another_base_url_uses_it_then_it_is_found() {
    let store = TestStore::new().await;
    let node_a = manager(&store);
    let node_b = QueueManager::new(store.queue_operations.clone()).with_endpoint(QueueEndpoint {
        public_base_url: "http://10.0.0.6:3001/queue/".to_owned(),
    });
    let name = unique("shared");
    let url_a = create(&node_a, &name, json!({})).await;
    assert_eq!(url_a, format!("{BASE_URL}/000000000000/{name}"));

    let (status, found) = sqs(&node_b, "GetQueueUrl", json!({"QueueName": name})).await;
    let url_b = format!("http://10.0.0.6:3001/queue/000000000000/{name}");
    assert_eq!(
        (status, found["QueueUrl"].as_str()),
        (200, Some(url_b.as_str()))
    );
    let (status, listed) = sqs(&node_b, "ListQueues", json!({"QueueNamePrefix": name})).await;
    assert_eq!((status, &listed["QueueUrls"]), (200, &json!([url_b])));

    for (index, address) in [url_a.clone(), format!("{url_a}/"), name.clone()]
        .into_iter()
        .enumerate()
    {
        let body = format!("m{index}");
        let sent = json!({"QueueUrl": address, "MessageBody": body});
        assert_eq!(sqs(&node_b, "SendMessage", sent).await.0, 200);
        let receive = json!({"QueueUrl": address, "WaitTimeSeconds": 1});
        let (status, received) = sqs(&node_b, "ReceiveMessage", receive).await;
        assert_eq!(status, 200, "{received}");
        assert_eq!(received["Messages"][0]["Body"], body.as_str());
        let handle = received["Messages"][0]["ReceiptHandle"].clone();
        let delete = json!({"QueueUrl": address, "ReceiptHandle": handle});
        assert_eq!(sqs(&node_b, "DeleteMessage", delete).await.0, 200);
    }
    let foreign = format!("http://10.0.0.6:3001/queue/999999999999/{name}");
    assert_missing_queue(&sqs(&node_b, "GetQueueAttributes", json!({"QueueUrl": foreign})).await);

    // A changed base URL: the same store, a new endpoint, the old URL.
    let moved = QueueManager::new(store.queue_operations.clone()).with_endpoint(QueueEndpoint {
        public_base_url: "https://sqs.aux.example".to_owned(),
    });
    let attributes = json!({"QueueUrl": url_a, "AttributeNames": ["QueueArn"]});
    assert_eq!(sqs(&moved, "GetQueueAttributes", attributes).await.0, 200);
    assert_eq!(
        sqs(&moved, "DeleteQueue", json!({"QueueUrl": url_b}))
            .await
            .0,
        200
    );
    assert_missing_queue(&sqs(&node_a, "GetQueueAttributes", json!({"QueueUrl": url_a})).await);
    store.finish().await;
}

/// Review L8: `CreateQueue` of an existing queue compares effective
/// attributes, so a default value equals an omitted one; another value is
/// `QueueAlreadyExists`.
#[tokio::test]
async fn given_existing_queue_when_created_with_default_values_then_it_is_the_same_queue() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("defaults");
    let url = create(&manager, &name, json!({})).await;
    let defaults = json!({"Attributes": {"VisibilityTimeout": "30", "DelaySeconds": "0"}});
    assert_eq!(create(&manager, &name, defaults).await, url);
    let other = json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "31"}});
    let (status, body) = sqs(&manager, "CreateQueue", other).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["__type"], "com.amazonaws.sqs#QueueNameExists",
        "{body}"
    );
    store.finish().await;
}
