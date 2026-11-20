//! AWS emulator behavioural suite ports (query protocol).
//!
//! Every expected code, message and status is what AWS answered in the suite
//! script that the test names. The tests drive the `/pubsub` route with
//! form-encoded requests over in-memory SQLite.

use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use storage::DatabaseManager;
use tower::ServiceExt;

use crate::{AppState, router::server_router};

const ACCOUNT: &str = "000000000000";
const TOPIC_GONE: &str = "Topic does not exist";
const SUBSCRIPTION_GONE: &str = "Subscription does not exist";
const ARN_TOO_SHORT: &str =
    "Invalid parameter: SubscriptionArn Reason: An ARN must have at least 6 elements, not 1";

struct Reply {
    status: StatusCode,
    body: String,
}

impl Reply {
    fn expect_ok(&self) -> &Self {
        assert_eq!(self.status, StatusCode::OK, "{}", self.body);
        self
    }

    /// The text of the first `<name>` element, unescaped.
    fn tag(&self, name: &str) -> String {
        scalars(&self.body)
            .into_iter()
            .find(|(element, _)| element == name)
            .unwrap_or_else(|| panic!("no <{name}> in {}", self.body))
            .1
    }

    fn token(&self) -> Option<String> {
        scalars(&self.body)
            .into_iter()
            .find(|(element, _)| element == "NextToken")
            .map(|(_, value)| value)
    }

    fn error(&self) -> (String, String) {
        assert!(
            self.status.is_client_error(),
            "expected an error, got {}: {}",
            self.status,
            self.body
        );
        (self.tag("Code"), self.tag("Message"))
    }

    fn assert_error(&self, code: &str, message: &str) {
        assert_eq!(
            self.error(),
            (code.to_owned(), message.to_owned()),
            "{}",
            self.body
        );
    }

    fn assert_invalid(&self, message: &str) {
        self.assert_error("InvalidParameter", message);
    }

    /// `NotFound` with HTTP 404 (suite script
    /// `overlapping_filters_fan_one_message…`).
    fn assert_not_found(&self, message: &str) {
        self.assert_error("NotFound", message);
        assert_eq!(self.status, StatusCode::NOT_FOUND, "{}", self.body);
    }
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Every element without children, in document order, as (name, unescaped
/// text).
fn scalars(xml: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(open) = rest.find('<') {
        rest = rest.get(open + 1..).unwrap_or_default();
        let Some(end) = rest.find('>') else { break };
        let element = rest.get(..end).unwrap_or_default();
        let after = rest.get(end + 1..).unwrap_or_default();
        if element.starts_with('/') || element.starts_with('?') || element.ends_with('/') {
            rest = after;
            continue;
        }
        let name = element.split_whitespace().next().unwrap_or(element);
        let closing = format!("</{name}>");
        match after.find('<') {
            Some(text_end)
                if after
                    .get(text_end..)
                    .unwrap_or_default()
                    .starts_with(&closing) =>
            {
                let text = after.get(..text_end).unwrap_or_default();
                found.push((name.to_owned(), unescape(text)));
                rest = after.get(text_end + closing.len()..).unwrap_or_default();
            }
            _ => rest = after,
        }
    }
    found
}

/// The `<entry><key>…</key><value>…</value></entry>` pairs of an attribute map.
fn entries(xml: &str) -> HashMap<String, String> {
    xml.split("<entry>")
        .skip(1)
        .map(|segment| {
            let fields: HashMap<String, String> = scalars(segment).into_iter().collect();
            (fields["key"].clone(), fields["value"].clone())
        })
        .collect()
}

/// The children of every `<member>`.
fn members(xml: &str) -> Vec<HashMap<String, String>> {
    xml.split("<member>")
        .skip(1)
        .map(|segment| scalars(segment).into_iter().collect())
        .collect()
}

async fn call_owned(app: &Router, pairs: Vec<(String, String)>) -> Reply {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pubsub")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    Reply {
        status,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

async fn call(app: &Router, pairs: &[(&str, &str)]) -> Reply {
    call_owned(
        app,
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    )
    .await
}

async fn sqs(app: &Router, action: &str, body: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/queue")
                .header("content-type", "application/x-amz-json-1.0")
                .header("x-amz-target", format!("AmazonSQS.{action}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{action}");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn fixture() -> (Router, Arc<AppState>) {
    let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let mut root = config::RootConfig::default();
    root.queue.enabled = true;
    root.pubsub.enabled = true;
    let state = Arc::new(
        crate::service_host::prepare_app_state(database, &root, "127.0.0.1:3000").unwrap(),
    );
    state.publish_startup_readiness();
    (server_router(Arc::clone(&state), false), state)
}

fn queue_arn(name: &str) -> String {
    format!("arn:aws:sqs:us-east-1:{ACCOUNT}:{name}")
}

async fn topic(app: &Router, name: &str) -> String {
    let reply = call(app, &[("Action", "CreateTopic"), ("Name", name)]).await;
    reply.expect_ok().tag("TopicArn")
}

async fn subscribe(
    app: &Router,
    topic: &str,
    protocol: &str,
    endpoint: &str,
    attributes: &[(&str, &str)],
    return_arn: bool,
) -> Reply {
    let mut pairs = vec![
        ("Action".to_owned(), "Subscribe".to_owned()),
        ("TopicArn".to_owned(), topic.to_owned()),
        ("Protocol".to_owned(), protocol.to_owned()),
        ("Endpoint".to_owned(), endpoint.to_owned()),
    ];
    if return_arn {
        pairs.push(("ReturnSubscriptionArn".to_owned(), "true".to_owned()));
    }
    for (index, (name, value)) in attributes.iter().enumerate() {
        pairs.push((
            format!("Attributes.entry.{}.key", index + 1),
            (*name).to_owned(),
        ));
        pairs.push((
            format!("Attributes.entry.{}.value", index + 1),
            (*value).to_owned(),
        ));
    }
    call_owned(app, pairs).await
}

/// A confirmed `sqs` subscription of a queue that need not exist.
async fn sqs_subscription(
    app: &Router,
    topic: &str,
    queue: &str,
    attributes: &[(&str, &str)],
) -> String {
    let reply = subscribe(app, topic, "sqs", &queue_arn(queue), attributes, true).await;
    reply.expect_ok().tag("SubscriptionArn")
}

async fn set_subscription(app: &Router, arn: &str, name: &str, value: &str) -> Reply {
    call(
        app,
        &[
            ("Action", "SetSubscriptionAttributes"),
            ("SubscriptionArn", arn),
            ("AttributeName", name),
            ("AttributeValue", value),
        ],
    )
    .await
}

async fn get_subscription(app: &Router, arn: &str) -> Reply {
    call(
        app,
        &[
            ("Action", "GetSubscriptionAttributes"),
            ("SubscriptionArn", arn),
        ],
    )
    .await
}

async fn subscription_attributes(app: &Router, arn: &str) -> HashMap<String, String> {
    entries(&get_subscription(app, arn).await.expect_ok().body)
}

async fn delete_topic(app: &Router, arn: &str) -> Reply {
    call(app, &[("Action", "DeleteTopic"), ("TopicArn", arn)]).await
}

async fn set_topic(app: &Router, arn: &str, name: &str, value: &str) -> Reply {
    call(
        app,
        &[
            ("Action", "SetTopicAttributes"),
            ("TopicArn", arn),
            ("AttributeName", name),
            ("AttributeValue", value),
        ],
    )
    .await
}

async fn topic_attributes(app: &Router, arn: &str) -> HashMap<String, String> {
    let reply = call(app, &[("Action", "GetTopicAttributes"), ("TopicArn", arn)]).await;
    entries(&reply.expect_ok().body)
}

async fn subscriptions_of(app: &Router, topic: &str) -> Vec<HashMap<String, String>> {
    let reply = call(
        app,
        &[("Action", "ListSubscriptionsByTopic"), ("TopicArn", topic)],
    )
    .await;
    members(&reply.expect_ok().body)
}

/// `MessageAttributes.entry.<index>` pairs of one attribute; `scope` is a batch
/// member prefix.
fn attribute(
    scope: &str,
    index: usize,
    name: &str,
    data_type: &str,
    value: Option<(&str, &str)>,
) -> Vec<(String, String)> {
    let prefix = format!("{scope}MessageAttributes.entry.{index}");
    let mut pairs = vec![
        (format!("{prefix}.Name"), name.to_owned()),
        (format!("{prefix}.Value.DataType"), data_type.to_owned()),
    ];
    if let Some((field, value)) = value {
        pairs.push((format!("{prefix}.Value.{field}"), value.to_owned()));
    }
    pairs
}

async fn publish(
    app: &Router,
    topic: &str,
    message: &str,
    attributes: Vec<Vec<(String, String)>>,
) -> Reply {
    let mut pairs = vec![
        ("Action".to_owned(), "Publish".to_owned()),
        ("TopicArn".to_owned(), topic.to_owned()),
        ("Message".to_owned(), message.to_owned()),
    ];
    pairs.extend(attributes.into_iter().flatten());
    call_owned(app, pairs).await
}

/// An entry is (id, message, attribute pairs built with the member prefix
/// `scope(position)`).
type BatchEntry<'a> = (&'a str, &'a str, Vec<(String, String)>);

async fn publish_batch(app: &Router, topic: &str, entries: &[BatchEntry<'_>]) -> Reply {
    let mut pairs = vec![
        ("Action".to_owned(), "PublishBatch".to_owned()),
        ("TopicArn".to_owned(), topic.to_owned()),
    ];
    for (position, (id, message, attributes)) in entries.iter().enumerate() {
        let prefix = batch_scope(position + 1);
        pairs.push((format!("{prefix}Id"), (*id).to_owned()));
        pairs.push((format!("{prefix}Message"), (*message).to_owned()));
        pairs.extend(attributes.iter().cloned());
    }
    call_owned(app, pairs).await
}

fn batch_scope(position: usize) -> String {
    format!("PublishBatchRequestEntries.member.{position}.")
}

fn json_of(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

/// AWS behavioural: create_topic_with_different_tags_rejected.py
#[tokio::test]
async fn given_existing_topic_when_created_with_different_tags_then_invalid_parameter_and_tags_kept()
 {
    let (app, _state) = fixture().await;
    let create = |team: Option<&'static str>| {
        let app = app.clone();
        async move {
            let mut pairs = vec![("Action", "CreateTopic"), ("Name", "tagconf")];
            if let Some(team) = team {
                pairs.extend([
                    ("Tags.member.1.Key", "sim2real"),
                    ("Tags.member.1.Value", "check"),
                    ("Tags.member.2.Key", "team"),
                    ("Tags.member.2.Value", team),
                ]);
            }
            call(&app, &pairs).await
        }
    };
    let arn = create(Some("alpha")).await.expect_ok().tag("TopicArn");
    assert_eq!(create(Some("alpha")).await.expect_ok().tag("TopicArn"), arn);
    assert_eq!(create(None).await.expect_ok().tag("TopicArn"), arn);

    create(Some("beta"))
        .await
        .assert_invalid("Invalid parameter: Tags Reason: Topic already exists with different tags");

    let listed = call(
        &app,
        &[("Action", "ListTagsForResource"), ("ResourceArn", &arn)],
    )
    .await;
    let stored: HashMap<String, String> = members(&listed.expect_ok().body)
        .into_iter()
        .map(|tag| (tag["Key"].clone(), tag["Value"].clone()))
        .collect();
    assert_eq!(
        stored,
        HashMap::from([
            ("sim2real".to_owned(), "check".to_owned()),
            ("team".to_owned(), "alpha".to_owned()),
        ])
    );
}

/// AWS behavioural: display_name_over_100_characters_rejected.py
#[tokio::test]
async fn given_display_name_over_100_characters_when_set_then_invalid_parameter_and_100_stored() {
    let (app, _state) = fixture().await;
    let reply = call(
        &app,
        &[
            ("Action", "CreateTopic"),
            ("Name", "longdn"),
            ("Attributes.entry.1.key", "DisplayName"),
            ("Attributes.entry.1.value", "before"),
        ],
    )
    .await;
    let arn = reply.expect_ok().tag("TopicArn");

    set_topic(&app, &arn, "DisplayName", &"d".repeat(101))
        .await
        .assert_invalid("Invalid parameter: DisplayName");
    assert_eq!(topic_attributes(&app, &arn).await["DisplayName"], "before");

    set_topic(&app, &arn, "DisplayName", &"d".repeat(100))
        .await
        .expect_ok();
    assert_eq!(
        topic_attributes(&app, &arn).await["DisplayName"],
        "d".repeat(100)
    );
}

/// AWS behavioural: set_topic_attributes_unknown_name_fails.py
/// AWS behavioural: topic_mutable_attributes_round_trip_and_clear.py
/// (DisplayName legs)
#[tokio::test]
async fn given_topic_when_unknown_attribute_set_then_refused_and_display_name_sets_and_clears() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "attrs").await;
    assert_eq!(topic_attributes(&app, &arn).await["DisplayName"], "");

    set_topic(&app, &arn, "NotARealAttribute", "x")
        .await
        .assert_invalid("Invalid parameter: AttributeName");
    assert_eq!(topic_attributes(&app, &arn).await["DisplayName"], "");

    set_topic(&app, &arn, "DisplayName", "Alerts")
        .await
        .expect_ok();
    assert_eq!(topic_attributes(&app, &arn).await["DisplayName"], "Alerts");
    set_topic(&app, &arn, "DisplayName", "").await.expect_ok();
    assert_eq!(topic_attributes(&app, &arn).await["DisplayName"], "");
}

/// AWS behavioural: topic_delete_lifecycle_and_idempotent_delete.py
/// AWS behavioural:
/// topic_policy_round_trips_and_deleted_topic_set_attributes_not_found.py
/// AWS behavioural: subscribe_to_nonexistent_topic_fails.py
/// AWS behavioural: publish_batch_to_a_deleted_topic_not_found.py
/// AWS behavioural:
/// overlapping_filters_fan_one_message_to_every_matching_queue_and_the_deleted_topic_refuses_publish_and_listing.
/// py AWS behavioural: e2e_topic_fanout_filter_teardown_flow.py
#[tokio::test]
async fn given_deleted_topic_when_used_then_not_found_404_and_delete_is_idempotent() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "doomed").await;
    sqs_subscription(&app, &arn, "doomed-q", &[]).await;
    call(
        &app,
        &[("Action", "GetTopicAttributes"), ("TopicArn", &arn)],
    )
    .await
    .expect_ok();

    delete_topic(&app, &arn).await.expect_ok();
    let get = call(
        &app,
        &[("Action", "GetTopicAttributes"), ("TopicArn", &arn)],
    )
    .await;
    get.assert_not_found(TOPIC_GONE);
    delete_topic(&app, &arn).await.expect_ok();
    let get = call(
        &app,
        &[("Action", "GetTopicAttributes"), ("TopicArn", &arn)],
    )
    .await;
    get.assert_not_found(TOPIC_GONE);

    set_topic(&app, &arn, "DisplayName", "x")
        .await
        .assert_not_found(TOPIC_GONE);
    publish(&app, &arn, "after-delete", vec![])
        .await
        .assert_not_found(TOPIC_GONE);
    let entries: Vec<_> = (0..3)
        .map(|index| (["e0", "e1", "e2"][index], "refused", Vec::new()))
        .collect();
    publish_batch(&app, &arn, &entries)
        .await
        .assert_not_found(TOPIC_GONE);
    call(
        &app,
        &[("Action", "ListSubscriptionsByTopic"), ("TopicArn", &arn)],
    )
    .await
    .assert_not_found(TOPIC_GONE);
    subscribe(&app, &arn, "sqs", &queue_arn("again"), &[], true)
        .await
        .assert_not_found(TOPIC_GONE);
    let listed = call(&app, &[("Action", "ListTopics")]).await;
    assert!(!listed.expect_ok().body.contains(&arn), "{}", listed.body);

    // Recreating the name reuses the ARN and accepts a subscription again.
    assert_eq!(topic(&app, "doomed").await, arn);
    subscribe(&app, &arn, "sqs", &queue_arn("again"), &[], true)
        .await
        .expect_ok();
}

/// AWS behavioural: list_tags_for_nonexistent_topic_fails.py
#[tokio::test]
async fn given_deleted_topic_when_tags_listed_then_resource_not_found() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "tags-gone").await;
    call(
        &app,
        &[("Action", "ListTagsForResource"), ("ResourceArn", &arn)],
    )
    .await
    .expect_ok();
    call(&app, &[("Action", "DeleteTopic"), ("TopicArn", &arn)])
        .await
        .expect_ok();
    call(
        &app,
        &[("Action", "ListTagsForResource"), ("ResourceArn", &arn)],
    )
    .await
    .assert_error("ResourceNotFound", "Resource does not exist");
}

/// AWS behavioural: empty_vs_absent_message_attribute_rejected.py
/// AWS behavioural:
/// message_attribute_value_in_the_wrong_field_for_its_data_type_rejected.py
#[tokio::test]
async fn given_message_attribute_empty_or_in_wrong_field_when_published_then_parameter_value_invalid()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "attr-field").await;
    let cases = [
        (
            attribute(
                "",
                1,
                "payload",
                "Binary",
                Some(("StringValue", "aGVsbG8=")),
            ),
            "The message attribute 'payload' with type 'Binary' must use field 'Binary'.",
        ),
        (
            attribute("", 1, "label", "String", Some(("BinaryValue", "Ymx1ZQ=="))),
            "The message attribute 'label' with type 'String' must use field 'String'.",
        ),
        (
            attribute("", 1, "label", "String", None),
            "The message attribute 'label' must contain non-empty message attribute value for \
             message attribute type 'String'.",
        ),
        (
            attribute("", 1, "count", "Number", None),
            "The message attribute 'count' must contain non-empty message attribute value for \
             message attribute type 'Number'.",
        ),
        (
            attribute("", 1, "marker", "String", Some(("StringValue", ""))),
            "The message attribute 'marker' must contain non-empty message attribute value for \
             message attribute type 'String'.",
        ),
    ];
    for (pairs, message) in cases {
        publish(&app, &arn, "refused", vec![pairs])
            .await
            .assert_error("ParameterValueInvalid", message);
    }

    let accepted = publish(
        &app,
        &arn,
        "accepted",
        vec![
            attribute(
                "",
                1,
                "payload",
                "Binary",
                Some(("BinaryValue", "AP9oZWxsbw==")),
            ),
            attribute("", 2, "label", "String", Some(("StringValue", "blue"))),
            attribute("", 3, "count", "Number", Some(("StringValue", "3"))),
        ],
    )
    .await;
    assert!(!accepted.expect_ok().tag("MessageId").is_empty());
}

/// AWS behavioural: message_attribute_name_length_and_period_rules_rejected.py
#[tokio::test]
async fn given_message_attribute_names_when_published_then_length_and_period_rules_hold() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "attr-name").await;
    let refusals = [
        (
            "n".repeat(257),
            "Length of message attribute name must be less than 256 bytes.",
        ),
        (
            ".lead".to_owned(),
            "Invalid message attribute name starting with character '.' was found.",
        ),
        (
            "trail.".to_owned(),
            "Invalid message attribute name ending with character '.' was found.",
        ),
        (
            "a..b".to_owned(),
            "Message attribute name can not have successive '.' character.",
        ),
    ];
    for (name, message) in refusals {
        let attributes = vec![attribute(
            "",
            1,
            &name,
            "String",
            Some(("StringValue", "v")),
        )];
        publish(&app, &arn, "refused", attributes)
            .await
            .assert_error("ParameterValueInvalid", message);
    }
    let accepted = vec![
        attribute(
            "",
            1,
            &"n".repeat(256),
            "String",
            Some(("StringValue", "long")),
        ),
        attribute("", 2, "a.b.c", "String", Some(("StringValue", "dotted"))),
    ];
    assert!(
        !publish(&app, &arn, "accepted", accepted)
            .await
            .expect_ok()
            .tag("MessageId")
            .is_empty()
    );
}

/// AWS behavioural: publish_batch_validation_is_request_level.py
#[tokio::test]
async fn given_batch_with_malformed_number_when_published_then_whole_request_fails_and_nothing_is_delivered()
 {
    let (app, state) = fixture().await;
    let arn = topic(&app, "batch-request").await;
    let created = sqs(&app, "CreateQueue", json!({"QueueName": "batch-sink"})).await;
    let url = created["QueueUrl"].as_str().unwrap().to_owned();
    let attributes = sqs(
        &app,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
    )
    .await;
    let queue = attributes["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_owned();
    let reply = subscribe(
        &app,
        &arn,
        "sqs",
        &queue,
        &[("RawMessageDelivery", "true")],
        true,
    )
    .await;
    reply.expect_ok();

    let broken = attribute(
        &batch_scope(2),
        1,
        "broken",
        "Number",
        Some(("StringValue", "not-a-number")),
    );
    let refused = publish_batch(
        &app,
        &arn,
        &[
            ("good-1", "good-one", vec![]),
            ("bad-1", "bad-attrs", broken),
            ("good-2", "good-two", vec![]),
        ],
    )
    .await;
    refused.assert_error(
        "ParameterValueInvalid",
        "Could not cast message attribute 'broken' value to number.",
    );
    let manager = state.pubsub_manager.as_ref().unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 0);

    let repaired = publish_batch(
        &app,
        &arn,
        &[
            ("good-1", "good-one", vec![]),
            ("good-2", "good-two", vec![]),
        ],
    )
    .await;
    let successful = members(&repaired.expect_ok().body);
    let ids: Vec<&str> = successful
        .iter()
        .map(|entry| entry["Id"].as_str())
        .collect();
    assert_eq!(ids, ["good-1", "good-2"]);
    assert_ne!(successful[0]["MessageId"], successful[1]["MessageId"]);
    assert!(!repaired.body.contains("<Failed>"), "{}", repaired.body);

    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 2);
    let received = sqs(
        &app,
        "ReceiveMessage",
        json!({"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0}),
    )
    .await;
    let mut bodies: Vec<&str> = received["Messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["Body"].as_str().unwrap())
        .collect();
    bodies.sort_unstable();
    assert_eq!(bodies, ["good-one", "good-two"]);
}

/// AWS behavioural: publish_batch_empty_fails.py
///
/// An SDK sends an empty entry list as the bare `PublishBatchRequestEntries`
/// parameter, which the suite script sees as `EmptyBatchRequest`. Probe A7
/// (`docs/sqs-sns-aws-probes.md`) recorded `ValidationError` for a request
/// without the parameter; both stay.
#[tokio::test]
async fn given_batch_without_entries_when_published_then_empty_batch_request() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "batch-empty").await;
    call(
        &app,
        &[
            ("Action", "PublishBatch"),
            ("TopicArn", &arn),
            ("PublishBatchRequestEntries", ""),
        ],
    )
    .await
    .assert_error(
        "EmptyBatchRequest",
        "The batch request doesn't contain any entries.",
    );
    call(&app, &[("Action", "PublishBatch"), ("TopicArn", &arn)])
        .await
        .assert_error(
            "ValidationError",
            "1 validation error detected: Value null at 'publishBatchRequestEntries' failed to \
             satisfy constraint: Member must not be null",
        );

    let control = publish_batch(&app, &arn, &[("e0", "positive-control", vec![])]).await;
    let successful = members(&control.expect_ok().body);
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "e0");
    assert!(!control.body.contains("<Failed>"), "{}", control.body);
}

/// AWS behavioural: publish_without_target_fails.py
#[tokio::test]
async fn given_publish_without_target_when_sent_then_invalid_parameter_and_topic_publish_succeeds()
{
    let (app, _state) = fixture().await;
    let arn = topic(&app, "no-target").await;
    call(
        &app,
        &[("Action", "Publish"), ("Message", "no target specified")],
    )
    .await
    .assert_invalid(
        "Invalid parameter: TopicArn or TargetArn Reason: no value for required parameter",
    );
    publish(&app, &arn, "control", vec![]).await.expect_ok();
}

/// AWS behavioural: publish_message_size_limit_enforced.py
/// AWS behavioural: publish_size_limit_counts_message_attributes.py
#[tokio::test]
async fn given_body_and_attributes_over_262144_bytes_when_published_then_message_too_long() {
    const LIMIT: usize = 262_144;
    let (app, _state) = fixture().await;
    let arn = topic(&app, "too-big").await;
    let too_long = "Invalid parameter: Message too long";

    publish(&app, &arn, &"x".repeat(LIMIT + 1024), vec![])
        .await
        .assert_invalid(too_long);
    publish(&app, &arn, &"y".repeat(LIMIT - 1024), vec![])
        .await
        .expect_ok();

    let body = "b".repeat(LIMIT - 8192);
    let bulk = |value: String| {
        vec![attribute(
            "",
            1,
            "bulk",
            "String",
            Some(("StringValue", &value)),
        )]
    };
    publish(&app, &arn, &body, vec![]).await.expect_ok();
    publish(&app, &arn, "tiny", bulk("a".repeat(12_288)))
        .await
        .expect_ok();
    publish(&app, &arn, &body, bulk("a".repeat(12_288)))
        .await
        .assert_invalid(too_long);

    let many = (0..40)
        .map(|index| {
            attribute(
                "",
                index + 1,
                &format!("k{index:02}"),
                "String",
                Some(("StringValue", &"c".repeat(8192))),
            )
        })
        .collect();
    publish(&app, &arn, "small", many)
        .await
        .assert_invalid(too_long);
    publish(&app, &arn, "after-rejections", bulk("small".to_owned()))
        .await
        .expect_ok();
}

/// AWS behavioural: op_confirm_subscription_invalid_token_rejected.py
///
/// Conflict, kept as recorded: the AWS fixture
/// `crates/pubsub/tests/fixtures/aws/query-errors.json` (`invalid_token`,
/// `sns_parity.md` row "Invalid confirmation token") answers an unissued token
/// with `Invalid token`; the script expects `Invalid parameter: Token`. The
/// fixture does not keep its request, so the requests cannot be told apart and
/// Aux keeps the recorded text.
#[tokio::test]
async fn given_unissued_token_when_confirming_then_invalid_parameter_token_and_nothing_created() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "confirm").await;
    call(
        &app,
        &[
            ("Action", "ConfirmSubscription"),
            ("TopicArn", &arn),
            ("Token", &"a".repeat(64)),
        ],
    )
    .await
    .assert_invalid("Invalid token");
    assert!(subscriptions_of(&app, &arn).await.is_empty());
}

/// AWS behavioural: pending_subscription_arn_placeholder_not_resolvable.py (an
/// `https` endpoint stands in for `email`, which Aux does not implement)
#[tokio::test]
async fn given_pending_subscription_when_placeholder_arn_used_then_invalid_parameter() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "pending").await;
    let confirmed = sqs_subscription(&app, &arn, "pending-q", &[]).await;
    assert!(confirmed.starts_with(&format!("{arn}:")), "{confirmed}");

    let endpoint = "https://example.test/pending-hook";
    let pending = subscribe(&app, &arn, "https", endpoint, &[], false).await;
    assert_eq!(
        pending.expect_ok().tag("SubscriptionArn"),
        "pending confirmation"
    );

    let listed = subscriptions_of(&app, &arn).await;
    let entry: Vec<_> = listed
        .iter()
        .filter(|entry| entry["Protocol"] == "https")
        .collect();
    assert_eq!(entry.len(), 1);
    assert_eq!(entry[0]["SubscriptionArn"], "PendingConfirmation");
    assert_eq!(entry[0]["TopicArn"], arn);
    assert_eq!(entry[0]["Endpoint"], endpoint);

    get_subscription(&app, "PendingConfirmation")
        .await
        .assert_invalid(ARN_TOO_SHORT);
    set_subscription(&app, "PendingConfirmation", "RawMessageDelivery", "true")
        .await
        .assert_invalid(ARN_TOO_SHORT);
    let after = subscriptions_of(&app, &arn).await;
    assert_eq!(
        after
            .iter()
            .filter(|entry| entry["SubscriptionArn"] == "PendingConfirmation")
            .count(),
        1
    );
    let attributes = subscription_attributes(&app, &confirmed).await;
    assert_eq!(attributes["PendingConfirmation"], "false");
    assert_eq!(attributes["TopicArn"], arn);
}

/// AWS behavioural: unsubscribe_malformed_arn_fails.py
#[tokio::test]
async fn given_malformed_subscription_arn_when_unsubscribed_then_invalid_parameter_and_real_survives()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "unsub-bad").await;
    let subscription = sqs_subscription(&app, &arn, "unsub-bad-q", &[]).await;

    call(
        &app,
        &[("Action", "Unsubscribe"), ("SubscriptionArn", "not-an-arn")],
    )
    .await
    .assert_invalid(ARN_TOO_SHORT);
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(attributes["SubscriptionArn"], subscription);

    call(
        &app,
        &[
            ("Action", "Unsubscribe"),
            ("SubscriptionArn", &subscription),
        ],
    )
    .await
    .expect_ok();
    get_subscription(&app, &subscription)
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
}

/// AWS behavioural: unsubscribe_nonexistent_is_idempotent.py
#[tokio::test]
async fn given_unknown_subscription_arn_when_unsubscribed_twice_then_ok_and_nothing_is_created() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "unsub-unknown").await;
    let unknown = format!("{arn}:{}", uuid::Uuid::new_v4());
    for _ in 0..2 {
        call(
            &app,
            &[("Action", "Unsubscribe"), ("SubscriptionArn", &unknown)],
        )
        .await
        .expect_ok();
    }
    get_subscription(&app, &unknown)
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
    assert!(subscriptions_of(&app, &arn).await.is_empty());
}

/// AWS behavioural: unsubscribe_removes_subscription.py
/// AWS behavioural: set_subscription_attributes_nonexistent_fails.py
#[tokio::test]
async fn given_unsubscribed_or_unknown_subscription_when_read_or_written_then_not_found() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "unsub-removes").await;
    let removed = sqs_subscription(&app, &arn, "removes-q", &[]).await;
    let real = sqs_subscription(&app, &arn, "real-q", &[]).await;
    set_subscription(&app, &real, "RawMessageDelivery", "true")
        .await
        .expect_ok();
    assert_eq!(
        subscription_attributes(&app, &real).await["RawMessageDelivery"],
        "true"
    );

    call(
        &app,
        &[("Action", "Unsubscribe"), ("SubscriptionArn", &removed)],
    )
    .await
    .expect_ok();
    get_subscription(&app, &removed)
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
    set_subscription(&app, &removed, "RawMessageDelivery", "true")
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
    let listed: Vec<String> = subscriptions_of(&app, &arn)
        .await
        .into_iter()
        .map(|entry| entry["SubscriptionArn"].clone())
        .collect();
    assert_eq!(listed, [real]);

    let unknown = format!("{arn}:{}", uuid::Uuid::new_v4());
    set_subscription(&app, &unknown, "RawMessageDelivery", "true")
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
    get_subscription(&app, &unknown)
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
}

/// AWS behavioural: unsubscribe_pending_subscription_rejected.py (`https`
/// stands in for `email`)
#[tokio::test]
async fn given_pending_subscription_when_unsubscribed_then_invalid_parameter_and_it_stays_pending()
{
    let (app, _state) = fixture().await;
    let arn = topic(&app, "unsub-pending").await;
    let pending = subscribe(&app, &arn, "https", "https://example.test/pend", &[], true)
        .await
        .expect_ok()
        .tag("SubscriptionArn");
    assert!(pending.starts_with(&format!("{arn}:")), "{pending}");
    let confirmed = sqs_subscription(&app, &arn, "unsub-pending-q", &[]).await;

    call(
        &app,
        &[("Action", "Unsubscribe"), ("SubscriptionArn", &pending)],
    )
    .await
    .assert_invalid(
        "Invalid parameter: SubscriptionArn Reason: Cannot unsubscribe a subscription that is \
         pending confirmation",
    );
    assert_eq!(
        subscription_attributes(&app, &pending).await["PendingConfirmation"],
        "true"
    );

    call(
        &app,
        &[("Action", "Unsubscribe"), ("SubscriptionArn", &confirmed)],
    )
    .await
    .expect_ok();
    get_subscription(&app, &confirmed)
        .await
        .assert_error("NotFound", SUBSCRIPTION_GONE);
}

/// AWS behavioural: list_topics_and_subscriptions_forged_next_token_rejected.py
/// AWS behavioural: list_subscriptions_by_topic_pagination_exactness.py
/// (garbage token leg)
#[tokio::test]
async fn given_unissued_next_token_when_listing_then_invalid_parameter_next_token() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "forged").await;
    let forged = "c2ltMnJlYWwtbm90LWEtcmVhbC10b2tlbg==";
    for action in ["ListTopics", "ListSubscriptions"] {
        call(&app, &[("Action", action), ("NextToken", forged)])
            .await
            .assert_invalid("Invalid parameter: NextToken");
    }
    call(
        &app,
        &[
            ("Action", "ListSubscriptionsByTopic"),
            ("TopicArn", &arn),
            ("NextToken", "sim2real-behav-not-a-real-token"),
        ],
    )
    .await
    .assert_invalid("Invalid parameter: NextToken");

    let subscription = sqs_subscription(&app, &arn, "forged-q", &[]).await;
    assert!(
        call(&app, &[("Action", "ListTopics")])
            .await
            .expect_ok()
            .body
            .contains(&arn)
    );
    let listed = call(&app, &[("Action", "ListSubscriptions")]).await;
    assert!(
        listed.expect_ok().body.contains(&subscription),
        "{}",
        listed.body
    );
}

/// One page walk over `action` with the fixed `form` pairs.
async fn walk(app: &Router, form: &[(&str, &str)]) -> Vec<Vec<HashMap<String, String>>> {
    let mut pages = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut pairs: Vec<(String, String)> = form
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        if let Some(token) = &token {
            pairs.push(("NextToken".to_owned(), token.clone()));
        }
        let reply = call_owned(app, pairs).await;
        reply.expect_ok();
        pages.push(members(&reply.body));
        token = reply.token();
        if token.is_none() {
            return pages;
        }
        assert!(pages.len() < 10, "the walk did not end");
    }
}

/// AWS behavioural: list_subscriptions_by_topic_pagination_exactness.py
/// AWS behavioural: list_subscriptions_paginates_and_drops_unsubscribed.py
#[tokio::test]
async fn given_105_subscriptions_when_listed_then_pages_hold_each_once_with_fields_and_drop_unsubscribed()
 {
    const COUNT: usize = 105;
    const PAGE_MAX: usize = 100;
    let (app, _state) = fixture().await;
    let arn = topic(&app, "paging").await;
    let mut endpoints = HashMap::new();
    for index in 0..COUNT {
        let queue = format!("paging-q-{index:03}");
        let subscription = sqs_subscription(&app, &arn, &queue, &[]).await;
        endpoints.insert(subscription, queue_arn(&queue));
    }
    assert_eq!(endpoints.len(), COUNT);
    let mine: Vec<&String> = endpoints.keys().collect();

    let by_topic = walk(
        &app,
        &[("Action", "ListSubscriptionsByTopic"), ("TopicArn", &arn)],
    )
    .await;
    assert!(by_topic.len() >= 2, "{} page(s)", by_topic.len());
    let flat: Vec<&str> = by_topic
        .iter()
        .flatten()
        .map(|entry| entry["SubscriptionArn"].as_str())
        .collect();
    assert_eq!(
        flat.len(),
        COUNT,
        "page sizes {:?}",
        by_topic.iter().map(Vec::len).collect::<Vec<_>>()
    );
    assert_eq!(
        flat.iter().collect::<std::collections::HashSet<_>>().len(),
        COUNT
    );
    for subscription in &mine {
        assert!(flat.contains(&subscription.as_str()));
    }
    let first_again = call(
        &app,
        &[("Action", "ListSubscriptionsByTopic"), ("TopicArn", &arn)],
    )
    .await;
    assert_eq!(
        members(&first_again.expect_ok().body).len(),
        by_topic[0].len()
    );

    let all = walk(&app, &[("Action", "ListSubscriptions")]).await;
    assert!(all.len() >= 2, "{} page(s)", all.len());
    assert!(all.iter().all(|page| page.len() <= PAGE_MAX));
    let listed: Vec<&HashMap<String, String>> = all
        .iter()
        .flatten()
        .filter(|entry| endpoints.contains_key(&entry["SubscriptionArn"]))
        .collect();
    assert_eq!(listed.len(), COUNT);
    for entry in &listed {
        assert_eq!(entry["TopicArn"], arn);
        assert_eq!(entry["Protocol"], "sqs");
        assert_eq!(entry["Owner"], ACCOUNT);
        assert_eq!(entry["Endpoint"], endpoints[&entry["SubscriptionArn"]]);
    }

    let gone = mine.iter().min().unwrap().to_string();
    call(
        &app,
        &[("Action", "Unsubscribe"), ("SubscriptionArn", &gone)],
    )
    .await
    .expect_ok();
    let after = walk(&app, &[("Action", "ListSubscriptions")]).await;
    let remaining: Vec<&str> = after
        .iter()
        .flatten()
        .map(|entry| entry["SubscriptionArn"].as_str())
        .filter(|subscription| endpoints.contains_key(*subscription))
        .collect();
    assert_eq!(remaining.len(), COUNT - 1);
    assert!(!remaining.contains(&gone.as_str()));
}

/// AWS behavioural: filter_policy_scope_constrains_policy_shape.py (the
/// vocabulary leg) AWS behavioural:
/// filter_policy_scope_switch_away_from_message_body_rejected_while_policy_is_nested.
/// py
///
/// Conflict, kept as recorded: the scripts refuse a nested policy under
/// `MessageAttributes` scope (`Filter policy scope MessageAttributes does not
/// support nested filter policy`). Probe A3 (`docs/sqs-sns-aws-probes.md`,
/// "nested object ... with `MessageAttributes` scope", `sns_parity.md` row 24)
/// recorded `200` for the same policy and scope, so Aux accepts it and
/// this test follows the probe.
#[tokio::test]
async fn given_filter_policy_scope_when_unknown_value_set_then_refused_and_nested_policy_follows_the_probe()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "fpscope").await;
    let baseline = r#"{"event":["created"]}"#;
    let nested = r#"{"store":{"city":["seattle"]}}"#;
    let subscription =
        sqs_subscription(&app, &arn, "fpscope-q", &[("FilterPolicy", baseline)]).await;
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(baseline));
    assert_eq!(attributes["FilterPolicyScope"], "MessageAttributes");

    set_subscription(&app, &subscription, "FilterPolicyScope", "MessageHeaders")
        .await
        .assert_invalid(
            "Invalid parameter: FilterPolicyScope: Invalid value [MessageHeaders]. Please use \
             either MessageBody or MessageAttributes",
        );
    assert_eq!(
        subscription_attributes(&app, &subscription).await["FilterPolicyScope"],
        "MessageAttributes"
    );

    set_subscription(&app, &subscription, "FilterPolicyScope", "MessageBody")
        .await
        .expect_ok();
    set_subscription(&app, &subscription, "FilterPolicy", nested)
        .await
        .expect_ok();
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(attributes["FilterPolicyScope"], "MessageBody");
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(nested));

    set_subscription(&app, &subscription, "FilterPolicy", baseline)
        .await
        .expect_ok();
    set_subscription(
        &app,
        &subscription,
        "FilterPolicyScope",
        "MessageAttributes",
    )
    .await
    .expect_ok();
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(attributes["FilterPolicyScope"], "MessageAttributes");
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(baseline));
}

/// AWS behavioural: filter_policy_unknown_operator_rejected.py
#[tokio::test]
async fn given_filter_policy_operators_when_set_then_known_ones_round_trip_and_unknown_ones_are_refused()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "fpops").await;
    let baseline = r#"{"price":[{"numeric":[">",5]}]}"#;
    let subscription = sqs_subscription(&app, &arn, "fpops-q", &[("FilterPolicy", baseline)]).await;

    for accepted in [
        r#"{"source_ip":[{"cidr":"10.0.0.0/24"}]}"#,
        r#"{"host":[{"wildcard":"*.example.com"}]}"#,
        r#"{"host":[{"anything-but":{"wildcard":"*.test.com"}}]}"#,
        r#"{"host":[{"prefix":{"equals-ignore-case":"ABC"}}]}"#,
    ] {
        set_subscription(&app, &subscription, "FilterPolicy", accepted)
            .await
            .expect_ok();
        let attributes = subscription_attributes(&app, &subscription).await;
        assert_eq!(attributes["FilterPolicy"], accepted);
    }
    set_subscription(&app, &subscription, "FilterPolicy", baseline)
        .await
        .expect_ok();

    let refused = [
        (
            r#"{"price":[{"greater-than":5}]}"#,
            "Invalid parameter: FilterPolicy: Unrecognized match type greater-than\n at [Source: \
             (String)\"{\"price\":[{\"greater-than\":5}]}\"; line: 1, column: 28]",
        ),
        (
            r#"{"$and":[{"a":["1"]},{"b":["2"]}]}"#,
            "Invalid parameter: FilterPolicy: Unrecognized match type a\n at [Source: \
             (String)\"{\"$and\":[{\"a\":[\"1\"]},{\"b\":[\"2\"]}]}\"; line: 1, column: 16]",
        ),
        (
            r#"{"price":[{"numeric":["!=",5]}]}"#,
            "Invalid parameter: FilterPolicy: Unrecognized numeric range operator: !=\n at \
             [Source: (String)\"{\"price\":[{\"numeric\":[\"!=\",5]}]}\"; line: 1, column: 29]",
        ),
        (
            r#"{"ip":[{"anything-but":{"cidr":"10.0.0.0/8"}}]}"#,
            "Invalid parameter: FilterPolicy: Unsupported anything-but pattern: cidr\n at \
             [Source: (String)\"{\"ip\":[{\"anything-but\":{\"cidr\":\"10.0.0.0/8\"}}]}\"; line: \
             1, column: 33]",
        ),
        (
            r#"{"coupon":[{"exists":"yes"}]}"#,
            "Invalid parameter: FilterPolicy: exists match pattern must be either true or \
             false.\n at [Source: (String)\"{\"coupon\":[{\"exists\":\"yes\"}]}\"; line: 1, \
             column: 23]",
        ),
    ];
    for (policy, message) in refused {
        set_subscription(&app, &subscription, "FilterPolicy", policy)
            .await
            .assert_invalid(message);
        let attributes = subscription_attributes(&app, &subscription).await;
        assert_eq!(attributes["FilterPolicy"], baseline);
    }

    let control = r#"{"price":[{"numeric":[">=",10,"<",20]}],"event":["ship"]}"#;
    set_subscription(&app, &subscription, "FilterPolicy", control)
        .await
        .expect_ok();
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(control));
}

/// AWS behavioural:
/// filter_policy_message_body_scope_counts_nested_leaf_keys_toward_the_key_limit.
/// py AWS behavioural: filter_policy_operator_vocabulary_round_trip.py (six-key
/// Subscribe leg) AWS behavioural:
/// subscription_filter_policy_update_and_validation.py
#[tokio::test]
async fn given_filter_policies_when_over_five_keys_or_malformed_then_refused_and_stored_policy_kept()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "fpkeys").await;
    let five = r#"{"order":{"customer":{"tier":["gold"],"region":["eu"]},"total":[{"numeric":[">",0]}]},"source":["web"],"kind":["created"]}"#;
    let six = r#"{"order":{"customer":{"tier":["gold"],"region":["eu"],"vip":["yes"]},"total":[{"numeric":[">",0]}]},"source":["web"],"kind":["created"]}"#;
    let subscription = sqs_subscription(
        &app,
        &arn,
        "fpkeys-q",
        &[("FilterPolicyScope", "MessageBody"), ("FilterPolicy", five)],
    )
    .await;
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(attributes["FilterPolicyScope"], "MessageBody");
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(five));
    set_subscription(&app, &subscription, "FilterPolicy", six)
        .await
        .assert_invalid(
            "Invalid parameter: FilterPolicy: Filter policy can not have more than 5 keys",
        );
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(five));

    let six_flat = r#"{"event":["a"],"store":[{"prefix":"sea"}],"region":[{"suffix":"-west-2"}],"customer_tier":[{"anything-but":"free"}],"channel":[{"anything-but":["fax","pager"]}],"overflow":["anything"]}"#;
    subscribe(
        &app,
        &arn,
        "sqs",
        &queue_arn("fpkeys-six"),
        &[("FilterPolicy", six_flat)],
        true,
    )
    .await
    .assert_invalid(
        "Invalid parameter: Attributes Reason: FilterPolicy: Filter policy can not have more than \
         5 keys",
    );

    let plain = sqs_subscription(
        &app,
        &arn,
        "fpkeys-plain",
        &[("FilterPolicy", r#"{"event":["created"]}"#)],
    )
    .await;
    set_subscription(
        &app,
        &plain,
        "FilterPolicy",
        r#"{"event":["updated","deleted"]}"#,
    )
    .await
    .expect_ok();
    let updated = subscription_attributes(&app, &plain).await;
    assert_eq!(
        json_of(&updated["FilterPolicy"]),
        json!({"event": ["updated", "deleted"]})
    );
    set_subscription(
        &app,
        &plain,
        "FilterPolicy",
        r#"{"event":"must-be-a-list"}"#,
    )
    .await
    .assert_invalid(
        "Invalid parameter: FilterPolicy: \"event\" must be an object or an array\n at [Source: \
         (String)\"{\"event\":\"must-be-a-list\"}\"; line: 1, column: 11]",
    );
    let kept = subscription_attributes(&app, &plain).await;
    assert_eq!(
        json_of(&kept["FilterPolicy"]),
        json!({"event": ["updated", "deleted"]})
    );
}

/// AWS behavioural:
/// subscribe_with_an_unknown_attribute_rejected_and_creates_nothing.py (the
/// refusal and the empty listing; delivery is in
/// `aws_behavioral_pubsub_tests.rs`)
#[tokio::test]
async fn given_subscribe_with_misspelt_attribute_when_sent_then_invalid_parameter_and_nothing_created()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "unknown-attr").await;
    let policy = r#"{"tier":["gold"]}"#;
    subscribe(
        &app,
        &arn,
        "sqs",
        &queue_arn("unknown-attr-q"),
        &[("FilterPolicy", policy), ("RawMessgeDelivery", "true")],
        true,
    )
    .await
    .assert_invalid("Invalid parameter: Attributes Reason: Unknown attribute RawMessgeDelivery");
    assert!(subscriptions_of(&app, &arn).await.is_empty());

    let corrected = sqs_subscription(
        &app,
        &arn,
        "unknown-attr-q",
        &[("FilterPolicy", policy), ("RawMessageDelivery", "true")],
    )
    .await;
    let attributes = subscription_attributes(&app, &corrected).await;
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(policy));
    assert_eq!(attributes["RawMessageDelivery"], "true");
    let listed = subscriptions_of(&app, &arn).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["SubscriptionArn"], corrected);
}

/// AWS behavioural: subscribe_same_endpoint_is_idempotent.py
#[tokio::test]
async fn given_repeated_subscribe_when_attributes_differ_then_refused_else_same_arn() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "idempotent").await;
    let first = sqs_subscription(&app, &arn, "idem-q", &[]).await;
    assert_eq!(sqs_subscription(&app, &arn, "idem-q", &[]).await, first);
    let listed = subscriptions_of(&app, &arn).await;
    assert_eq!(listed.len(), 1);

    let policy = r#"{"event":["created"]}"#;
    subscribe(
        &app,
        &arn,
        "sqs",
        &queue_arn("idem-q"),
        &[("FilterPolicy", policy)],
        false,
    )
    .await
    .assert_invalid(
        "Invalid parameter: Attributes Reason: Subscription already exists with different \
         attributes",
    );
    assert_eq!(subscriptions_of(&app, &arn).await.len(), 1);
    assert!(
        !subscription_attributes(&app, &first)
            .await
            .contains_key("FilterPolicy")
    );

    let with_policy = sqs_subscription(&app, &arn, "idem-q3", &[("FilterPolicy", policy)]).await;
    assert_eq!(
        sqs_subscription(&app, &arn, "idem-q3", &[("FilterPolicy", policy)]).await,
        with_policy
    );
    // A repeat that omits Attributes is not a difference.
    assert_eq!(
        sqs_subscription(&app, &arn, "idem-q3", &[]).await,
        with_policy
    );
    let attributes = subscription_attributes(&app, &with_policy).await;
    assert_eq!(attributes["FilterPolicy"], policy);

    let other = sqs_subscription(&app, &arn, "idem-q2", &[]).await;
    assert_ne!(other, first);
    assert_ne!(other, with_policy);
    assert_eq!(subscriptions_of(&app, &arn).await.len(), 3);
}

/// AWS behavioural:
/// raw_message_delivery_flag_is_case_insensitive_and_reads_back_lowercase.py
/// (the flag legs; the delivered format is in `aws_behavioral_pubsub_tests.rs`)
#[tokio::test]
async fn given_raw_message_delivery_flag_when_set_with_other_case_then_lowercase_and_yes_is_refused()
 {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "rawcase").await;
    let subscription =
        sqs_subscription(&app, &arn, "rawcase-q", &[("RawMessageDelivery", "True")]).await;
    assert_eq!(
        subscription_attributes(&app, &subscription).await["RawMessageDelivery"],
        "true"
    );
    set_subscription(&app, &subscription, "RawMessageDelivery", "FALSE")
        .await
        .expect_ok();
    assert_eq!(
        subscription_attributes(&app, &subscription).await["RawMessageDelivery"],
        "false"
    );
    set_subscription(&app, &subscription, "RawMessageDelivery", "yes")
        .await
        .assert_invalid(
            "Invalid parameter: RawMessageDelivery: Invalid value [yes]. Must be true or false.",
        );
    assert_eq!(
        subscription_attributes(&app, &subscription).await["RawMessageDelivery"],
        "false"
    );
}

/// AWS behavioural: subscription_redrive_policy_non_sqs_target_rejected.py
/// AWS behavioural: subscription_redrive_policy_round_trip_and_replace.py
#[tokio::test]
async fn given_redrive_policy_when_set_then_queue_targets_replace_and_other_values_are_refused() {
    let (app, _state) = fixture().await;
    let arn = topic(&app, "redrive").await;
    let subscription =
        sqs_subscription(&app, &arn, "redrive-q", &[("RawMessageDelivery", "true")]).await;
    assert!(
        !subscription_attributes(&app, &subscription)
            .await
            .contains_key("RedrivePolicy")
    );

    let policy = |target: &str| json!({"deadLetterTargetArn": target}).to_string();
    set_subscription(&app, &subscription, "RedrivePolicy", &policy(&arn))
        .await
        .assert_invalid(
            "Invalid parameter: RedrivePolicy: deadLetterTargetArn must be a SQS queue",
        );
    assert!(
        !subscription_attributes(&app, &subscription)
            .await
            .contains_key("RedrivePolicy")
    );

    for target in ["redrive-dlq1", "redrive-dlq2"] {
        set_subscription(
            &app,
            &subscription,
            "RedrivePolicy",
            &policy(&queue_arn(target)),
        )
        .await
        .expect_ok();
        let attributes = subscription_attributes(&app, &subscription).await;
        assert_eq!(
            json_of(&attributes["RedrivePolicy"]),
            json!({"deadLetterTargetArn": queue_arn(target)})
        );
    }
    set_subscription(&app, &subscription, "RedrivePolicy", "")
        .await
        .assert_invalid("Invalid parameter: RedrivePolicy: missing required value");
    let attributes = subscription_attributes(&app, &subscription).await;
    assert_eq!(
        json_of(&attributes["RedrivePolicy"]),
        json!({"deadLetterTargetArn": queue_arn("redrive-dlq2")})
    );
    assert_eq!(attributes["RawMessageDelivery"], "true");
}
