//! AWS emulator behavioural suite ports.
//!
//! Topics, subscriptions, publish, filter policies and delivery to SQS on the
//! provider that `STORAGE_TEST_BACKEND` selects, driven through `PubsubManager`
//! the way the route does. Every expected value is what AWS answered in the
//! suite script that the test names. Query protocol wire cases are in
//! `storage-api/src/routes/pubsub/aws_behavioral_route_tests.rs`.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use pubsub::PubsubManager;
use pubsub_provider::{
    CreateTopicRequest, GetSubscriptionAttributesRequest, GetTopicAttributesRequest,
    ListSubscriptionsRequest, ListTopicsRequest, MessageAttribute, PublishBatchEntry,
    PublishBatchResult, PublishRequest, PubsubError, SetSubscriptionAttributesRequest,
    SubscribeRequest, SubscriptionArn, SubscriptionProtocol, TopicArn, TopicName,
};
use serde_json::{Value, json};

use super::{
    pubsub_contract_tests::queue_arn,
    pubsub_delivery_contract_tests::{manager, queue, unique},
    test_support::TestStore,
};

/// Bound of every wait in this file.
const WAIT_LIMIT: Duration = Duration::from_secs(10);

struct Rig {
    store: TestStore,
    manager: PubsubManager,
    _queues: Arc<queue::QueueManager>,
}

/// A queue of the store: the name that SNS endpoints use and the URL that SQS
/// uses.
struct Queue {
    name: String,
    url: String,
}

impl Rig {
    async fn new() -> Self {
        let store = TestStore::new().await;
        let (manager, queues) = manager(&store, None);
        Self {
            store,
            manager,
            _queues: queues,
        }
    }

    async fn topic(&self, label: &str) -> TopicArn {
        self.manager
            .create_topic(unique(label))
            .await
            .unwrap()
            .topic_arn
    }

    async fn queue(&self, label: &str) -> Queue {
        let name = unique(label);
        let url = queue(&self.store, &name, &[]).await;
        Queue { name, url }
    }

    async fn subscribe_to(
        &self,
        topic: &TopicArn,
        protocol: SubscriptionProtocol,
        endpoint: String,
        attributes: &[(&str, &str)],
    ) -> Result<String, PubsubError> {
        // Like an SDK, retry a conflict that outlasted the provider's bounded
        // retries when many callers contend on the topic counters.
        let mut attempts = 0;
        loop {
            let result = self
                .manager
                .subscribe(SubscribeRequest {
                    topic_arn: topic.clone(),
                    protocol,
                    endpoint: endpoint.clone(),
                    attributes: attributes
                        .iter()
                        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                        .collect(),
                    extra_json: Value::Null,
                })
                .await
                .map(|response| response.subscription_arn);
            attempts += 1;
            match result {
                Err(error) if attempts < 10 && error.to_string().contains("native conflict") => {}
                other => return other,
            }
        }
    }

    async fn subscribe(
        &self,
        topic: &TopicArn,
        queue: &Queue,
        attributes: &[(&str, &str)],
    ) -> SubscriptionArn {
        let arn = self
            .subscribe_to(
                topic,
                SubscriptionProtocol::Queue,
                queue_arn(&queue.name),
                attributes,
            )
            .await
            .unwrap();
        SubscriptionArn::new(arn).unwrap()
    }

    /// A subscription that stays pending: `Subscribe` answers `pending
    /// confirmation`.
    async fn subscribe_pending(&self, topic: &TopicArn) -> String {
        let answer = self
            .subscribe_to(
                topic,
                SubscriptionProtocol::Https,
                format!("https://example.test/{}", unique("hook")),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(answer, "pending confirmation");
        answer
    }

    async fn publish(
        &self,
        topic: &TopicArn,
        message: &str,
        attributes: Vec<(&str, MessageAttribute)>,
    ) -> Result<String, PubsubError> {
        self.manager
            .publish(PublishRequest {
                topic_arn: topic.clone(),
                message: message.to_owned(),
                subject: None,
                message_attributes: attributes
                    .into_iter()
                    .map(|(name, value)| (name.to_owned(), value))
                    .collect(),
            })
            .await
            .map(|response| response.message_id.as_str().to_owned())
    }

    async fn set_subscription(&self, arn: &SubscriptionArn, name: &str, value: &str) {
        self.try_set_subscription(arn, name, value).await.unwrap();
    }

    async fn try_set_subscription(
        &self,
        arn: &SubscriptionArn,
        name: &str,
        value: &str,
    ) -> Result<(), PubsubError> {
        self.manager
            .set_subscription_attributes(SetSubscriptionAttributesRequest {
                subscription_arn: arn.clone(),
                attributes: HashMap::from([(name.to_owned(), value.to_owned())]),
            })
            .await
    }

    async fn subscription_attributes(
        &self,
        arn: &SubscriptionArn,
    ) -> Result<HashMap<String, String>, PubsubError> {
        self.manager
            .get_subscription_attributes(GetSubscriptionAttributesRequest {
                subscription_arn: arn.clone(),
            })
            .await
            .map(|response| response.attributes)
    }

    async fn topic_attributes(
        &self,
        topic: &TopicArn,
    ) -> Result<HashMap<String, String>, PubsubError> {
        self.manager
            .get_topic_attributes(GetTopicAttributesRequest {
                topic_arn: topic.clone(),
            })
            .await
            .map(|response| response.attributes)
    }

    /// Every subscription ARN of a topic, in listed order, across pages.
    async fn listed(&self, topic: &TopicArn) -> Vec<(SubscriptionArn, String)> {
        let mut listed = Vec::new();
        let mut next_token = None;
        loop {
            let page = self
                .manager
                .list_subscriptions(ListSubscriptionsRequest {
                    topic_arn: Some(topic.clone()),
                    next_token,
                })
                .await
                .unwrap();
            listed.extend(
                page.subscriptions
                    .into_iter()
                    .map(|subscription| (subscription.subscription_arn, subscription.endpoint)),
            );
            next_token = page.next_token;
            if next_token.is_none() {
                return listed;
            }
        }
    }

    async fn topics(&self) -> Vec<TopicArn> {
        let mut arns = Vec::new();
        let mut next_token = None;
        loop {
            let page = self
                .manager
                .list_topics(ListTopicsRequest { next_token })
                .await
                .unwrap();
            arns.extend(page.topics.into_iter().map(|topic| topic.topic_arn));
            next_token = page.next_token;
            if next_token.is_none() {
                return arns;
            }
        }
    }

    /// Runs delivery passes until `expected` records are settled, then once
    /// more for strays; returns the settled total.
    async fn pump(&self, expected: usize) -> usize {
        let deadline = Instant::now() + WAIT_LIMIT;
        let mut settled = 0;
        while settled < expected {
            settled += self.manager.process_due_deliveries(100).await.unwrap();
            assert!(
                settled >= expected || Instant::now() < deadline,
                "{settled} of {expected} deliveries settled within {WAIT_LIMIT:?}"
            );
            if settled < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        settled + self.manager.process_due_deliveries(100).await.unwrap()
    }

    /// Everything in the queue, taken (it stays invisible for the rest of the
    /// test).
    async fn received(&self, queue: &Queue) -> Vec<queue_provider::MessageResponse> {
        let mut all = Vec::new();
        loop {
            let page = self
                .store
                .queue_operations
                .receive_messages(
                    &queue.url,
                    10,
                    Some(storage_types::DurationSeconds::from(300)),
                    None,
                )
                .await
                .unwrap()
                .messages;
            if page.is_empty() {
                return all;
            }
            all.extend(page);
        }
    }

    /// The published text of every message in the queue: the envelope's
    /// `Message`, else the body.
    async fn texts(&self, queue: &Queue) -> Vec<String> {
        self.received(queue)
            .await
            .into_iter()
            .map(|message| published(&message.body))
            .collect()
    }

    async fn texts_set(&self, queue: &Queue) -> HashSet<String> {
        self.texts(queue).await.into_iter().collect()
    }
}

fn published(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(Value::Object(envelope)) if envelope.get("Type") == Some(&json!("Notification")) => {
            envelope["Message"].as_str().unwrap().to_owned()
        }
        _ => body.to_owned(),
    }
}

fn string(value: &str) -> MessageAttribute {
    MessageAttribute::String(value.to_owned())
}

fn number(value: &str) -> MessageAttribute {
    MessageAttribute::Number(value.to_owned())
}

fn assert_refused(error: &PubsubError, kind: &str, message: &str) {
    assert_eq!(
        (error.aws_query_error_type(), error.aws_query_message()),
        (kind, message.to_owned()),
        "{error:?}"
    );
}

fn json_of(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

fn expected(texts: &[&str]) -> HashSet<String> {
    texts.iter().map(|text| (*text).to_owned()).collect()
}

/// AWS behavioural:
/// clearing_a_filter_policy_with_an_empty_value_restores_unfiltered_delivery.py
#[tokio::test]
async fn given_filtered_subscription_when_filter_policy_is_cleared_then_withheld_messages_are_delivered()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("fpclear").await;
    let (filtered, twin) = (rig.queue("fpclear-f").await, rig.queue("fpclear-t").await);
    let subscription = rig
        .subscribe(
            &topic,
            &filtered,
            &[("FilterPolicy", r#"{"color":["red"]}"#)],
        )
        .await;
    rig.subscribe(&topic, &twin, &[]).await;

    rig.publish(&topic, "blue-filtered", vec![("color", string("blue"))])
        .await
        .unwrap();
    rig.publish(&topic, "red", vec![("color", string("red"))])
        .await
        .unwrap();
    rig.pump(3).await;
    assert_eq!(
        rig.texts_set(&twin).await,
        expected(&["blue-filtered", "red"])
    );
    assert_eq!(rig.texts_set(&filtered).await, expected(&["red"]));

    rig.set_subscription(&subscription, "FilterPolicy", "")
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert!(!attributes.contains_key("FilterPolicy"), "{attributes:?}");

    rig.publish(&topic, "blue-after-clear", vec![("color", string("blue"))])
        .await
        .unwrap();
    rig.pump(2).await;
    assert_eq!(
        rig.texts_set(&filtered).await,
        expected(&["blue-after-clear"])
    );
    rig.publish(&topic, "no-attributes", vec![]).await.unwrap();
    rig.pump(2).await;
    assert_eq!(rig.texts_set(&filtered).await, expected(&["no-attributes"]));
    rig.store.finish().await;
}

/// AWS behavioural: concurrent_create_topic_same_name_converges.py
#[tokio::test]
async fn given_ten_concurrent_creates_of_one_name_then_one_topic_with_one_arn() {
    let rig = Rig::new().await;
    let name = unique("race");
    let create = || {
        rig.manager.create_topic_with(CreateTopicRequest {
            name: TopicName::new(name.clone()).unwrap(),
            attributes: HashMap::from([("DisplayName".to_owned(), "race".to_owned())]),
            tags: std::collections::BTreeMap::new(),
        })
    };
    let created = futures::future::join_all((0..10).map(|_| create())).await;
    let arns: HashSet<String> = created
        .into_iter()
        .map(|topic| topic.unwrap().topic_arn.as_str().to_owned())
        .collect();
    assert_eq!(arns.len(), 1, "concurrent creators got {} ARNs", arns.len());
    let arn = arns.into_iter().next().unwrap();
    assert!(arn.starts_with("arn:aws:sns:"), "{arn}");
    assert_eq!(arn.rsplit(':').next(), Some(name.as_str()));

    let named: Vec<TopicArn> = rig
        .topics()
        .await
        .into_iter()
        .filter(|topic| topic.as_str().rsplit(':').next() == Some(name.as_str()))
        .collect();
    assert_eq!(named.len(), 1);
    assert_eq!(
        rig.topic_attributes(&named[0]).await.unwrap()["DisplayName"],
        "race"
    );
    rig.store.finish().await;
}

/// AWS behavioural: e2e_topic_fanout_filter_teardown_flow.py
#[tokio::test]
async fn given_two_filtered_queues_when_torn_down_then_each_got_its_message_and_everything_is_gone()
{
    let rig = Rig::new().await;
    let topic = rig.topic("fanout").await;
    let (queue_a, queue_b) = (rig.queue("fanout-a").await, rig.queue("fanout-b").await);
    let raw = |policy: &'static str| [("FilterPolicy", policy), ("RawMessageDelivery", "true")];
    let sub_a = rig
        .subscribe(&topic, &queue_a, &raw(r#"{"team":["a"]}"#))
        .await;
    let sub_b = rig
        .subscribe(&topic, &queue_b, &raw(r#"{"team":["b"]}"#))
        .await;

    rig.publish(&topic, "marker-a", vec![("team", string("a"))])
        .await
        .unwrap();
    rig.publish(&topic, "marker-b", vec![("team", string("b"))])
        .await
        .unwrap();
    assert_eq!(rig.pump(2).await, 2);
    assert_eq!(rig.texts(&queue_a).await, ["marker-a"]);
    assert_eq!(rig.texts(&queue_b).await, ["marker-b"]);

    for subscription in [&sub_a, &sub_b] {
        rig.manager.unsubscribe(subscription.clone()).await.unwrap();
        let error = rig.subscription_attributes(subscription).await.unwrap_err();
        assert_refused(&error, "NotFound", "Subscription does not exist");
        assert!((400..500).contains(&error.aws_query_status_code()));
    }
    assert!(rig.listed(&topic).await.is_empty());

    rig.manager.delete_topic(topic.clone()).await.unwrap();
    let error = rig.topic_attributes(&topic).await.unwrap_err();
    assert_refused(&error, "NotFound", "Topic does not exist");
    assert!(!rig.topics().await.contains(&topic));
    let error = rig
        .publish(&topic, "after-delete", vec![])
        .await
        .unwrap_err();
    assert_eq!(error.aws_query_error_type(), "NotFound");
    assert!((400..500).contains(&error.aws_query_status_code()));
    assert!(!error.aws_query_message().is_empty());

    let control = rig.topic("fanout-control").await;
    assert!(
        !rig.publish(&control, "control-ok", vec![])
            .await
            .unwrap()
            .is_empty()
    );
    rig.store.finish().await;
}

/// AWS behavioural:
/// overlapping_filters_fan_one_message_to_every_matching_queue_and_the_deleted_topic_refuses_publish_and_listing.
/// py
#[tokio::test]
async fn given_overlapping_filters_when_published_then_each_queue_gets_its_matches_and_deleted_topic_refuses()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("overlap").await;
    let filters = [
        (
            "control",
            None,
            vec!["only-a", "all-three", "only-c", "none"],
        ),
        (
            "a",
            Some(r#"{"team":["a","shared"]}"#),
            vec!["only-a", "all-three"],
        ),
        ("b", Some(r#"{"team":["b","shared"]}"#), vec!["all-three"]),
        (
            "c",
            Some(r#"{"priority":[{"numeric":[">",5]}]}"#),
            vec!["all-three", "only-c"],
        ),
    ];
    let mut queues = Vec::new();
    for (label, policy, wanted) in &filters {
        let queue = rig.queue(&format!("overlap-{label}")).await;
        let mut attributes = vec![("RawMessageDelivery", "true")];
        attributes.extend(policy.map(|policy| ("FilterPolicy", policy)));
        rig.subscribe(&topic, &queue, &attributes).await;
        queues.push((queue, wanted));
    }
    for (label, team, priority) in [
        ("only-a", "a", "1"),
        ("all-three", "shared", "9"),
        ("only-c", "z", "7"),
        ("none", "z", "5"),
    ] {
        let attributes = vec![("team", string(team)), ("priority", number(priority))];
        rig.publish(&topic, label, attributes).await.unwrap();
    }
    assert_eq!(rig.pump(9).await, 9);
    for (queue, wanted) in &queues {
        assert_eq!(
            rig.texts_set(queue).await,
            expected(wanted),
            "{}",
            queue.name
        );
    }

    rig.manager.delete_topic(topic.clone()).await.unwrap();
    let error = rig.topic_attributes(&topic).await.unwrap_err();
    assert_refused(&error, "NotFound", "Topic does not exist");
    assert_eq!(error.aws_query_status_code(), 404);
    assert!(!rig.topics().await.contains(&topic));
    let error = rig
        .manager
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic.clone()),
            next_token: None,
        })
        .await
        .unwrap_err();
    assert_refused(&error, "NotFound", "Topic does not exist");
    assert_eq!(error.aws_query_status_code(), 404);
    let error = rig
        .publish(&topic, "after-delete", vec![])
        .await
        .unwrap_err();
    assert_refused(&error, "NotFound", "Topic does not exist");
    assert_eq!(error.aws_query_status_code(), 404);
    rig.store.finish().await;
}

/// Publishes each JSON body to a topic with an unfiltered raw twin and a raw
/// queue under a `MessageBody` filter; returns the bodies that the filtered
/// queue holds.
async fn body_scope_delivery(policy: &str, bodies: &[Value], admitted: usize) -> Vec<Value> {
    let rig = Rig::new().await;
    let topic = rig.topic("fpbody").await;
    let (twin, filtered) = (rig.queue("fpbody-t").await, rig.queue("fpbody-f").await);
    rig.subscribe(&topic, &twin, &[("RawMessageDelivery", "true")])
        .await;
    let subscription = rig
        .subscribe(
            &topic,
            &filtered,
            &[
                ("RawMessageDelivery", "true"),
                ("FilterPolicyScope", "MessageBody"),
                ("FilterPolicy", policy),
            ],
        )
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(policy));

    for body in bodies {
        rig.publish(&topic, &body.to_string(), vec![])
            .await
            .unwrap();
    }
    assert_eq!(
        rig.pump(bodies.len() + admitted).await,
        bodies.len() + admitted
    );
    let twin_bodies: Vec<Value> = rig
        .texts(&twin)
        .await
        .iter()
        .map(|text| json_of(text))
        .collect();
    assert_eq!(twin_bodies.len(), bodies.len());
    let held = rig
        .texts(&filtered)
        .await
        .iter()
        .map(|text| json_of(text))
        .collect();
    rig.store.finish().await;
    held
}

/// AWS behavioural:
/// filter_policy_message_body_scope_cidr_and_or_conjunction_decide_delivery.py
#[tokio::test]
async fn given_message_body_or_of_cidr_and_value_when_published_then_either_branch_delivers() {
    let policy = r#"{"$or":[{"client":{"ip":[{"cidr":"10.0.0.0/8"}]}},{"client":{"vip":[true]}}]}"#;
    let bodies = [
        json!({"which": "cidr_match", "client": {"ip": "10.1.2.3"}}),
        json!({"which": "vip_match", "client": {"vip": true}}),
        json!({"which": "neither", "client": {"ip": "192.168.1.1", "vip": false}}),
    ];
    let held = body_scope_delivery(policy, &bodies, 2).await;
    let which: HashSet<&str> = held
        .iter()
        .map(|body| body["which"].as_str().unwrap())
        .collect();
    assert_eq!(which, HashSet::from(["cidr_match", "vip_match"]));
    for body in &held {
        assert!(
            bodies.contains(body),
            "an admitted body changed in delivery"
        );
    }
    assert_eq!(held.len(), 2);
}

/// AWS behavioural:
/// filter_policy_message_body_scope_numeric_exists_and_anything_but_decide_delivery.
/// py
#[tokio::test]
async fn given_message_body_numeric_exists_and_anything_but_when_published_then_only_the_full_match_delivers()
 {
    let policy = r#"{"order":{"total":[{"numeric":[">",100]}],"status":[{"exists":false}]},"tag":[{"anything-but":"blocked"}]}"#;
    let bodies = [
        json!({"which": "match", "order": {"total": 150}, "tag": "ok"}),
        json!({"which": "fail_numeric", "order": {"total": 50}, "tag": "ok"}),
        json!({"which": "fail_exists", "order": {"total": 150, "status": "shipped"}, "tag": "ok"}),
        json!({"which": "fail_anything_but", "order": {"total": 150}, "tag": "blocked"}),
    ];
    let held = body_scope_delivery(policy, &bodies, 1).await;
    assert_eq!(held, [bodies[0].clone()]);
}

/// AWS behavioural: filter_policy_operator_vocabulary_round_trip.py (the
/// six-key leg is in the route tests)
#[tokio::test]
async fn given_every_filter_operator_when_subscribed_then_policy_reads_back_byte_exact_and_replaces_and_clears()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("fpops").await;
    let groups = [
        r#"{"event":["order_placed","order_cancelled"],"store":[{"prefix":"sea"}],"region":[{"suffix":"-west-2"}],"customer_tier":[{"anything-but":"free"}],"channel":[{"anything-but":["fax","pager"]}]}"#,
        r#"{"sku":[{"anything-but":{"prefix":"TEST-"}}],"attempt":[{"anything-but":[1,2]}],"price":[{"numeric":[">",0,"<=",100]}],"quantity":[{"numeric":["=",5]}],"coupon":[{"exists":false}]}"#,
        r#"{"order_id":[{"exists":true}],"status":[{"equals-ignore-case":"PENDING"}]}"#,
    ];
    let mut first = None;
    for policy in groups {
        let queue = rig.queue("fpops-q").await;
        let subscription = rig
            .subscribe(&topic, &queue, &[("FilterPolicy", policy)])
            .await;
        let attributes = rig.subscription_attributes(&subscription).await.unwrap();
        assert_eq!(attributes["FilterPolicy"], policy);
        first.get_or_insert(subscription);
    }
    let first = first.unwrap();

    let replacement = r#"{"$or":[{"metric_name":["CPUUtilization"]},{"namespace":["AWS/EC2"],"period":[{"numeric":[">=",60]}]}],"severity":[{"anything-but":{"prefix":"INFO"}}]}"#;
    rig.set_subscription(&first, "FilterPolicy", replacement)
        .await;
    let attributes = rig.subscription_attributes(&first).await.unwrap();
    let replaced = json_of(&attributes["FilterPolicy"]);
    assert_eq!(replaced, json_of(replacement));
    assert!(
        replaced.get("event").is_none(),
        "the previous policy lingers: {replaced}"
    );

    rig.set_subscription(&first, "FilterPolicy", "{}").await;
    let attributes = rig.subscription_attributes(&first).await.unwrap();
    let cleared = attributes.get("FilterPolicy").map_or("{}", String::as_str);
    assert_eq!(json_of(cleared), json!({}));
    rig.store.finish().await;
}

/// AWS behavioural: list_subscriptions_by_topic_reflects_state.py
/// AWS behavioural: resubscribe_after_unsubscribe_mints_a_new_arn.py
#[tokio::test]
async fn given_subscriptions_when_unsubscribed_then_listing_drops_them_and_resubscribing_mints_a_new_arn()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("listsub").await;
    let (queue_one, queue_two) = (rig.queue("listsub-1").await, rig.queue("listsub-2").await);
    let one = rig
        .subscribe(&topic, &queue_one, &[("RawMessageDelivery", "true")])
        .await;
    let two = rig.subscribe(&topic, &queue_two, &[]).await;
    assert_ne!(one, two);
    let arns = |listed: Vec<(SubscriptionArn, String)>| -> HashSet<SubscriptionArn> {
        listed.into_iter().map(|(arn, _)| arn).collect()
    };
    assert_eq!(
        arns(rig.listed(&topic).await),
        HashSet::from([one.clone(), two.clone()])
    );

    // The same endpoint and attributes while live is the same subscription.
    let again = rig
        .subscribe(&topic, &queue_one, &[("RawMessageDelivery", "true")])
        .await;
    assert_eq!(again, one);

    rig.manager.unsubscribe(one.clone()).await.unwrap();
    assert_eq!(arns(rig.listed(&topic).await), HashSet::from([two.clone()]));
    let error = rig.subscription_attributes(&one).await.unwrap_err();
    assert_refused(&error, "NotFound", "Subscription does not exist");

    let second = rig.subscribe(&topic, &queue_one, &[]).await;
    assert_ne!(second, one);
    assert!(
        second.as_str().starts_with(&format!("{topic}:")),
        "{second}"
    );
    let attributes = rig.subscription_attributes(&second).await.unwrap();
    assert_eq!(attributes["RawMessageDelivery"], "false");
    let error = rig.subscription_attributes(&one).await.unwrap_err();
    assert_refused(&error, "NotFound", "Subscription does not exist");
    rig.store.finish().await;
}

/// AWS behavioural: publish_binary_message_attribute_byte_preserved.py
#[tokio::test]
async fn given_binary_attribute_with_every_byte_when_raw_delivered_then_queue_attribute_is_byte_exact()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("binattr").await;
    let queue = rig.queue("binattr-q").await;
    rig.subscribe(&topic, &queue, &[("RawMessageDelivery", "true")])
        .await;
    let payload: Vec<u8> = (0..=255).collect();
    rig.publish(
        &topic,
        "binattr-body",
        vec![("payload", MessageAttribute::Binary(payload.clone()))],
    )
    .await
    .unwrap();
    assert_eq!(rig.pump(1).await, 1);

    let messages = rig.received(&queue).await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "binattr-body");
    let attribute = &messages[0].message_attributes.as_ref().unwrap()["payload"];
    assert_eq!(attribute.data_type, "Binary");
    assert_eq!(
        attribute.binary_value.as_deref(),
        Some(STANDARD.encode(&payload).as_str())
    );
    rig.store.finish().await;
}

/// AWS behavioural: publish_unicode_body_preserved.py
#[tokio::test]
async fn given_unicode_and_emoji_body_when_raw_delivered_then_queue_body_is_identical() {
    let rig = Rig::new().await;
    let topic = rig.topic("unicode").await;
    let queue = rig.queue("unicode-q").await;
    rig.subscribe(&topic, &queue, &[("RawMessageDelivery", "true")])
        .await;
    let body = "unicode \u{65e5}\u{672c}\u{8a9e} \u{1F680} caf\u{e9} \u{e9}l\u{e8}ve \
                \u{4e2d}\u{6587}\u{6d4b}\u{8bd5}";
    rig.publish(&topic, body, vec![]).await.unwrap();
    assert_eq!(rig.pump(1).await, 1);
    let messages = rig.received(&queue).await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, body);
    rig.store.finish().await;
}

fn entry(topic: &TopicArn, id: &str, message: &str) -> PublishBatchEntry {
    PublishBatchEntry {
        id: id.to_owned(),
        request: PublishRequest {
            topic_arn: topic.clone(),
            message: message.to_owned(),
            subject: None,
            message_attributes: HashMap::new(),
        },
        message_structure: None,
    }
}

fn successful_ids(results: &[PublishBatchResult]) -> Vec<&str> {
    results
        .iter()
        .map(|result| match result {
            PublishBatchResult::Successful { id, .. } => id.as_str(),
            PublishBatchResult::Failed { id, error } => panic!("entry {id} failed: {error:?}"),
        })
        .collect()
}

/// AWS behavioural: publish_batch_to_a_deleted_topic_not_found.py
#[tokio::test]
async fn given_deleted_topic_when_batch_published_then_not_found_and_a_live_topic_batch_delivers() {
    let rig = Rig::new().await;
    let (doomed, live) = (
        rig.topic("pbdel-doomed").await,
        rig.topic("pbdel-live").await,
    );
    let queue = rig.queue("pbdel-q").await;
    rig.subscribe(&doomed, &queue, &[]).await;
    rig.subscribe(&live, &queue, &[]).await;

    let before = rig
        .manager
        .publish_batch(
            doomed.clone(),
            vec![entry(&doomed, "before", "before-delete")],
        )
        .await
        .unwrap();
    assert_eq!(successful_ids(&before), ["before"]);
    assert_eq!(rig.pump(1).await, 1);
    assert_eq!(rig.texts(&queue).await, ["before-delete"]);

    rig.manager.delete_topic(doomed.clone()).await.unwrap();
    let refused: Vec<PublishBatchEntry> = (0..3)
        .map(|index| entry(&doomed, &format!("e{index}"), &format!("refused-{index}")))
        .collect();
    let error = rig
        .manager
        .publish_batch(doomed.clone(), refused)
        .await
        .unwrap_err();
    assert_refused(&error, "NotFound", "Topic does not exist");

    let after = rig
        .manager
        .publish_batch(
            live.clone(),
            vec![
                entry(&live, "s0", "sibling-0"),
                entry(&live, "s1", "sibling-1"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(successful_ids(&after), ["s0", "s1"]);
    assert_eq!(rig.pump(2).await, 2);
    let messages = rig.received(&queue).await;
    let texts: HashSet<String> = messages
        .iter()
        .map(|message| published(&message.body))
        .collect();
    assert_eq!(texts, expected(&["sibling-0", "sibling-1"]));
    for message in &messages {
        assert_eq!(json_of(&message.body)["TopicArn"], live.as_str());
    }
    rig.store.finish().await;
}

/// AWS behavioural:
/// raw_message_delivery_flag_is_case_insensitive_and_reads_back_lowercase.py
/// (the delivered format; the flag legs are in the route tests)
#[tokio::test]
async fn given_raw_message_delivery_flag_when_toggled_then_delivery_switches_between_raw_and_envelope()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("rawcase").await;
    let queue = rig.queue("rawcase-q").await;
    let subscription = rig
        .subscribe(&topic, &queue, &[("RawMessageDelivery", "True")])
        .await;
    assert_eq!(
        rig.subscription_attributes(&subscription).await.unwrap()["RawMessageDelivery"],
        "true"
    );

    rig.publish(&topic, "marker-on", vec![]).await.unwrap();
    assert_eq!(rig.pump(1).await, 1);
    let messages = rig.received(&queue).await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "marker-on");

    rig.set_subscription(&subscription, "RawMessageDelivery", "FALSE")
        .await;
    rig.publish(&topic, "marker-off", vec![]).await.unwrap();
    assert_eq!(rig.pump(1).await, 1);
    let messages = rig.received(&queue).await;
    assert_eq!(messages.len(), 1);
    let envelope = json_of(&messages[0].body);
    assert_eq!(envelope["Type"], "Notification");
    assert_eq!(envelope["Message"], "marker-off");
    rig.store.finish().await;
}

/// AWS behavioural:
/// subscribe_with_an_unknown_attribute_rejected_and_creates_nothing.py (the
/// delivery legs; the refusal is in the route tests)
#[tokio::test]
async fn given_publish_before_subscribe_when_corrected_subscription_filters_then_only_matching_raw_messages_arrive()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("subattr").await;
    let queue = rig.queue("subattr-q").await;
    let policy = r#"{"tier":["gold"]}"#;
    rig.publish(&topic, "orphan", vec![("tier", string("gold"))])
        .await
        .unwrap();
    assert_eq!(rig.pump(0).await, 0);

    let subscription = rig
        .subscribe(
            &topic,
            &queue,
            &[("FilterPolicy", policy), ("RawMessageDelivery", "true")],
        )
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert_eq!(json_of(&attributes["FilterPolicy"]), json_of(policy));
    assert_eq!(attributes["RawMessageDelivery"], "true");
    assert_eq!(rig.listed(&topic).await.len(), 1);

    rig.publish(&topic, "filtered-out", vec![("tier", string("silver"))])
        .await
        .unwrap();
    rig.publish(&topic, "delivered", vec![("tier", string("gold"))])
        .await
        .unwrap();
    assert_eq!(rig.pump(1).await, 1);
    assert_eq!(rig.texts(&queue).await, ["delivered"]);
    rig.store.finish().await;
}

/// AWS behavioural: subscription_created_with_attributes_roundtrip.py
#[tokio::test]
async fn given_subscribe_with_attributes_then_they_read_back_with_the_default_scope() {
    let rig = Rig::new().await;
    let topic = rig.topic("subattrs").await;
    let queue = rig.queue("subattrs-q").await;
    let subscription = rig
        .subscribe(
            &topic,
            &queue,
            &[
                ("RawMessageDelivery", "true"),
                ("FilterPolicy", r#"{"event":["approved"]}"#),
            ],
        )
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert_eq!(attributes["RawMessageDelivery"], "true");
    assert_eq!(
        json_of(&attributes["FilterPolicy"]),
        json!({"event": ["approved"]})
    );
    assert_eq!(attributes["FilterPolicyScope"], "MessageAttributes");
    rig.store.finish().await;
}

/// AWS behavioural:
/// subscription_delivery_policy_reads_back_normalized_on_an_sqs_subscription.py
#[tokio::test]
async fn given_delivery_policy_on_sqs_subscription_then_it_reads_back_normalized_and_survives_other_writes()
 {
    let rig = Rig::new().await;
    let topic = rig.topic("delpolicy").await;
    let queue = rig.queue("delpolicy-q").await;
    let subscription = rig.subscribe(&topic, &queue, &[]).await;
    let normalized = json!({
        "healthyRetryPolicy": {
            "minDelayTarget": 1, "maxDelayTarget": 10, "numRetries": 5,
            "numMaxDelayRetries": null, "numNoDelayRetries": null,
            "numMinDelayRetries": null, "backoffFunction": null
        },
        "sicklyRetryPolicy": null, "throttlePolicy": null, "requestPolicy": null,
        "guaranteed": false
    });

    let sent = r#"{"healthyRetryPolicy":{"numRetries":5,"minDelayTarget":1,"maxDelayTarget":10}}"#;
    rig.set_subscription(&subscription, "DeliveryPolicy", sent)
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert_eq!(json_of(&attributes["DeliveryPolicy"]), normalized);
    assert_eq!(attributes["Protocol"], "sqs");

    rig.set_subscription(&subscription, "RawMessageDelivery", "true")
        .await;
    let attributes = rig.subscription_attributes(&subscription).await.unwrap();
    assert_eq!(attributes["RawMessageDelivery"], "true");
    assert_eq!(json_of(&attributes["DeliveryPolicy"]), normalized);
    rig.store.finish().await;
}

async fn counters(rig: &Rig, topic: &TopicArn) -> (String, String, String) {
    let attributes = rig.topic_attributes(topic).await.unwrap();
    (
        attributes["SubscriptionsConfirmed"].clone(),
        attributes["SubscriptionsPending"].clone(),
        attributes["SubscriptionsDeleted"].clone(),
    )
}

/// AWS behavioural: topic_attributes_subscription_counters_track_state.py
/// (`https` stands in for `email` as the pending protocol)
#[tokio::test]
async fn given_subscribe_and_unsubscribe_when_topic_attributes_read_then_counters_follow_state() {
    let rig = Rig::new().await;
    let topic = rig.topic("counters").await;
    let zero = || "0".to_owned();
    assert_eq!(counters(&rig, &topic).await, (zero(), zero(), zero()));

    let queue = rig.queue("counters-q").await;
    let confirmed = rig.subscribe(&topic, &queue, &[]).await;
    assert_eq!(
        counters(&rig, &topic).await,
        ("1".to_owned(), zero(), zero())
    );

    rig.subscribe_pending(&topic).await;
    let (confirmed_count, pending_count, _) = counters(&rig, &topic).await;
    assert_eq!(
        (confirmed_count.as_str(), pending_count.as_str()),
        ("1", "1")
    );

    rig.manager.unsubscribe(confirmed.clone()).await.unwrap();
    let (confirmed_count, pending_count, _) = counters(&rig, &topic).await;
    assert_eq!(
        (confirmed_count.as_str(), pending_count.as_str()),
        ("0", "1")
    );
    let error = rig.subscription_attributes(&confirmed).await.unwrap_err();
    assert_refused(&error, "NotFound", "Subscription does not exist");
    rig.store.finish().await;
}

/// AWS behavioural:
/// topic_subscription_counters_converge_after_concurrent_subscribes_and_unsubscribes.
/// py (`https` stands in for `email` as the pending protocol)
#[tokio::test]
async fn given_concurrent_subscribes_and_unsubscribes_then_counters_match_the_listing() {
    const CONFIRMED: usize = 16;
    const PENDING: usize = 4;
    const REMOVED: usize = 10;
    let rig = Rig::new().await;
    let topic = rig.topic("ctrs").await;
    let queues: Vec<Queue> = {
        let mut queues = Vec::new();
        for index in 0..CONFIRMED {
            queues.push(rig.queue(&format!("ctrs-{index}")).await);
        }
        queues
    };
    let hooks: Vec<String> = (0..PENDING)
        .map(|_| format!("https://example.test/{}", unique("hook")))
        .collect();

    let confirmed = futures::future::join_all(queues.iter().map(|queue| {
        rig.subscribe_to(
            &topic,
            SubscriptionProtocol::Queue,
            queue_arn(&queue.name),
            &[],
        )
    }));
    let pending = futures::future::join_all(
        hooks
            .iter()
            .map(|hook| rig.subscribe_to(&topic, SubscriptionProtocol::Https, hook.clone(), &[])),
    );
    let (confirmed, pending) = futures::future::join(confirmed, pending).await;
    let confirmed: Vec<SubscriptionArn> = confirmed
        .into_iter()
        .map(|arn| SubscriptionArn::new(arn.unwrap()).unwrap())
        .collect();
    assert!(
        pending
            .iter()
            .all(|answer| answer.as_deref().ok() == Some("pending confirmation"))
    );
    assert_eq!(confirmed.iter().collect::<HashSet<_>>().len(), CONFIRMED);

    let (confirmed_count, pending_count, _) = counters(&rig, &topic).await;
    assert_eq!(
        (confirmed_count.as_str(), pending_count.as_str()),
        ("16", "4")
    );
    let mut endpoints: Vec<String> = rig
        .listed(&topic)
        .await
        .into_iter()
        .map(|(_, endpoint)| endpoint)
        .collect();
    let mut wanted: Vec<String> = queues
        .iter()
        .map(|queue| queue_arn(&queue.name))
        .chain(hooks.iter().cloned())
        .collect();
    endpoints.sort();
    wanted.sort();
    assert_eq!(endpoints, wanted);

    let removed = futures::future::join_all(confirmed[..REMOVED].iter().map(|arn| async {
        let mut attempts = 0;
        loop {
            let result = rig.manager.unsubscribe(arn.clone()).await;
            attempts += 1;
            match result {
                Err(error) if attempts < 10 && error.to_string().contains("native conflict") => {}
                other => return other,
            }
        }
    }))
    .await;
    assert!(removed.iter().all(Result::is_ok));
    let (confirmed_count, pending_count, _) = counters(&rig, &topic).await;
    assert_eq!(
        (confirmed_count.as_str(), pending_count.as_str()),
        ("6", "4")
    );
    let listed: HashSet<SubscriptionArn> = rig
        .listed(&topic)
        .await
        .into_iter()
        .map(|(arn, _)| arn)
        .collect();
    for arn in &confirmed[..REMOVED] {
        assert!(!listed.contains(arn));
    }
    for arn in &confirmed[REMOVED..] {
        assert!(listed.contains(arn));
    }
    assert_eq!(listed.len(), CONFIRMED - REMOVED + PENDING);
    rig.store.finish().await;
}
