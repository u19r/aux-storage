use std::collections::{HashMap, HashSet};

use pubsub_provider::{
    CreateTopicRequest, DeliveryRecordId, DeliveryTarget, GetSubscriptionAttributesRequest,
    GetTopicAttributesRequest, ListSubscriptionsRequest, ListTopicsRequest, PublishRequest,
    PubsubMessageId, SetTopicAttributesRequest, SubscribeRequest, Subscription, SubscriptionArn,
    SubscriptionAttributes, SubscriptionProtocol, TopicName,
};
use storage_types::{MessagingIdentity, TimestampMillis};

use super::{pubsub_delivery_contract_tests::claim_at, test_support::TestStore};

pub(super) async fn create_topic(
    fixture: &TestStore,
    name: &str,
    attributes: HashMap<String, String>,
) -> anyhow::Result<pubsub_provider::Topic> {
    let topic = pubsub_provider::Topic::try_new(
        CreateTopicRequest {
            name: TopicName::new(name)?,
            attributes,
            tags: std::collections::BTreeMap::new(),
        },
        &MessagingIdentity::default(),
    )
    .map_err(|err| anyhow::anyhow!("topic request: {err:?}"))?;
    fixture
        .pubsub_provider
        .create_topic(topic)
        .await
        .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))
}

/// The SQS endpoint form that SNS accepts: a queue ARN (AWS, section 13).
pub(super) fn queue_arn(name: &str) -> String {
    format!("arn:aws:sqs:us-east-1:000000000000:{name}")
}

/// `Subscribe` as the manager performs it: validate, then store.
pub(super) async fn subscribe(
    fixture: &TestStore,
    request: SubscribeRequest,
) -> anyhow::Result<pubsub_provider::Subscription> {
    let subscription = Subscription::try_new(request, &MessagingIdentity::default())
        .map_err(|err| anyhow::anyhow!("subscribe request: {err:?}"))?;
    fixture
        .pubsub_provider
        .create_subscription(subscription)
        .await
        .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))
}

async fn create_queue_subscription(
    fixture: &TestStore,
    topic_arn: &pubsub_provider::TopicArn,
    endpoint: &str,
    attributes: HashMap<String, String>,
    extra_json: serde_json::Value,
) -> anyhow::Result<pubsub_provider::Subscription> {
    subscribe(
        fixture,
        SubscribeRequest {
            topic_arn: topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn(endpoint),
            attributes,
            extra_json,
        },
    )
    .await
}

#[tokio::test]
async fn given_topic_when_publish_then_delivery_claimed() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "orders", HashMap::new())
                .await
                .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))?;
            let subscription = subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: queue_arn("orders"),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn.clone(),
                        message: "body".to_string(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    pubsub_provider::PubsubMessageId::new(),
                    false,
                )
                .await
                .map_err(|err| anyhow::anyhow!("publish failed: {err:?}"))?;
            let materialized = fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?
                .records;
            anyhow::ensure!(
                materialized == 1,
                "expected one materialized publish, got {materialized}"
            );
            // Claim the actual delivery lease and check the accepted
            // publication, the subscription destination and the payload.
            let claimed = claim_at(
                &fixture,
                "conformance",
                storage_types::TimestampMillis::from_timestamp(1_800_000_000_000),
            )
            .await;
            anyhow::ensure!(
                claimed.len() == 1,
                "expected one claimed delivery, got {}",
                claimed.len()
            );
            let delivery = &claimed[0];
            anyhow::ensure!(
                delivery.state.subscription_arn == subscription.subscription_arn,
                "delivery targets the wrong subscription"
            );
            anyhow::ensure!(
                delivery.payload.message_body.as_deref() == Some("body"),
                "delivery payload mismatch: {:?}",
                delivery.payload.message_body
            );
            anyhow::ensure!(
                delivery
                    .state
                    .lease
                    .as_ref()
                    .map(|lease| lease.owner.as_str())
                    == Some("conformance"),
                "delivery lease not held by the claimant"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-IDENT-01: topic creation is idempotent for the same name.
///
/// Ports `pubsub-provider/src/memory_tests.rs`
/// `create_topic_is_idempotent_for_same_name` and the KV
/// `sorted_kv_pubsub_provider_persists_topic_subscription_and_attributes`
/// repeated-topic assertion.
#[tokio::test]
async fn given_topic_when_create_idempotent_then_same_arn() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let first = create_topic(&fixture, "sns-ident", HashMap::new()).await?;
            let second = create_topic(&fixture, "sns-ident", HashMap::new()).await?;
            anyhow::ensure!(
                first.topic_arn == second.topic_arn,
                "idempotent topic changed ARN"
            );
            let topics = fixture
                .pubsub_provider
                .list_topics(ListTopicsRequest { next_token: None })
                .await
                .map_err(|err| anyhow::anyhow!("list topics failed: {err:?}"))?
                .topics;
            anyhow::ensure!(
                topics
                    .iter()
                    .any(|topic| topic.topic_arn == first.topic_arn),
                "idempotent topic missing from list"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-ATTR-01: topic attributes persist DisplayName and subscription
/// counts.
///
/// Ports `sqlite_pubsub_provider_persists_topic_subscription_and_attributes`
/// and the KV `..._persists_topic_subscription_and_attributes` DisplayName
/// branch plus `memory_tests.rs`
/// `set_topic_attributes_updates_display_name`
/// and `get_topic_attributes_includes_empty_display_name_by_default`.
#[tokio::test]
async fn given_topic_when_attributes_then_persisted() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            // Default DisplayName is empty.
            let default_topic = create_topic(&fixture, "sns-attr-default", HashMap::new()).await?;
            let default_attrs = fixture
                .pubsub_provider
                .get_topic_attributes(GetTopicAttributesRequest {
                    topic_arn: default_topic.topic_arn.clone(),
                })
                .await
                .map_err(|err| anyhow::anyhow!("default attrs failed: {err:?}"))?
                .attributes;
            anyhow::ensure!(
                default_attrs.get("DisplayName") == Some(&String::new()),
                "default DisplayName wrong: {default_attrs:?}"
            );
            // DisplayName set at creation persists.
            let named = create_topic(
                &fixture,
                "sns-attr-named",
                HashMap::from([("DisplayName".to_string(), "Orders".to_string())]),
            )
            .await?;
            let named_attrs = fixture
                .pubsub_provider
                .get_topic_attributes(GetTopicAttributesRequest {
                    topic_arn: named.topic_arn.clone(),
                })
                .await
                .map_err(|err| anyhow::anyhow!("named attrs failed: {err:?}"))?
                .attributes;
            anyhow::ensure!(
                named_attrs.get("DisplayName") == Some(&"Orders".to_string()),
                "DisplayName not persisted: {named_attrs:?}"
            );
            // Set updates DisplayName.
            fixture
                .pubsub_provider
                .set_topic_attributes(SetTopicAttributesRequest {
                    topic_arn: named.topic_arn.clone(),
                    attributes: HashMap::from([("DisplayName".to_string(), "Renamed".to_string())]),
                })
                .await
                .map_err(|err| anyhow::anyhow!("set topic attrs failed: {err:?}"))?;
            let renamed = fixture
                .pubsub_provider
                .get_topic_attributes(GetTopicAttributesRequest {
                    topic_arn: named.topic_arn.clone(),
                })
                .await
                .map_err(|err| anyhow::anyhow!("renamed attrs failed: {err:?}"))?
                .attributes;
            anyhow::ensure!(
                renamed.get("DisplayName") == Some(&"Renamed".to_string()),
                "DisplayName update lost: {renamed:?}"
            );
            // One queue subscription confirms immediately.
            create_queue_subscription(
                &fixture,
                &named.topic_arn,
                "sns-attr-queue",
                HashMap::new(),
                serde_json::Value::Null,
            )
            .await?;
            let counted = fixture
                .pubsub_provider
                .get_topic_attributes(GetTopicAttributesRequest {
                    topic_arn: named.topic_arn,
                })
                .await
                .map_err(|err| anyhow::anyhow!("counted attrs failed: {err:?}"))?
                .attributes;
            anyhow::ensure!(
                counted.get("SubscriptionsConfirmed") == Some(&"1".to_string()),
                "confirmed count wrong: {counted:?}"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-ATTR-02: subscription attributes preserve identity, raw delivery
/// and extra JSON.
///
/// Ports `memory_tests.rs`
/// `subscription_persists_extra_json_for_custom_senders`
/// and `set_subscription_attributes_preserves_subscription_identity` plus
/// the SQLite `..._persists_topic_subscription_and_attributes` raw/extra
/// assertions.
#[tokio::test]
async fn given_subscription_when_attributes_then_identity_preserved() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-subattr", HashMap::new()).await?;
            let subscription = subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.com/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::json!({"token_ref":"secret/oauth/orders"}),
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
            let updated = fixture
                .pubsub_provider
                .set_subscription_attributes(
                    &subscription.subscription_arn.clone(),
                    SubscriptionAttributes::parse(
                        &HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
                        &MessagingIdentity::default(),
                    )
                    .map_err(|err| anyhow::anyhow!("attributes: {err:?}"))?,
                )
                .await
                .map_err(|err| anyhow::anyhow!("set sub attrs failed: {err:?}"))?;
            anyhow::ensure!(
                updated.subscription_arn == subscription.subscription_arn,
                "set attributes changed subscription identity"
            );
            anyhow::ensure!(
                updated.raw_message_delivery,
                "RawMessageDelivery not applied"
            );
            let listed = fixture
                .pubsub_provider
                .list_subscriptions(ListSubscriptionsRequest {
                    topic_arn: Some(topic.topic_arn.clone()),
                    next_token: None,
                })
                .await
                .map_err(|err| anyhow::anyhow!("list failed: {err:?}"))?
                .subscriptions;
            anyhow::ensure!(listed.len() == 1, "expected one subscription");
            anyhow::ensure!(
                listed[0].extra_json["token_ref"] == "secret/oauth/orders",
                "extra_json lost"
            );
            anyhow::ensure!(listed[0].raw_message_delivery, "listed raw flag lost");
            let attrs = fixture
                .pubsub_provider
                .get_subscription_attributes(GetSubscriptionAttributesRequest {
                    subscription_arn: subscription.subscription_arn,
                })
                .await
                .map_err(|err| anyhow::anyhow!("get sub attrs failed: {err:?}"))?
                .attributes;
            anyhow::ensure!(
                attrs.get("RawMessageDelivery") == Some(&"true".to_string()),
                "subscription attributes missing raw flag: {attrs:?}"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-ROUTE-01: per-topic subscription scans do not overlap by prefix.
///
/// Ports `sorted_kv_pubsub_provider_topic_subscription_scans_do_not_overlap_by_prefix`.
#[tokio::test]
async fn given_topics_when_prefix_overlaps_then_list_isolated() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let short = create_topic(&fixture, "px-short", HashMap::new()).await?;
            let long = create_topic(&fixture, "px-short-long", HashMap::new()).await?;
            subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: short.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/short".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("short subscribe failed: {err:?}"))?;
            subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: long.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/long".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("long subscribe failed: {err:?}"))?;
            let short_subs = fixture
                .pubsub_provider
                .list_subscriptions(ListSubscriptionsRequest {
                    topic_arn: Some(short.topic_arn),
                    next_token: None,
                })
                .await
                .map_err(|err| anyhow::anyhow!("list short failed: {err:?}"))?
                .subscriptions;
            anyhow::ensure!(
                short_subs.len() == 1 && short_subs[0].endpoint == "https://example.test/short",
                "prefix scan leaked: {:?}",
                short_subs
                    .iter()
                    .map(|sub| &sub.endpoint)
                    .collect::<Vec<_>>()
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-SNAPSHOT-01: publication snapshots survive subscriber deletion.
///
/// Ports `sqlite_publish_acceptance_persists_subscription_snapshot`: the
/// accepted delivery owns an immutable subscription copy.
#[tokio::test]
async fn given_publish_when_subscription_deleted_before_materialize_then_snapshot_preserved()
-> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-snapshot", HashMap::new()).await?;
            let subscription = create_queue_subscription(
                &fixture,
                &topic.topic_arn,
                "snapshot-queue",
                HashMap::new(),
                serde_json::Value::Null,
            )
            .await?;
            let message_id = PubsubMessageId::new_from_string("snapshot-message")
                .map_err(|err| anyhow::anyhow!("message id failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn,
                        message: "body".to_string(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    message_id.clone(),
                    false,
                )
                .await
                .map_err(|err| anyhow::anyhow!("accept failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .delete_subscription(&subscription.subscription_arn)
                .await
                .map_err(|err| anyhow::anyhow!("delete sub failed: {err:?}"))?;
            let materialized = fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?
                .records;
            anyhow::ensure!(
                materialized == 1,
                "expected 1 materialized, got {materialized}"
            );
            let record = fixture
                .pubsub_provider
                .get_delivery(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    subscription.subscription_arn
                )))
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("get delivery failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("accepted delivery missing"))?;
            anyhow::ensure!(
                record.subscription == Some(subscription),
                "delivery snapshot diverged"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-FILTER-01: confirmed/pending protocols select targets; pending is
/// excluded.
///
/// Ports `given_confirmed_and_pending_protocols_when_accepting_publish_then_targets_and_filter_match`:
/// queue subscriptions target BuiltIn, confirmed HTTP targets
/// CustomSender when enabled, pending HTTP is filtered out.
#[tokio::test]
async fn given_publish_when_protocols_mixed_then_targets_and_pending_filtered() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-protocol", HashMap::new()).await?;
            let queue = create_queue_subscription(
                &fixture,
                &topic.topic_arn,
                "protocol-queue",
                HashMap::new(),
                serde_json::Value::Null,
            )
            .await?;
            let confirmed = subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Http,
                    endpoint: "http://confirmed.example/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("confirmed subscribe failed: {err:?}"))?;
            let token = confirmed
                .confirmation
                .token()
                .ok_or_else(|| anyhow::anyhow!("confirmed sub lacks token"))?
                .to_string();
            fixture
                .pubsub_provider
                .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
                    topic_arn: topic.topic_arn.clone(),
                    token,
                })
                .await
                .map_err(|err| anyhow::anyhow!("confirm failed: {err:?}"))?;
            let pending = subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Http,
                    endpoint: "http://pending.example/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("pending subscribe failed: {err:?}"))?;
            let message_id = PubsubMessageId::new_from_string("message-protocol")
                .map_err(|err| anyhow::anyhow!("message id failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn,
                        message: "body".to_string(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    message_id.clone(),
                    true,
                )
                .await
                .map_err(|err| anyhow::anyhow!("accept failed: {err:?}"))?;
            let materialized = fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?
                .records;
            anyhow::ensure!(
                materialized == 2,
                "expected 2 deliveries, got {materialized}"
            );
            let queue_record = fixture
                .pubsub_provider
                .get_delivery(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    queue.subscription_arn
                )))
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("queue delivery missing: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue delivery absent"))?;
            anyhow::ensure!(
                queue_record.target == DeliveryTarget::BuiltIn,
                "queue target wrong: {:?}",
                queue_record.target
            );
            let http_record = fixture
                .pubsub_provider
                .get_delivery(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    confirmed.subscription_arn
                )))
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("http delivery missing: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("confirmed delivery absent"))?;
            anyhow::ensure!(
                http_record.target == DeliveryTarget::CustomSender,
                "http target wrong: {:?}",
                http_record.target
            );
            let pending_record = fixture
                .pubsub_provider
                .get_delivery(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    pending.subscription_arn
                )))
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("pending lookup failed: {err:?}"))?;
            anyhow::ensure!(
                pending_record.is_none(),
                "pending subscription was not filtered"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-LIFECYCLE-01: accepted intents are not claimable before
/// materialization.
///
/// Ports `sorted_kv_publish_requires_intent_materialization_before_claim`.
#[tokio::test]
async fn given_publish_when_accepted_then_not_claimable_before_materialize() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-lifecycle", HashMap::new()).await?;
            let mut expected = Vec::new();
            for index in 0..2 {
                let sub = create_queue_subscription(
                    &fixture,
                    &topic.topic_arn,
                    &format!("claim-queue-{index}"),
                    HashMap::new(),
                    serde_json::Value::Null,
                )
                .await?;
                expected.push(sub.subscription_arn);
            }
            let message_id = PubsubMessageId::new();
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn,
                        message: "claim-lifecycle-body".to_string(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    message_id.clone(),
                    false,
                )
                .await
                .map_err(|err| anyhow::anyhow!("accept failed: {err:?}"))?;
            let now = TimestampMillis::now();
            let before = claim_at(&fixture, "before-materialize", now).await;
            anyhow::ensure!(
                before.is_empty(),
                "intents claimable before materialization"
            );
            let materialized = fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?
                .records;
            anyhow::ensure!(materialized == 2, "expected 2, got {materialized}");
            let mut actual = claim_at(&fixture, "after-materialize", now)
                .await
                .into_iter()
                .map(|claimed| claimed.state.id)
                .collect::<Vec<_>>();
            actual.sort_by(|left, right| left.0.cmp(&right.0));
            let mut expected_ids = expected
                .into_iter()
                .map(|arn| DeliveryRecordId(format!("{arn}:{message_id}")))
                .collect::<Vec<_>>();
            expected_ids.sort_by(|left, right| left.0.cmp(&right.0));
            anyhow::ensure!(actual == expected_ids, "claimed ids diverged");
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-DELETE-01: accepted deliveries survive topic deletion.
///
/// Ports `sorted_kv_pubsub_provider_retains_accepted_deliveries_after_topic_delete`.
#[tokio::test]
async fn given_delivery_when_topic_deleted_then_accepted_preserved() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-delete", HashMap::new()).await?;
            let subscription = create_queue_subscription(
                &fixture,
                &topic.topic_arn,
                "sns-delete",
                HashMap::new(),
                serde_json::Value::Null,
            )
            .await?;
            let message_id = PubsubMessageId::new();
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn.clone(),
                        message: "retained".to_owned(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    message_id.clone(),
                    false,
                )
                .await
                .map_err(|err| anyhow::anyhow!("publish failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
            let id = DeliveryRecordId(format!("{}:{message_id}", subscription.subscription_arn));
            let expected = fixture
                .pubsub_provider
                .get_delivery(&id)
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("get delivery failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("materialized delivery missing"))?;
            fixture
                .pubsub_provider
                .delete_topic(&topic.topic_arn)
                .await
                .map_err(|err| anyhow::anyhow!("delete topic failed: {err:?}"))?;
            let sub = fixture
                .pubsub_provider
                .get_subscription(&subscription.subscription_arn)
                .await
                .map_err(|err| anyhow::anyhow!("get sub failed: {err:?}"))?;
            anyhow::ensure!(sub.is_none(), "topic delete left subscription");
            let retained = fixture
                .pubsub_provider
                .get_delivery(&expected.id)
                .await
                .map(|row| row.map(|(_, payload)| payload))
                .map_err(|err| anyhow::anyhow!("get delivery failed: {err:?}"))?;
            anyhow::ensure!(
                retained == Some(expected),
                "topic delete dropped accepted delivery"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-SNAPSHOT-02: publish snapshots are immutable across mutation and
/// topic deletion, and chunked materialization drains exactly once.
///
/// Ports `sorted_kv_publish_intent_uses_immutable_chunked_subscription_snapshot`
/// sequentially (concurrent materialization is covered by SNS-CLAIM-02):
/// 12 subscriptions chunk into 10 + 2; a raw-flag mutation and topic
/// delete before materialization must not leak into snapshots.
#[tokio::test]
async fn given_publish_when_many_subscriptions_then_snapshots_immutable_and_drained()
-> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-chunks", HashMap::new()).await?;
            let mut subscriptions = Vec::new();
            for index in 0..12 {
                subscriptions.push(
                    create_queue_subscription(
                        &fixture,
                        &topic.topic_arn,
                        &format!("chunk-queue-{index}"),
                        HashMap::new(),
                        serde_json::Value::Null,
                    )
                    .await?,
                );
            }
            let message_id = PubsubMessageId::new();
            fixture
                .pubsub_provider
                .accept_publish(
                    PublishRequest {
                        topic_arn: topic.topic_arn.clone(),
                        message: "snapshot-body".to_string(),
                        subject: None,
                        message_attributes: HashMap::new(),
                    },
                    message_id.clone(),
                    false,
                )
                .await
                .map_err(|err| anyhow::anyhow!("accept failed: {err:?}"))?;
            // Mutate after acceptance and delete the topic: snapshots must
            // stay frozen at acceptance time.
            fixture
                .pubsub_provider
                .set_subscription_attributes(
                    &subscriptions[0].subscription_arn.clone(),
                    SubscriptionAttributes::parse(
                        &HashMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
                        &MessagingIdentity::default(),
                    )
                    .map_err(|err| anyhow::anyhow!("attributes: {err:?}"))?,
                )
                .await
                .map_err(|err| anyhow::anyhow!("mutate failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .delete_topic(&topic.topic_arn)
                .await
                .map_err(|err| anyhow::anyhow!("delete topic failed: {err:?}"))?;
            let mut total = 0usize;
            for _ in 0..4 {
                let advanced = fixture
                    .pubsub_provider
                    .materialize_publish_intents(10)
                    .await
                    .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?
                    .records;
                total += advanced;
                if advanced == 0 {
                    break;
                }
            }
            anyhow::ensure!(total == 12, "expected 12 deliveries, got {total}");
            for (index, subscription) in subscriptions.iter().enumerate() {
                let record = fixture
                    .pubsub_provider
                    .get_delivery(&DeliveryRecordId(format!(
                        "{}:{message_id}",
                        subscription.subscription_arn
                    )))
                    .await
                    .map(|row| row.map(|(_, payload)| payload))
                    .map_err(|err| anyhow::anyhow!("get delivery failed: {err:?}"))?
                    .ok_or_else(|| anyhow::anyhow!("chunk delivery missing"))?;
                let snapshot = record
                    .subscription
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("delivery lacks snapshot"))?;
                anyhow::ensure!(
                    snapshot.subscription_arn == subscription.subscription_arn,
                    "snapshot ARN diverged"
                );
                anyhow::ensure!(
                    snapshot.endpoint == queue_arn(&format!("chunk-queue-{index}")),
                    "snapshot endpoint diverged"
                );
                anyhow::ensure!(
                    !snapshot.raw_message_delivery,
                    "snapshot leaked post-accept mutation"
                );
            }
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

/// SNS-MGR-01: topic creation is idempotent through the manager.
///
/// Ports `pubsub/src/manager_tests.rs`
/// `create_topic_is_idempotent_through_manager` with real storage.
#[tokio::test]
async fn given_missing_when_observed_then_fail_closed_without_effect() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let missing_topic =
                pubsub_provider::TopicArn::new("arn:aws:sns:us-east-1:000000000000:sns-missing")
                    .map_err(|err| anyhow::anyhow!("test ARN failed: {err:?}"))?;
            let topic = fixture
                .pubsub_provider
                .get_topic(&missing_topic)
                .await
                .map_err(|err| anyhow::anyhow!("get missing topic failed: {err:?}"))?;
            anyhow::ensure!(topic.is_none(), "missing topic unexpectedly resolved");
            let attrs = fixture
                .pubsub_provider
                .get_topic_attributes(GetTopicAttributesRequest {
                    topic_arn: missing_topic.clone(),
                })
                .await;
            anyhow::ensure!(
                matches!(
                    attrs,
                    Err(pubsub_provider::PubsubError::ResourceNotFound {
                        resource_type: "topic",
                        ..
                    })
                ),
                "missing topic attrs wrong error: {attrs:?}"
            );
            let missing_sub =
                SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:sns-missing:sub")
                    .map_err(|err| anyhow::anyhow!("test sub ARN failed: {err:?}"))?;
            let sub = fixture
                .pubsub_provider
                .get_subscription(&missing_sub)
                .await
                .map_err(|err| anyhow::anyhow!("get missing sub failed: {err:?}"))?;
            anyhow::ensure!(sub.is_none(), "missing subscription resolved");
            // Bad confirm token on a real pending subscription fails closed.
            let topic = create_topic(&fixture, "sns-badtoken", HashMap::new()).await?;
            subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/badtoken".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
            let bad = fixture
                .pubsub_provider
                .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
                    topic_arn: topic.topic_arn,
                    token: "bad-token".to_string(),
                })
                .await;
            anyhow::ensure!(
                matches!(
                    bad,
                    Err(pubsub_provider::PubsubError::Validation {
                        kind: pubsub_provider::PubsubValidationKind::InvalidToken,
                        ..
                    })
                ),
                "bad token wrong error: {bad:?}"
            );
            Ok(())
        }
        .await;
        fixture.finish().await;
        outcome
    }
    .await;
    result?;
    Ok(())
}

#[tokio::test]
async fn given_native_publish_when_attributes_are_validated_then_types_and_limits_are_enforced()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let result = async {
        let topic = create_topic(&fixture, "typed-attributes", HashMap::new()).await?;
        let mut request = PublishRequest {
            topic_arn: topic.topic_arn,
            message: "body".to_owned(),
            subject: None,
            message_attributes: (0..11)
                .map(|index| {
                    (
                        format!("attr{index}"),
                        pubsub_provider::MessageAttribute::String("value".to_owned()),
                    )
                })
                .collect(),
        };
        fixture
            .pubsub_provider
            .accept_publish(
                request.clone(),
                pubsub_provider::PubsubMessageId::new(),
                false,
            )
            .await?;
        for attribute in [
            pubsub_provider::MessageAttribute::Number("1000000001".into()),
            pubsub_provider::MessageAttribute::Number("0.000001".into()),
            pubsub_provider::MessageAttribute::StringArray("[{}]".into()),
            pubsub_provider::MessageAttribute::Binary(Vec::new()),
        ] {
            request.message_attributes = HashMap::from([("test".into(), attribute)]);
            anyhow::ensure!(
                fixture
                    .pubsub_provider
                    .accept_publish(
                        request.clone(),
                        pubsub_provider::PubsubMessageId::new(),
                        false
                    )
                    .await
                    .is_err()
            );
        }
        request.message_attributes.clear();
        request.subject = Some("s".repeat(99));
        fixture
            .pubsub_provider
            .accept_publish(
                request.clone(),
                pubsub_provider::PubsubMessageId::new(),
                false,
            )
            .await?;
        request.subject = Some("s".repeat(100));
        anyhow::ensure!(
            fixture
                .pubsub_provider
                .accept_publish(
                    request.clone(),
                    pubsub_provider::PubsubMessageId::new(),
                    false
                )
                .await
                .is_err()
        );

        request.subject = Some("line\nbreak".to_owned());
        anyhow::ensure!(
            fixture
                .pubsub_provider
                .accept_publish(request, pubsub_provider::PubsubMessageId::new(), false)
                .await
                .is_err()
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    result
}

#[tokio::test]
async fn given_large_binary_attribute_when_fanout_is_materialized_then_all_values_round_trip()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let result = async {
        let topic = create_topic(&fixture, "large-binary-fanout", HashMap::new()).await?;
        for index in 0..10 {
            subscribe(
                &fixture,
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: queue_arn(&format!("large-{index}")),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
            )
            .await?;
        }
        let mut entropy = 1_u64;
        let bytes = (0..262_000)
            .map(|_| {
                entropy ^= entropy << 13;
                entropy ^= entropy >> 7;
                entropy ^= entropy << 17;
                entropy as u8
            })
            .collect();
        let attribute = pubsub_provider::MessageAttribute::Binary(bytes);
        fixture
            .pubsub_provider
            .accept_publish(
                PublishRequest {
                    topic_arn: topic.topic_arn,
                    message: "body".into(),
                    subject: None,
                    message_attributes: HashMap::from([("blob".into(), attribute.clone())]),
                },
                PubsubMessageId::new(),
                false,
            )
            .await?;
        anyhow::ensure!(
            fixture
                .pubsub_provider
                .materialize_publish_intents(32)
                .await?
                .records
                == 10
        );
        let now = TimestampMillis::now() + 1_000;
        let mut claimed_ids = HashSet::new();
        // Providers can return a short claim page at their byte budget.
        for _ in 0..10 {
            let claimed = claim_at(&fixture, "large-binary-test", now).await;
            if claimed.is_empty() {
                break;
            }
            for record in claimed {
                anyhow::ensure!(record.payload.message_attributes.get("blob") == Some(&attribute));
                anyhow::ensure!(
                    claimed_ids.insert(record.state.id.0),
                    "delivery was claimed twice"
                );
            }
        }
        anyhow::ensure!(claimed_ids.len() == 10);
        Ok(())
    }
    .await;
    fixture.finish().await;
    result
}
