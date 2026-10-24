use std::collections::{HashMap, HashSet};

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryClaimLimit, DeliveryRecord,
    DeliveryRecordId, DeliveryRecordKind, DeliveryRecordUpdate, DeliveryStatus, DeliveryTarget,
    GetSubscriptionAttributesRequest, GetTopicAttributesRequest, ListSubscriptionsRequest,
    ListTopicsRequest, PublishRequest, PubsubMessageId, SetSubscriptionAttributesRequest,
    SetTopicAttributesRequest, SubscribeRequest, SubscriptionArn, SubscriptionProtocol, TopicName,
};
use storage_types::TimestampMillis;

use super::test_support::TestStore;

fn fixed_time(millis: i64) -> TimestampMillis {
    TimestampMillis::from_timestamp(millis)
}

fn orders_subscription_arn() -> SubscriptionArn {
    SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:orders:sub").unwrap()
}

fn delivery_record(
    id: &str,
    status: DeliveryStatus,
    next_attempt_at: Option<TimestampMillis>,
    lease_expires_at: Option<TimestampMillis>,
) -> DeliveryRecord {
    let now = fixed_time(1_000);
    DeliveryRecord {
        id: DeliveryRecordId(id.to_string()),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new(),
        subscription_arn: orders_subscription_arn(),
        subscription: None,
        message_body: None,
        subject: None,
        message_attributes: Default::default(),
        target: DeliveryTarget::BuiltIn,
        status,
        attempts: 1,
        claim_generation: 0,
        next_attempt_at,
        lease_owner: lease_expires_at.map(|_| "worker-a".to_string()),
        lease_expires_at,
        last_error: None,
        created_at: now,
        updated_at: now,
    }
}

fn delivery_update(
    record: &DeliveryRecord,
    status: DeliveryStatus,
    updated_at: TimestampMillis,
) -> DeliveryRecordUpdate {
    DeliveryRecordUpdate {
        record_id: record.id.clone(),
        expected_claim_generation: record.claim_generation,
        expected_lease_owner: record.lease_owner.clone().expect("claimed delivery owner"),
        status,
        attempts: record.attempts.saturating_add(1),
        next_attempt_at: None,
        last_error: None,
        updated_at,
    }
}

async fn create_topic(
    fixture: &TestStore,
    name: &str,
    attributes: HashMap<String, String>,
) -> anyhow::Result<pubsub_provider::Topic> {
    fixture
        .pubsub_provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(name)?,
            attributes,
        })
        .await
        .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))
}

async fn create_queue_subscription(
    fixture: &TestStore,
    topic_arn: &pubsub_provider::TopicArn,
    endpoint: &str,
    attributes: HashMap<String, String>,
    extra_json: serde_json::Value,
) -> anyhow::Result<pubsub_provider::Subscription> {
    fixture
        .pubsub_provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: endpoint.to_string(),
            attributes,
            extra_json,
        })
        .await
        .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))
}

#[tokio::test]
async fn given_topic_when_publish_then_delivery_claimed() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = fixture
                .pubsub_provider
                .create_topic(CreateTopicRequest {
                    name: TopicName::new("orders")?,
                    attributes: HashMap::new(),
                })
                .await
                .map_err(|err| anyhow::anyhow!("create topic failed: {err:?}"))?;
            let subscription = fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Queue,
                    endpoint: "http://localhost/000000000000/orders".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
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
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
            anyhow::ensure!(
                materialized == 1,
                "expected one materialized publish, got {materialized}"
            );
            // Claim the actual delivery lease and check the accepted
            // publication, the subscription destination and the payload.
            let claimed = fixture
                .pubsub_provider
                .claim_delivery_records(pubsub_provider::ClaimDeliveryRecordsRequest {
                    owner: "conformance".to_string(),
                    now: storage_types::TimestampMillis::from_timestamp(1_800_000_000_000),
                    lease_expires_at: storage_types::TimestampMillis::from_timestamp(
                        1_800_006_000_000,
                    ),
                    limit: pubsub_provider::DeliveryClaimLimit::try_from(10usize)
                        .map_err(|err| anyhow::anyhow!("claim limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("claim failed: {err:?}"))?
                .records;
            anyhow::ensure!(
                claimed.len() == 1,
                "expected one claimed delivery, got {}",
                claimed.len()
            );
            let delivery = &claimed[0];
            anyhow::ensure!(
                delivery.subscription_arn == subscription.subscription_arn,
                "delivery targets the wrong subscription"
            );
            anyhow::ensure!(
                delivery.message_body.as_deref() == Some("body"),
                "delivery payload mismatch: {:?}",
                delivery.message_body
            );
            anyhow::ensure!(
                delivery.lease_owner.as_deref() == Some("conformance"),
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
            let subscription = fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.com/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::json!({"token_ref":"secret/oauth/orders"}),
                })
                .await
                .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
            let updated = fixture
                .pubsub_provider
                .set_subscription_attributes(SetSubscriptionAttributesRequest {
                    subscription_arn: subscription.subscription_arn.clone(),
                    attributes: HashMap::from([(
                        "RawMessageDelivery".to_string(),
                        "true".to_string(),
                    )]),
                })
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
            fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: short.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/short".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
                .await
                .map_err(|err| anyhow::anyhow!("short subscribe failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: long.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/long".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
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
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
            anyhow::ensure!(
                materialized == 1,
                "expected 1 materialized, got {materialized}"
            );
            let record = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    subscription.subscription_arn
                )))
                .await
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
            let confirmed = fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Http,
                    endpoint: "http://confirmed.example/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
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
            let pending = fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Http,
                    endpoint: "http://pending.example/hook".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
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
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
            anyhow::ensure!(
                materialized == 2,
                "expected 2 deliveries, got {materialized}"
            );
            let queue_record = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    queue.subscription_arn
                )))
                .await
                .map_err(|err| anyhow::anyhow!("queue delivery missing: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue delivery absent"))?;
            anyhow::ensure!(
                queue_record.target == DeliveryTarget::BuiltIn,
                "queue target wrong: {:?}",
                queue_record.target
            );
            let http_record = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    confirmed.subscription_arn
                )))
                .await
                .map_err(|err| anyhow::anyhow!("http delivery missing: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("confirmed delivery absent"))?;
            anyhow::ensure!(
                http_record.target == DeliveryTarget::CustomSender,
                "http target wrong: {:?}",
                http_record.target
            );
            let pending_record = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId(format!(
                    "{}:{message_id}",
                    pending.subscription_arn
                )))
                .await
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
            let claim_limit = DeliveryClaimLimit::try_from(10usize)
                .map_err(|err| anyhow::anyhow!("claim limit: {err:?}"))?;
            let claim_request = |owner: &str| ClaimDeliveryRecordsRequest {
                owner: owner.to_string(),
                now,
                lease_expires_at: TimestampMillis::from_timestamp(now.timestamp_millis() + 30_000),
                limit: claim_limit,
            };
            let before = fixture
                .pubsub_provider
                .claim_delivery_records(claim_request("before-materialize"))
                .await
                .map_err(|err| anyhow::anyhow!("claim before failed: {err:?}"))?
                .records;
            anyhow::ensure!(
                before.is_empty(),
                "intents claimable before materialization"
            );
            let materialized = fixture
                .pubsub_provider
                .materialize_publish_intents(10)
                .await
                .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
            anyhow::ensure!(materialized == 2, "expected 2, got {materialized}");
            let mut actual = fixture
                .pubsub_provider
                .claim_delivery_records(claim_request("after-materialize"))
                .await
                .map_err(|err| anyhow::anyhow!("claim after failed: {err:?}"))?
                .records
                .into_iter()
                .map(|record| record.id)
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

/// SNS-CLAIM-01: due records are claimed once in id order; future and
/// terminal records are filtered.
///
/// Ports `sqlite_pubsub_provider_claims_due_delivery_records_once` (order
/// + once) and `memory_tests.rs`
///   `claim_delivery_records_leases_due_records_only_once` (future + delivered
///   filtering).
#[tokio::test]
async fn given_deliveries_when_claimed_then_once_and_ordered() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let now = fixed_time(1_000);
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-due-b",
                    DeliveryStatus::Pending,
                    Some(now),
                    None,
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put due-b failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-due-a",
                    DeliveryStatus::Pending,
                    Some(now),
                    None,
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put due-a failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-future",
                    DeliveryStatus::Pending,
                    Some(fixed_time(3_000)),
                    None,
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put future failed: {err:?}"))?;
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-delivered",
                    DeliveryStatus::Delivered,
                    None,
                    None,
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put delivered failed: {err:?}"))?;
            let first = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-a".to_string(),
                    now,
                    lease_expires_at: fixed_time(2_000),
                    limit: DeliveryClaimLimit::try_from(10usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("first claim failed: {err:?}"))?
                .records;
            anyhow::ensure!(first.len() == 2, "expected 2 due, got {}", first.len());
            anyhow::ensure!(
                first[0].id.0 == "sns-due-a" && first[1].id.0 == "sns-due-b",
                "claim order wrong: {:?}",
                first.iter().map(|record| &record.id.0).collect::<Vec<_>>()
            );
            anyhow::ensure!(
                first
                    .iter()
                    .all(|record| record.lease_owner.as_deref() == Some("worker-a")),
                "claim owner wrong"
            );
            let second = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-b".to_string(),
                    now,
                    lease_expires_at: fixed_time(2_000),
                    limit: DeliveryClaimLimit::try_from(10usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("second claim failed: {err:?}"))?
                .records;
            anyhow::ensure!(second.is_empty(), "claimed records leaked to second worker");
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

/// SNS-CLAIM-02: competing workers never double-claim.
///
/// Ports `sorted_kv_pubsub_provider_competing_workers_never_double_claim`.
#[tokio::test]
async fn given_deliveries_when_claimed_concurrently_then_never_double_claimed() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let now = fixed_time(1_000);
            let mut records = Vec::new();
            for index in 0..10 {
                records.push(delivery_record(
                    &format!("sns-competing-{index}"),
                    DeliveryStatus::Pending,
                    Some(now),
                    None,
                ));
            }
            fixture
                .pubsub_provider
                .put_delivery_records(records)
                .await
                .map_err(|err| anyhow::anyhow!("put batch failed: {err:?}"))?;
            let first =
                fixture
                    .pubsub_provider
                    .claim_delivery_records(ClaimDeliveryRecordsRequest {
                        owner: "worker-a".to_string(),
                        now,
                        lease_expires_at: fixed_time(2_000),
                        limit: DeliveryClaimLimit::try_from(10usize)
                            .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                    });
            let second =
                fixture
                    .pubsub_provider
                    .claim_delivery_records(ClaimDeliveryRecordsRequest {
                        owner: "worker-b".to_string(),
                        now,
                        lease_expires_at: fixed_time(2_000),
                        limit: DeliveryClaimLimit::try_from(10usize)
                            .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                    });
            let (first, second) = tokio::join!(first, second);
            let first = first
                .map_err(|err| anyhow::anyhow!("first concurrent claim: {err:?}"))?
                .records;
            let second = second
                .map_err(|err| anyhow::anyhow!("second concurrent claim: {err:?}"))?
                .records;
            let unique = first
                .iter()
                .chain(&second)
                .map(|record| record.id.0.clone())
                .collect::<HashSet<_>>();
            anyhow::ensure!(
                first.len() + second.len() == 10,
                "concurrent claims lost work: {} + {}",
                first.len(),
                second.len()
            );
            anyhow::ensure!(unique.len() == 10, "concurrent claims duplicated");
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

/// SNS-LEASE-01: expired leases are recoverable by another worker.
///
/// Ports `sqlite_pubsub_provider_recovers_expired_delivery_leases`,
/// `sorted_kv_..._recovers_expired_delivery_leases` and
/// `memory_tests.rs` `claim_delivery_records_recovers_expired_leases`.
#[tokio::test]
async fn given_delivery_when_lease_expired_then_recoverable() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-expired",
                    DeliveryStatus::RetryScheduled,
                    Some(fixed_time(1_000)),
                    Some(fixed_time(1_500)),
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put expired failed: {err:?}"))?;
            let records = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-b".to_string(),
                    now: fixed_time(2_000),
                    lease_expires_at: fixed_time(3_000),
                    limit: DeliveryClaimLimit::try_from(10usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("recovery claim failed: {err:?}"))?
                .records;
            anyhow::ensure!(records.len() == 1, "expired lease not recovered");
            anyhow::ensure!(
                records[0].lease_owner.as_deref() == Some("worker-b"),
                "recovery owner wrong"
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

/// SNS-STALE-01 (negative): stale claims are rejected without
/// resurrection; payloads are immutable.
///
/// Ports `sqlite_delivery_update_rejects_stale_claim_without_resurrection`,
/// `sorted_kv_pubsub_provider_rejects_stale_and_aba_delivery_updates`
/// (stale half) and `memory_tests.rs`
/// `update_delivery_record_rejects_stale_claim_without_resurrection`.
/// Repeated puts use insert-if-absent semantics and must not overwrite
/// the active claim.
#[tokio::test]
async fn given_delivery_when_stale_claim_then_rejected_without_resurrection() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let mut record = delivery_record(
                "sns-stale-claim",
                DeliveryStatus::Pending,
                Some(fixed_time(1_000)),
                None,
            );
            record.message_body = Some("immutable-body".to_string());
            fixture
                .pubsub_provider
                .put_delivery_record(record.clone())
                .await
                .map_err(|err| anyhow::anyhow!("put failed: {err:?}"))?;
            let first = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-a".to_string(),
                    now: fixed_time(1_000),
                    lease_expires_at: fixed_time(2_000),
                    limit: DeliveryClaimLimit::try_from(1usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("first claim failed: {err:?}"))?
                .records
                .pop()
                .ok_or_else(|| anyhow::anyhow!("first claim empty"))?;
            anyhow::ensure!(first.claim_generation == 1, "first generation wrong");
            let second = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-b".to_string(),
                    now: fixed_time(2_000),
                    lease_expires_at: fixed_time(3_000),
                    limit: DeliveryClaimLimit::try_from(1usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("second claim failed: {err:?}"))?
                .records
                .pop()
                .ok_or_else(|| anyhow::anyhow!("second claim empty"))?;
            anyhow::ensure!(second.claim_generation == 2, "second generation wrong");
            // Repeated creation must not overwrite the active claim.
            let mut repeated = record;
            repeated.status = DeliveryStatus::Failed;
            fixture
                .pubsub_provider
                .put_delivery_record(repeated)
                .await
                .map_err(|err| anyhow::anyhow!("repeated put failed: {err:?}"))?;
            let stale = fixture
                .pubsub_provider
                .update_delivery_record(delivery_update(
                    &first,
                    DeliveryStatus::RetryScheduled,
                    fixed_time(2_001),
                ))
                .await;
            match stale {
                Err(pubsub_provider::PubsubError::DeliveryClaimLost { ref delivery_id })
                    if delivery_id == "sns-stale-claim" => {}
                Err(other) => anyhow::bail!("stale update wrong error: {other:?}"),
                Ok(()) => anyhow::bail!("stale update unexpectedly succeeded"),
            }
            let current = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId("sns-stale-claim".to_string()))
                .await
                .map_err(|err| anyhow::anyhow!("get current failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("current delivery missing"))?;
            anyhow::ensure!(
                current.lease_owner.as_deref() == Some("worker-b"),
                "stale update stole ownership"
            );
            anyhow::ensure!(
                current.message_body.as_deref() == Some("immutable-body"),
                "stale update mutated payload"
            );
            // The current claim can still complete.
            fixture
                .pubsub_provider
                .update_delivery_record(delivery_update(
                    &second,
                    DeliveryStatus::Delivered,
                    fixed_time(2_002),
                ))
                .await
                .map_err(|err| anyhow::anyhow!("current update failed: {err:?}"))?;
            let delivered = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId("sns-stale-claim".to_string()))
                .await
                .map_err(|err| anyhow::anyhow!("get delivered failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("delivered missing"))?;
            anyhow::ensure!(
                delivered.status == DeliveryStatus::Delivered,
                "current update lost: {:?}",
                delivered.status
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

/// SNS-ABA-01 (negative): same-worker ABA updates are rejected by
/// generation.
///
/// Ports `sqlite_delivery_update_generation_rejects_same_worker_aba` and
/// the ABA half of the KV/memory stale tests.
#[tokio::test]
async fn given_delivery_when_same_worker_aba_then_rejected() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            fixture
                .pubsub_provider
                .put_delivery_record(delivery_record(
                    "sns-same-worker-aba",
                    DeliveryStatus::Pending,
                    Some(fixed_time(1_000)),
                    None,
                ))
                .await
                .map_err(|err| anyhow::anyhow!("put failed: {err:?}"))?;
            let first = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-a".to_string(),
                    now: fixed_time(1_000),
                    lease_expires_at: fixed_time(2_000),
                    limit: DeliveryClaimLimit::try_from(1usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("first claim failed: {err:?}"))?
                .records
                .pop()
                .ok_or_else(|| anyhow::anyhow!("first claim empty"))?;
            let current = fixture
                .pubsub_provider
                .claim_delivery_records(ClaimDeliveryRecordsRequest {
                    owner: "worker-a".to_string(),
                    now: fixed_time(2_000),
                    lease_expires_at: fixed_time(3_000),
                    limit: DeliveryClaimLimit::try_from(1usize)
                        .map_err(|err| anyhow::anyhow!("limit: {err:?}"))?,
                })
                .await
                .map_err(|err| anyhow::anyhow!("second claim failed: {err:?}"))?
                .records
                .pop()
                .ok_or_else(|| anyhow::anyhow!("second claim empty"))?;
            anyhow::ensure!(
                first.claim_generation != current.claim_generation,
                "ABA generations did not advance"
            );
            let stale = fixture
                .pubsub_provider
                .update_delivery_record(delivery_update(
                    &first,
                    DeliveryStatus::RetryScheduled,
                    fixed_time(2_001),
                ))
                .await;
            anyhow::ensure!(
                matches!(
                    stale,
                    Err(pubsub_provider::PubsubError::DeliveryClaimLost { .. })
                ),
                "ABA stale update unexpectedly succeeded"
            );
            fixture
                .pubsub_provider
                .update_delivery_record(delivery_update(
                    &current,
                    DeliveryStatus::Delivered,
                    fixed_time(2_002),
                ))
                .await
                .map_err(|err| anyhow::anyhow!("current update failed: {err:?}"))?;
            let final_record = fixture
                .pubsub_provider
                .get_delivery_record(&DeliveryRecordId("sns-same-worker-aba".to_string()))
                .await
                .map_err(|err| anyhow::anyhow!("get final failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("final missing"))?;
            anyhow::ensure!(
                final_record.status == DeliveryStatus::Delivered,
                "ABA current update lost"
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

/// SNS-BATCH-01 (negative): one invalid delivery poisons the whole batch.
///
/// Ports `sqlite_delivery_batch_rolls_back_every_record_when_one_is_invalid`.
#[tokio::test]
async fn given_deliveries_when_batch_invalid_then_all_rollback() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let first = delivery_record("sns-batch-valid", DeliveryStatus::Pending, None, None);
            let mut invalid =
                delivery_record("sns-batch-invalid", DeliveryStatus::Pending, None, None);
            invalid.claim_generation = u64::MAX;
            let result = fixture
                .pubsub_provider
                .put_delivery_records(vec![first.clone(), invalid.clone()])
                .await;
            anyhow::ensure!(result.is_err(), "invalid batch unexpectedly succeeded");
            for id in [first.id, invalid.id] {
                let found = fixture
                    .pubsub_provider
                    .get_delivery_record(&id)
                    .await
                    .map_err(|err| anyhow::anyhow!("get after rollback failed: {err:?}"))?;
                anyhow::ensure!(found.is_none(), "batch rollback left record {}", id.0);
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

/// SNS-DELETE-01: accepted deliveries survive topic deletion.
///
/// Ports `sorted_kv_pubsub_provider_retains_accepted_deliveries_after_topic_delete`.
#[tokio::test]
async fn given_delivery_when_topic_deleted_then_accepted_preserved() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let topic = create_topic(&fixture, "sns-delete", HashMap::new()).await?;
            let subscription = fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/delete".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
                .await
                .map_err(|err| anyhow::anyhow!("subscribe failed: {err:?}"))?;
            let mut record = delivery_record("sns-retained", DeliveryStatus::Pending, None, None);
            // delivery_record defaults to Pending with no next-attempt; use an
            // explicit created-at helper shape instead of status juggling.
            record.subscription_arn = subscription.subscription_arn.clone();
            let expected = record.clone();
            fixture
                .pubsub_provider
                .put_delivery_record(record)
                .await
                .map_err(|err| anyhow::anyhow!("put failed: {err:?}"))?;
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
                .get_delivery_record(&expected.id)
                .await
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
                .set_subscription_attributes(SetSubscriptionAttributesRequest {
                    subscription_arn: subscriptions[0].subscription_arn.clone(),
                    attributes: HashMap::from([(
                        "RawMessageDelivery".to_string(),
                        "true".to_string(),
                    )]),
                })
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
                    .map_err(|err| anyhow::anyhow!("materialize failed: {err:?}"))?;
                total += advanced;
                if advanced == 0 {
                    break;
                }
            }
            anyhow::ensure!(total == 12, "expected 12 deliveries, got {total}");
            for (index, subscription) in subscriptions.iter().enumerate() {
                let record = fixture
                    .pubsub_provider
                    .get_delivery_record(&DeliveryRecordId(format!(
                        "{}:{message_id}",
                        subscription.subscription_arn
                    )))
                    .await
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
                    snapshot.endpoint == format!("chunk-queue-{index}"),
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
            fixture
                .pubsub_provider
                .create_subscription(SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: "https://example.test/badtoken".to_string(),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                })
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
