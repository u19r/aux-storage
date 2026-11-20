//! The stale-delete attempt of the pending-subscription sweep (spec OWN-A-g
//! 5.4): only a pending subscription at least 48 hours old is deleted.

use std::{collections::HashMap, sync::Arc, time::Duration};

use pubsub_provider::{
    ConfirmSubscriptionRequest, CreateTopicRequest, PubsubError, PubsubProvider,
    PubsubValidationKind, SubscribeRequest, Subscription, SubscriptionArn, SubscriptionProtocol,
    Topic, TopicName, after,
};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, MessagingIdentity, TimestampMillis};

type Store = SqlStore<SqliteBackend>;

const HOUR: Duration = Duration::from_secs(3_600);

async fn sqlite_provider() -> Arc<Store> {
    let driver = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .expect("sqlite");
    Arc::new(
        SqlStore::initialize_schema(driver, storage_types::InitialStreamLineages::ONE, true)
            .await
            .expect("sqlite schema"),
    )
}

async fn delete_at(store: &Store, arn: &SubscriptionArn, now: TimestampMillis) -> bool {
    pubsub_provider::PubsubPort::run(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::BackgroundPage),
        Arc::new((arn.clone(), now)),
        |attempt, input, _| {
            Box::pin(pubsub_provider::delete_stale_subscription_in_attempt(
                attempt, &input.0, input.1,
            ))
        },
    )
    .await
    .unwrap()
}

async fn https_subscription(store: &Store, topic: &Topic, path: &str) -> Subscription {
    store
        .create_subscription(
            Subscription::try_new(
                SubscribeRequest {
                    topic_arn: topic.topic_arn.clone(),
                    protocol: SubscriptionProtocol::Https,
                    endpoint: format!("https://example.test/{path}"),
                    attributes: HashMap::new(),
                    extra_json: serde_json::Value::Null,
                },
                &MessagingIdentity::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn given_each_row_state_when_stale_delete_runs_then_only_a_stale_pending_row_is_deleted() {
    let store = sqlite_provider().await;
    let topic = store
        .create_topic(
            Topic::try_new(
                CreateTopicRequest {
                    name: TopicName::new("stale").unwrap(),
                    attributes: HashMap::new(),
                    tags: Default::default(),
                },
                &MessagingIdentity::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let pending = https_subscription(&store, &topic, "p").await;
    let confirmed = https_subscription(&store, &topic, "c").await;
    let confirmed_token = confirmed.confirmation.token().unwrap().to_string();
    store
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token: confirmed_token,
        })
        .await
        .unwrap();
    let created = pending.created_at;
    let pending_token = pending.confirmation.token().unwrap().to_string();

    // A missing row.
    let missing = SubscriptionArn::new(format!(
        "{}:00000000-0000-7000-8000-000000000000",
        topic.topic_arn
    ))
    .unwrap();
    assert!(!delete_at(&store, &missing, after(created, 49 * HOUR)).await);

    // A confirmed row, however old.
    assert!(
        !delete_at(
            &store,
            &confirmed.subscription_arn,
            after(confirmed.created_at, 49 * HOUR)
        )
        .await
    );
    assert!(
        store
            .get_subscription(&confirmed.subscription_arn)
            .await
            .unwrap()
            .is_some()
    );

    // A pending row one millisecond short of 48 hours.
    let just_before =
        TimestampMillis::from_timestamp(after(created, 48 * HOUR).timestamp_millis() - 1);
    assert!(!delete_at(&store, &pending.subscription_arn, just_before).await);
    let still = store
        .get_subscription(&pending.subscription_arn)
        .await
        .unwrap()
        .expect("young pending row stays");
    assert!(still.confirmation.pending_confirmation());

    // A pending row at 48 hours.
    let due = after(created, 48 * HOUR);
    assert!(delete_at(&store, &pending.subscription_arn, due).await);
    assert!(
        store
            .get_subscription(&pending.subscription_arn)
            .await
            .unwrap()
            .is_none()
    );
    let confirm = store
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token: pending_token,
        })
        .await;
    assert!(
        matches!(
            confirm,
            Err(PubsubError::Validation {
                kind: PubsubValidationKind::InvalidToken,
                ..
            })
        ),
        "the deleted row's token no longer confirms"
    );

    // Idempotent.
    assert!(!delete_at(&store, &pending.subscription_arn, due).await);
}
