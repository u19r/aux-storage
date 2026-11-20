//! The stale pending-subscription sweep (spec OWN-A-g 5.9) on the provider
//! that `STORAGE_TEST_BACKEND` selects. The store clock starts at wall time
//! and moves only by `advance_time`; `created_at` is wall time at
//! `Subscribe`, so 47 hours is young and 49 hours is stale.

use std::{collections::HashMap, num::NonZeroU32, time::Duration};

use pubsub_provider::{
    ConfirmSubscriptionRequest, ListSubscriptionsRequest, PubsubError, PubsubValidationKind,
    STALE_SUBSCRIPTION_PAGE, SubscribeRequest, Subscription, SubscriptionArn, SubscriptionProtocol,
    TopicArn,
};

use super::{
    pubsub_contract_tests::{create_topic, subscribe},
    pubsub_delivery_contract_tests::{https_subscription, sqs_subscription, unique},
    queue_claim_tests::probed_store,
    queue_state_split_tests::shared_database,
    test_support::TestStore,
};

const HOUR: Duration = Duration::from_secs(3_600);

async fn pending(store: &TestStore, topic: &TopicArn) -> Subscription {
    subscribe(
        store,
        SubscribeRequest {
            topic_arn: topic.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: format!("https://example.test/{}", unique("stale")),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        },
    )
    .await
    .unwrap()
}

/// Every turn until the last page; (turns, deleted). Other tests' rows may
/// share a database (FoundationDB, Postgres), so callers assert on their own
/// rows.
async fn sweep_all(store: &TestStore, limit: NonZeroU32) -> (usize, usize) {
    let (mut cursor, mut turns, mut deleted) = (None, 0, 0);
    loop {
        let sweep = store
            .pubsub_provider
            .sweep_stale_subscriptions(cursor, limit)
            .await
            .unwrap();
        turns += 1;
        deleted += sweep.deleted;
        assert!(turns < 10_000, "the sweep did not reach its last page");
        match sweep.next {
            Some(next) => cursor = Some(next),
            None => return (turns, deleted),
        }
    }
}

async fn is_present(store: &TestStore, arn: &SubscriptionArn) -> bool {
    store
        .pubsub_provider
        .get_subscription(arn)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn given_pending_subscription_47h_old_when_swept_then_it_stays_pending() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("stale-young"), HashMap::new())
        .await
        .unwrap();
    let young = pending(&store, &topic.topic_arn).await;

    store.advance_time(47 * HOUR);
    sweep_all(&store, STALE_SUBSCRIPTION_PAGE).await;

    let row = store
        .pubsub_provider
        .get_subscription(&young.subscription_arn)
        .await
        .unwrap()
        .expect("a 47 hour old pending subscription stays");
    assert!(row.confirmation.pending_confirmation());
    store.finish().await;
}

#[tokio::test]
async fn given_pending_subscription_49h_old_when_swept_then_it_and_its_token_are_gone() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("stale-old"), HashMap::new())
        .await
        .unwrap();
    let old = pending(&store, &topic.topic_arn).await;
    let token = old.confirmation.token().unwrap().to_owned();

    store.advance_time(49 * HOUR);
    sweep_all(&store, STALE_SUBSCRIPTION_PAGE).await;

    assert!(!is_present(&store, &old.subscription_arn).await);
    let listed = store
        .pubsub_provider
        .list_subscriptions(ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn.clone()),
            next_token: None,
        })
        .await
        .unwrap();
    assert!(
        listed
            .subscriptions
            .iter()
            .all(|row| row.subscription_arn != old.subscription_arn),
        "the swept subscription is still listed"
    );
    let confirm = store
        .pubsub_provider
        .confirm_subscription(ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token,
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
        "the swept subscription's token still confirms"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_confirmed_and_queue_subscriptions_when_49h_pass_then_they_stay() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("stale-kept"), HashMap::new())
        .await
        .unwrap();
    let confirmed = https_subscription(&store, &topic.topic_arn, &[]).await;
    let queued = sqs_subscription(&store, &topic.topic_arn, &unique("q"), &[]).await;

    store.advance_time(49 * HOUR);
    sweep_all(&store, STALE_SUBSCRIPTION_PAGE).await;

    assert!(is_present(&store, &confirmed.subscription_arn).await);
    assert!(is_present(&store, &queued.subscription_arn).await);
    store.finish().await;
}

#[tokio::test]
async fn given_five_stale_pending_when_swept_two_per_page_then_each_is_deleted_across_pages() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("stale-pages"), HashMap::new())
        .await
        .unwrap();
    let mut arns = Vec::new();
    for _ in 0..5 {
        arns.push(pending(&store, &topic.topic_arn).await.subscription_arn);
    }

    store.advance_time(49 * HOUR);
    let (turns, deleted) = sweep_all(&store, NonZeroU32::new(2).unwrap()).await;

    for arn in &arns {
        assert!(!is_present(&store, arn).await, "a stale row survived");
    }
    if !shared_database() {
        assert_eq!(turns, 3, "pages of 2, 2 and 1 rows");
        assert_eq!(deleted, 5);
    }
    store.finish().await;
}

#[tokio::test]
async fn given_topic_deleted_with_pending_subscription_when_49h_pass_then_the_orphan_is_deleted() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("stale-orphan"), HashMap::new())
        .await
        .unwrap();
    let orphan = pending(&store, &topic.topic_arn).await;
    store
        .pubsub_provider
        .delete_topic(&topic.topic_arn)
        .await
        .unwrap();

    store.advance_time(49 * HOUR);
    let (_, deleted) = sweep_all(&store, STALE_SUBSCRIPTION_PAGE).await;

    assert!(!is_present(&store, &orphan.subscription_arn).await);
    if shared_database() {
        assert!(deleted >= 1);
    } else {
        assert_eq!(deleted, 1);
    }
    store.finish().await;
}

#[tokio::test]
async fn given_two_stores_on_one_database_when_both_sweep_at_once_then_the_row_is_deleted_once_without_error()
 {
    if !shared_database() {
        eprintln!("skipped: the selected provider has one database per store");
        return;
    }
    let a = TestStore::new().await;
    let b = TestStore::new().await;
    let topic = create_topic(&a, &unique("stale-two"), HashMap::new())
        .await
        .unwrap();
    let mut arns = Vec::new();
    for _ in 0..3 {
        arns.push(pending(&a, &topic.topic_arn).await.subscription_arn);
    }

    a.advance_time(49 * HOUR);
    b.advance_time(49 * HOUR);
    let page = STALE_SUBSCRIPTION_PAGE;
    let ((_, deleted_a), (_, deleted_b)) = tokio::join!(sweep_all(&a, page), sweep_all(&b, page));

    eprintln!("deleted by store a: {deleted_a}, by store b: {deleted_b}");
    for arn in &arns {
        assert!(
            !is_present(&a, arn).await,
            "store a still reads a stale row"
        );
        assert!(
            !is_present(&b, arn).await,
            "store b still reads a stale row"
        );
    }
    a.finish().await;
    b.finish().await;
}

#[tokio::test]
async fn given_nothing_stale_when_one_turn_runs_then_one_read_and_no_write() {
    let (store, probe) = probed_store().await;
    // Rows that other runs left stale on a shared database are removed first.
    sweep_all(&store, STALE_SUBSCRIPTION_PAGE).await;
    let topic = create_topic(&store, &unique("stale-census"), HashMap::new())
        .await
        .unwrap();
    pending(&store, &topic.topic_arn).await;

    probe.reset();
    let sweep = store
        .pubsub_provider
        .sweep_stale_subscriptions(None, STALE_SUBSCRIPTION_PAGE)
        .await
        .unwrap();

    eprintln!(
        "stale sweep census: calls {} mutations {} commits {}",
        probe.calls(),
        probe.mutations(),
        probe.commits()
    );
    assert_eq!(probe.calls(), 1, "one read wave or one statement");
    assert_eq!(probe.mutations(), 0);
    assert_eq!(sweep.deleted, 0);
    if !shared_database() {
        assert!(sweep.next.is_none());
    }
    store.finish().await;
}
