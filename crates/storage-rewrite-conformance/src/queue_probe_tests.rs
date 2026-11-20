//! The read-only receive probe reports queue defaults and the earliest due
//! time through each provider's `read_next_due_time`.

use std::{collections::HashMap, time::Duration};

use queue_provider::{Queue, ReceiveProbe, SendMessageRequest, queue_message_for_send};
use storage_types::DurationSeconds;

use super::test_support::TestStore;

async fn queue(store: &TestStore, attributes: &[(&str, &str)]) -> String {
    let name = format!("probe_{}", uuid::Uuid::new_v4().simple());
    let url = format!("http://localhost/queue/{name}");
    store
        .queue_operations
        .create_queue(Queue {
            queue_name: name,
            queue_url: url.clone(),
            attributes: attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect::<HashMap<_, _>>(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue");
    url
}

async fn send(store: &TestStore, url: &str, delay_seconds: Option<u32>) {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: url.to_owned(),
                message_body: "probe".to_owned(),
                delay_seconds: delay_seconds.map(i64::from),
                message_attributes: None,
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message");
}

async fn probe(store: &TestStore, url: &str) -> Option<ReceiveProbe> {
    store
        .queue_operations
        .probe_receive(url)
        .await
        .expect("probe queue")
}

#[tokio::test]
async fn given_queue_states_when_probed_then_defaults_and_earliest_due_time_are_reported() {
    let store = TestStore::new().await;

    let missing = "http://localhost/queue/probe_missing";
    assert_eq!(probe(&store, missing).await, None);

    let empty = queue(&store, &[("ReceiveMessageWaitTimeSeconds", "7")]).await;
    assert_eq!(
        probe(&store, &empty).await,
        Some(ReceiveProbe {
            wait_time_seconds: 7,
            next_due_in: None,
        })
    );

    let due = queue(&store, &[]).await;
    send(&store, &due, Some(30)).await;
    send(&store, &due, None).await;
    assert_eq!(
        probe(&store, &due).await,
        Some(ReceiveProbe {
            wait_time_seconds: 0,
            next_due_in: Some(Duration::ZERO),
        })
    );

    let delayed = queue(&store, &[]).await;
    send(&store, &delayed, Some(30)).await;
    assert_eq!(
        probe(&store, &delayed)
            .await
            .and_then(|probe| probe.next_due_in),
        Some(Duration::from_secs(30))
    );
    store.advance_time(Duration::from_secs(10));
    assert_eq!(
        probe(&store, &delayed)
            .await
            .and_then(|probe| probe.next_due_in),
        Some(Duration::from_secs(20))
    );

    let in_flight = queue(&store, &[]).await;
    send(&store, &in_flight, None).await;
    let claimed = store
        .queue_operations
        .receive_messages(&in_flight, 1, Some(DurationSeconds::from(45)), None)
        .await
        .expect("claim message");
    assert_eq!(claimed.messages.len(), 1);
    // A leased message is due for a new claim only after the clock-skew
    // margin (W2 decision D14).
    assert_eq!(
        probe(&store, &in_flight)
            .await
            .and_then(|probe| probe.next_due_in),
        Some(
            Duration::from_secs(45)
                + Duration::from_millis(queue_provider::DEFAULT_MAX_CLOCK_SKEW_MILLIS as u64)
        )
    );
}

// W2-F1: a leased message inside the clock-skew margin is not due for this
// claim, and an empty claim still reports when it becomes due.
#[tokio::test]
async fn given_lease_inside_the_skew_margin_when_a_claim_is_empty_then_it_reports_the_guarded_lease()
 {
    let store = TestStore::new().await;
    let url = queue(&store, &[]).await;
    send(&store, &url, None).await;
    let lease = DurationSeconds::from(1);
    let held = store
        .queue_operations
        .receive_messages(&url, 1, Some(lease), None)
        .await
        .expect("claim message");
    assert_eq!(held.messages.len(), 1);

    store.advance_time(Duration::from_millis(1_100));
    let guarded = store
        .queue_operations
        .receive_messages(&url, 1, Some(lease), None)
        .await
        .expect("claim inside the margin");
    assert!(guarded.messages.is_empty(), "two holders inside one lease");
    assert!(guarded.drained, "no message is due for this claim");
    assert_eq!(guarded.next_due_in, Some(Duration::from_millis(400)));
    store.finish().await;
}
