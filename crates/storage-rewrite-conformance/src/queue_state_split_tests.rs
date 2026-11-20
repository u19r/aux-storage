//! W7 message state split and catalog hint on the provider that
//! `STORAGE_TEST_BACKEND` selects: claims, visibility changes and deletes do
//! not rewrite or read the body, an expired id can be reused in one attempt,
//! checkpoints are bounded, and a stale catalog hint costs one retried attempt.
//!
//! The tests assert counts of durable bytes, chunk reads, attempts and waves,
//! never elapsed time.

use std::{collections::HashMap, time::Duration};

use queue_provider::{
    MESSAGE_ID_LEN, MessageId, Queue, QueueError, QueueValidationKind, ReceiptHandle,
    ReceivedMessages, SendMessageRequest, queue_message_for_send,
};
use storage_types::DurationSeconds;

use super::{
    dynamo_wave_census_tests::{DurableMeter, measure},
    native_census_test_support::{CensusCounts, NativeCensus},
    test_support::TestStore,
};

/// Deterministic alphanumeric body of `len` characters that does not compress.
fn incompressible_body(len: usize, seed: u64) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(ALPHABET[(state % 62) as usize])
        })
        .collect()
}

/// Durable bytes of one window stay far below a 256 KiB body: the body was not
/// rewritten.
fn assert_body_not_rewritten(label: &str, counts: &CensusCounts) {
    let durable = counts.durable;
    match TestStore::selected_backend().as_str() {
        "foundationdb" => assert!(durable.mutation_bytes < 16 * 1024, "{label}: {counts}"),
        "rocksdb" => assert!(durable.write_batch_bytes < 16 * 1024, "{label}: {counts}"),
        "postgres" => {
            assert!(durable.wal_bytes > 0, "{label}: WAL not measured: {counts}");
            assert!(durable.wal_bytes < 64 * 1024, "{label}: {counts}");
        }
        // SQLite and Turso rewrite the whole row record (accepted in W7-C).
        _ => {}
    }
}

/// A second store over one database exists only on shared database providers.
pub(crate) fn shared_database() -> bool {
    matches!(
        TestStore::selected_backend().as_str(),
        "foundationdb" | "postgres"
    )
}

async fn queue_with(store: &TestStore, label: &str, attributes: HashMap<String, String>) -> String {
    let name = format!("{label}_{}", uuid::Uuid::new_v4().simple());
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name,
            attributes,
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue")
        .queue_url
}

async fn queue(store: &TestStore, label: &str) -> String {
    queue_with(store, label, HashMap::new()).await
}

async fn send(store: &TestStore, queue_url: &str, body: &str) {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.to_owned(),
                message_body: body.to_owned(),
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message");
}

async fn receive(store: &TestStore, queue_url: &str, max: u32) -> ReceivedMessages {
    store
        .queue_operations
        .receive_messages(queue_url, max, Some(DurationSeconds::from(30)), None)
        .await
        .expect("receive")
}

#[tokio::test]
async fn given_256_kib_message_when_claimed_changed_and_deleted_then_the_body_is_not_rewritten() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&store, "split_body").await;
    let body = incompressible_body(262_144, 1);
    // `measure` runs each step twice (warm-up, then measured), so two messages
    // are sent and the receipts of the claims feed the visibility change and
    // the delete.
    send(&store, &queue_url, &body).await;
    send(&store, &queue_url, &body).await;
    let meter = DurableMeter::open(&store, &TestStore::selected_backend()).await;
    let receipts = std::sync::Mutex::new(Vec::<String>::new());
    let (store_ref, url_ref, body_ref, receipts_ref) = (&store, &queue_url, &body, &receipts);

    let claim = measure(&census, &meter, |_| async move {
        let received = receive(store_ref, url_ref, 1).await;
        assert_eq!(received.messages.len(), 1);
        assert_eq!(received.messages[0].body, *body_ref);
        receipts_ref
            .lock()
            .expect("receipts")
            .push(received.messages[0].receipt_handle.clone());
    })
    .await;

    let change = measure(&census, &meter, |version| async move {
        let receipt = receipts_ref.lock().expect("receipts")[usize::from(version)].clone();
        store_ref
            .queue_operations
            .change_message_visibility(url_ref, ReceiptHandle(receipt), DurationSeconds::from(60))
            .await
            .expect("change visibility");
    })
    .await;
    let change_chunks = census.chunk_key_reads();

    let delete = measure(&census, &meter, |version| async move {
        let receipt = receipts_ref.lock().expect("receipts")[usize::from(version)].clone();
        store_ref
            .queue_operations
            .delete_message(url_ref, ReceiptHandle(receipt))
            .await
            .expect("delete");
    })
    .await;
    let delete_chunks = census.chunk_key_reads();

    eprintln!("split claim: {claim}");
    eprintln!("split change: {change} chunk_reads={change_chunks}");
    eprintln!("split delete: {delete} chunk_reads={delete_chunks}");
    assert_body_not_rewritten("claim", &claim);
    assert_body_not_rewritten("change", &change);
    assert_body_not_rewritten("delete", &delete);
    if TestStore::selected_backend() == "foundationdb" {
        assert_eq!(change_chunks, 0, "visibility change read the body");
        assert_eq!(delete_chunks, 0, "delete read the body");
    }
    store.finish().await;
}

#[tokio::test]
async fn given_expired_row_when_a_publication_reuses_its_id_then_the_new_body_is_stored() {
    let store = TestStore::new().await;
    let queue_url = queue_with(
        &store,
        "split_reuse",
        HashMap::from([("MessageRetentionPeriod".to_owned(), "60".to_owned())]),
    )
    .await;
    let mut message = queue_message_for_send(
        SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body: "old".into(),
            ..Default::default()
        },
        store.now(),
    );
    message.message_id = MessageId::from_bytes([7; MESSAGE_ID_LEN]);
    store
        .queue_operations
        .publish_message(message.clone())
        .await
        .expect("first publication");

    store.advance_time(Duration::from_secs(61));
    let mut again = queue_message_for_send(
        SendMessageRequest {
            queue_url: queue_url.clone(),
            message_body: "old".into(),
            ..Default::default()
        },
        store.now(),
    );
    again.message_id = message.message_id;
    store
        .queue_operations
        .publish_message(again)
        .await
        .expect("publication of the expired id");

    let received = receive(&store, &queue_url, 10).await;
    let bodies = received
        .messages
        .iter()
        .map(|message| message.body.as_str())
        .collect::<Vec<_>>();
    // the claim deletes the expired first row, so a received message is the
    // second insert
    assert_eq!(bodies, ["old"]);
    store.finish().await;
}

#[tokio::test]
async fn given_checkpoint_over_64_kib_when_updated_then_invalid_parameter() {
    let store = TestStore::new().await;
    let queue_url = queue(&store, "split_checkpoint").await;
    send(&store, &queue_url, "checkpointed").await;
    let received = receive(&store, &queue_url, 1).await;
    let receipt = received.messages[0].receipt_handle.clone();

    let error = store
        .queue_operations
        .update_message_snapshot_checkpoint(
            &queue_url,
            ReceiptHandle(receipt.clone()),
            "c".repeat(65_537),
        )
        .await
        .expect_err("65,537 bytes exceed the bound");
    assert!(
        matches!(
            error,
            QueueError::Validation {
                kind: QueueValidationKind::InvalidParameterValue,
                ..
            }
        ),
        "{error:?}"
    );

    store
        .queue_operations
        .update_message_snapshot_checkpoint(
            &queue_url,
            ReceiptHandle(receipt.clone()),
            "c".repeat(65_536),
        )
        .await
        .expect("65,536 bytes fit");
    store
        .queue_operations
        .change_message_visibility(&queue_url, ReceiptHandle(receipt), DurationSeconds::from(0))
        .await
        .expect("make due");
    let again = receive(&store, &queue_url, 1).await;
    assert_eq!(again.messages.len(), 1, "the 64 KiB state is readable");
    assert_eq!(again.messages[0].body, "checkpointed");
    store.finish().await;
}

#[tokio::test]
async fn given_checkpoint_under_64_kib_but_over_96_kib_escaped_when_updated_then_invalid_parameter()
{
    let store = TestStore::new().await;
    let queue_url = queue(&store, "split_checkpoint_escaped").await;
    send(&store, &queue_url, "checkpointed").await;
    let received = receive(&store, &queue_url, 1).await;
    let receipt = received.messages[0].receipt_handle.clone();

    // 60,000 bytes of input; every quote escapes to two bytes, so the JSON form
    // is 120,002 bytes.
    let error = store
        .queue_operations
        .update_message_snapshot_checkpoint(&queue_url, ReceiptHandle(receipt), "\"".repeat(60_000))
        .await
        .expect_err("the escaped JSON form exceeds 96 KiB");
    assert!(
        matches!(
            error,
            QueueError::Validation {
                kind: QueueValidationKind::InvalidParameterValue,
                ..
            }
        ),
        "{error:?}"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_hinted_queue_when_another_node_purges_then_the_next_receive_retries_once_and_reads_the_new_incarnation()
 {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            TestStore::selected_backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&a, "split_purge").await;
    send(&a, &queue_url, "m1").await;
    assert_eq!(receive(&a, &queue_url, 1).await.messages.len(), 1);

    let b = a.open_same_database().await;
    b.queue_operations
        .purge_queue(&queue_url)
        .await
        .expect("purge");
    b.queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "m2".to_owned(),
                ..Default::default()
            },
            a.now(),
        ))
        .await
        .expect("send m2");

    census.reset();
    let received = receive(&a, &queue_url, 10).await;
    let bodies = received
        .messages
        .iter()
        .map(|message| message.body.as_str())
        .collect::<Vec<_>>();
    assert_eq!(bodies, ["m2"]);
    let counts = census.snapshot();
    assert_eq!(
        counts.begins, 2,
        "the hinted attempt fails the check and one retry reads the new row: {counts}"
    );
    if TestStore::selected_backend() == "foundationdb" {
        // W7-H: the failed try reads the read version and one merged
        // due/catalog scan; the moved row refreshes the hint, so the retry is
        // hinted (read version, scan, state+body get, commit).
        assert_eq!(counts.waves, 6, "{counts}");
        assert_eq!(counts.commits, 1, "{counts}");
    }
    drop(b);
    a.finish().await;
}

/// With the stale hint every entry fails validation, no insert runs, and only
/// the end-of-attempt catalog check turns the attempt into a retry with the new
/// limit.
#[tokio::test]
async fn given_hinted_queue_when_another_node_raises_the_size_limit_then_a_larger_send_succeeds() {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            TestStore::selected_backend()
        );
        return;
    }
    let a = TestStore::new().await;
    let queue_url = queue_with(
        &a,
        "split_size",
        HashMap::from([("MaximumMessageSize".to_owned(), "1024".to_owned())]),
    )
    .await;
    send(&a, &queue_url, "warm").await;
    assert_eq!(receive(&a, &queue_url, 1).await.messages.len(), 1);

    let b = a.open_same_database().await;
    b.queue_operations
        .set_queue_attributes(
            &queue_url,
            HashMap::from([("MaximumMessageSize".to_owned(), "2048".to_owned())]),
        )
        .await
        .expect("raise the size limit");

    let body = incompressible_body(1_500, 3);
    a.queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: body.clone(),
                ..Default::default()
            },
            a.now(),
        ))
        .await
        .expect("the larger send succeeds under the new limit");
    let received = receive(&a, &queue_url, 10).await;
    assert!(received.messages.iter().any(|message| message.body == body));
    drop(b);
    a.finish().await;
}

#[tokio::test]
async fn given_hinted_queue_when_another_node_changes_the_visibility_timeout_then_a_visibility_change_retries_once_hinted()
 {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            TestStore::selected_backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&a, "split_visibility").await;
    send(&a, &queue_url, "m1").await;
    let received = receive(&a, &queue_url, 1).await;
    let receipt = received.messages[0].receipt_handle.clone();

    let b = a.open_same_database().await;
    b.queue_operations
        .set_queue_attributes(
            &queue_url,
            HashMap::from([("VisibilityTimeout".to_owned(), "45".to_owned())]),
        )
        .await
        .expect("set visibility timeout");

    census.reset();
    a.queue_operations
        .change_message_visibility(
            &queue_url,
            ReceiptHandle(receipt),
            DurationSeconds::from(60),
        )
        .await
        .expect("change visibility under the moved row");
    let counts = census.snapshot();
    assert_eq!(
        counts.begins, 2,
        "the hinted try fails the merged check, one retry: {counts}"
    );
    if TestStore::selected_backend() == "foundationdb" {
        // Failed try: read version + mixed get[receipt, catalog]. Retry with
        // the refreshed hint: read version, mixed get, state get, commit.
        assert_eq!(counts.waves, 6, "{counts}");
        assert_eq!(counts.commits, 1, "{counts}");
    }
    drop(b);
    a.finish().await;
}

#[tokio::test]
async fn given_hinted_queue_when_another_node_sets_a_delay_then_a_send_uses_it() {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            TestStore::selected_backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&a, "split_delay").await;
    send(&a, &queue_url, "warm").await;
    assert_eq!(receive(&a, &queue_url, 1).await.messages.len(), 1);

    let b = a.open_same_database().await;
    b.queue_operations
        .set_queue_attributes(
            &queue_url,
            HashMap::from([("DelaySeconds".to_owned(), "60".to_owned())]),
        )
        .await
        .expect("set delay");

    census.reset();
    send(&a, &queue_url, "late").await;
    let counts = census.snapshot();
    assert_eq!(
        counts.begins, 2,
        "the hinted send fails the merged check, one retry: {counts}"
    );
    if TestStore::selected_backend() == "foundationdb" {
        // Failed try: read version + mixed get[state presence, catalog].
        // Retry with the refreshed hint: read version, mixed get, commit.
        assert_eq!(counts.waves, 5, "{counts}");
        assert_eq!(counts.commits, 1, "{counts}");
    }
    let received = receive(&a, &queue_url, 10).await;
    assert!(
        received
            .messages
            .iter()
            .all(|message| message.body != "late"),
        "the send used the stale delay of 0"
    );
    drop(b);
    a.finish().await;
}

#[tokio::test]
async fn given_hinted_queue_when_received_then_it_uses_one_wave_fewer() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&store, "split_waves").await;
    let messages = ["m0", "m1"]
        .map(|body| {
            queue_message_for_send(
                SendMessageRequest {
                    queue_url: queue_url.clone(),
                    message_body: body.to_owned(),
                    ..Default::default()
                },
                store.now(),
            )
        })
        .to_vec();
    for result in store
        .queue_operations
        .send_messages(messages)
        .await
        .expect("send batch")
    {
        result.expect("send entry");
    }
    store.advance_time(Duration::from_secs(6));

    census.reset();
    assert_eq!(receive(&store, &queue_url, 1).await.messages.len(), 1);
    let cold = census.snapshot();
    census.reset();
    assert_eq!(receive(&store, &queue_url, 1).await.messages.len(), 1);
    let hinted = census.snapshot();

    eprintln!("split waves cold: {cold}");
    eprintln!("split waves hinted: {hinted}");
    match TestStore::selected_backend().as_str() {
        // W7-H: read version, scan[due window, due after, catalog row],
        // state+body get, commit.
        "foundationdb" => {
            assert_eq!(
                (cold.waves, cold.keys, cold.ranges),
                (5, 3, 2),
                "cold: {cold}"
            );
            assert_eq!(
                (hinted.waves, hinted.keys, hinted.ranges),
                (4, 2, 3),
                "hinted: {hinted}"
            );
            assert_eq!(
                hinted.conflicts.read_keys, cold.conflicts.read_keys,
                "{cold} / {hinted}"
            );
            assert_eq!(hinted.rows_read, cold.rows_read, "{cold} / {hinted}");
        }
        "rocksdb" => {
            assert_eq!(hinted.waves + 1, cold.waves, "{cold} / {hinted}");
            assert_eq!(hinted.keys + 1, cold.keys, "{cold} / {hinted}");
            assert_eq!(hinted.ranges, cold.ranges + 1, "{cold} / {hinted}");
        }
        "postgres" => assert_eq!(hinted.waves + 1, cold.waves, "{cold} / {hinted}"),
        _ => {}
    }
    store.finish().await;
}

/// A send under a purged incarnation lands in the new one after one retry, and
/// on a queue another node deleted the hint is removed so one cold retry
/// answers "no queue" without looping.
#[tokio::test]
async fn given_hinted_queue_when_another_node_purges_then_deletes_then_sends_retry_once() {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            TestStore::selected_backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new_with_census(census.clone()).await;
    let queue_url = queue(&a, "split_purge_delete").await;
    send(&a, &queue_url, "warm").await;
    assert_eq!(receive(&a, &queue_url, 1).await.messages.len(), 1);

    let b = a.open_same_database().await;
    b.queue_operations
        .purge_queue(&queue_url)
        .await
        .expect("purge");

    census.reset();
    send(&a, &queue_url, "late").await;
    let counts = census.snapshot();
    assert_eq!(
        counts.begins, 2,
        "the hinted send fails the incarnation check, one retry: {counts}"
    );
    let bodies = receive(&a, &queue_url, 10)
        .await
        .messages
        .into_iter()
        .map(|message| message.body)
        .collect::<Vec<_>>();
    assert_eq!(bodies, ["late"], "the send landed in the new incarnation");

    b.queue_operations
        .delete_queue(&queue_url)
        .await
        .expect("delete");
    census.reset();
    let sent = a
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "gone".to_owned(),
                ..Default::default()
            },
            a.now(),
        ))
        .await;
    assert!(sent.is_err(), "send to a deleted queue");
    // The absent catalog row removes the hint: one cold retry finds no queue;
    // no loop.
    let counts = census.snapshot();
    assert_eq!(counts.begins, 2, "{counts}");
    drop(b);
    a.finish().await;
}

/// W7 A-C: the state of a message is its own row, so one visibility change of
/// ten 1 MiB messages stages no body and stays inside the 10 MB transaction
/// limit that the combined rows once exceeded (CMVB).
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=foundationdb"]
async fn given_ten_1_mib_messages_when_change_visibility_batch_runs_then_all_ten_entries_succeed() {
    /// The most receives that gather ten messages (a response may hold fewer).
    const MAX_RECEIVES: usize = 20;
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let store = TestStore::new().await;
    let queue_url = queue(&store, "split_cmvb").await;
    let bodies = (0..10_u64)
        .map(|seed| incompressible_body(1_048_576, seed + 1))
        .collect::<Vec<_>>();
    for body in &bodies {
        send(&store, &queue_url, body).await;
    }

    let mut received = Vec::new();
    for _ in 0..MAX_RECEIVES {
        if received.len() == bodies.len() {
            break;
        }
        received.extend(receive(&store, &queue_url, 10).await.messages);
    }
    assert_eq!(
        received.len(),
        bodies.len(),
        "all ten messages are received"
    );
    let mut received_bodies = received
        .iter()
        .map(|message| message.body.clone())
        .collect::<Vec<_>>();
    let mut sent_bodies = bodies.clone();
    received_bodies.sort();
    sent_bodies.sort();
    assert!(received_bodies == sent_bodies, "every body round-trips");

    let entries = received
        .iter()
        .map(|message| {
            (
                ReceiptHandle(message.receipt_handle.clone()),
                DurationSeconds::from(60),
            )
        })
        .collect::<Vec<_>>();
    let results = store
        .queue_operations
        .change_message_visibilities(&queue_url, entries)
        .await
        .expect("change visibility batch");
    assert_eq!(results.len(), 10);
    for (index, result) in results.into_iter().enumerate() {
        result.unwrap_or_else(|error| panic!("entry {index} failed: {error}"));
    }
    assert!(
        receive(&store, &queue_url, 10).await.messages.is_empty(),
        "the raised timeouts keep all ten in flight"
    );
    store.finish().await;
}
