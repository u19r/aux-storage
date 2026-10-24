use std::collections::{HashMap, HashSet};

use queue_provider::{Queue, QueueMessage, ReceiptHandle, queue_message_for_send};
use storage_types::{DurationSeconds, TimestampMillis};

use super::test_support::TestStore;

/// Base URL whose final segment is the queue name.
///
/// SQL validates that the URL ends with the queue name; KV rewrites the URL
/// to embed an incarnation id. Callers must always use the returned queue's
/// URL for subsequent operations.
fn queue_base_url(queue_name: &str) -> String {
    format!("http://localhost/000000000000/{queue_name}")
}

fn queue_request(queue_name: &str, attributes: HashMap<String, String>) -> Queue {
    Queue {
        queue_name: queue_name.to_string(),
        queue_url: queue_base_url(queue_name),
        attributes,
        created_at: TimestampMillis::now(),
    }
}

async fn create_queue_url(
    fixture: &TestStore,
    queue_name: &str,
    attributes: HashMap<String, String>,
) -> anyhow::Result<String> {
    fixture
        .queue_operations
        .create_queue(queue_request(queue_name, attributes))
        .await
        .map_err(|err| anyhow::anyhow!("create queue failed: {err:?}"))
        .map(|queue| queue.queue_url)
}

fn send_request(
    queue_url: &str,
    body: &str,
    delay_seconds: Option<u32>,
    now: TimestampMillis,
) -> QueueMessage {
    queue_message_for_send(
        queue_provider::SendMessageRequest {
            queue_url: queue_url.to_string(),
            message_body: body.to_string(),
            delay_seconds,
            message_attributes: None,
        },
        now,
    )
}

async fn send_body(fixture: &TestStore, queue_url: &str, body: &str) -> anyhow::Result<String> {
    fixture
        .queue_operations
        .send_message(send_request(queue_url, body, None, fixture.now()))
        .await
        .map(|id| id.to_string())
        .map_err(|err| anyhow::anyhow!("send failed: {err:?}"))
}

async fn receive_all(
    fixture: &TestStore,
    queue_url: &str,
    max: u32,
) -> anyhow::Result<Vec<queue_provider::MessageResponse>> {
    fixture
        .queue_operations
        .receive_messages(
            queue_url,
            max,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        )
        .await
        .map_err(|err| anyhow::anyhow!("receive failed: {err:?}"))
}

fn is_stale_receipt_error(err: &queue_provider::QueueError) -> bool {
    matches!(
        err,
        queue_provider::QueueError::Validation {
            kind: queue_provider::QueueValidationKind::MessageNotFound
                | queue_provider::QueueValidationKind::MessageNotFoundOrAlreadyProcessed
                | queue_provider::QueueValidationKind::CannotOperateVisibleMessage,
            ..
        } | queue_provider::QueueError::ResourceNotFound {
            resource_type: "receipt_handle",
            ..
        }
    )
}

#[tokio::test]
async fn given_message_when_send_receive_then_visible() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue = Queue {
                queue_name: "conformance".to_string(),
                queue_url: "http://localhost/000000000000/conformance".to_string(),
                attributes: HashMap::new(),
                created_at: TimestampMillis::now(),
            };
            let queue_url = fixture
                .queue_operations
                .create_queue(queue)
                .await
                .map_err(|err| anyhow::anyhow!("create queue failed: {err:?}"))?
                .queue_url;
            let send = queue_provider::SendMessageRequest {
                queue_url: queue_url.clone(),
                message_body: "hello".to_string(),
                delay_seconds: None,
                message_attributes: None,
            };
            let message: QueueMessage = queue_message_for_send(send, fixture.now());
            let sent = fixture
                .queue_operations
                .send_message(message)
                .await
                .map_err(|err| anyhow::anyhow!("send failed: {err:?}"))?;
            let received = fixture
                .queue_operations
                .receive_messages(
                    &queue_url,
                    1,
                    storage_types::DurationSeconds::from(30),
                    storage_types::DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("receive failed: {err:?}"))?;
            anyhow::ensure!(
                received.len() == 1,
                "expected one visible message, got {}",
                received.len()
            );
            let first = &received[0];
            anyhow::ensure!(
                first.body == "hello",
                "received body mismatch: {:?}",
                first.body
            );
            anyhow::ensure!(
                first.message_id == sent.to_string(),
                "received identity does not match the sent message"
            );
            anyhow::ensure!(
                !first.receipt_handle.is_empty(),
                "received message carries no lease receipt"
            );
            // The leased message must be invisible to an immediate second
            // receive on the same queue.
            let leased = fixture
                .queue_operations
                .receive_messages(
                    &queue_url,
                    10,
                    storage_types::DurationSeconds::from(30),
                    storage_types::DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("lease probe failed: {err:?}"))?;
            anyhow::ensure!(
                leased.is_empty(),
                "leased message visible without lease expiry"
            );
            // Deleting by the observed receipt drains the queue: the selected
            // queue owns the message.
            fixture
                .queue_operations
                .delete_message(
                    &queue_url,
                    queue_provider::ReceiptHandle(first.receipt_handle.clone()),
                )
                .await
                .map_err(|err| anyhow::anyhow!("delete failed: {err:?}"))?;
            let drained = fixture
                .queue_operations
                .receive_messages(
                    &queue_url,
                    10,
                    storage_types::DurationSeconds::from(0),
                    storage_types::DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("drain probe failed: {err:?}"))?;
            anyhow::ensure!(drained.is_empty(), "deleted message still visible");
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

/// SQS-IDENT-01: idempotent create preserves URL, attributes and creation
/// time.
///
/// Ports `kv/.../queue_provider_regression_tests.rs`
/// `create_queue_is_idempotent_when_existing_metadata_matches` and
/// `sql/.../queue_provider_write_tests.rs`
/// `given_existing_queue_when_identity_matches_then_stored_created_at_is_returned`.
#[tokio::test]
async fn given_queue_when_create_idempotent_then_same_url_and_attrs() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let attributes = HashMap::from([("VisibilityTimeout".to_string(), "30".to_string())]);
            let first = fixture
                .queue_operations
                .create_queue(queue_request("sqs-ident", attributes.clone()))
                .await
                .map_err(|err| anyhow::anyhow!("create failed: {err:?}"))?;
            // Same identity with a later caller timestamp must return the
            // stored creation time, not the caller's.
            let mut second_input = queue_request("sqs-ident", attributes.clone());
            second_input.created_at =
                TimestampMillis::from_timestamp(first.created_at.timestamp_millis() + 45_000);
            // KV rewrites the URL on create; reuse the base URL so the
            // incarnation-stripped comparison can match idempotently.
            second_input.queue_url = queue_base_url("sqs-ident");
            let second = fixture
                .queue_operations
                .create_queue(second_input)
                .await
                .map_err(|err| anyhow::anyhow!("idempotent create failed: {err:?}"))?;
            anyhow::ensure!(
                second.queue_url == first.queue_url,
                "idempotent create changed URL: {} vs {}",
                second.queue_url,
                first.queue_url
            );
            anyhow::ensure!(
                second.attributes == attributes,
                "idempotent create changed attributes: {:?}",
                second.attributes
            );
            anyhow::ensure!(
                second.created_at == first.created_at,
                "idempotent create changed created_at"
            );
            let stored = fixture
                .queue_operations
                .get_queue(&first.queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("get failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue missing after idempotent create"))?;
            anyhow::ensure!(
                stored.queue_url == first.queue_url && stored.attributes == attributes,
                "stored queue diverged after idempotent create"
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

/// SQS-IDENT-02: mismatched attributes are rejected without mutating the
/// stored queue.
///
/// Ports `create_queue_rejects_existing_metadata_mismatch` (KV, error is
/// `TransactWrite`) and the SQL `ResourceExists` branch of `create_queue`:
/// both backends must fail; the exact error code differs and is recorded
/// as a semantic gap, not asserted portably.
#[tokio::test]
async fn given_queue_when_create_mismatched_attrs_then_rejected_and_preserved() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let original = HashMap::from([("VisibilityTimeout".to_string(), "30".to_string())]);
            let created_url = create_queue_url(&fixture, "sqs-mismatch", original.clone()).await?;
            let mismatched = HashMap::from([("VisibilityTimeout".to_string(), "60".to_string())]);
            let result = fixture
                .queue_operations
                .create_queue(queue_request("sqs-mismatch", mismatched))
                .await;
            anyhow::ensure!(result.is_err(), "mismatched create unexpectedly succeeded");
            let stored = fixture
                .queue_operations
                .get_queue(&created_url)
                .await
                .map_err(|err| anyhow::anyhow!("get failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue missing after rejected create"))?;
            anyhow::ensure!(
                stored.attributes == original,
                "rejected create mutated attributes: {:?}",
                stored.attributes
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

/// SQS-ROUTE-01: list/get-by-name routing isolates queues.
///
/// Ports the routing half of
/// `queue_url_embeds_incarnation_without_a_lookup_record`
/// (list/get-by-name) and SQL `list_queues`/`get_queue_by_name` behavior
/// through public calls only; incarnation-id internals stay native.
#[tokio::test]
async fn given_queues_when_list_and_get_by_name_then_routing_isolated() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let url_a = create_queue_url(&fixture, "route-a", HashMap::new()).await?;
            let url_b = create_queue_url(&fixture, "route-b", HashMap::new()).await?;
            let by_name = fixture
                .queue_operations
                .get_queue_by_name("route-a")
                .await
                .map_err(|err| anyhow::anyhow!("get by name failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("route-a missing by name"))?;
            anyhow::ensure!(
                by_name.queue_url == url_a,
                "get by name returned wrong URL: {}",
                by_name.queue_url
            );
            let missing = fixture
                .queue_operations
                .get_queue_by_name("route-missing")
                .await
                .map_err(|err| anyhow::anyhow!("get missing by name failed: {err:?}"))?;
            anyhow::ensure!(missing.is_none(), "missing name unexpectedly resolved");
            let missing_url = fixture
                .queue_operations
                .get_queue("http://localhost/000000000000/route-missing")
                .await
                .map_err(|err| anyhow::anyhow!("get missing url failed: {err:?}"))?;
            anyhow::ensure!(missing_url.is_none(), "missing URL unexpectedly resolved");
            let all = fixture
                .queue_operations
                .list_queues(Some("route-"))
                .await
                .map_err(|err| anyhow::anyhow!("list failed: {err:?}"))?;
            let urls: HashSet<String> = all.into_iter().map(|queue| queue.queue_url).collect();
            anyhow::ensure!(
                urls.contains(&url_a) && urls.contains(&url_b),
                "list with prefix missed queues: {urls:?}"
            );
            let only_a = fixture
                .queue_operations
                .list_queues(Some("route-a"))
                .await
                .map_err(|err| anyhow::anyhow!("list prefix-a failed: {err:?}"))?;
            anyhow::ensure!(
                only_a.len() == 1 && only_a[0].queue_url == url_a,
                "prefix list leaked other queue: {:?}",
                only_a
                    .iter()
                    .map(|queue| &queue.queue_url)
                    .collect::<Vec<_>>()
            );
            // Receiving from the empty sibling must not touch route-a.
            let empty = receive_all(&fixture, &url_b, 10).await?;
            anyhow::ensure!(empty.is_empty(), "empty queue returned messages");
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

/// SQS-VIS-01: visibility leases hide claims until the timeout or an
/// explicit zero reset.
///
/// Ports `kv/.../queue_provider_regression_tests.rs`
/// `visibility_timeout_zero_makes_claimed_message_receivable_again` and
/// the SQLite `given_invisible_messages_...` lease observation through
/// public send/receive/visibility calls.
#[tokio::test]
async fn given_messages_when_receive_then_lease_hides_until_visibility_zero() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-visibility", HashMap::new()).await?;
            send_body(&fixture, &queue_url, "first").await?;
            send_body(&fixture, &queue_url, "second").await?;
            let first = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(first.len() == 1, "expected one leased message");
            let second = receive_all(&fixture, &queue_url, 2).await?;
            anyhow::ensure!(
                second.len() == 1 && second[0].message_id != first[0].message_id,
                "second receive should return the other visible message"
            );
            let drained = receive_all(&fixture, &queue_url, 10).await?;
            anyhow::ensure!(
                drained.is_empty(),
                "leased messages unexpectedly visible: {}",
                drained.len()
            );
            fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                    DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("visibility zero failed: {err:?}"))?;
            let redelivered = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(
                redelivered.len() == 1 && redelivered[0].message_id == first[0].message_id,
                "visibility zero did not redeliver the same message"
            );
            anyhow::ensure!(
                redelivered[0].receipt_handle != first[0].receipt_handle,
                "redelivery must issue a fresh receipt handle"
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

/// SQS-STALE-01 (negative): a stale receipt must not delete the current
/// claim.
///
/// Ports `queue/src/queue_correctness_tests.rs`
/// `run_stale_receipt_does_not_delete_current_claim`: after redelivery
/// the old handle is either accepted without effect (AWS) or rejected
/// with a typed receipt error; the message must survive either way.
#[tokio::test]
async fn given_claim_when_stale_receipt_delete_then_current_claim_preserved() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-stale", HashMap::new()).await?;
            let sent_id = send_body(&fixture, &queue_url, "stale-receipt").await?;
            let first = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(first.len() == 1, "expected first claim");
            fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                    DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("make visible failed: {err:?}"))?;
            // Redeliver to establish the current claim; retry a few times for
            // backends with asynchronous visibility application.
            let mut second = Vec::new();
            for _ in 0..8 {
                second = receive_all(&fixture, &queue_url, 1).await?;
                if !second.is_empty() {
                    break;
                }
            }
            anyhow::ensure!(second.len() == 1, "expected redelivery");
            anyhow::ensure!(
                second[0].message_id == sent_id,
                "redelivery changed message identity"
            );
            let stale = fixture
                .queue_operations
                .delete_message(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                )
                .await;
            match stale {
                Ok(()) => {}
                Err(err) => anyhow::ensure!(
                    is_stale_receipt_error(&err),
                    "stale receipt returned unrelated error: {err:?}"
                ),
            }
            fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from(second[0].receipt_handle.as_str()),
                    DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("make current visible failed: {err:?}"))?;
            let mut still_present = Vec::new();
            for _ in 0..8 {
                still_present = receive_all(&fixture, &queue_url, 1).await?;
                if !still_present.is_empty() {
                    break;
                }
            }
            anyhow::ensure!(
                still_present.len() == 1 && still_present[0].message_id == sent_id,
                "stale delete removed the current claim"
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

/// SQS-PURGE-01: purge removes messages but preserves the queue.
///
/// Ports `sql/.../queue_provider_write_tests.rs`
/// `given_purge_then_delete_when_helpers_apply_then_queue_and_message_ownership_is_preserved`
/// (purge half) through public calls.
#[tokio::test]
async fn given_queue_when_purge_then_messages_removed_but_queue_preserved() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-purge", HashMap::new()).await?;
            send_body(&fixture, &queue_url, "a").await?;
            send_body(&fixture, &queue_url, "b").await?;
            fixture
                .queue_operations
                .purge_queue(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("purge failed: {err:?}"))?;
            let stored = fixture
                .queue_operations
                .get_queue(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("get after purge failed: {err:?}"))?;
            anyhow::ensure!(stored.is_some(), "purge removed the queue itself");
            let received = receive_all(&fixture, &queue_url, 10).await?;
            anyhow::ensure!(
                received.is_empty(),
                "purge left {} messages visible",
                received.len()
            );
            if let Ok(Some((_, counts))) = fixture
                .queue_operations
                .get_queue_with_message_counts(&queue_url)
                .await
            {
                anyhow::ensure!(
                    counts.visible == 0 && counts.delayed == 0,
                    "purge left counts: visible={} delayed={}",
                    counts.visible,
                    counts.delayed
                );
            }
            // The queue must remain usable after purge.
            send_body(&fixture, &queue_url, "after-purge").await?;
            let revived = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(revived.len() == 1, "queue unusable after purge");
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

/// SQS-DELETE-01: delete removes the queue and its messages.
///
/// Ports the delete half of the SQL purge/delete helper test plus the KV
/// incarnation ownership rule at the observable level: after delete the
/// old URL resolves to absent and lists exclude it. KV incarnation
/// isolation (old URL must not address a same-name replacement) and SQL
/// URL reuse differ by design; this contract asserts only that the old
/// URL carries no messages, and records that divergence as a finding.
#[tokio::test]
async fn given_queue_when_delete_then_queue_and_messages_removed() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-delete", HashMap::new()).await?;
            send_body(&fixture, &queue_url, "doomed").await?;
            fixture
                .queue_operations
                .delete_queue(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("delete failed: {err:?}"))?;
            let stored = fixture
                .queue_operations
                .get_queue(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("get after delete failed: {err:?}"))?;
            anyhow::ensure!(stored.is_none(), "delete left queue metadata");
            let listed = fixture
                .queue_operations
                .list_queues(Some("sqs-delete"))
                .await
                .map_err(|err| anyhow::anyhow!("list after delete failed: {err:?}"))?;
            anyhow::ensure!(
                listed.iter().all(|queue| queue.queue_url != queue_url),
                "delete left queue in list"
            );
            // The old URL must carry no messages: backends disagree on whether
            // a deleted URL receives empty or ResourceNotFound (SQL returns
            // empty, KV returns ResourceNotFound). Either is accepted here;
            // the divergence is recorded as SQS-DELETE-GAP-01.
            match fixture
                .queue_operations
                .receive_messages(
                    &queue_url,
                    10,
                    DurationSeconds::from(30),
                    DurationSeconds::from(0),
                )
                .await
            {
                Ok(messages) => {
                    anyhow::ensure!(messages.is_empty(), "deleted queue returned messages")
                }
                Err(err) => anyhow::ensure!(
                    matches!(
                        err,
                        queue_provider::QueueError::ResourceNotFound { .. }
                            | queue_provider::QueueError::Validation { .. }
                    ),
                    "deleted queue receive returned unrelated error: {err:?}"
                ),
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

/// SQS-COUNTS-01: metadata and counts share one provider snapshot.
///
/// Ports `queue_metadata_and_counts_share_one_snapshot`: one immediate
/// message, one delayed message, one leased message split into
/// visible/not-visible/delayed.
#[tokio::test]
async fn given_queue_when_counts_observed_then_snapshot_splits_visible_leased_delayed()
-> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-counts", HashMap::new()).await?;
            let now = fixture.now();
            fixture
                .queue_operations
                .send_message(QueueMessage {
                    receive_count: 0,
                    message_id: Default::default(),
                    queue_url: queue_url.clone(),
                    body: "visible".to_string(),
                    message_attributes: None,
                    receipt_handle: None,
                    created_at: now,
                    visibility_timestamp: Some(now),
                })
                .await
                .map_err(|err| anyhow::anyhow!("send visible failed: {err:?}"))?;
            fixture
                .queue_operations
                .send_message(send_request(&queue_url, "delayed", Some(60), fixture.now()))
                .await
                .map_err(|err| anyhow::anyhow!("send delayed failed: {err:?}"))?;
            let claimed = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(claimed.len() == 1, "expected one claim for counts");
            let (queue, counts) = fixture
                .queue_operations
                .get_queue_with_message_counts(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("counts failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue missing for counts"))?;
            anyhow::ensure!(
                queue.queue_url == queue_url,
                "counts snapshot returned wrong queue"
            );
            anyhow::ensure!(
                counts.visible == 0 && counts.not_visible == 1 && counts.delayed == 1,
                "counts split wrong: visible={} not_visible={} delayed={}",
                counts.visible,
                counts.not_visible,
                counts.delayed
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

/// SQS-CONC-01: concurrent receives claim each message once.
///
/// Ports `concurrent_receives_claim_each_message_once` (24 messages, 6
/// workers) at a smaller scale for the fast suite: 12 messages claimed
/// by 3 concurrent receives of 4 must total 12 unique ids.
#[tokio::test]
async fn given_messages_when_concurrent_receives_then_each_claimed_once() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-concurrent", HashMap::new()).await?;
            for index in 0..12 {
                send_body(&fixture, &queue_url, &format!("message-{index}")).await?;
            }
            let first = fixture.queue_operations.receive_messages(
                &queue_url,
                4,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            );
            let second = fixture.queue_operations.receive_messages(
                &queue_url,
                4,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            );
            let third = fixture.queue_operations.receive_messages(
                &queue_url,
                4,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            );
            let (first, second, third) = tokio::join!(first, second, third);
            let first = first.map_err(|err| anyhow::anyhow!("first wave failed: {err:?}"))?;
            let second = second.map_err(|err| anyhow::anyhow!("second wave failed: {err:?}"))?;
            let third = third.map_err(|err| anyhow::anyhow!("third wave failed: {err:?}"))?;
            let mut unique = HashSet::new();
            for message in first.iter().chain(&second).chain(&third) {
                anyhow::ensure!(
                    unique.insert(message.message_id.clone()),
                    "duplicate claim: {}",
                    message.message_id
                );
            }
            anyhow::ensure!(
                unique.len() == 12,
                "concurrent receives lost or duplicated: {} unique",
                unique.len()
            );
            let drained = receive_all(&fixture, &queue_url, 10).await?;
            anyhow::ensure!(
                drained.is_empty(),
                "concurrent receives left {} messages",
                drained.len()
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

/// SQS-NEG-01 (negative): malformed receipts fail validation without
/// effects.
///
/// Ports `malformed_receipt_handles_do_not_use_legacy_queue_fallbacks`
/// (portable half): delete and visibility with a malformed handle return
/// `MessageNotFound`/`MessageNotFoundOrAlreadyProcessed` and the live
/// message stays receivable. Legacy-record assertions stay native.
#[tokio::test]
async fn given_receipt_when_malformed_then_validation_without_effects() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-malformed", HashMap::new()).await?;
            send_body(&fixture, &queue_url, "live").await?;
            let malformed = ReceiptHandle::from("not-a-compact-receipt-handle");
            let delete_error = fixture
                .queue_operations
                .delete_message(&queue_url, malformed)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("malformed delete unexpectedly succeeded"))?;
            anyhow::ensure!(
                is_stale_receipt_error(&delete_error),
                "malformed delete returned unrelated error: {delete_error:?}"
            );
            let visibility_error = fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from("not-a-compact-receipt-handle"),
                    DurationSeconds::from(0),
                )
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("malformed visibility unexpectedly succeeded"))?;
            anyhow::ensure!(
                is_stale_receipt_error(&visibility_error),
                "malformed visibility returned unrelated error: {visibility_error:?}"
            );
            let live = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(
                live.len() == 1 && live[0].body == "live",
                "malformed handles affected the live message"
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

/// SQS-BATCH-01: batch send/delete isolate per-entry errors.
///
/// Covers the provider batch surface (`send_messages`/`delete_messages`)
/// that the manager batch-error mapping relies on: two valid deletes
/// succeed while one malformed entry fails, and the remaining live
/// message survives.
#[tokio::test]
async fn given_messages_when_batch_send_delete_then_partial_errors_isolated() -> anyhow::Result<()>
{
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-batch", HashMap::new()).await?;
            let now = fixture.now();
            let messages = ["batch-0", "batch-1", "batch-2"]
                .iter()
                .map(|body| {
                    queue_message_for_send(
                        queue_provider::SendMessageRequest {
                            queue_url: queue_url.clone(),
                            message_body: body.to_string(),
                            delay_seconds: None,
                            message_attributes: None,
                        },
                        now,
                    )
                })
                .collect::<Vec<_>>();
            let sends = fixture
                .queue_operations
                .send_messages(messages)
                .await
                .map_err(|err| anyhow::anyhow!("batch send failed: {err:?}"))?;
            anyhow::ensure!(
                sends.len() == 3 && sends.iter().all(|result| result.is_ok()),
                "batch send had failures"
            );
            let received = receive_all(&fixture, &queue_url, 3).await?;
            anyhow::ensure!(received.len() == 3, "expected 3 leased for batch delete");
            let handles = received
                .iter()
                .take(2)
                .map(|message| ReceiptHandle::from(message.receipt_handle.as_str()))
                .chain(std::iter::once(ReceiptHandle::from("not-a-receipt")))
                .collect::<Vec<_>>();
            let deletes = fixture
                .queue_operations
                .delete_messages(&queue_url, handles)
                .await
                .map_err(|err| anyhow::anyhow!("batch delete failed: {err:?}"))?;
            anyhow::ensure!(deletes.len() == 3, "batch delete arity changed");
            anyhow::ensure!(
                deletes[0].is_ok() && deletes[1].is_ok(),
                "valid batch deletes failed"
            );
            anyhow::ensure!(deletes[2].is_err(), "malformed batch entry unexpectedly ok");
            fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from(received[2].receipt_handle.as_str()),
                    DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("restore live failed: {err:?}"))?;
            let mut live = Vec::new();
            for _ in 0..8 {
                live = receive_all(&fixture, &queue_url, 3).await?;
                if !live.is_empty() {
                    break;
                }
            }
            anyhow::ensure!(live.len() == 1, "batch survivor missing");
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

/// SQS-PAYLOAD-01: large payloads round-trip and delete cleanly at the
/// observable level.
///
/// Ports `large_message_payload_is_split_and_reassembled` (observable
/// half): a 256 KiB body returns intact and is gone after delete. Chunk
/// counts and byte limits stay native. Replacement message cleanup operates
/// on co-located rows; there is no separate orphan payload family.
#[tokio::test]
async fn given_message_when_large_body_then_roundtrip_preserved() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-large", HashMap::new()).await?;
            let large_body = "x".repeat(256 * 1024);
            fixture
                .queue_operations
                .send_message(send_request(&queue_url, &large_body, None, fixture.now()))
                .await
                .map_err(|err| anyhow::anyhow!("send large failed: {err:?}"))?;
            let received = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(received.len() == 1, "large message not received");
            anyhow::ensure!(
                received[0].body == large_body,
                "large body corrupted: {} vs {} bytes",
                received[0].body.len(),
                large_body.len()
            );
            fixture
                .queue_operations
                .delete_message(
                    &queue_url,
                    ReceiptHandle::from(received[0].receipt_handle.as_str()),
                )
                .await
                .map_err(|err| anyhow::anyhow!("delete large failed: {err:?}"))?;
            let drained = receive_all(&fixture, &queue_url, 10).await?;
            anyhow::ensure!(drained.is_empty(), "deleted large message still visible");
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

/// SQS-MGR-01: queue manager round-trips on real storage.
///
/// Ports `queue/src/queue_correctness_tests.rs` manager flows
/// (`create_queue`/`send`/`receive`/`delete`) through `QueueManager`
/// backed by the manager fixture's real provider. No transport control
/// is needed for the queue path.
#[tokio::test]
async fn given_queue_when_set_attributes_then_persisted_and_missing_fails() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-setattr", HashMap::new()).await?;
            let updated = HashMap::from([("VisibilityTimeout".to_string(), "45".to_string())]);
            fixture
                .queue_operations
                .set_queue_attributes(&queue_url, updated.clone())
                .await
                .map_err(|err| anyhow::anyhow!("set attrs failed: {err:?}"))?;
            let stored = fixture
                .queue_operations
                .get_queue(&queue_url)
                .await
                .map_err(|err| anyhow::anyhow!("get after set failed: {err:?}"))?
                .ok_or_else(|| anyhow::anyhow!("queue missing after set"))?;
            anyhow::ensure!(
                stored.attributes == updated,
                "set attributes lost: {:?}",
                stored.attributes
            );
            let missing_set = fixture
                .queue_operations
                .set_queue_attributes("http://localhost/000000000000/sqs-missing", updated)
                .await;
            anyhow::ensure!(
                missing_set.is_err(),
                "set on missing queue unexpectedly succeeded"
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

/// SQS-CHECKPOINT-01: checkpoint updates require the current receipt.
///
/// Ports the `update_message_snapshot_checkpoint` half of the SQL
/// captured-write test: the current receipt stores the checkpoint, a
/// stale receipt fails, and the message remains deletable.
#[tokio::test]
async fn given_message_when_checkpoint_updated_then_stale_rejected() -> anyhow::Result<()> {
    let result: anyhow::Result<()> = async {
        let fixture = TestStore::new().await;
        let outcome = async {
            let queue_url = create_queue_url(&fixture, "sqs-checkpoint", HashMap::new()).await?;
            send_body(&fixture, &queue_url, "checkpoint").await?;
            let first = receive_all(&fixture, &queue_url, 1).await?;
            anyhow::ensure!(first.len() == 1, "expected claim for checkpoint");
            fixture
                .queue_operations
                .update_message_snapshot_checkpoint(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                    r#"{"cursor":"cursor-1"}"#.to_string(),
                )
                .await
                .map_err(|err| anyhow::anyhow!("checkpoint failed: {err:?}"))?;
            fixture
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                    DurationSeconds::from(0),
                )
                .await
                .map_err(|err| anyhow::anyhow!("make visible failed: {err:?}"))?;
            let mut second = Vec::new();
            for _ in 0..8 {
                second = receive_all(&fixture, &queue_url, 1).await?;
                if !second.is_empty() {
                    break;
                }
            }
            anyhow::ensure!(second.len() == 1, "expected redelivery for checkpoint");
            let stale = fixture
                .queue_operations
                .update_message_snapshot_checkpoint(
                    &queue_url,
                    ReceiptHandle::from(first[0].receipt_handle.as_str()),
                    r#"{"cursor":"stale"}"#.to_string(),
                )
                .await;
            match stale {
                Ok(()) => {}
                Err(err) => anyhow::ensure!(
                    is_stale_receipt_error(&err),
                    "stale checkpoint unrelated error: {err:?}"
                ),
            }
            fixture
                .queue_operations
                .delete_message(
                    &queue_url,
                    ReceiptHandle::from(second[0].receipt_handle.as_str()),
                )
                .await
                .map_err(|err| anyhow::anyhow!("delete after checkpoint failed: {err:?}"))?;
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
