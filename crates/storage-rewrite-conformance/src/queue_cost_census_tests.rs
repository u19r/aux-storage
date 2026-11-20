//! Queue cost census (W7-G 5.4): prints per-operation native counts for 1 KiB
//! and 256 KiB bodies, cold and with a fresh catalog hint. It asserts that
//! each step succeeds; on Postgres it also pins waves and the lazy BEGIN
//! (PG-BEGIN). It runs before and after W7 and the lines compare.

use std::{collections::HashMap, time::Duration};

use queue_provider::{
    MESSAGE_ID_LEN, MessageId, Queue, ReceiptHandle, SendMessageRequest, queue_message_for_send,
};
use storage_types::DurationSeconds;

use super::{
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

async fn queue(store: &TestStore, label: &str) -> String {
    let name = format!("{label}_{}", uuid::Uuid::new_v4().simple());
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name,
            attributes: HashMap::new(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue")
        .queue_url
}

fn message(store: &TestStore, queue_url: &str, body: String) -> queue_provider::QueueMessage {
    queue_message_for_send(
        SendMessageRequest {
            queue_url: queue_url.to_owned(),
            message_body: body,
            ..Default::default()
        },
        store.now(),
    )
}

async fn send(store: &TestStore, queue_url: &str, body: String) {
    store
        .queue_operations
        .send_message(message(store, queue_url, body))
        .await
        .expect("send message");
}

async fn receive(store: &TestStore, queue_url: &str, max: u32) -> Vec<String> {
    store
        .queue_operations
        .receive_messages(queue_url, max, Some(DurationSeconds::from(30)), None)
        .await
        .expect("receive")
        .messages
        .into_iter()
        .map(|message| message.receipt_handle)
        .collect()
}

fn report(census: &NativeCensus, size: usize, mode: &str, op: &str) -> CensusCounts {
    let counts = census.snapshot();
    eprintln!(
        "queue census backend={} body={size} mode={mode} op={op}: {counts} chunk_reads={} \
         durable={}",
        TestStore::selected_backend(),
        census.chunk_key_reads(),
        counts.durable_summary(),
    );
    if TestStore::selected_backend() == "postgres" {
        assert_eq!(counts.lone_begins, 0, "{op}: {counts}");
        assert_eq!(counts.begins, 1, "{op}: {counts}");
        // BEGIN rides with the first read; COMMIT is its own wave (PG-BEGIN).
        if mode == "cold" {
            let expected = if op == "send_1" { 3 } else { 4 };
            assert_eq!(counts.waves, expected, "{op}: {counts}");
        }
    }
    counts
}

/// W7-H: a hinted operation reads the catalog row inside its first message
/// read. FoundationDB: one wave fewer, conflict read keys and rows read equal;
/// a receive reads the row as a range entry (keys -1, ranges +1). RocksDB:
/// only the receive scan merges (a mixed get is one call per protection).
fn assert_hinted_rows(rows: &[(usize, &'static str, &'static str, CensusCounts)]) {
    let backend = TestStore::selected_backend();
    for (size, _, op, hinted) in rows.iter().filter(|row| row.1 == "hinted") {
        let (_, _, _, cold) = rows
            .iter()
            .find(|row| row.0 == *size && row.1 == "cold" && row.2 == *op)
            .expect("cold row of the same op");
        let scan = u64::from(op.starts_with("receive"));
        let label = format!("body={size} op={op}: cold {cold} / hinted {hinted}");
        match backend.as_str() {
            "foundationdb" => {
                let cold_waves = match (*op, *size) {
                    ("send_1", _) => 4,
                    ("receive_1" | "receive_10", 262_144) => 6,
                    _ => 5,
                };
                assert_eq!(cold.waves, cold_waves, "{label}");
                assert_eq!(hinted.waves + 1, cold.waves, "{label}");
                assert_eq!(hinted.keys + scan, cold.keys, "{label}");
                assert_eq!(hinted.ranges, cold.ranges + scan, "{label}");
                assert_eq!(
                    hinted.conflicts.read_keys, cold.conflicts.read_keys,
                    "{label}"
                );
                assert_eq!(hinted.rows_read, cold.rows_read, "{label}");
            }
            "rocksdb" => {
                assert_eq!(hinted.waves + scan, cold.waves, "{label}");
                assert_eq!(hinted.rows_read, cold.rows_read, "{label}");
            }
            "postgres" => {
                if *op == "send_1" {
                    // pin from the next run
                    println!("postgres send_1 {label}");
                } else {
                    assert_eq!(hinted.waves + 1, cold.waves, "{label}");
                }
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn queue_cost_census_counts_operations_for_1_kib_and_256_kib_bodies() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let mut rows: Vec<(usize, &'static str, &'static str, CensusCounts)> = Vec::new();
    for size in [1_024_usize, 262_144] {
        for mode in ["cold", "hinted"] {
            let queue_url = queue(&store, "census").await;
            let messages = (0..11_u64)
                .map(|n| message(&store, &queue_url, incompressible_body(size, n + 1)))
                .collect::<Vec<_>>();
            for result in store
                .queue_operations
                .send_messages(messages)
                .await
                .expect("send batch")
            {
                result.expect("send entry");
            }

            let prepare = |census: &NativeCensus| {
                if mode == "cold" {
                    store.advance_time(Duration::from_secs(6));
                }
                census.reset();
            };

            prepare(&census);
            let first = receive(&store, &queue_url, 1).await;
            assert_eq!(first.len(), 1);
            rows.push((
                size,
                mode,
                "receive_1",
                report(&census, size, mode, "receive_1"),
            ));
            let r1 = first.into_iter().next().expect("one receipt");

            prepare(&census);
            let ten = receive(&store, &queue_url, 10).await;
            assert_eq!(ten.len(), 10);
            rows.push((
                size,
                mode,
                "receive_10",
                report(&census, size, mode, "receive_10"),
            ));

            prepare(&census);
            store
                .queue_operations
                .change_message_visibility(
                    &queue_url,
                    ReceiptHandle(r1.clone()),
                    DurationSeconds::from(60),
                )
                .await
                .expect("change visibility");
            rows.push((
                size,
                mode,
                "change_visibility_1",
                report(&census, size, mode, "change_visibility_1"),
            ));

            prepare(&census);
            store
                .queue_operations
                .delete_message(&queue_url, ReceiptHandle(r1))
                .await
                .expect("delete one");
            rows.push((
                size,
                mode,
                "delete_1",
                report(&census, size, mode, "delete_1"),
            ));

            prepare(&census);
            let results = store
                .queue_operations
                .delete_messages(&queue_url, ten.into_iter().map(ReceiptHandle).collect())
                .await
                .expect("delete batch");
            assert_eq!(results.len(), 10);
            for result in results {
                result.expect("delete entry");
            }
            rows.push((
                size,
                mode,
                "delete_batch_10",
                report(&census, size, mode, "delete_batch_10"),
            ));

            prepare(&census);
            send(&store, &queue_url, incompressible_body(size, 99)).await;
            rows.push((size, mode, "send_1", report(&census, size, mode, "send_1")));
        }
    }
    assert_hinted_rows(&rows);
    store.finish().await;
}

/// W7-H: the receive probe (`read_due_head`) and a publication
/// (`read_messages_by_id`) read the catalog row inside their first read.
/// FoundationDB: one wave fewer than cold; the probe reads the row as a range
/// entry (keys -1, ranges +1); a publication keeps conflict read keys and rows
/// read. RocksDB: only the probe scan merges (a mixed get is one call per
/// protection). Postgres: the hinted catalog check rides in the first message
/// read (`CheckedSqlAttempt`), so hinted is one wave fewer, as in
/// `assert_hinted_rows`. The spec (W7-H-a section 7) gives deltas, not absolute
/// counts, so only deltas are pinned; the absolute counts are printed.
#[tokio::test]
async fn queue_cost_census_pins_the_merged_probe_and_publication_waves() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(census.clone()).await;
    let mut rows: Vec<(&'static str, &'static str, CensusCounts)> = Vec::new();
    for mode in ["cold", "hinted"] {
        let queue_url = queue(&store, "merged").await;
        // The send reads the catalog row, which leaves a fresh hint.
        send(&store, &queue_url, incompressible_body(1_024, 7)).await;
        let prepare = |census: &NativeCensus| {
            if mode == "cold" {
                store.advance_time(Duration::from_secs(6));
            }
            census.reset();
        };

        prepare(&census);
        let probed = store
            .queue_operations
            .probe_receive(&queue_url)
            .await
            .expect("probe queue");
        assert!(probed.is_some(), "the probe finds the queue");
        rows.push(("probe", mode, census.snapshot()));

        // Hinted keeps the hint the send left (and the probe refreshed); cold
        // expires it again.
        prepare(&census);
        // A publication carries its stable identity; a default id is invalid.
        let mut publication = message(&store, &queue_url, incompressible_body(1_024, 9));
        publication.message_id = MessageId::from_bytes([9; MESSAGE_ID_LEN]);
        store
            .queue_operations
            .publish_message(publication)
            .await
            .expect("publish message");
        rows.push(("publish", mode, census.snapshot()));
    }
    for (op, mode, counts) in &rows {
        eprintln!(
            "queue merged census backend={} mode={mode} op={op}: {counts}",
            TestStore::selected_backend(),
        );
    }
    let backend = TestStore::selected_backend();
    for op in ["probe", "publish"] {
        let find = |mode: &str| {
            rows.iter()
                .find(|row| row.0 == op && row.1 == mode)
                .map(|row| row.2.clone())
                .expect("measured row")
        };
        let (cold, hinted) = (find("cold"), find("hinted"));
        let scan = u64::from(op == "probe");
        let label = format!("op={op}: cold {cold} / hinted {hinted}");
        match backend.as_str() {
            "foundationdb" => {
                assert_eq!(hinted.waves + 1, cold.waves, "{label}");
                assert_eq!(hinted.keys + scan, cold.keys, "{label}");
                assert_eq!(hinted.ranges, cold.ranges + scan, "{label}");
                assert_eq!(
                    hinted.conflicts.read_keys, cold.conflicts.read_keys,
                    "{label}"
                );
                assert_eq!(hinted.rows_read, cold.rows_read, "{label}");
            }
            "rocksdb" => {
                assert_eq!(hinted.waves + scan, cold.waves, "{label}");
                assert_eq!(hinted.keys + scan, cold.keys, "{label}");
                assert_eq!(hinted.ranges, cold.ranges + scan, "{label}");
                assert_eq!(hinted.rows_read, cold.rows_read, "{label}");
            }
            // The hinted catalog check joins the first message read (b27: probe
            // 3 -> 2, publication 4 -> 3 waves).
            "postgres" => {
                assert_eq!(hinted.waves + 1, cold.waves, "{label}");
                assert_eq!(hinted.begins, 1, "{label}");
                assert_eq!(hinted.lone_begins, 0, "{label}");
            }
            _ => {}
        }
    }
    store.finish().await;
}
