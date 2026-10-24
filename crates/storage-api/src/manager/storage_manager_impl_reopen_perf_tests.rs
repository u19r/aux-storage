//! Paired probes for the reopened storage-api candidates.
//!
//! These probes deliberately compare the complete local operation used by a
//! candidate against its current replacement.  They are allocation reports,
//! not acceptance of a production change: the receipt records whether the
//! source-current path is retained after the pair and wire gates are checked.

use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    hint::black_box,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use serde_json::json;
use storage_types::{AttributeValue, GetItemRequest, KeyAttributes};

const ITERATIONS: usize = 256;
const KEY_BYTES: usize = 100;
const VALUE_BYTES: usize = 1_024;

#[test]
fn reopened_storage_api_allocation_pairs_report_baseline_and_candidate() {
    let get = json_to::<GetItemRequest>(json!({
        "TableName": "reopen",
        "Key": {"pk": {"S": format!("tenant#{}", "k".repeat(KEY_BYTES - 7))}},
        "ProjectionExpression": "pk, sk",
        "ConsistentRead": true
    }));
    assert_eq!(get.consistent_read, Some(true));

    let missing_key = serde_json::from_value::<GetItemRequest>(json!({
        "TableName": "reopen"
    }));
    assert!(
        missing_key.is_err(),
        "missing key must retain AWS JSON validation shape"
    );

    let items = realistic_items();
    let (legacy, reused) = measure_pair(
        "pagination_json_buffer",
        || pagination_legacy(&items),
        || pagination_reused(&items),
    );
    assert_eq!(pagination_legacy(&items), pagination_reused(&items));
    emit_pair("ASTRA-804", legacy, reused);

    let keys = realistic_keys();
    let (legacy, indexed) = measure_pair(
        "read_sequence_key_index",
        || linear_key_dedup(&keys),
        || indexed_key_dedup(&keys),
    );
    assert_eq!(linear_key_dedup(&keys), indexed_key_dedup(&keys));
    emit_pair("ASTRA-807", legacy, indexed);

    let fanout = (0..100).collect::<Vec<_>>();
    let (legacy, candidate) = measure_pair(
        "read_sequence_batch_split",
        || batch_split_off_legacy(&fanout),
        || batch_drain_candidate(&fanout),
    );
    assert_eq!(
        batch_split_off_legacy(&fanout),
        batch_drain_candidate(&fanout)
    );
    emit_pair("ASTRA-808", legacy, candidate);

    let (legacy, typed) = measure_pair(
        "key_validation_fingerprint",
        || canonical_key_bytes(&keys[0]),
        || typed_key_fingerprint(&keys[0]),
    );
    assert!(!canonical_key_bytes(&keys[0]).is_empty());
    assert_ne!(typed_key_fingerprint(&keys[0]), 0);
    emit_pair("ASTRA-809", legacy, typed);

    let token_bytes = (0..1_200).map(|value| value as u8).collect::<Vec<_>>();
    let (legacy, candidate) = measure_pair(
        "read_sequence_token_hex",
        || token_hex_legacy(&token_bytes),
        || token_hex_candidate(&token_bytes),
    );
    assert_eq!(
        token_hex_legacy(&token_bytes),
        token_hex_candidate(&token_bytes)
    );
    emit_pair("ASTRA-810", legacy, candidate);
}

#[test]
#[ignore = "manual paired CPU probe; run with --ignored --nocapture --test-threads=1"]
fn reopened_storage_api_cpu_pairs_report_baseline_and_candidate() {
    let items = realistic_items();
    let (legacy, reused) = measure_cpu(|| pagination_legacy(&items), || pagination_reused(&items));
    println!(
        "ASTRA-804 pagination_json_buffer iterations={ITERATIONS} legacy_ns={:.2} \
         candidate_ns={:.2}",
        ns_per_iter(legacy),
        ns_per_iter(reused)
    );

    let keys = realistic_keys();
    let (legacy, indexed) = measure_cpu(|| linear_key_dedup(&keys), || indexed_key_dedup(&keys));
    println!(
        "ASTRA-807 read_sequence_key_index iterations={ITERATIONS} legacy_ns={:.2} \
         candidate_ns={:.2}",
        ns_per_iter(legacy),
        ns_per_iter(indexed)
    );

    let fanout = (0..100).collect::<Vec<_>>();
    let (legacy, candidate) = measure_cpu(
        || batch_split_off_legacy(&fanout),
        || batch_drain_candidate(&fanout),
    );
    println!(
        "ASTRA-808 read_sequence_batch_split iterations={ITERATIONS} legacy_ns={:.2} \
         candidate_ns={:.2}",
        ns_per_iter(legacy),
        ns_per_iter(candidate)
    );

    let (legacy, typed) = measure_cpu(
        || canonical_key_bytes(&keys[0]),
        || typed_key_fingerprint(&keys[0]),
    );
    println!(
        "ASTRA-809 key_validation_fingerprint iterations={ITERATIONS} legacy_ns={:.2} \
         candidate_ns={:.2}",
        ns_per_iter(legacy),
        ns_per_iter(typed)
    );

    let token_bytes = (0..1_200).map(|value| value as u8).collect::<Vec<_>>();
    let (legacy, candidate) = measure_cpu(
        || token_hex_legacy(&token_bytes),
        || token_hex_candidate(&token_bytes),
    );
    println!(
        "ASTRA-810 read_sequence_token_hex iterations={ITERATIONS} legacy_ns={:.2} \
         candidate_ns={:.2}",
        ns_per_iter(legacy),
        ns_per_iter(candidate)
    );
}

#[test]
#[ignore = "manual ASTRA-812 allocation holdout; the paired CPU profile already rejects indexed \
            lookup"]
fn reopened_storage_api_node_lookup_allocation_holdout() {
    let names = (0..500).map(|index| format!("node-{index}"));
    let names = names.collect::<Vec<_>>();
    let target = names.last().expect("node names").as_str();
    let legacy = measure_alloc("linear_node_lookup", || {
        for _ in 0..ITERATIONS {
            black_box(names.iter().position(|name| name == target));
        }
    });
    let indexed = measure_alloc("indexed_node_lookup", || {
        let index = names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect::<HashMap<_, _>>();
        for _ in 0..ITERATIONS {
            black_box(index.get(target));
        }
    });
    emit_pair("ASTRA-812", legacy, indexed);
}

fn measure_pair<L, C>(
    _label: &'static str,
    legacy: impl Fn() -> L,
    candidate: impl Fn() -> C,
) -> (
    alloc_counter::AllocationReport<'static>,
    alloc_counter::AllocationReport<'static>,
) {
    let legacy_report = measure_alloc("legacy", || {
        black_box(legacy());
    });
    let candidate_report = measure_alloc("candidate", || {
        black_box(candidate());
    });
    (legacy_report, candidate_report)
}

fn measure_alloc<T>(
    label: &'static str,
    operation: impl Fn() -> T,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "reopened_storage_api_allocation_pairs_report_baseline_and_candidate",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        operation();
    }
    guard.finish()
}

fn emit_pair(
    id: &str,
    legacy: alloc_counter::AllocationReport<'static>,
    candidate: alloc_counter::AllocationReport<'static>,
) {
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    println!(
        "{id} allocations legacy={} candidate={} bytes legacy={} candidate={}",
        legacy.allocation_count,
        candidate.allocation_count,
        legacy.allocated_bytes,
        candidate.allocated_bytes
    );
}

fn measure_cpu<L, C>(legacy: impl Fn() -> L, candidate: impl Fn() -> C) -> (Duration, Duration) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(legacy());
    }
    let legacy_elapsed = started.elapsed();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(candidate());
    }
    (legacy_elapsed, started.elapsed())
}

fn ns_per_iter(duration: Duration) -> f64 {
    duration.as_nanos() as f64 / ITERATIONS as f64
}

fn json_to<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).expect("realistic storage-api request fixture")
}

fn realistic_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..64)
        .map(|index| {
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("tenant#{}", "k".repeat(96))),
                ),
                (
                    "sk".to_string(),
                    AttributeValue::S(format!("item#{index:04}")),
                ),
                (
                    "gsi1pk".to_string(),
                    AttributeValue::S("gsi#one".to_string()),
                ),
                (
                    "gsi1sk".to_string(),
                    AttributeValue::S(format!("item#{index:04}")),
                ),
                (
                    "gsi2pk".to_string(),
                    AttributeValue::S("gsi#two".to_string()),
                ),
                (
                    "gsi2sk".to_string(),
                    AttributeValue::S(format!("item#{index:04}")),
                ),
                (
                    "ttl".to_string(),
                    AttributeValue::N("4102444800".to_string()),
                ),
                (
                    "payload".to_string(),
                    AttributeValue::S("x".repeat(VALUE_BYTES)),
                ),
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                ("version".to_string(), AttributeValue::N(index.to_string())),
            ])
        })
        .collect()
}

fn pagination_legacy(items: &[HashMap<String, AttributeValue>]) -> usize {
    items
        .iter()
        .map(|item| serde_json::to_vec(item).expect("serialize item").len())
        .sum()
}

fn pagination_reused(items: &[HashMap<String, AttributeValue>]) -> usize {
    let mut buffer = Vec::new();
    items
        .iter()
        .map(|item| {
            buffer.clear();
            serde_json::to_writer(&mut buffer, item).expect("serialize item");
            buffer.len()
        })
        .sum()
}

fn realistic_keys() -> Vec<KeyAttributes> {
    (0..100)
        .map(|index| {
            KeyAttributes::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("tenant#{}", "k".repeat(96))),
                ),
                ("sk".to_string(), AttributeValue::N(index.to_string())),
            ])
        })
        .collect()
}

fn linear_key_dedup(keys: &[KeyAttributes]) -> usize {
    let mut unique = Vec::<&KeyAttributes>::new();
    for key in keys {
        if !unique.iter().any(|seen| key_equal(seen, key)) {
            unique.push(key);
        }
    }
    unique.len()
}

fn indexed_key_dedup(keys: &[KeyAttributes]) -> usize {
    let mut unique = Vec::<&KeyAttributes>::new();
    let mut indexed = HashMap::<u64, Vec<usize>>::new();
    for key in keys {
        let fingerprint = typed_key_fingerprint(key);
        let is_new = indexed
            .get(&fingerprint)
            .is_none_or(|indices| indices.iter().all(|index| !key_equal(unique[*index], key)));
        if is_new {
            let index = unique.len();
            unique.push(key);
            indexed.entry(fingerprint).or_default().push(index);
        }
    }
    unique.len()
}

fn batch_split_off_legacy(values: &[usize]) -> usize {
    let mut remaining = values.to_vec();
    let mut consumed = 0;
    while remaining.len() > 25 {
        let tail = remaining.split_off(25);
        consumed += remaining.len();
        remaining = tail;
    }
    consumed + remaining.len()
}

fn batch_drain_candidate(values: &[usize]) -> usize {
    let mut remaining = values.to_vec();
    let mut consumed = 0;
    while remaining.len() > 25 {
        let tail = remaining.drain(25..).collect::<Vec<_>>();
        consumed += remaining.len();
        remaining = tail;
    }
    consumed + remaining.len()
}

fn key_equal(left: &KeyAttributes, right: &KeyAttributes) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|(name, value)| right.get(name) == Some(value))
}

fn canonical_key_bytes(key: &KeyAttributes) -> Vec<u8> {
    serde_json::to_vec(key).expect("canonical key JSON")
}

fn token_hex_legacy(bytes: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = vec![0_u8; bytes.len() * 2];
    for (index, byte) in bytes.iter().copied().enumerate() {
        encoded[index * 2] = HEX_DIGITS[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = HEX_DIGITS[usize::from(byte & 0x0f)];
    }
    encoded.into_iter().map(char::from).collect()
}

fn token_hex_candidate(bytes: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes.iter().copied() {
        encoded.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn typed_key_fingerprint(key: &KeyAttributes) -> u64 {
    let mut entries = 0_u64;
    for (name, value) in key.iter() {
        let mut entry = DefaultHasher::new();
        name.hash(&mut entry);
        std::mem::discriminant(value).hash(&mut entry);
        match value {
            AttributeValue::S(value) | AttributeValue::B(value) => value.hash(&mut entry),
            AttributeValue::N(value) => match storage_types::canonical_dynamodb_number(value) {
                Ok(value) => value.hash(&mut entry),
                Err(_) => value.hash(&mut entry),
            },
            other => format!("{other:?}").hash(&mut entry),
        }
        entries = entries.wrapping_add(entry.finish());
    }
    let mut state = DefaultHasher::new();
    key.len().hash(&mut state);
    entries.hash(&mut state);
    state.finish()
}
