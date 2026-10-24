use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use storage_types::TimestampMillis;
use uuid::Uuid;

use super::execution_inputs::ReceiveMessagesInput;
use crate::{ReceiveMessageRequest, constants::MAX_RECEIVE_MESSAGES};

const ITERATIONS: usize = 1_000;

fn unique_entropy() -> Vec<Uuid> {
    (0..MAX_RECEIVE_MESSAGES)
        .map(|index| Uuid::from_u128(u128::from(index) + 1))
        .collect()
}

fn legacy_hash_set_validation(entropy: &[Uuid]) -> bool {
    let mut unique = std::collections::HashSet::with_capacity(entropy.len());
    entropy.iter().all(|value| unique.insert(*value))
}

fn measure_candidate(entropy: &[Uuid]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "receive_entropy_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("bounded_linear_scan"),
    );
    let mut valid = true;
    for _ in 0..ITERATIONS {
        valid &= entropy
            .iter()
            .enumerate()
            .all(|(index, value)| !entropy[..index].iter().any(|previous| previous == value));
    }
    black_box(valid);
    guard.finish()
}

fn measure_legacy(entropy: &[Uuid]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "receive_entropy_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("hash_set"),
    );
    let mut valid = true;
    for _ in 0..ITERATIONS {
        valid &= legacy_hash_set_validation(entropy);
    }
    black_box(valid);
    guard.finish()
}

#[test]
fn given_ten_unique_receipt_entropies_when_validating_then_no_hash_table_is_allocated() {
    let entropy = unique_entropy();
    let legacy = measure_legacy(&entropy);
    let candidate = measure_candidate(&entropy);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);

    assert_eq!(candidate.allocation_count, 0);
    assert_eq!(candidate.allocated_bytes, 0);
    assert!(legacy.allocation_count > candidate.allocation_count);
    assert!(legacy.allocated_bytes > candidate.allocated_bytes);
}

#[test]
fn given_duplicate_receipt_entropies_when_validating_then_the_existing_error_is_preserved() {
    let mut entropy = unique_entropy();
    entropy[9] = entropy[0];
    let input = ReceiveMessagesInput::try_new(
        ReceiveMessageRequest {
            queue_url: "https://queue.example.test/jobs".to_string(),
            max_number_of_messages: Some(10),
            visibility_timeout: Some(30),
            wait_time_seconds: None,
            attribute_names: None,
            message_attribute_names: None,
        },
        TimestampMillis::from(1),
        entropy,
    )
    .expect_err("duplicate entropy must be rejected");
    assert_eq!(
        input.aws_query_message(),
        "receive message receipt-handle entropy supply contains duplicates"
    );
}

#[test]
#[ignore = "manual CPU probe; run with --ignored --nocapture"]
fn receive_entropy_validation_cpu_probe() {
    let entropy = unique_entropy();
    let legacy_started = Instant::now();
    let mut legacy_checksum = false;
    for _ in 0..100_000 {
        legacy_checksum ^= legacy_hash_set_validation(&entropy);
    }
    let legacy_elapsed = legacy_started.elapsed();

    let candidate_started = Instant::now();
    let mut candidate_checksum = false;
    for _ in 0..100_000 {
        candidate_checksum ^= entropy
            .iter()
            .enumerate()
            .all(|(index, value)| !entropy[..index].iter().any(|previous| previous == value));
    }
    let candidate_elapsed = candidate_started.elapsed();
    println!(
        "hash_set elapsed_ns={} bounded_linear_scan elapsed_ns={} reduction_pct={:.2} \
         checksum={legacy_checksum}/{candidate_checksum}",
        legacy_elapsed.as_nanos(),
        candidate_elapsed.as_nanos(),
        (1.0 - candidate_elapsed.as_nanos() as f64 / legacy_elapsed.as_nanos() as f64) * 100.0,
    );
}
