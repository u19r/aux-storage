use std::collections::HashSet;

use alloc_counter::AllocationGuard;
use scheduled_bg_jobs::store::StoreKey;

use super::read::unique_keys;

const UNIQUE_KEYS_ITERATIONS: usize = 128;

fn duplicate_keys() -> Vec<StoreKey> {
    (0..100)
        .map(|index| {
            StoreKey::try_from(format!("scheduled-key-{}", index % 50).into_bytes())
                .expect("fixture key is non-empty")
        })
        .collect()
}

fn hashset_unique_keys(keys: &[StoreKey]) -> Vec<StoreKey> {
    let mut seen = HashSet::with_capacity(keys.len());
    let mut unique = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key) {
            unique.push(key.clone());
        }
    }
    unique
}

fn measure_linear_unique_keys(keys: &[StoreKey]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "scheduled_unique_keys_linear",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..UNIQUE_KEYS_ITERATIONS {
        std::hint::black_box(unique_keys(keys));
    }
    guard.finish()
}

fn measure_hashset_unique_keys(keys: &[StoreKey]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "scheduled_unique_keys_hashset",
        file!(),
        line!(),
        Some("candidate"),
    );
    for _ in 0..UNIQUE_KEYS_ITERATIONS {
        std::hint::black_box(hashset_unique_keys(keys));
    }
    guard.finish()
}

#[test]
fn given_bounded_duplicate_batch_when_deduplicating_then_linear_scan_avoids_hashset_overhead() {
    let keys = duplicate_keys();
    let linear = unique_keys(&keys);
    let hashset = hashset_unique_keys(&keys);
    assert_eq!(hashset, linear);

    let baseline = measure_linear_unique_keys(&keys);
    let candidate = measure_hashset_unique_keys(&keys);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(
        baseline.allocated_bytes < candidate.allocated_bytes,
        "expected bounded linear deduplication to allocate fewer bytes, baseline={} candidate={}",
        baseline.allocated_bytes,
        candidate.allocated_bytes
    );
}
