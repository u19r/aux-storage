use alloc_counter::AllocationGuard;
use serde_json::json;

use crate::{
    constants::{
        SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES, SYSTEM_STREAM_CHECKPOINT_TOKEN_ID_BYTES,
    },
    *,
};

fn scope() -> SystemStreamScope {
    SystemStreamScope::try_new("catalog", 7).expect("scope")
}

fn position(shard: &str, lineage: &str, offset: &str) -> SystemStreamPosition {
    SystemStreamPosition::try_new(shard, lineage, offset).expect("position")
}

fn token(nibble: char) -> SystemStreamCheckpointToken {
    SystemStreamCheckpointToken::try_from(format!("v1.{}", nibble.to_string().repeat(32)))
        .expect("syntactic token")
}

fn checkpoint() -> SystemStreamCheckpoint {
    SystemStreamCheckpoint::try_new(
        scope(),
        vec![
            position("shard-b", "lineage-b", "offset-b"),
            position("shard-a", "lineage-a", "offset-a"),
        ],
    )
    .expect("checkpoint")
}

#[test]
fn scope_wire_shape_is_versioned_and_validated() {
    let encoded = serde_json::to_value(scope()).expect("scope json");
    assert_eq!(
        encoded,
        json!({"Version": 1, "ScopeId": "catalog", "Generation": 7})
    );
    assert!(
        serde_json::from_value::<SystemStreamScope>(json!({
            "Version": 2,
            "ScopeId": "catalog",
            "Generation": 7,
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<SystemStreamScope>(json!({
            "Version": 1,
            "ScopeId": "catalog",
            "Generation": 7,
            "PhysicalTable": "must-not-be-carried",
        }))
        .is_err()
    );
}

#[test]
fn checkpoint_token_is_opaque_versioned_and_bounded() {
    let token = token('4');
    assert!(!token.as_str().contains("catalog"));
    assert!(!token.as_str().contains("shard-a"));
    assert_eq!(
        token.as_str().len(),
        SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES
    );
    assert_eq!(
        SystemStreamCheckpointToken::try_from(token.as_str()).expect("token"),
        token
    );
    assert_eq!(
        format!("{token:?}"),
        "SystemStreamCheckpointToken(redacted)"
    );
}

#[test]
fn checkpoint_token_rejects_malformed_versions_and_forged_issued_id() {
    let issued = token('4');
    let mut tampered = issued.as_str().to_string();
    tampered.replace_range(0..3, "v2.");
    assert_eq!(
        SystemStreamCheckpointToken::try_from(tampered),
        Err(SystemStreamContractError::UnsupportedCheckpointVersion)
    );
    assert!(matches!(
        SystemStreamCheckpointToken::try_from("legacy-raw-token"),
        Err(SystemStreamContractError::MalformedCheckpoint)
    ));
    assert!(matches!(
        SystemStreamCheckpointToken::try_from("v1.4242424242424242424242424242424A"),
        Err(SystemStreamContractError::NonCanonicalCheckpoint)
    ));
    let forged = token('3');
    assert_eq!(
        forged.ensure_issued([&issued]),
        Err(SystemStreamContractError::UnknownCheckpoint)
    );
}

fn legacy_token_parse(value: &str) -> Result<String, SystemStreamContractError> {
    let value = value.to_owned();
    if value.is_empty() || value.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }
    let Some(encoded_id) = value.strip_prefix("v1.") else {
        if value.starts_with('v') {
            return Err(SystemStreamContractError::UnsupportedCheckpointVersion);
        }
        return Err(SystemStreamContractError::MalformedCheckpoint);
    };
    if encoded_id.len() != SYSTEM_STREAM_CHECKPOINT_TOKEN_ID_BYTES * 2 {
        return Err(SystemStreamContractError::MalformedCheckpoint);
    }
    let id = hex::decode(encoded_id).map_err(|_| SystemStreamContractError::MalformedCheckpoint)?;
    if hex::encode(id) != encoded_id {
        return Err(SystemStreamContractError::NonCanonicalCheckpoint);
    }
    Ok(value)
}

#[test]
fn checkpoint_token_direct_validation_preserves_bytes_and_errors() {
    let canonical = token('4').as_str().to_owned();
    let mut uppercase = canonical.clone();
    uppercase.replace_range(3..4, "A");
    let mut malformed = canonical.clone();
    malformed.replace_range(3..4, "g");
    let inputs = [
        canonical.as_str(),
        uppercase.as_str(),
        malformed.as_str(),
        "v2.44444444444444444444444444444444",
        "v1.4444",
        "legacy-raw-token",
    ];

    for input in inputs {
        let expected = legacy_token_parse(input);
        let actual = SystemStreamCheckpointToken::try_from(input).map(String::from);
        assert_eq!(actual, expected, "token validation changed for {input:?}");
    }
}

fn measure_token_validation_allocations(
    label: &'static str,
    input: &str,
    candidate: bool,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "checkpoint_token_validation_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let parsed = if candidate {
            SystemStreamCheckpointToken::try_from(input).map(String::from)
        } else {
            legacy_token_parse(input)
        };
        let _ = std::hint::black_box(parsed);
    }
    guard.finish()
}

#[test]
fn checkpoint_token_direct_validation_avoids_decode_and_invalid_input_allocations() {
    let valid = token('4');
    let mut invalid = valid.as_str().to_owned();
    invalid.replace_range(3..4, "g");

    for input in [valid.as_str(), invalid.as_str()] {
        let legacy = measure_token_validation_allocations("legacy_hex_validation", input, false);
        let candidate = measure_token_validation_allocations("direct_validation", input, true);
        let _ = alloc_counter::emit_report(&legacy);
        let _ = alloc_counter::emit_report(&candidate);
        assert!(candidate.allocation_count < legacy.allocation_count);
        assert!(candidate.allocated_bytes < legacy.allocated_bytes);
    }
}

fn write_legacy_bounded_string(value: &SystemStreamScopeId, output: &mut Vec<u8>) {
    let owned: String = value.clone().into();
    serde_json::to_writer(output, &owned).expect("legacy serialization");
}

fn write_current_bounded_string(value: &SystemStreamScopeId, output: &mut Vec<u8>) {
    serde_json::to_writer(output, value).expect("current serialization");
}

fn measure_bounded_string_allocations(
    label: &'static str,
    write_value: fn(&SystemStreamScopeId, &mut Vec<u8>),
) -> alloc_counter::AllocationReport<'static> {
    let value = SystemStreamScopeId::try_new("catalog").expect("scope id");
    let mut output = Vec::with_capacity(16);
    let guard = AllocationGuard::start(
        module_path!(),
        "bounded_string_serialization_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        output.clear();
        write_value(&value, &mut output);
        std::hint::black_box(&output);
    }
    guard.finish()
}

#[test]
fn bounded_string_serializer_preserves_wire_bytes_without_cloning() {
    let value = SystemStreamScopeId::try_new("catalog").expect("scope id");
    let current = serde_json::to_vec(&value).expect("current bytes");
    let mut legacy = Vec::new();
    write_legacy_bounded_string(&value, &mut legacy);
    assert_eq!(current, legacy);

    let legacy_report =
        measure_bounded_string_allocations("legacy_clone_into_string", write_legacy_bounded_string);
    let current_report =
        measure_bounded_string_allocations("borrowed_serialize_str", write_current_bounded_string);
    let _ = alloc_counter::emit_report(&legacy_report);
    let _ = alloc_counter::emit_report(&current_report);
    assert!(current_report.allocation_count < legacy_report.allocation_count);
    assert!(current_report.allocated_bytes < legacy_report.allocated_bytes);
}

fn swap_frontier_positions(mut state: Vec<u8>) -> Vec<u8> {
    let mut cursor = 1;
    let scope_len = usize::from(u16::from_be_bytes([state[cursor], state[cursor + 1]]));
    cursor += 2 + scope_len + 8 + 2;
    let first_start = cursor;
    for _ in 0..3 {
        let length = usize::from(u16::from_be_bytes([state[cursor], state[cursor + 1]]));
        cursor += 2 + length;
    }
    let second_start = cursor;
    for _ in 0..3 {
        let length = usize::from(u16::from_be_bytes([state[cursor], state[cursor + 1]]));
        cursor += 2 + length;
    }
    let second_end = cursor;
    let first = state[first_start..second_start].to_vec();
    let second = state[second_start..second_end].to_vec();
    state.splice(first_start..second_end, second.into_iter().chain(first));
    state
}

fn measure_checkpoint_decode_allocations(
    label: &'static str,
    state: &[u8],
    reencode: bool,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "checkpoint_decode_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let checkpoint = SystemStreamCheckpoint::decode_state(state).expect("checkpoint state");
        if reencode {
            assert_eq!(checkpoint.encode_state().expect("re-encode"), state);
        }
        let _ = std::hint::black_box(checkpoint);
    }
    guard.finish()
}

#[test]
fn checkpoint_decode_checks_canonical_order_without_reencoding() {
    let state = checkpoint().encode_state().expect("state");
    assert_eq!(
        SystemStreamCheckpoint::decode_state(&state),
        Ok(checkpoint())
    );
    let noncanonical = swap_frontier_positions(state.clone());
    assert_eq!(
        SystemStreamCheckpoint::decode_state(&noncanonical),
        Err(SystemStreamContractError::NonCanonicalCheckpoint)
    );

    let legacy = measure_checkpoint_decode_allocations("decode_and_reencode", &state, true);
    let candidate = measure_checkpoint_decode_allocations("direct_canonical_decode", &state, false);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}

fn legacy_frontier_catalog_valid(
    checkpoint: &SystemStreamCheckpoint,
    catalog: &[SystemStreamPosition],
) -> bool {
    let catalog: Vec<_> = catalog.iter().collect();
    checkpoint.frontier().iter().all(|position| {
        catalog.iter().any(|candidate| {
            candidate.shard_id() == position.shard_id()
                && candidate.lineage_id() == position.lineage_id()
        })
    })
}

fn frontier_catalog_fixture() -> (SystemStreamCheckpoint, Vec<SystemStreamPosition>) {
    let frontier = (0..128)
        .map(|index| position(&format!("shard-{index:03}"), "lineage", "offset"))
        .collect();
    let checkpoint = SystemStreamCheckpoint::try_new(scope(), frontier).expect("frontier");
    let catalog = (0..8_192)
        .rev()
        .map(|index| position(&format!("shard-{index:03}"), "lineage", "offset"))
        .collect();
    (checkpoint, catalog)
}

#[test]
fn frontier_catalog_index_preserves_large_catalog_miss_error() {
    let (checkpoint, mut catalog) = frontier_catalog_fixture();
    catalog[8_191] = position("not-a-frontier-shard", "lineage", "offset");
    assert_eq!(
        checkpoint.validate_frontier_catalog(catalog.iter()),
        Err(SystemStreamContractError::UnknownLineage)
    );
}

#[test]
#[ignore = "isolated CPU probe; run explicitly for the paired workload"]
fn frontier_catalog_index_cpu_profile() {
    let (checkpoint, catalog) = frontier_catalog_fixture();
    let legacy_start = std::time::Instant::now();
    for _ in 0..16 {
        std::hint::black_box(legacy_frontier_catalog_valid(&checkpoint, &catalog));
    }
    let legacy = legacy_start.elapsed();

    let candidate_start = std::time::Instant::now();
    for _ in 0..16 {
        let _ = std::hint::black_box(checkpoint.validate_frontier_catalog(catalog.iter()));
    }
    let candidate = candidate_start.elapsed();
    eprintln!(
        "frontier_catalog_cpu_profile frontier=128 catalog=8192 legacy_ns={} candidate_ns={}",
        legacy.as_nanos(),
        candidate.as_nanos()
    );
    assert!(candidate < legacy);
}

#[test]
fn checkpoint_rejects_empty_oversized_and_duplicate_frontiers() {
    assert_eq!(
        SystemStreamCheckpoint::try_new(scope(), Vec::new()),
        Err(SystemStreamContractError::EmptyFrontier)
    );
    let duplicate = vec![
        position("shard-a", "lineage-a", "offset-a"),
        position("shard-a", "lineage-b", "offset-b"),
    ];
    assert_eq!(
        SystemStreamCheckpoint::try_new(scope(), duplicate),
        Err(SystemStreamContractError::DuplicateShard)
    );
    let oversized = (0..=SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES)
        .map(|index| position(&format!("shard-{index}"), "lineage", "offset"))
        .collect();
    assert_eq!(
        SystemStreamCheckpoint::try_new(scope(), oversized),
        Err(SystemStreamContractError::FrontierTooLarge)
    );
}

#[test]
fn checkpoint_state_codec_is_deterministic_and_validated() {
    let scope = SystemStreamScope::try_new("catalog", 7).expect("scope");
    let checkpoint = SystemStreamCheckpoint::try_new(
        scope,
        vec![
            SystemStreamPosition::try_new("shard-b", "lineage-b", "offset-b").expect("position"),
            SystemStreamPosition::try_new("shard-a", "lineage-a", "offset-a").expect("position"),
        ],
    )
    .expect("checkpoint");
    let state = checkpoint.encode_state().expect("state");
    assert_eq!(state, checkpoint.encode_state().expect("same state"));
    assert_eq!(SystemStreamCheckpoint::decode_state(&state), Ok(checkpoint));

    let mut trailing = state;
    trailing.push(0);
    assert_eq!(
        SystemStreamCheckpoint::decode_state(&trailing),
        Err(SystemStreamContractError::MalformedCheckpoint)
    );
}

#[test]
fn checkpoint_scope_generation_and_lineage_are_checked() {
    let checkpoint = checkpoint();
    let wrong_scope = SystemStreamScope::try_new("other", 7).expect("scope");
    assert_eq!(
        checkpoint.validate_scope(&wrong_scope),
        Err(SystemStreamContractError::WrongScope)
    );
    let wrong_generation = SystemStreamScope::try_new("catalog", 8).expect("scope");
    assert_eq!(
        checkpoint.validate_scope(&wrong_generation),
        Err(SystemStreamContractError::WrongGeneration)
    );
    let known = [
        position("shard-a", "lineage-a", "offset-a"),
        position("shard-b", "lineage-b", "offset-b"),
    ];
    assert!(checkpoint.validate_frontier_catalog(known.iter()).is_ok());
    let wrong_pair = [position("shard-a", "lineage-b", "offset-a")];
    assert_eq!(
        checkpoint.validate_frontier_catalog(wrong_pair.iter()),
        Err(SystemStreamContractError::UnknownLineage)
    );
    let missing = [position("shard-a", "lineage-a", "offset-a")];
    assert_eq!(
        checkpoint.validate_frontier_catalog(missing.iter()),
        Err(SystemStreamContractError::UnknownLineage)
    );
}
