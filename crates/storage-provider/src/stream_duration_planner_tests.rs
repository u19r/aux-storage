use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, KeyAttributes, StreamRetentionDuration, TableName, TimestampMillis,
};

use crate::{
    StreamTrimScopeKind,
    maintenance::{
        ItemStreamDurationInput, ItemStreamTtlIntent, plan_item_stream_duration,
        validate_transaction_item_ttl_intents,
    },
    plan_table_stream_duration,
};

fn key(value: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".to_string(), AttributeValue::S(value.to_string()))])
}

#[test]
fn table_duration_plan_schedules_finite_and_skips_forever_due_marker() {
    let finite = plan_table_stream_duration(
        TableName::new("orders"),
        "table/orders",
        7,
        StreamRetentionDuration::FiniteHours(2),
        StreamRetentionDuration::FiniteHours(2),
        TimestampMillis::from_timestamp(1_000),
    );

    assert!(finite.trim_state.scope.kind == StreamTrimScopeKind::Table);
    assert_eq!(
        finite.trim_state.next_due_at,
        Some(TimestampMillis::from_timestamp(7_201_000))
    );
    assert!(finite.due_marker.is_some());

    let forever = plan_table_stream_duration(
        TableName::new("orders"),
        "table/orders",
        8,
        StreamRetentionDuration::Forever,
        StreamRetentionDuration::FiniteHours(2),
        TimestampMillis::from_timestamp(1_000),
    );
    assert_eq!(forever.trim_state.next_due_at, None);
    assert!(forever.due_marker.is_none());
}

#[test]
fn table_duration_plan_schedules_default_shrink_and_expansion() {
    let default_plan = plan_table_stream_duration(
        TableName::new("orders"),
        "table/orders",
        1,
        StreamRetentionDuration::default(),
        StreamRetentionDuration::default(),
        TimestampMillis::from_timestamp(1_000),
    );
    assert_eq!(
        default_plan.trim_state.next_due_at,
        Some(TimestampMillis::from_timestamp(259_201_000))
    );

    let shrink = plan_table_stream_duration(
        TableName::new("orders"),
        "table/orders",
        2,
        StreamRetentionDuration::FiniteHours(1),
        StreamRetentionDuration::FiniteHours(1),
        TimestampMillis::from_timestamp(1_000),
    );
    let expansion = plan_table_stream_duration(
        TableName::new("orders"),
        "table/orders",
        3,
        StreamRetentionDuration::FiniteHours(168),
        StreamRetentionDuration::FiniteHours(168),
        TimestampMillis::from_timestamp(1_000),
    );

    assert!(shrink.trim_state.next_due_at < default_plan.trim_state.next_due_at);
    assert!(expansion.trim_state.next_due_at > default_plan.trim_state.next_due_at);
    assert_ne!(shrink.policy_version, expansion.policy_version);
}

#[test]
fn item_duration_plan_clamps_effective_retention_to_table_retention() {
    let plan = plan_item_stream_duration(ItemStreamDurationInput {
        table_name: TableName::new("orders"),
        scope_id: "item/orders/1".into(),
        key_hash: "hash-1".into(),
        requested_retention: StreamRetentionDuration::FiniteHours(1),
        table_retention: StreamRetentionDuration::FiniteHours(72),
        updated_at: TimestampMillis::from_timestamp(1_000),
    });

    assert!(plan.state.scope.kind == StreamTrimScopeKind::Item);
    assert_eq!(
        plan.state.retention,
        StreamRetentionDuration::FiniteHours(1)
    );
    assert_eq!(
        plan.state.effective_retention,
        StreamRetentionDuration::FiniteHours(72)
    );
    assert_eq!(
        plan.state.next_due_at,
        Some(TimestampMillis::from_timestamp(259_201_000))
    );
}

#[test]
fn item_duration_plan_keeps_forever_unscheduled() {
    let plan = plan_item_stream_duration(ItemStreamDurationInput {
        table_name: TableName::new("orders"),
        scope_id: "item/orders/1".into(),
        key_hash: "hash-1".into(),
        requested_retention: StreamRetentionDuration::Forever,
        table_retention: StreamRetentionDuration::FiniteHours(72),
        updated_at: TimestampMillis::from_timestamp(1_000),
    });

    assert_eq!(
        plan.state.effective_retention,
        StreamRetentionDuration::Forever
    );
    assert_eq!(plan.state.next_due_at, None);
    assert!(plan.next_marker.is_none());
}

#[test]
fn transaction_ttl_intents_reject_conflicts_for_same_item() {
    let table_name = TableName::new("orders");
    let intents = vec![
        ItemStreamTtlIntent {
            table_name: table_name.clone(),
            item_key: key("1"),
            retention: StreamRetentionDuration::FiniteHours(1),
        },
        ItemStreamTtlIntent {
            table_name,
            item_key: key("1"),
            retention: StreamRetentionDuration::FiniteHours(2),
        },
    ];

    let err = validate_transaction_item_ttl_intents(&intents)
        .expect_err("conflicting declarations should fail");

    assert!(
        err.to_string()
            .contains("conflicting custom item stream TTL")
    );
}

#[test]
fn transaction_ttl_intents_allow_repeated_same_policy_for_same_item() {
    let table_name = TableName::new("orders");
    let intents = vec![
        ItemStreamTtlIntent {
            table_name: table_name.clone(),
            item_key: key("1"),
            retention: StreamRetentionDuration::FiniteHours(2),
        },
        ItemStreamTtlIntent {
            table_name,
            item_key: key("1"),
            retention: StreamRetentionDuration::FiniteHours(2),
        },
    ];

    validate_transaction_item_ttl_intents(&intents)
        .expect("same ttl declaration for same item should be idempotent");
}

#[test]
fn transaction_ttl_intents_validation_drops_the_disposable_seen_allocation() {
    let intents = (0..64)
        .map(|index| ItemStreamTtlIntent {
            table_name: TableName::new("orders"),
            item_key: key(&index.to_string()),
            retention: StreamRetentionDuration::FiniteHours(2),
        })
        .collect::<Vec<_>>();

    let baseline = measure_seen_vector_baseline(&intents);
    let optimized = measure_current_validation(&intents);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(optimized.allocation_count < baseline.allocation_count);
    assert!(optimized.allocated_bytes < baseline.allocated_bytes);
}

fn measure_seen_vector_baseline(
    intents: &[ItemStreamTtlIntent],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "transaction_ttl_intents_seen_vector_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..64 {
        let mut seen: Vec<&ItemStreamTtlIntent> = Vec::with_capacity(intents.len());
        for intent in intents {
            if intent.item_key.is_empty() {
                break;
            }
            if seen.iter().any(|existing| {
                existing.table_name == intent.table_name
                    && existing.item_key == intent.item_key
                    && existing.retention != intent.retention
            }) {
                break;
            }
            seen.push(intent);
        }
        std::hint::black_box(seen);
    }
    guard.finish()
}

fn measure_current_validation(
    intents: &[ItemStreamTtlIntent],
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "transaction_ttl_intents_current_validation",
        file!(),
        line!(),
        Some("optimized"),
    );
    for _ in 0..64 {
        let result = validate_transaction_item_ttl_intents(intents);
        drop(std::hint::black_box(result));
    }
    guard.finish()
}

#[test]
fn given_item_duration_when_planned_then_policy_identity_matches_effective_duration() {
    let plan = crate::plan_item_stream_duration(crate::ItemStreamDurationInput {
        table_name: TableName::new("orders"),
        scope_id: ("item/orders/1").into(),
        key_hash: ("hash-1").into(),
        requested_retention: StreamRetentionDuration::FiniteHours(24),
        table_retention: StreamRetentionDuration::FiniteHours(72),
        updated_at: TimestampMillis::from_timestamp(1_000),
    });
    assert_eq!(plan.state.policy_version, (24_u64 << 32) | 72);
    assert_eq!(
        plan.next_marker.unwrap().policy_version,
        plan.state.policy_version
    );
}

#[test]
fn given_duration_pairs_when_planned_then_identity_is_stable_and_distinguishes_policy() {
    use StreamRetentionDuration::{FiniteHours, Forever};
    for (requested, table, expected) in [
        (FiniteHours(24), FiniteHours(72), (24_u64 << 32) | 72),
        (FiniteHours(25), FiniteHours(72), (25_u64 << 32) | 72),
        (FiniteHours(24), FiniteHours(73), (24_u64 << 32) | 73),
        (FiniteHours(72), FiniteHours(24), (72_u64 << 32) | 72),
        (FiniteHours(72), FiniteHours(25), (72_u64 << 32) | 72),
        (
            FiniteHours(24),
            Forever,
            (24_u64 << 32) | u64::from(u32::MAX),
        ),
        (Forever, FiniteHours(24), u64::MAX),
        (Forever, Forever, u64::MAX),
    ] {
        for time in [1_000, 2_000] {
            let plan = plan_item_stream_duration(ItemStreamDurationInput {
                table_name: TableName::new("orders"),
                scope_id: "item/orders/1".into(),
                key_hash: "hash-1".into(),
                requested_retention: requested,
                table_retention: table,
                updated_at: TimestampMillis::from_timestamp(time),
            });
            assert_eq!(plan.state.policy_version, expected);
            if let Some(marker) = plan.next_marker {
                assert_eq!(marker.policy_version, expected);
                assert!(plan.state.next_due_at.is_some());
            } else {
                assert_eq!(plan.state.effective_retention, Forever);
            }
        }
    }
}
