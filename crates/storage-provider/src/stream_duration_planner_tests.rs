use storage_types::{
    AttributeValue, KeyAttributes, StreamRetentionDuration, TableName, TimestampMillis,
};

use crate::{
    ItemStreamTtlIntent, StreamTrimScopeKind, plan_item_stream_duration,
    plan_table_stream_duration, validate_transaction_item_ttl_intents,
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

    assert_eq!(finite.trim_state.scope.kind, StreamTrimScopeKind::Table);
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
    assert_eq!(forever.due_marker, None);
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
    let plan = plan_item_stream_duration(
        ItemStreamTtlIntent {
            table_name: TableName::new("orders"),
            item_key: key("1"),
            retention: StreamRetentionDuration::FiniteHours(1),
        },
        "item/orders/1",
        "hash-1",
        3,
        StreamRetentionDuration::FiniteHours(72),
        TimestampMillis::from_timestamp(1_000),
    )
    .expect("item ttl should plan");

    assert_eq!(plan.trim_state.scope.kind, StreamTrimScopeKind::Item);
    assert_eq!(
        plan.requested_retention,
        StreamRetentionDuration::FiniteHours(1)
    );
    assert_eq!(
        plan.effective_retention,
        StreamRetentionDuration::FiniteHours(72)
    );
    assert_eq!(
        plan.trim_state.next_due_at,
        Some(TimestampMillis::from_timestamp(259_201_000))
    );
}

#[test]
fn item_duration_plan_keeps_forever_unscheduled() {
    let plan = plan_item_stream_duration(
        ItemStreamTtlIntent {
            table_name: TableName::new("orders"),
            item_key: key("1"),
            retention: StreamRetentionDuration::Forever,
        },
        "item/orders/1",
        "hash-1",
        3,
        StreamRetentionDuration::FiniteHours(72),
        TimestampMillis::from_timestamp(1_000),
    )
    .expect("item ttl should plan");

    assert_eq!(plan.effective_retention, StreamRetentionDuration::Forever);
    assert_eq!(plan.trim_state.next_due_at, None);
    assert_eq!(plan.due_marker, None);
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
