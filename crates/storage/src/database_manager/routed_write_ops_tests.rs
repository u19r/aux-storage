use storage_types::{TableName, TableNamespace};

use super::{RoutedWriteTargetRole, WriteTargetSet, fan_out_route_write_payload};
use crate::namespace_routing::{NamespaceRoute, NamespaceStorageMode, RouteTarget};

fn route_with_two_targets() -> NamespaceRoute {
    NamespaceRoute {
        namespace: TableNamespace::new(),
        storage_mode: NamespaceStorageMode::Dedicated,
        read_target: RouteTarget {
            connection_id: "conn-a".to_string(),
            table_name: TableName::new("namespace-a"),
            loc: 1,
        },
        write_targets: vec![
            RouteTarget {
                connection_id: "conn-a".to_string(),
                table_name: TableName::new("namespace-a"),
                loc: 1,
            },
            RouteTarget {
                connection_id: "conn-b".to_string(),
                table_name: TableName::new("namespace-b"),
                loc: 2,
            },
        ],
        writes_paused: false,
    }
}

#[test]
fn write_target_set_clones_for_n_minus_one_and_consumes_once() {
    let mut targets =
        WriteTargetSet::new(2, String::from("payload"), "payload").expect("build write target set");

    assert_eq!(targets.take(0).expect("first target"), "payload");
    assert_eq!(targets.take(1).expect("second target"), "payload");
    assert!(targets.take(1).is_err());
}

#[test]
fn fan_out_route_write_payload_assigns_primary_then_migration_roles() {
    let route = route_with_two_targets();
    let mut seen = Vec::new();

    fan_out_route_write_payload(
        &route,
        String::from("payload"),
        "payload",
        |target, role, payload| {
            seen.push((target.connection_id.clone(), role, payload));
            Ok(())
        },
    )
    .expect("fan out routed payload");

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1, RoutedWriteTargetRole::Primary);
    assert_eq!(seen[1].1, RoutedWriteTargetRole::Migration);
    assert_eq!(seen[0].2, "payload");
    assert_eq!(seen[1].2, "payload");
}
