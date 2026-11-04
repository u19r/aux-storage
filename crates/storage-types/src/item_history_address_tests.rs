use crate::{ItemHistoryAddress, StreamName};

#[test]
fn given_two_labels_when_history_id_is_equal_then_physical_history_is_equal() {
    for suffix in ["stream-table", "stream-item/key/with/slashes"] {
        let first = ItemHistoryAddress {
            history_id: 1_u32.into(),
            stream_name: StreamName::from(format!("first/{suffix}")),
        };
        let renamed = ItemHistoryAddress {
            history_id: first.history_id,
            stream_name: StreamName::from(format!("renamed/{suffix}")),
        };
        let physical = first.physical_stream_name().unwrap();
        assert_eq!(physical, renamed.physical_stream_name().unwrap());
        assert_eq!(
            physical.as_ref(),
            format!("history/00000000000000000000000000000001/{suffix}").as_bytes()
        );
    }
}

#[test]
fn given_reused_name_when_history_id_changes_then_old_data_has_a_different_namespace() {
    let old = ItemHistoryAddress {
        history_id: 1_u32.into(),
        stream_name: StreamName::from("orders/stream-item/key"),
    };
    let fresh = ItemHistoryAddress {
        history_id: 2_u32.into(),
        stream_name: old.stream_name.clone(),
    };
    assert_ne!(
        old.physical_stream_name().unwrap(),
        fresh.physical_stream_name().unwrap()
    );
}

#[test]
fn given_invalid_logical_history_when_lowered_then_no_physical_fallback_is_selected() {
    for name in [
        "orders",
        "/stream-table",
        "orders/arbitrary",
        "orders/stream-item/",
        "orders/stream-table/extra",
    ] {
        let address = ItemHistoryAddress {
            history_id: 1_u32.into(),
            stream_name: StreamName::from(name),
        };
        assert!(address.physical_stream_name().is_err());
    }
}
