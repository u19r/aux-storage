use crate::StreamName;

#[test]
fn stream_name_converts_between_owned_and_borrowed_bytes() {
    let stream = StreamName::from("events");

    assert_eq!(Vec::<u8>::from(&stream), b"events");
    assert_eq!(String::from(stream.clone()), "events");
    assert_eq!(stream.as_ref(), b"events");
}

#[test]
fn given_reserved_history_names_when_parsed_then_only_canonical_nonzero_ids_are_accepted() {
    let history = crate::TableHistoryId::from(42u32);
    assert_eq!(
        history.append_stream_name().table_history_id().unwrap(),
        Some(history)
    );
    assert_eq!(StreamName::from("events").table_history_id().unwrap(), None);
    for name in [
        "history/00000000000000000000000000000000/append-table",
        "history/not-a-table-id/append-table",
        "history/0000000000000000000000000000002A/append-table",
        "history/0000000000000000000000000000002a/",
    ] {
        assert!(StreamName::from(name).table_history_id().is_err());
    }
}
