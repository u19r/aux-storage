use crate::ItemProvenance;

#[test]
fn given_local_or_opaque_stored_metadata_when_read_and_serialized_then_exact_bytes_and_redaction_survive()
 {
    for bytes in [
        ItemProvenance::LOCAL_BYTES,
        br#"{"kind":"replicated","winner":{"future":42}}"#,
        r#"{ "future": [1e2, "葉\\\"", {"duplicate":1,"duplicate":2}] }"#.as_bytes(),
        b"{}",
    ] {
        let value = ItemProvenance::decode(bytes).unwrap();
        let read: ItemProvenance = serde_json::from_slice(bytes).unwrap();
        assert_eq!(value, read);
        assert_eq!(value.as_bytes(), bytes);
        assert_eq!(serde_json::to_vec(&value).unwrap(), bytes);
        assert_eq!(serde_json::to_vec(&read).unwrap(), bytes);
        assert_eq!(format!("{value:?}"), "ItemProvenance(<redacted>)");
        assert_eq!(value.clone(), value);
        let padded = [b" ".as_slice(), bytes, b"\n"].concat();
        assert!(ItemProvenance::decode(&padded).is_err());
        assert_eq!(
            serde_json::from_slice::<ItemProvenance>(&padded).unwrap(),
            value
        );
    }
    assert_eq!(
        serde_json::to_vec(&ItemProvenance::default()).unwrap(),
        ItemProvenance::LOCAL_BYTES
    );
}

#[test]
fn given_invalid_provenance_envelopes_when_read_then_both_boundaries_reject_them() {
    for bytes in [
        b"null".as_slice(),
        b"[]",
        b"42",
        b"\"text\"",
        b"{",
        b"{} {}",
        b"{\"x\":\xff}",
    ] {
        assert!(ItemProvenance::decode(bytes).is_err());
        assert!(serde_json::from_slice::<ItemProvenance>(bytes).is_err());
    }
}
