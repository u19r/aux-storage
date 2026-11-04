use crate::{SystemStreamOffset, TableStreamSequence};

#[test]
fn given_native_offsets_when_sequences_roundtrip_then_all_stamp_bytes_and_order_are_preserved() {
    for offsets in [
        vec![
            "00000000000000000000",
            "00000000000000000001",
            "18446744073709551615",
        ],
        vec!["00000000000000000001-ns", "18446744073709551615-ns"],
        vec![
            "00000000000000000001-vs00000000",
            "00000000000000000001-vs00000001",
            "00000000000000000001-vs00010000",
            "00000000000000000002-vs00000000",
            "18446744073709551615-vsffffffff",
        ],
    ] {
        let mut previous = None;
        for offset in offsets {
            let offset = SystemStreamOffset::try_new(offset).unwrap();
            let sequence = TableStreamSequence::try_from(&offset).unwrap();
            let encoded = sequence.to_string();
            assert_eq!(encoded.len(), 40);
            assert!(encoded.bytes().all(|byte| byte.is_ascii_digit()));
            assert_eq!(encoded.parse::<TableStreamSequence>().unwrap(), sequence);
            assert_eq!(
                serde_json::from_str::<TableStreamSequence>(
                    &serde_json::to_string(&sequence).unwrap()
                )
                .unwrap(),
                sequence
            );
            if let Some(stamp) = offset.versionstamp() {
                assert_eq!(*sequence.native_key().as_bytes(), stamp);
            }
            if let Some(previous) = previous {
                assert!(sequence > previous);
                assert!(encoded > previous.to_string());
            }
            previous = Some(sequence);
        }
    }
}

#[test]
fn given_invalid_public_sequences_when_parsed_then_native_overflow_and_non_numeric_values_are_rejected()
 {
    for value in [
        "1".to_owned(),
        "0".repeat(20),
        "0".repeat(41),
        "-000000000000000000000".into(),
        "00000000000000000000a".into(),
        format!("{:040}", 1u128 << 96),
        format!("{:040}", u128::MAX),
    ] {
        assert!(value.parse::<TableStreamSequence>().is_err());
    }
    assert_eq!(
        "000000000000000000001"
            .parse::<TableStreamSequence>()
            .unwrap()
            .to_string(),
        format!("{:040}", 1)
    );
}
