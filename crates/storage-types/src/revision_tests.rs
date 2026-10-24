use crate::Revision;

#[test]
fn given_unsigned_revisions_when_serialized_then_scalar_values_round_trip() {
    for value in [0, 1, i64::MAX as u64, 1_u64 << 63, u64::MAX] {
        let revision = Revision::from(value);
        let json = serde_json::to_string(&revision).expect("revision serialization");
        assert_eq!(json, value.to_string());
        assert_eq!(
            serde_json::from_str::<Revision>(&json).expect("revision deserialization"),
            revision
        );
        assert_eq!(u64::from(revision), value);
    }
}
