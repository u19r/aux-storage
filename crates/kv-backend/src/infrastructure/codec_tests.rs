use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, DurableRecordKey, RecordAddress};
use storage_types::{InfrastructureResultKind, StorageEnum, StorageResult};

use super::codec::{decode_record, encode_record};

#[test]
fn given_unsigned_revision_and_opaque_payload_when_encoded_then_only_revision_prefix_is_added()
-> StorageResult<()> {
    let key = key()?;
    let record = DurableRecord {
        key: key.clone(),
        revision: u64::MAX.into(),
        bytes: Arc::from(&b"\0opaque\xff"[..]),
    };
    let mut expected = u64::MAX.to_be_bytes().to_vec();
    expected.extend_from_slice(&record.bytes);
    assert_eq!(expected, encode_record(&record)?);
    let decoded = decode_record(key.clone(), expected)?;
    assert_eq!(key, decoded.key);
    assert_eq!(record.revision, decoded.revision);
    assert_eq!(record.bytes, decoded.bytes);
    Ok(())
}

#[test]
fn given_truncated_revision_when_decoded_then_typed_encoding_error_preserves_absence()
-> StorageResult<()> {
    for length in 0..8 {
        let result = decode_record(key()?, vec![0; length]);
        assert!(matches!(
            result.unwrap_err().as_ref(),
            StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { .. })
        ));
    }
    let decoded = decode_record(key()?, vec![0; 8])?;
    assert_eq!(0, u64::from(decoded.revision));
    assert!(decoded.bytes.is_empty());
    Ok(())
}

fn key() -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "codec".into(),
        resource: b"r".to_vec(),
        generation: 1,
        suffix: vec![],
    }
    .try_into()
}
