use storage_types::{ErrorClass, storage_serde};

use super::{
    append_receipt::*,
    constants::{STREAM_APPEND_PROOF_BYTES, STREAM_APPEND_RECEIPT_ENCODED_BYTES},
};

#[test]
fn given_proof_lengths_when_constructed_then_only_one_to_bound_bytes_are_accepted() {
    let empty = StreamAppendProof::try_new(vec![]).unwrap_err();
    assert_eq!(empty.class(), ErrorClass::Internal);
    assert!(StreamAppendProof::try_new(vec![7]).is_ok());
    assert!(StreamAppendProof::try_new(vec![7; STREAM_APPEND_PROOF_BYTES]).is_ok());
    let over = StreamAppendProof::try_new(vec![7; STREAM_APPEND_PROOF_BYTES + 1]).unwrap_err();
    assert_eq!(over.class(), ErrorClass::Internal);

    assert!(
        serde_json::from_value::<StreamAppendProof>(serde_json::json!(vec![
            7u8;
            STREAM_APPEND_PROOF_BYTES
                + 1
        ]))
        .is_err()
    );
    let decoded =
        serde_json::from_value::<StreamAppendProof>(serde_json::json!(vec![7u8; 1])).unwrap();
    assert_eq!(decoded.as_bytes(), [7]);
}

#[test]
fn given_widest_receipt_when_encoded_then_within_encoded_bound_and_round_trips() {
    let request = TableAppendRequest::new(
        StreamAppendOperationId::fresh(),
        storage_types::TableHistoryTarget {
            table_name: storage_types::TableName::new("t"),
            history_id: storage_types::TableHistoryId::from([255; 16]),
        },
        b"x",
    );
    let mut receipt = StreamAppendReceipt::new(
        &request,
        StreamAppendProof::try_new(vec![255; STREAM_APPEND_PROOF_BYTES]).unwrap(),
        StreamAppendAllocation::AttemptAllocated(storage_types::StreamItemId::from([255; 12])),
    );
    receipt.request_digest = [255; 32];

    let json = serde_json::to_vec(&receipt).unwrap();
    assert!(
        json.len() + storage_types::STORAGE_SERDE_ENCODED_PREFIX_LEN
            <= STREAM_APPEND_RECEIPT_ENCODED_BYTES
    );
    let bytes = storage_serde::to_bytes(&receipt).unwrap();
    assert!(bytes.len() <= STREAM_APPEND_RECEIPT_ENCODED_BYTES);
    assert_eq!(
        storage_serde::from_bytes::<StreamAppendReceipt>(&bytes).unwrap(),
        receipt
    );
    println!(
        "OWN_B_RECEIPT_BYTES json={} encoded={} bound={}",
        json.len(),
        bytes.len(),
        STREAM_APPEND_RECEIPT_ENCODED_BYTES
    );
}
