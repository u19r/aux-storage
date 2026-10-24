use std::num::{NonZeroU32, NonZeroUsize};

use alloc_counter::AllocationGuard;
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    Check, CommitOperationId, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use storage_types::{AttributeMap, AttributeValue, KeyAttributes};

use super::encoding::{
    data_item, decode_continuation, decode_data_item, decode_receipt_fingerprint, receipt_item,
    transaction_fingerprint,
};
use crate::provider::scheduled_job_store::constants::{
    KEY_ATTRIBUTE, PARTITION_KEY, SORT_ATTRIBUTE, VALUE_ATTRIBUTE,
};

const RECEIPT_DECODE_ITERATIONS: usize = 128;

fn transaction(max_bytes: usize) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::from([7; 16]),
        checks: vec![Check::Absent(StoreKey::try_from(b"k".as_slice()).unwrap())],
        mutations: vec![Mutation::Put(
            StoreKey::try_from(b"k".as_slice()).unwrap(),
            Bytes::from_static(b"v"),
        )],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(4).unwrap(),
            max_mutations: NonZeroU32::new(4).unwrap(),
            max_bytes: NonZeroUsize::new(max_bytes).unwrap(),
        },
    })
    .unwrap()
}

#[test]
fn given_different_request_limits_when_canonicalizing_then_receipt_identity_is_stable() {
    assert_eq!(
        transaction_fingerprint(&transaction(128)),
        transaction_fingerprint(&transaction(512))
    );
}

#[test]
fn given_binary_data_when_encoding_then_dynamodb_base64_round_trips() {
    let key = StoreKey::try_from(b"key".as_slice()).expect("key is valid");
    let item = AttributeMap::try_from(data_item(&key, b"value").expect("encode row"))
        .expect("decode wire row");
    assert_eq!(
        item.get(VALUE_ATTRIBUTE),
        Some(&AttributeValue::B("dmFsdWU=".to_string()))
    );
    let decoded = decode_data_item(&item).expect("valid data row");
    assert_eq!(decoded, (key, Bytes::from_static(b"value")));
}

fn measure_receipt_decode_with_redundant_copy(
    item: &AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "receipt_decode_with_redundant_copy",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..RECEIPT_DECODE_ITERATIONS {
        let decoded = decode_receipt_fingerprint(item).expect("receipt decodes");
        std::hint::black_box(decoded.to_vec());
    }
    guard.finish()
}

fn measure_receipt_decode_without_copy(
    item: &AttributeMap,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "receipt_decode_without_copy",
        file!(),
        line!(),
        Some("candidate"),
    );
    for _ in 0..RECEIPT_DECODE_ITERATIONS {
        std::hint::black_box(decode_receipt_fingerprint(item).expect("receipt decodes"));
    }
    guard.finish()
}

#[test]
fn given_encoded_receipt_when_decoding_content_then_result_is_not_copied_again() {
    let item = AttributeMap::try_from(
        receipt_item(CommitOperationId::from([9; 16]), &[7; 32], "committed")
            .expect("encode receipt"),
    )
    .expect("decode receipt wire row");
    let baseline = measure_receipt_decode_with_redundant_copy(&item);
    let candidate = measure_receipt_decode_without_copy(&item);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(
        candidate.allocated_bytes < baseline.allocated_bytes,
        "expected receipt decode to avoid a second allocation, baseline={} candidate={}",
        baseline.allocated_bytes,
        candidate.allocated_bytes
    );
}

#[test]
fn given_a_foreign_partition_when_decoding_data_then_the_row_is_rejected() {
    let mut item = AttributeMap::try_from(
        data_item(&StoreKey::try_from(b"key".as_slice()).unwrap(), b"value").expect("encode row"),
    )
    .expect("decode wire row");
    item.insert(
        KEY_ATTRIBUTE.to_string(),
        AttributeValue::S("other-partition".to_string()),
    );
    assert!(decode_data_item(&item).is_err());
}

#[test]
fn given_a_receipt_cursor_when_decoding_a_range_then_the_cursor_is_rejected() {
    let key = KeyAttributes::from([
        (
            KEY_ATTRIBUTE.to_string(),
            AttributeValue::S(PARTITION_KEY.to_string()),
        ),
        (
            SORT_ATTRIBUTE.to_string(),
            AttributeValue::S("R#receipt".to_string()),
        ),
    ]);
    assert!(decode_continuation(&key).is_err());
}
