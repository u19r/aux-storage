use replication_contract_tests::HarnessResult;
use replication_provider::recovery::receipt::{
    AttemptBinding, ReceiptObservation, RecoveryDecision,
};
use replication_types::protocol::CanonicalDigest;

#[test]
fn given_unknown_commit_when_no_receipt_exists_then_wait_without_retry() -> HarnessResult<()> {
    let binding = AttemptBinding {
        operation: [1; 16].try_into()?,
        content: CanonicalDigest::of(b"input"),
    };
    assert!(matches!(
        binding.reconcile::<()>(ReceiptObservation::Unresolved)?,
        RecoveryDecision::Wait
    ));
    assert!(matches!(
        binding.reconcile(ReceiptObservation::Durable {
            binding,
            receipt: 42
        })?,
        RecoveryDecision::Completed(42)
    ));
    assert!(matches!(
        binding.reconcile::<()>(ReceiptObservation::ProvenAborted { binding })?,
        RecoveryDecision::RetryFresh
    ));
    let mismatch = AttemptBinding {
        content: CanonicalDigest::of(b"other"),
        ..binding
    };
    assert!(
        binding
            .reconcile::<()>(ReceiptObservation::ProvenAborted { binding: mismatch })
            .is_err()
    );
    let wrong_operation = AttemptBinding {
        operation: [2; 16].try_into()?,
        ..binding
    };
    for other in [mismatch, wrong_operation] {
        assert!(
            binding
                .reconcile(ReceiptObservation::Durable {
                    binding: other,
                    receipt: 42
                })
                .is_err()
        );
    }
    Ok(())
}
