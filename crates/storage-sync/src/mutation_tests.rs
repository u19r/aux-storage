use crate::{
    ResolvedSyncMutationBatch, SyncMutationId, SyncProposalBatch, SyncProposalId,
    SyncProposalResponse, mutation::SyncMutationError,
};

#[test]
fn sync_mutation_id_rejects_empty_values() {
    let error = SyncMutationId::new("  ").expect_err("empty id should fail");

    assert_eq!(error, SyncMutationError::EmptyMutationId);
    assert_eq!(error.to_string(), "sync mutation id must not be empty");
}

#[test]
fn sync_proposal_id_rejects_empty_values() {
    let error = SyncProposalId::new("  ").expect_err("empty id should fail");

    assert_eq!(error, SyncMutationError::EmptyProposalId);
    assert_eq!(error.to_string(), "sync proposal id must not be empty");
}

#[test]
fn sync_proposal_batch_and_response_keep_proposal_identity() {
    let proposal_id = SyncProposalId::new("proposal-1").unwrap();
    let batch = SyncProposalBatch::new(
        proposal_id.clone(),
        ResolvedSyncMutationBatch::new(Vec::new()),
    );
    let response = SyncProposalResponse::new(proposal_id.clone(), Vec::new());

    assert!(batch.is_empty());
    assert_eq!(batch.proposal_id, proposal_id);
    assert_eq!(response.proposal_id.as_str(), "proposal-1");
}
