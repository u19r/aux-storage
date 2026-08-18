use storage_types::{ItemStreamVersion, TableName};

use crate::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncMutationId, SyncMutationResponse,
    SyncProposalBatch, SyncProposalId, SyncProposalResponse, SyncPutMutation,
    mutation::SyncMutationError,
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

#[test]
fn given_sync_v1_batch_when_validated_then_peer_is_rejected() {
    let mut batch = ResolvedSyncMutationBatch::new(Vec::new());
    batch.protocol_version = 1;

    assert_eq!(
        batch.validate_protocol(),
        Err(SyncMutationError::IncompatibleProtocolVersion {
            expected: crate::SYNC_PROTOCOL_VERSION,
            actual: 1,
        })
    );
}

#[test]
fn given_sync_put_without_indexers_when_deserialized_then_peer_is_rejected() {
    let mut encoded = serde_json::to_value(sync_put_batch(vec!["customer_id".to_string()]))
        .expect("sync batch JSON");
    encoded["mutations"][0]["indexers"] = serde_json::Value::Null;
    encoded["mutations"][0]
        .as_object_mut()
        .expect("put mutation")
        .remove("indexers");

    assert!(serde_json::from_value::<ResolvedSyncMutationBatch>(encoded).is_err());
}

#[test]
fn given_sync_put_with_indexers_when_serialized_then_order_round_trips() {
    let batch = sync_put_batch(vec!["customer_id".to_string(), "region_id".to_string()]);
    let decoded: ResolvedSyncMutationBatch =
        serde_json::from_value(serde_json::to_value(&batch).expect("serialize sync batch"))
            .expect("deserialize sync batch");

    assert_eq!(decoded, batch);
    assert!(matches!(
        &decoded.mutations[0],
        ResolvedSyncMutation::Put(put)
            if put.indexers == ["customer_id", "region_id"]
    ));
}

fn sync_put_batch(indexers: Vec<String>) -> ResolvedSyncMutationBatch {
    ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new("mutation-1").expect("mutation id"),
        table_name: TableName::new("orders"),
        key_json: r#"{"pk":{"S":"tenant"}}"#.to_string(),
        item_json: r#"{"pk":{"S":"tenant"},"customer_id":{"S":"customer-1"}}"#.to_string(),
        indexers,
        old_item_json: None,
        old_indexers: None,
        target_item_stream_version: ItemStreamVersion::new(1),
        response: SyncMutationResponse::default(),
    })])
}
