use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncMutationId, SyncMutationResponse,
    SyncProposalBatch, SyncProposalId, SyncPutMutation,
};
use storage_types::{ItemStreamVersion, TableName};

use crate::manager::sync_raft_proposal_coalescer::SyncRaftProposalCoalescer;

#[tokio::test]
async fn given_safe_disjoint_proposals_when_concurrent_then_coalesces_into_one_batch() {
    let coalescer = SyncRaftProposalCoalescer::new(Duration::from_millis(10));
    let propose_calls = Arc::new(AtomicUsize::new(0));
    let left_calls = propose_calls.clone();
    let right_calls = propose_calls.clone();

    let left = coalescer.propose(item_proposal("left", "item#1", 1), move |batch| {
        left_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 2);
            Ok(vec![response("left"), response("right")])
        }
    });
    let right = coalescer.propose(item_proposal("right", "item#2", 2), move |batch| {
        right_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 1);
            Ok(vec![response("unexpected")])
        }
    });

    let (left, right) = tokio::join!(left, right);

    assert_eq!(propose_calls.load(Ordering::Acquire), 1);
    assert_eq!(response_labels(&left.expect("left response")), vec!["left"]);
    assert_eq!(
        response_labels(&right.expect("right response")),
        vec!["right"]
    );
}

#[tokio::test]
async fn given_conflicting_proposals_when_concurrent_then_preserves_serial_batches() {
    let coalescer = SyncRaftProposalCoalescer::new(Duration::from_millis(10));
    let propose_calls = Arc::new(AtomicUsize::new(0));
    let left_calls = propose_calls.clone();
    let right_calls = propose_calls.clone();

    let left = coalescer.propose(item_proposal("left", "item#1", 1), move |batch| {
        left_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 1);
            Ok(vec![response("left")])
        }
    });
    let right = coalescer.propose(item_proposal("right", "item#1", 2), move |batch| {
        right_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 1);
            Ok(vec![response("right")])
        }
    });

    let (left, right) = tokio::join!(left, right);

    assert_eq!(propose_calls.load(Ordering::Acquire), 2);
    assert_eq!(response_labels(&left.expect("left response")), vec!["left"]);
    assert_eq!(
        response_labels(&right.expect("right response")),
        vec!["right"]
    );
}

#[tokio::test]
async fn given_batch_would_exceed_operation_cap_when_concurrent_then_preserves_serial_batches() {
    let coalescer =
        SyncRaftProposalCoalescer::new_with_max_operations(Duration::from_millis(10), 1);
    let propose_calls = Arc::new(AtomicUsize::new(0));
    let left_calls = propose_calls.clone();
    let right_calls = propose_calls.clone();

    let left = coalescer.propose(item_proposal("left", "item#1", 1), move |batch| {
        left_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 1);
            Ok(vec![response("left")])
        }
    });
    let right = coalescer.propose(item_proposal("right", "item#2", 2), move |batch| {
        right_calls.fetch_add(1, Ordering::AcqRel);
        async move {
            assert_eq!(batch.mutations.len(), 1);
            Ok(vec![response("right")])
        }
    });

    let (left, right) = tokio::join!(left, right);

    assert_eq!(propose_calls.load(Ordering::Acquire), 2);
    assert_eq!(response_labels(&left.expect("left response")), vec!["left"]);
    assert_eq!(
        response_labels(&right.expect("right response")),
        vec!["right"]
    );
}

fn item_proposal(id: &str, key: &str, version: u64) -> SyncProposalBatch {
    SyncProposalBatch::new(
        SyncProposalId::new(id).expect("proposal id"),
        ResolvedSyncMutationBatch::new(vec![ResolvedSyncMutation::Put(SyncPutMutation {
            indexers: Vec::new(),
            old_indexers: None,
            mutation_id: SyncMutationId::new(format!("{id}#put")).expect("mutation id"),
            table_name: TableName::new("table"),
            key_json: key_json(key),
            item_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
            old_item_json: None,
            target_item_stream_version: ItemStreamVersion::new(version),
            response: SyncMutationResponse::default(),
        })]),
    )
}

fn key_json(key: &str) -> String {
    format!(r#"{{"pk":{{"S":"{key}"}}}}"#)
}

fn response(label: &str) -> SyncMutationResponse {
    SyncMutationResponse {
        response_json: Some(format!(r#"{{"label":"{label}"}}"#)),
    }
}

fn response_labels(response: &storage_sync::SyncProposalResponse) -> Vec<String> {
    response
        .responses
        .iter()
        .map(|response| {
            let payload = response.response_json.as_ref().expect("response payload");
            serde_json::from_str::<serde_json::Value>(payload)
                .expect("response json")
                .get("label")
                .and_then(serde_json::Value::as_str)
                .expect("label")
                .to_string()
        })
        .collect()
}
