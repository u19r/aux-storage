use std::sync::Arc;

use replication_contract_tests::native::commit_fault::{NativeCommitBoundary, NativeCommitFault};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, ReadProtection, TableName};
use tokio::{net::TcpListener, sync::watch};

use super::{
    executor::{TrimRequest, execute_trim},
    proposal::{PublicTrimProposal, TrimParticipantBinding, TrimPolicy},
    record::{TrimDecision, TrimOutcome, TrimWorkState},
};
use crate::{
    history::table_cut::{CanonicalTableCutAction, CanonicalTableCutRequest},
    test_support as fixture,
    transport::{TcpTransport, test_support::configuration},
};

#[tokio::test]
async fn given_native_trim_inventory_when_settlement_fails_or_time_is_unqualified_then_cancellation_preserves_history_and_recovers_exact_intent()
 {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let destination_path = directory.path().join("destination.sqlite");
    let before = Arc::new(NativeCommitFault::new(NativeCommitBoundary::BeforeCommit));
    let source = fixture::open_with_probe(&source_path, Some(before.clone())).await;
    let destination = Arc::new(fixture::open(&destination_path).await);
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_endpoint = listener.local_addr().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let transport =
        TcpTransport::try_new(configuration(1, source_endpoint, destination_endpoint)).unwrap();
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
    let receiver = TcpTransport::try_new(configuration(2, destination_endpoint, source_endpoint))
        .unwrap()
        .receiver(
            Arc::clone(&destination),
            fixture::journal(2),
            fixture::codec(),
        )
        .unwrap();
    let (stop, stopping) = watch::channel(false);
    let serving = tokio::spawn(receiver.serve(listener, stopping));
    fixture::put(&source, 1, "retained").await;
    let frame = fixture::frame(&source, 1, 0, 1).await;
    let receipt = client.deliver(Arc::clone(&frame)).await.unwrap();
    fixture::acknowledge(&source, frame, receipt).await;
    let proposal = capture_proposal(&[&source, &destination]).await;
    assert_eq!(proposal.candidate.sources.len(), 128);
    assert!(serde_json::to_vec(proposal.as_ref()).unwrap().len() > 4096);
    before.sustain_faults();
    before.arm();
    let error = execute(&source, TrimRequest::Prepare(Arc::clone(&proposal)))
        .await
        .err()
        .expect("interrupted trim prepare acknowledged");
    assert!(before.was_observed());
    assert!(!error.is_commit_unknown());
    before.release_faults();
    assert!(
        execute(&source, TrimRequest::Observe(Arc::clone(&proposal)))
            .await
            .unwrap()
            .observation
            .is_none()
    );
    drop(source);
    let after = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let source = fixture::open_with_probe(&source_path, Some(after.clone())).await;
    storage_provider::native_admission::NativeAttemptSource::bind_work_limits(
        &source,
        fixture::budgets().attempt,
    )
    .unwrap();
    after.arm();
    let prepared = execute(&source, TrimRequest::Prepare(Arc::clone(&proposal)))
        .await
        .unwrap();
    assert!(after.was_observed());
    require_decision(&prepared, &proposal, TrimDecision::Preparing);

    let participant = client.prepare_trim(Arc::clone(&proposal)).await.unwrap();
    let prepared = execute(
        &source,
        TrimRequest::RecordPreparation {
            proposal: Arc::clone(&proposal),
            receipts: vec![participant],
        },
    )
    .await
    .unwrap();
    assert!(prepared.participants_prepared);
    let mut scanned = false;
    for _ in 0..256 {
        let outcome = execute(&source, TrimRequest::Advance(Arc::clone(&proposal)))
            .await
            .unwrap();
        require_decision(&outcome, &proposal, TrimDecision::Preparing);
        if outcome.work != TrimWorkState::Ready {
            scanned = true;
            break;
        }
    }
    assert!(scanned, "complete source inventory never finished scanning");
    let participant = client.observe_trim(Arc::clone(&proposal)).await.unwrap();
    assert!(
        execute(
            &source,
            TrimRequest::Commit {
                proposal: Arc::clone(&proposal),
                receipts: vec![participant]
            }
        )
        .await
        .is_err()
    );
    assert!(
        execute(&source, TrimRequest::InstallPrimary(Arc::clone(&proposal)))
            .await
            .is_err()
    );
    let mut altered = proposal.as_ref().clone();
    altered.policy.minimum_retention_ms = 2.try_into().unwrap();
    assert!(
        execute(&source, TrimRequest::Observe(Arc::new(altered)))
            .await
            .is_err(),
        "same operation accepted changed immutable intent"
    );
    after.arm();
    let cancelled = execute(&source, TrimRequest::Cancel(Arc::clone(&proposal)))
        .await
        .unwrap();
    assert!(after.was_observed());
    require_decision(&cancelled, &proposal, TrimDecision::Cancelled);
    release_cancellation(&source, &transport, &proposal, cancelled).await;
    require_decision(
        &execute(&source, TrimRequest::Prepare(Arc::clone(&proposal)))
            .await
            .unwrap(),
        &proposal,
        TrimDecision::Cancelled,
    );

    let mut absent = proposal.as_ref().clone();
    absent.operation = [92; 16].try_into().unwrap();
    let absent = Arc::new(absent);
    let cancelled = execute(&source, TrimRequest::Cancel(Arc::clone(&absent)))
        .await
        .unwrap();
    release_cancellation(&source, &transport, &absent, cancelled).await;
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
    drop(destination);
    drop(source);
    let source = fixture::reopen(&source_path).await;
    let destination = fixture::reopen(&destination_path).await;
    for proposal in [&proposal, &absent] {
        let outcome = execute(&source, TrimRequest::Observe(Arc::clone(proposal)))
            .await
            .unwrap();
        require_decision(&outcome, proposal, TrimDecision::Cancelled);
        assert!(outcome.cancellation_released);
    }
    fixture::assert_item(&source, Some("retained")).await;
    fixture::assert_item(&destination, Some("retained")).await;
    let original = fixture::frame(&source, 1, 0, 1).await;
    fixture::deliver(&destination, 2, original).await;
    fixture::assert_item(&destination, Some("retained")).await;
}

pub(crate) async fn capture_proposal(stores: &[&fixture::Store]) -> Arc<PublicTrimProposal> {
    let source = stores[0];
    let candidate = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                fixture::journal(1)
                    .load_table_source_vector(
                        attempt,
                        &CanonicalTableCutRequest {
                            serving_region: fixture::region(1),
                            serving_group: [1; 16].try_into().unwrap(),
                            table: TableName::new(fixture::NAME),
                            table_cut: CanonicalTableCutAction::Capture,
                        },
                        (&fixture::limits(), &fixture::codec()),
                        ReadProtection::Snapshot,
                    )
                    .await
            })
        })
        .await
        .unwrap();
    let mut participants = Vec::new();
    for (offset, store) in stores.iter().enumerate() {
        let index = u8::try_from(offset + 1).unwrap();
        participants.push(TrimParticipantBinding {
            region: fixture::region(index),
            state: fixture::installed_state(store, index, fixture::NAME).await,
        });
    }
    Arc::new(PublicTrimProposal {
        operation: [91; 16].try_into().unwrap(),
        primary: fixture::region(1),
        expected: candidate.trim,
        next: candidate.trim.checked_next().unwrap(),
        candidate,
        participants,
        policy: TrimPolicy {
            minimum_retention_ms: 1.try_into().unwrap(),
            clock_policy: replication_types::protocol::CanonicalDigest::of(
                b"unqualified-native-fixture",
            ),
        },
    })
}

async fn execute(
    source: &fixture::Store,
    request: TrimRequest,
) -> storage_types::StorageResult<TrimOutcome> {
    execute_trim(
        source,
        fixture::journal(1),
        Arc::new(request),
        (fixture::limits(), fixture::codec()),
    )
    .await
}

fn require_decision(outcome: &TrimOutcome, proposal: &PublicTrimProposal, decision: TrimDecision) {
    let observed = outcome
        .observation
        .as_ref()
        .expect("trim lost its immutable intent");
    assert!(observed.proposal == *proposal);
    assert!(observed.decision == decision);
}

async fn release_cancellation(
    source: &fixture::Store,
    transport: &TcpTransport,
    proposal: &Arc<PublicTrimProposal>,
    outcome: TrimOutcome,
) {
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
    let receipts = transport
        .settle_trim_participants(vec![client], outcome.observation.unwrap())
        .await
        .unwrap();
    let released = execute(
        source,
        TrimRequest::ReleaseCancellation {
            proposal: Arc::clone(proposal),
            receipts,
        },
    )
    .await
    .unwrap();
    require_decision(&released, proposal, TrimDecision::Cancelled);
    assert!(released.cancellation_released);
}
