use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use replication_contract_tests::native::commit_fault::{NativeCommitBoundary, NativeCommitFault};
use replication_provider::runtime_configuration::time_trust::{
    ChronyReportVersion, SourceAuthentication, SymmetricAlgorithm, TimeTrustConfiguration,
    TrustedTimeSource,
};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    journal::JournalStore,
    test_support as fixture,
    transport::{TcpTransport, test_support::configuration},
    trim::{
        executor::{TrimRequest, execute_qualified_trim, execute_trim},
        grant_turn::{PrimaryTrimGrantTurn, TrimGrantTurn},
        native_trim_tests::capture_proposal,
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimOutcome},
    },
};

#[tokio::test]
#[ignore = "requires Linux and Chrony 4.6; run just regional-trim-time-test"]
async fn given_authenticated_native_time_and_complete_inventory_when_trim_loses_ack_then_grant_install_finish_and_reopen_preserve_counted_authority()
 {
    let time = ChronyFixture::start().await;
    for invalid in [
        TimeTrustConfiguration { maximum_stdout_bytes: 1.try_into().unwrap(), ..time.trust.clone() },
        TimeTrustConfiguration { socket: time._directory.path().join("missing.sock"), ..time.trust.clone() },
        TimeTrustConfiguration { executable: time._directory.path().join("missing-chronyc"), ..time.trust.clone() },
        TimeTrustConfiguration { maximum_measurement_age_ms: 1.try_into().unwrap(), ..time.trust.clone() },
        TimeTrustConfiguration { sources: vec![TrustedTimeSource { authentication: SourceAuthentication::SymmetricKey { key_id: 8.try_into().unwrap(), algorithm: SymmetricAlgorithm::Sha256 }, ..time.trust.sources[0].clone() }], ..time.trust.clone() },
        TimeTrustConfiguration { sources: vec![TrustedTimeSource { authentication: SourceAuthentication::SymmetricKey { key_id: 7.try_into().unwrap(), algorithm: SymmetricAlgorithm::Sha512 }, ..time.trust.sources[0].clone() }], ..time.trust.clone() },
        TimeTrustConfiguration { sources: vec![TrustedTimeSource { authentication: SourceAuthentication::Nts { algorithm: replication_provider::runtime_configuration::time_trust::NtsAlgorithm::AesSivCmac256 }, ..time.trust.sources[0].clone() }], ..time.trust.clone() },
        TimeTrustConfiguration { sources: vec![TrustedTimeSource { address: "127.0.0.2".parse().unwrap(), ..time.trust.sources[0].clone() }], ..time.trust.clone() },
    ] {
        assert!(super::source::QualifiedTimeSample::acquire(&invalid, tokio::time::Instant::now() + Duration::from_secs(3)).await.is_none(), "unqualified host evidence acquired authority");
    }
    assert!(
        super::source::QualifiedTimeSample::acquire(&time.trust, tokio::time::Instant::now())
            .await
            .is_none()
    );
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let destination_path = directory.path().join("destination.sqlite");
    let fault = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let source = fixture::open_with_probe(&source_path, Some(fault.clone())).await;
    let destination = Arc::new(fixture::open(&destination_path).await);
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let (first, source_configuration) = bind_journal(1, (source_endpoint, endpoint), &time.trust);
    let (second, destination_configuration) =
        bind_journal(2, (endpoint, source_endpoint), &time.trust);
    let transport = TcpTransport::try_new(source_configuration).unwrap();
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
    let receiver = TcpTransport::try_new(destination_configuration)
        .unwrap()
        .receiver(
            Arc::clone(&destination),
            Arc::clone(&second),
            fixture::codec(),
        )
        .unwrap();
    let (stop, stopping) = watch::channel(false);
    let serving = tokio::spawn(receiver.serve(listener, stopping));
    fixture::put(&source, 1, "protected").await;
    let frame = fixture::frame(&source, 1, 0, 1).await;
    let receipt = client.deliver(Arc::clone(&frame)).await.unwrap();
    fixture::acknowledge(&source, Arc::clone(&frame), receipt).await;
    let mut proposal = capture_proposal(&[&source, &destination])
        .await
        .as_ref()
        .clone();
    proposal.policy.clock_policy = time.trust.policy;
    proposal.policy.minimum_retention_ms = time.trust.minimum_retention_ms;
    let proposal = Arc::new(proposal);
    assert_eq!(proposal.candidate.sources.len(), 128);
    let prepared =
        execute_bound(&source, &first, TrimRequest::Prepare(Arc::clone(&proposal))).await;
    let participant = client.prepare_trim(Arc::clone(&proposal)).await.unwrap();
    execute_bound(
        &source,
        &first,
        TrimRequest::RecordPreparation {
            proposal: Arc::clone(&proposal),
            receipts: vec![participant],
        },
    )
    .await;
    let participant = client.observe_trim(Arc::clone(&proposal)).await.unwrap();
    let turn = TrimGrantTurn::try_next(prepared.observation.as_ref().unwrap(), &[participant])
        .unwrap()
        .unwrap();
    execute_bound(
        &source,
        &first,
        TrimRequest::AuthorizeGrant(
            PrimaryTrimGrantTurn::try_from_primary(turn, &proposal.primary).unwrap(),
        ),
    )
    .await;
    let granted = qualify_grant(&source, &first, &proposal).await;
    assert!(matches!(
        granted.observation.as_ref().unwrap().decision,
        TrimDecision::Granted(_)
    ));
    let participant = client.observe_trim(Arc::clone(&proposal)).await.unwrap();
    let turn = TrimGrantTurn::try_next(granted.observation.as_ref().unwrap(), &[participant])
        .unwrap()
        .unwrap();
    client.authorize_trim_grant(turn).await.unwrap();
    qualify_grant(&destination, &second, &proposal).await;
    crate::journal::trim_reservation_test_support::verify_counted_custody(
        &source, &first, &proposal,
    )
    .await;
    crate::journal::trim_reservation_test_support::verify_counted_custody(
        destination.as_ref(),
        &second,
        &proposal,
    )
    .await;
    drop(source);
    for budget in [
        storage_types::NativeWorkLimits {
            call_guards: fixture::nz(8),
            ..fixture::budgets().attempt
        },
        storage_types::NativeWorkLimits {
            data_bytes: fixture::nz(4096),
            ..fixture::budgets().attempt
        },
    ] {
        let limited = fixture::open(&source_path).await;
        storage_provider::native_admission::NativeAttemptSource::bind_work_limits(&limited, budget)
            .unwrap();
        let participant = client.observe_trim(Arc::clone(&proposal)).await.unwrap();
        let capacity = execute_trim(
            &limited,
            Arc::clone(&first),
            Arc::new(TrimRequest::Commit {
                proposal: Arc::clone(&proposal),
                receipts: vec![participant],
            }),
            (fixture::limits(), fixture::codec()),
        )
        .await
        .err()
        .expect("complete variable-child commit ignored native admission");
        assert!(matches!(
            capacity.as_ref(),
            storage_types::StorageEnum::InfrastructureResult(
                storage_types::InfrastructureResultKind::NativeCapacityRejected
            )
        ));
    }
    let source = fixture::open_with_probe(&source_path, Some(fault.clone())).await;
    storage_provider::native_admission::NativeAttemptSource::bind_work_limits(
        &source,
        fixture::budgets().attempt,
    )
    .unwrap();
    let intact = execute_bound(&source, &first, TrimRequest::Observe(Arc::clone(&proposal))).await;
    assert!(matches!(
        intact.observation.as_ref().unwrap().decision,
        TrimDecision::Granted(_)
    ));
    assert!(intact.certificate.is_none());
    let participant = client.observe_trim(Arc::clone(&proposal)).await.unwrap();
    fault.arm();
    let committed = execute_bound(
        &source,
        &first,
        TrimRequest::Commit {
            proposal: Arc::clone(&proposal),
            receipts: vec![participant],
        },
    )
    .await;
    assert!(fault.was_observed());
    assert!(matches!(
        committed.observation.as_ref().unwrap().decision,
        TrimDecision::Committed(_)
    ));
    crate::journal::trim_reservation_test_support::verify_counted_custody(
        &source, &first, &proposal,
    )
    .await;
    assert!(
        execute_trim(
            &source,
            Arc::clone(&first),
            Arc::new(TrimRequest::Cancel(Arc::clone(&proposal))),
            (fixture::limits(), fixture::codec())
        )
        .await
        .is_err()
    );
    fault.arm();
    let installed = execute_bound(
        &source,
        &first,
        TrimRequest::InstallPrimary(Arc::clone(&proposal)),
    )
    .await;
    assert!(fault.was_observed());
    let certificate = installed.certificate.unwrap();
    crate::journal::trim_reservation_test_support::verify_counted_custody(
        &source, &first, &proposal,
    )
    .await;
    let receipts = transport
        .install_trim_participants(vec![client.clone()], certificate.clone())
        .await
        .unwrap();
    fault.arm();
    let completed = execute_bound(
        &source,
        &first,
        TrimRequest::Complete {
            proposal: Arc::clone(&proposal),
            receipts,
        },
    )
    .await;
    assert!(fault.was_observed());
    assert!(!completed.completion_released);
    let receipts = transport
        .finish_trim_participants(vec![client.clone()], completed.completion.unwrap())
        .await
        .unwrap();
    fault.arm();
    let released = execute_bound(
        &source,
        &first,
        TrimRequest::ReleaseCompletion {
            proposal: Arc::clone(&proposal),
            receipts,
        },
    )
    .await;
    assert!(fault.was_observed());
    assert!(released.completion_released);
    let replay = transport
        .install_trim_participants(vec![client.clone()], certificate)
        .await
        .unwrap();
    assert_eq!(replay.len(), 1);
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
    drop(destination);
    drop(source);
    let source = fixture::reopen(&source_path).await;
    let destination = fixture::reopen(&destination_path).await;
    let cut = capture_proposal(&[&source, &destination]).await;
    assert!(cut.candidate.trim == proposal.next);
    assert_eq!(
        cut.candidate.sources.len(),
        proposal.candidate.sources.len()
    );
    assert!(
        cut.candidate
            .sources
            .iter()
            .zip(&proposal.candidate.sources)
            .all(|(next, old)| next.after == old.through && next.through == old.through)
    );
    for (primary, store, journal) in [(true, &source, &first), (false, &destination, &second)] {
        let recovered =
            execute_bound(store, journal, TrimRequest::Observe(Arc::clone(&proposal))).await;
        assert!(matches!(
            recovered.observation.as_ref().unwrap().decision,
            TrimDecision::Completed(_)
        ));
        assert_eq!(recovered.completion_released, primary);
        assert!(recovered.observation.as_ref().unwrap().proposal == *proposal);
        assert!(recovered.installation.is_some());
        fixture::assert_item(store, Some("protected")).await;
    }
    finish_scheduled_trim((source, destination), (&first, &second), &time.trust, frame).await;
    require_multiple_introductions(directory.path(), &time.trust).await;
}

async fn require_multiple_introductions(
    directory: &std::path::Path,
    trust: &TimeTrustConfiguration,
) {
    use storage_provider::{DynamoOperations, DynamoPort, native_admission::NativeAttemptSource};
    use storage_types::{AttemptMode, ReadProtection};

    use crate::history::table_cut::{CanonicalTableCutAction, CanonicalTableCutRequest};
    let source_path = directory.join("introduction-source.sqlite");
    let source = fixture::open(&source_path).await;
    let destination = fixture::open(&directory.join("introduction-destination.sqlite")).await;
    let third = fixture::open(&directory.join("third.sqlite")).await;
    let fourth = fixture::open(&directory.join("fourth.sqlite")).await;
    let cluster =
        fixture::NativeCluster::start_with_time(&[&source, &destination, &third, &fourth], trust)
            .await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    let mut proposal = capture_proposal(&[&source, &destination])
        .await
        .as_ref()
        .clone();
    proposal.policy.clock_policy = trust.policy;
    proposal.policy.minimum_retention_ms = trust.minimum_retention_ms;
    let proposal = Arc::new(proposal);
    execute_bound(
        &source,
        &cluster.journals[0],
        TrimRequest::Prepare(Arc::clone(&proposal)),
    )
    .await;
    let mut released = false;
    // Each scan advances one source per control turn; the full runtime also
    // drives other queues.
    let max_turns = proposal.candidate.sources.len() * 8;
    for _ in 0..max_turns {
        cluster.drive_once().await;
        cluster.clock.advance(Duration::from_secs(1));
        let outcome = execute_bound(
            &source,
            &cluster.journals[0],
            TrimRequest::Observe(Arc::clone(&proposal)),
        )
        .await;
        if outcome.completion_released {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if !released {
        for (store, journal) in [
            (&source, &cluster.journals[0]),
            (&destination, &cluster.journals[1]),
        ] {
            let state =
                execute_bound(store, journal, TrimRequest::Advance(Arc::clone(&proposal))).await;
            eprintln!(
                "trim {}",
                serde_json::to_string(&(
                    state.observation.as_ref().map(|record| record.decision),
                    state.work,
                    state.participants_prepared,
                    state.grant_turn.is_some(),
                    state.prefix.is_some()
                ))
                .unwrap()
            );
        }
        panic!("four-region deployment did not install trim");
    }
    let request = || CanonicalTableCutRequest {
        serving_region: fixture::region(1),
        serving_group: [1; 16].try_into().unwrap(),
        table: storage_types::TableName::new(fixture::NAME),
        table_cut: CanonicalTableCutAction::Capture,
    };
    let original = cluster.history[0].table_cut(request()).await.unwrap().cut;
    assert_eq!(original.sources.len(), 128);
    for index in [3u8, 4] {
        cluster.operations[0].update_table(serde_json::from_value(serde_json::json!({"TableName": fixture::NAME, "ReplicaUpdates": [{"Create": {"RegionName": format!("region-{index}")}}]})).unwrap()).await.unwrap();
        let mut active = false;
        for _ in 0..usize::from(index) * 64 * 8 {
            cluster.drive_once().await;
            active = cluster.operations[usize::from(index - 1)]
                .describe_table(storage_types::DescribeTableRequest {
                    table_name: storage_types::TableName::new(fixture::NAME),
                })
                .await
                .is_ok_and(|response| {
                    response.table.table_status == storage_types::TableStatus::Active
                        && response.table.aux_configuration_primary_region.as_deref()
                            == Some("region-1")
                });
            if active {
                break;
            }
        }
        assert!(
            active,
            "addition did not install its exact trim introduction child"
        );
        let cut = cluster.history[0].table_cut(request()).await.unwrap().cut;
        assert!(
            cut.trim == original.trim,
            "source introduction replaced installed trim authority"
        );
        assert_eq!(cut.lineage, original.lineage);
        assert_eq!(cut.sources.len(), usize::from(index) * 64);
    }
    let complete = cluster.history[0].table_cut(request()).await.unwrap().cut;
    for budget in [
        storage_types::NativeWorkLimits {
            call_guards: fixture::nz(8),
            ..fixture::budgets().attempt
        },
        storage_types::NativeWorkLimits {
            data_bytes: fixture::nz(4096),
            ..fixture::budgets().attempt
        },
    ] {
        let limited = fixture::open(&source_path).await;
        limited.bind_work_limits(budget).unwrap();
        let journal = Arc::clone(&cluster.journals[0]);
        let request = request();
        let error = limited
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    journal
                        .load_table_source_vector(
                            attempt,
                            &request,
                            (&fixture::limits(), &fixture::codec()),
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            })
            .await
            .err()
            .expect("complete child assembly ignored native work admission");
        assert!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::InfrastructureResult(
                storage_types::InfrastructureResultKind::NativeCapacityRejected
            )
        ));
    }
    assert_eq!(
        serde_json::to_vec(&cluster.history[0].table_cut(request()).await.unwrap().cut).unwrap(),
        serde_json::to_vec(&complete).unwrap(),
        "failed child assembly changed trim authority"
    );
    cluster.shutdown().await;
}

async fn finish_scheduled_trim(
    stores: (fixture::Store, fixture::Store),
    journals: (&Arc<JournalStore>, &Arc<JournalStore>),
    trust: &TimeTrustConfiguration,
    previous: Arc<replication_types::regional_import::RegionalImport>,
) {
    use bg_job_store::ScheduledJobStoreSource;
    use bg_jobs_tokio::test_support::{TestClock, runtime_builder};
    use scheduled_bg_jobs::{
        DriveBudget, DriveBudgetInput, JobCatalog, Namespace, OwnerId, QueueSelection, Timestamp,
        runtime::MonotonicDeadline,
    };

    use crate::history::{
        read::{CanonicalHistoryOperations, NativeHistoryReader},
        selection::CanonicalViewSelection,
        view::CanonicalViewRequest,
    };
    let source = Arc::new(stores.0);
    let destination = Arc::new(stores.1);
    let listeners = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let endpoints = [
        listeners[0].local_addr().unwrap(),
        listeners[1].local_addr().unwrap(),
    ];
    let clock = Arc::new(TestClock::new(
        Timestamp::from_millis(storage_types::Clock::now(source.as_ref()).timestamp_millis()),
        MonotonicDeadline::ZERO,
    ));
    let mut workers = Vec::new();
    let mut receivers = Vec::new();
    let mut readers = Vec::new();
    let (stop, stopping) = watch::channel(false);
    for (offset, listener) in listeners.into_iter().enumerate() {
        let store = if offset == 0 { &source } else { &destination };
        let journal = if offset == 0 { journals.0 } else { journals.1 };
        let (_, configured) = bind_journal(
            (offset + 1) as u8,
            (endpoints[offset], endpoints[1 - offset]),
            trust,
        );
        let transport = TcpTransport::try_new(configured).unwrap();
        readers.push(NativeHistoryReader {
            port: Arc::clone(store),
            journal: Arc::clone(journal),
            codec: fixture::codec(),
            transport: transport.clone(),
        });
        let receiver = transport
            .receiver(Arc::clone(store), Arc::clone(journal), fixture::codec())
            .unwrap();
        receivers.push(tokio::spawn(receiver.serve(listener, stopping.clone())));
        let handler = crate::control::ControlJob::try_new(
            Arc::clone(store),
            Arc::clone(journal),
            fixture::codec(),
            transport,
        )
        .unwrap();
        let mut catalog = JobCatalog::new();
        handler
            .register(
                &mut catalog,
                bg_jobs_tokio::test_support::JobClock::wall_now(clock.as_ref()),
            )
            .unwrap();
        let budget = DriveBudget::try_from(DriveBudgetInput {
            queues: QueueSelection::all(),
            max_claims: 1,
            max_concurrency: 1,
            max_store_operations: 128,
            stop_claiming_after: Duration::from_secs(1),
            finish_after: Duration::from_secs(15),
        })
        .unwrap();
        let worker = runtime_builder(
            store.scheduled_job_store(Default::default()).unwrap(),
            catalog,
            budget,
        )
        .namespace(Namespace::try_new("regional-trim-recovery").unwrap())
        .owner(OwnerId::from([(offset + 11) as u8; 16]))
        .clock(clock.clone())
        .build()
        .unwrap();
        worker.ensure_seeds().await.unwrap();
        workers.push(worker);
    }
    fixture::delete(&source, 1).await;
    let original = fixture::frame(&source, 1, 1, 2).await;
    let receipt = readers[0]
        .transport
        .peer([2; 16].try_into().unwrap())
        .unwrap()
        .deliver(Arc::clone(&original))
        .await
        .unwrap();
    fixture::acknowledge(&source, Arc::clone(&original), receipt).await;
    let mut proposal = capture_proposal(&[&source, &destination])
        .await
        .as_ref()
        .clone();
    proposal.operation = [93; 16].try_into().unwrap();
    proposal.policy.clock_policy = trust.policy;
    proposal.policy.minimum_retention_ms = trust.minimum_retention_ms;
    let proposal = Arc::new(proposal);
    let mut views = Vec::new();
    for reader in &readers {
        let view = || CanonicalViewRequest {
            serving_region: reader.journal.region().clone(),
            serving_group: reader.transport.configuration().as_ref().as_ref().group,
            table: storage_types::TableName::new(fixture::NAME),
            lineage: proposal.candidate.lineage,
            membership: proposal.candidate.membership,
            receiver: reader.journal.origin(0u16.into()).unwrap(),
            selection: CanonicalViewSelection::All,
            checkpoint: None,
            after: None,
        };
        let cursor = reader.view(view()).await.unwrap().through;
        let mut request = view();
        request.after = Some(cursor);
        views.push(request);
    }
    execute_bound(
        &source,
        journals.0,
        TrimRequest::Prepare(Arc::clone(&proposal)),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut drives = Vec::new();
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "registered control jobs did not finish certified trim: {drives:?}"
        );
        drives.clear();
        for worker in &workers {
            drives.push(worker.drive_once().await.unwrap().counts);
        }
        let observed = execute_bound(
            &source,
            journals.0,
            TrimRequest::Observe(Arc::clone(&proposal)),
        )
        .await;
        if observed.completion_released {
            break;
        }
        clock.advance(Duration::from_millis(25));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for (store, journal) in [(&source, journals.0), (&destination, journals.1)] {
        let completed =
            execute_bound(store, journal, TrimRequest::Observe(Arc::clone(&proposal))).await;
        assert!(matches!(
            completed.observation.as_ref().unwrap().decision,
            TrimDecision::Completed(_)
        ));
        fixture::assert_item(store, None).await;
    }
    let cut = capture_proposal(&[&source, &destination]).await;
    assert!(cut.candidate.trim == proposal.next);
    for (reader, view) in readers.iter().zip(views) {
        let expired = reader
            .view(view)
            .await
            .err()
            .expect("certified trim accepted an old canonical view cursor");
        assert!(matches!(
            expired.as_ref(),
            storage_types::StorageEnum::HistoryExpired
        ));
    }
    fixture::deliver(&destination, 2, original).await;
    fixture::deliver(&destination, 2, previous).await;
    fixture::assert_item(&destination, None).await;
    drop(workers);
    stop.send(true).unwrap();
    for receiver in receivers {
        receiver.await.unwrap().unwrap();
    }
}

async fn execute_bound(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    request: TrimRequest,
) -> TrimOutcome {
    execute_trim(
        store,
        Arc::clone(journal),
        Arc::new(request),
        (fixture::limits(), fixture::codec()),
    )
    .await
    .unwrap()
}

async fn qualify_grant(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    proposal: &Arc<PublicTrimProposal>,
) -> TrimOutcome {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    for _ in 0..512 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "authenticated native time never qualified a complete prefix"
        );
        let qualification = journal.begin_trim_time(proposal.operation);
        let mut outcome =
            execute_bound(store, journal, TrimRequest::Advance(Arc::clone(proposal))).await;
        if let Some(prefix) = outcome.prefix.take() {
            let age = journal
                .observe_trim_time(
                    (
                        proposal,
                        tokio::time::Instant::now() + Duration::from_secs(10),
                    ),
                    Some(prefix),
                )
                .await;
            if let Some(age) = age {
                let outcome = execute_qualified_trim(
                    store,
                    Arc::clone(journal),
                    (Arc::clone(proposal), age),
                    (fixture::limits(), fixture::codec()),
                )
                .await
                .unwrap();
                if matches!(
                    outcome.observation.as_ref().unwrap().decision,
                    TrimDecision::Granted(_)
                ) {
                    return outcome;
                }
            }
            qualification.retain_anchor();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    panic!("complete native source scan did not qualify a grant");
}

fn bind_journal(
    index: u8,
    endpoints: (std::net::SocketAddr, std::net::SocketAddr),
    trust: &TimeTrustConfiguration,
) -> (
    Arc<JournalStore>,
    Arc<replication_provider::runtime_configuration::deployment::QualifiedConfiguration>,
) {
    let mut configured = configuration(index, endpoints.0, endpoints.1)
        .as_ref()
        .as_ref()
        .clone();
    configured.time_trust = Some(trust.clone());
    let qualified = configured.qualify(fixture::budgets()).unwrap();
    let journal = fixture::journal(index);
    journal.bind_time_trust(&qualified).unwrap();
    (journal, Arc::new(qualified))
}

struct ChronyFixture {
    _directory: tempfile::TempDir,
    children: Vec<Child>,
    trust: TimeTrustConfiguration,
}

impl ChronyFixture {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let prefix = PathBuf::from(
            std::env::var("REGIONAL_TEST_CHRONY_PREFIX").unwrap_or_else(|_| "/opt/chrony".into()),
        );
        let keys = directory.path().join("keys");
        std::fs::write(
            &keys,
            "7 SHA256 HEX:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        )
        .unwrap();
        std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(0o600)).unwrap();
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let mut children = Vec::new();
        for (name, extra) in [
            (
                "server",
                format!("port {port}\nbindaddress 127.0.0.1\nlocal stratum 1\nallow 127.0.0.1"),
            ),
            (
                "client",
                format!("port 0\nserver 127.0.0.1 port {port} key 7 iburst minpoll 0 maxpoll 0"),
            ),
        ] {
            let settings = directory.path().join(format!("{name}.conf"));
            let content = format!(
                "{extra}\ncmdport 0\nbindcmdaddress {}/{}.sock\npidfile {}/{}.pid\nkeyfile {}\n",
                directory.path().display(),
                name,
                directory.path().display(),
                name,
                keys.display()
            );
            std::fs::write(&settings, content).unwrap();
            children.push(
                Command::new(prefix.join("sbin/chronyd"))
                    .args(["-n", "-x", "-u", "root", "-f"])
                    .arg(settings)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
        }
        let trust = TimeTrustConfiguration {
            policy: replication_types::protocol::CanonicalDigest::of(b"native-chrony-fixture"),
            minimum_retention_ms: 20.try_into().unwrap(),
            executable: prefix.join("bin/chronyc"),
            socket: directory.path().join("client.sock"),
            report_version: ChronyReportVersion::V4_6,
            sources: vec![TrustedTimeSource {
                address: "127.0.0.1".parse().unwrap(),
                authentication: SourceAuthentication::SymmetricKey {
                    key_id: 7.try_into().unwrap(),
                    algorithm: SymmetricAlgorithm::Sha256,
                },
                upstream_accuracy_ns: 1000000.try_into().unwrap(),
            }],
            acquisition_timeout_ms: 2000.try_into().unwrap(),
            maximum_stdout_bytes: 65536.try_into().unwrap(),
            maximum_measurement_age_ms: 60000.try_into().unwrap(),
        };
        let mut fixture = Self {
            _directory: directory,
            children,
            trust,
        };
        let mut last_report = String::new();
        for _ in 0..300 {
            assert!(
                fixture
                    .children
                    .iter_mut()
                    .all(|child| child.try_wait().unwrap().is_none()),
                "Chrony fixture exited"
            );
            let output = Command::new(&fixture.trust.executable)
                .args(["-n", "-c", "-h"])
                .arg(&fixture.trust.socket)
                .arg("tracking")
                .output()
                .unwrap();
            let report = String::from_utf8(output.stdout).unwrap();
            if report.starts_with("7F000001,") && report.trim().ends_with(",Normal") {
                tokio::time::sleep(Duration::from_secs(3)).await;
                return fixture;
            }
            last_report = format!(
                "{}: {report} {}",
                output.status,
                String::from_utf8(output.stderr).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("authenticated native NTP source never synchronized: {last_report}");
    }
}

impl Drop for ChronyFixture {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
