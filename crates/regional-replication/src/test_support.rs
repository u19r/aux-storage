use std::{collections::HashMap, num::NonZeroUsize, path::Path, sync::Arc};

use infrastructure_provider::{limits::InfrastructureLimits, physical_store::PhysicalRecordStore};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    runtime_configuration::limits::{BacklogBudgets, NativeBudgets, WorkLimits},
};
use replication_types::{
    configuration::{installed::RegionalSourceBinding, membership::RegionalTableBinding},
    event::{content::CanonicalEventCodec, interval::ClosedSourceInterval},
    identity::{journal_partition::JournalPartitionCount, origin::OriginId, replica::RegionId},
    protocol::PayloadBounds,
    regional_import::RegionalImport,
};
use sql_backend::{SqlStore, sqlite::SqliteBackend};
use storage_provider::{DeleteItemShape, DynamoOperations, DynamoPort, PutItemShape};
use storage_types::{
    AttemptMode, AttributeDefinition, AttributeValue, BillingMode, Clock, CreateTableRequest,
    DeleteItemRequest, GetItemRequest, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    PutItemRequest, TableName, WireItem,
};

use crate::{
    import::{ImportExecution, execute_import},
    journal::JournalStore,
    origin::{OriginExecution, execute_item_write},
    source_frame::{SourceFrameRequest, load_source_frame},
};

pub type Store = SqlStore<SqliteBackend>;
pub const NAME: &str = "regional_slice";

pub(super) struct FixedClock(pub(super) storage_types::TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> storage_types::TimestampMillis {
        self.0
    }
}

pub fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
pub fn region(index: u8) -> RegionId {
    format!("region-{index}").parse().unwrap()
}
pub fn partitions() -> JournalPartitionCount {
    64.try_into().unwrap()
}

pub fn journal(index: u8) -> Arc<JournalStore> {
    journal_with_maximum_events(index, limits().frame_events)
}

pub fn journal_with_maximum_events(index: u8, maximum: NonZeroUsize) -> Arc<JournalStore> {
    journal_with_backlog(index, maximum, backlog())
}

fn journal_with_backlog(
    index: u8,
    maximum: NonZeroUsize,
    backlog: BacklogBudgets,
) -> Arc<JournalStore> {
    let journal = JournalStore::try_new(
        (
            region(index),
            [index; 16].try_into().unwrap(),
            1.into(),
            [7; 16].try_into().unwrap(),
        ),
        partitions(),
        physical_limits(),
        (backlog, 1000.try_into().unwrap()),
    )
    .unwrap();
    journal
        .require_original_capacity(budgets(), maximum)
        .unwrap();
    journal
        .native_admission
        .bind(Arc::new(
            crate::native_admission_test_support::FixtureAdmission::default(),
        ))
        .unwrap();
    Arc::new(journal)
}

pub fn physical_limits() -> InfrastructureLimits {
    InfrastructureLimits::try_new(nz(1024), nz(1048576), nz(8388608), nz(128)).unwrap()
}

pub fn budgets() -> NativeBudgets {
    NativeBudgets {
        attempt: storage_types::NativeWorkLimits {
            data_bytes: nz(67108864),
            call_guards: nz(100000),
        },
        reserved_attempts: nz(2),
        maximum_canonical_event_bytes: nz(65536),
        maximum_original_position_bytes: nz(1048576),
        maximum_acceptance_receipt_bytes: nz(4096),
        maximum_origin_receipt_bytes: nz(65536),
        maximum_history_declaration_bytes: nz(65536),
    }
}

pub fn backlog() -> BacklogBudgets {
    BacklogBudgets {
        hot_history_bytes: 268435456.try_into().unwrap(),
        acceptance_bytes: 201326592.try_into().unwrap(),
        recovery_reserve_bytes: 67108864.try_into().unwrap(),
        transfer_bytes_per_second: 1048576.try_into().unwrap(),
        concurrent_joins: nz(1),
    }
}

pub fn codec() -> Arc<CanonicalJsonCodec> {
    Arc::new(CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: nz(65536),
        manifest_bytes: nz(65536),
    }))
}

pub fn limits() -> Arc<WorkLimits> {
    Arc::new(WorkLimits {
        frame_bytes: nz(1048576),
        frame_events: nz(128),
        import_bytes: nz(1048576),
        import_events: nz(128),
        apply_window: nz(2),
        apply_concurrency: nz(2),
        queue_bytes: nz(536870912),
    })
}

pub async fn open(path: &Path) -> Store {
    open_with_probe(path, None).await
}

pub async fn open_with_probe(
    path: &Path,
    probe: Option<Arc<dyn sql_backend::SqlFaultProbe>>,
) -> Store {
    let backend = SqliteBackend::initialize(path).await.unwrap();
    let backend = if let Some(probe) = probe {
        backend.with_fault_probe(probe)
    } else {
        backend
    };
    let store = SqlStore::initialize_schema(backend).await.unwrap();
    store
        .record_capacity(physical_limits())
        .unwrap()
        .require_bounded_admission()
        .unwrap();
    store.initialize_records(physical_limits()).await.unwrap();
    store
}

pub async fn reopen(path: &Path) -> Store {
    let store = open(path).await;
    storage_provider::native_admission::NativeAttemptSource::bind_work_limits(
        &store,
        budgets().attempt,
    )
    .unwrap();
    store
}

pub(super) struct NativeCluster {
    pub operations: Vec<Arc<crate::serving::RegionalDynamo<Store>>>,
    pub journals: Vec<Arc<JournalStore>>,
    pub workers: Vec<bg_jobs_tokio::JobRuntime<bg_job_store::ScheduledJobStore>>,
    pub clock: Arc<bg_jobs_tokio::test_support::TestClock>,
    pub history: Vec<Arc<dyn crate::history::read::CanonicalHistoryOperations>>,
    services: Vec<crate::runtime::RegionalServices>,
}

impl NativeCluster {
    pub async fn start(stores: &[&Store]) -> Self {
        Self::start_with_backlog(stores, backlog()).await
    }

    pub async fn start_with_backlog(stores: &[&Store], backlog: BacklogBudgets) -> Self {
        Self::start_configuration(stores, backlog, None, None).await
    }

    #[cfg(target_os = "linux")]
    pub async fn start_with_time(
        stores: &[&Store],
        trust: &replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration,
    ) -> Self {
        Self::start_configuration(stores, backlog(), Some(trust), None).await
    }

    pub async fn start_with_archive(stores: &[&Store], root: &Path) -> Self {
        Self::start_configuration(stores, backlog(), None, Some(root)).await
    }

    async fn start_configuration(
        stores: &[&Store],
        backlog: BacklogBudgets,
        trust: Option<
            &replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration,
        >,
        archive: Option<&Path>,
    ) -> Self {
        use bg_job_store::ScheduledJobStoreSource;
        use bg_jobs_tokio::test_support::{TestClock, runtime_builder};
        use scheduled_bg_jobs::{
            DriveBudget, Namespace, OwnerId, QueueSelection, Timestamp, runtime::MonotonicDeadline,
        };
        assert!(stores.len() >= 2);
        let listeners = stores
            .iter()
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let endpoints = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap())
            .collect::<Vec<_>>();
        drop(listeners);
        let mut services = Vec::new();
        let mut workers = Vec::new();
        let mut operations = Vec::new();
        let mut journals = Vec::new();
        let mut history = Vec::new();
        let now = stores[0].now();
        let clock = Arc::new(TestClock::new(
            Timestamp::from_millis(now.timestamp_millis()),
            MonotonicDeadline::ZERO,
        ));
        for (offset, store) in stores.iter().copied().enumerate() {
            let index = u8::try_from(offset + 1).unwrap();
            let mut configuration =
                crate::transport::test_support::configuration(1, endpoints[0], endpoints[1])
                    .as_ref()
                    .as_ref()
                    .clone();
            configuration.region = region(index);
            configuration.group = [index; 16].try_into().unwrap();
            configuration.replica = [index; 16].try_into().unwrap();
            configuration.endpoint = endpoints[offset];
            configuration.backlog = backlog.clone();
            configuration.time_trust = trust.cloned();
            configuration.archive_root = archive.map(Path::to_path_buf);
            configuration.peers = endpoints
                .iter()
                .enumerate()
                .filter(|(peer, _)| *peer != offset)
                .map(|(peer, endpoint)| {
                    let index = u8::try_from(peer + 1).unwrap();
                    replication_provider::runtime_configuration::deployment::PeerConfiguration {
                        replica: [index; 16].try_into().unwrap(),
                        region: region(index),
                        group: [index; 16].try_into().unwrap(),
                        generation: 1.into(),
                        endpoint: *endpoint,
                    }
                })
                .collect();
            let startup = crate::runtime::RegionalStartup {
                configuration,
                budgets: budgets(),
                physical_limits: physical_limits(),
            };
            let regional =
                crate::runtime::RegionalRuntime::initialize(Arc::new(store.clone()), startup)
                    .await
                    .unwrap();
            operations.push(regional.operations());
            journals.push(regional.fixture_journal());
            let bound = regional.bind_services().await.unwrap();
            let worker = runtime_builder(
                store.scheduled_job_store(Default::default()).unwrap(),
                bound.job_catalog(),
                DriveBudget::try_resident(QueueSelection::all()).unwrap(),
            )
            .namespace(Namespace::try_new("regional-test").unwrap())
            .owner(OwnerId::from([(offset + 1) as u8; 16]))
            .clock(clock.clone())
            .build()
            .unwrap();
            worker.ensure_seeds().await.unwrap();
            workers.push(worker);
            let service = bound
                .prepare()
                .bind_native_admission(
                    "isolated-native-fixture",
                    Arc::new(crate::native_admission_test_support::FixtureAdmission::default()),
                )
                .unwrap()
                .start();
            history.push(service.history());
            services.push(service);
        }
        Self {
            operations,
            journals,
            workers,
            clock,
            history,
            services,
        }
    }

    pub async fn drive_once(&self) {
        for worker in &self.workers {
            worker.drive_once().await.unwrap();
        }
        self.clock.advance(std::time::Duration::from_millis(10));
    }

    pub async fn stop_receiver(&mut self, index: usize) {
        self.services.remove(index).shutdown().await.unwrap();
    }

    pub async fn shutdown(self) {
        drop(self.workers);
        drop(self.operations);
        for service in self.services {
            service.shutdown().await.unwrap();
        }
    }
}

impl NativeCluster {
    pub async fn install(&self, source: &Store, name: &str, members: &[u8]) {
        create_table(source, &TableName::new(name)).await;
        self.operations[0].update_table(serde_json::from_value(serde_json::json!({"TableName": name, "ReplicaUpdates": members.iter().map(|index| serde_json::json!({"Create": {"RegionName": format!("region-{index}")}})).collect::<Vec<_>>()})).unwrap()).await.unwrap();
        let mut active = false;
        for _ in 0..512 {
            self.drive_once().await;
            let mut ready = true;
            for index in std::iter::once(1).chain(members.iter().copied()) {
                let operation = &self.operations[usize::from(index - 1)];
                ready &= operation
                    .describe_table(storage_types::DescribeTableRequest {
                        table_name: TableName::new(name),
                    })
                    .await
                    .is_ok_and(|response| {
                        response.table.aux_configuration_primary_region.as_deref()
                            == Some("region-1")
                            && response.table.table_status == storage_types::TableStatus::Active
                    });
            }
            if ready {
                active = true;
                break;
            }
        }
        assert!(
            active,
            "native fixture bootstrap did not install complete control authority"
        );
        for _ in 0..2 {
            self.drive_once().await;
        }
    }
}

/// Install through real bootstrap and control jobs; an Active label is not
/// authority.
pub async fn install_pair(source: &Store, destination: &Store) {
    let pair = NativeCluster::start(&[source, destination]).await;
    pair.install(source, NAME, &[2]).await;
    pair.shutdown().await;
}

pub async fn create_table(store: &Store, name: &TableName) {
    store
        .create_table(CreateTableRequest::new(
            name.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();
}

pub fn key() -> KeyAttributes {
    let mut key = KeyAttributes::default();
    key.insert("pk", AttributeValue::S("customer".into()));
    key
}

pub fn item(value: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), AttributeValue::S("customer".into())),
        ("value".to_owned(), AttributeValue::S(value.into())),
    ]))
    .unwrap()
}

pub(super) async fn put(store: &Store, index: u8, value: &str) {
    let shape =
        PutItemShape::try_from_request(PutItemRequest::new(TableName::new(NAME), item(value)))
            .unwrap();
    let result = execute_item_write(
        store,
        OriginExecution {
            journal: journal(index),
            codec: codec(),
        },
        shape,
    )
    .await;
    result.unwrap_or_else(|failure| panic!("origin put: {:?}", failure.error));
}

pub(super) async fn delete(store: &Store, index: u8) {
    let shape =
        DeleteItemShape::try_from_request(DeleteItemRequest::new(TableName::new(NAME), key()))
            .unwrap();
    execute_item_write(
        store,
        OriginExecution {
            journal: journal(index),
            codec: codec(),
        },
        shape,
    )
    .await
    .unwrap_or_else(|failure| panic!("origin delete: {:?}", failure.error));
}

pub(super) async fn assert_item(store: &Store, value: Option<&str>) {
    let result = store
        .get_item(GetItemRequest::new(TableName::new(NAME), key()))
        .await
        .unwrap();
    assert_eq!(
        result.item.map(|item| item.into_attribute_map().unwrap()),
        value.map(|value| item(value).into_attribute_map().unwrap())
    );
}

pub(super) async fn installed_state(
    store: &Store,
    index: u8,
    name: &str,
) -> replication_types::configuration::installed::InstalledTableState {
    let cache = store.metadata_cache();
    let name = TableName::new(name);
    store
        .with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move {
                let (table, _) =
                    storage_provider::resolve_table_in_attempt(attempt, &cache, &name).await?;
                Ok(journal(index)
                    .load_capture_state(attempt, &table)
                    .await?
                    .unwrap()
                    .state()
                    .clone())
            })
        })
        .await
        .unwrap()
}

/// Relative fixture mutation positions start after the real enable control.
pub(super) async fn source_position(store: &Store, index: u8, mutations: u64) -> u64 {
    let state = installed_state(store, index, NAME).await;
    let partition = partitions().select(
        state.configuration.lineage,
        &codec().encode_key(&key()).unwrap(),
    );
    let base = store
        .with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move {
                let journal = journal(index);
                let control = journal
                    .load_configuration_control(
                        attempt,
                        &state,
                        storage_types::ReadProtection::Shared,
                    )
                    .await?;
                let id = replication_types::event::identity::CanonicalEventId::Control {
                    lineage: state.configuration.lineage,
                    operation: control.proposal.operation().id,
                };
                let original = journal
                    .load_acceptance(attempt, &id, &codec())
                    .await?
                    .unwrap();
                let replication_types::event::content::CanonicalPayload::System(control) =
                    &original.event.as_ref().payload
                else {
                    panic!("installed configuration lacks its canonical control");
                };
                Ok(
                    if control.publication.origin.region == region(index)
                        && control.publication.origin.journal_partition == partition
                    {
                        u64::from(control.publication.position)
                    } else {
                        0
                    },
                )
            })
        })
        .await
        .unwrap();
    base + mutations
}

pub(super) async fn frame(
    store: &Store,
    index: u8,
    after: u64,
    through: u64,
) -> Arc<RegionalImport> {
    let codec = codec();
    let state = installed_state(store, index, NAME).await;
    let after = source_position(store, index, after).await;
    let through = source_position(store, index, through).await;
    let destination = state
        .sources
        .iter()
        .find(|source| source.region == region(3 - index))
        .unwrap();
    let key = codec.encode_key(&key()).unwrap();
    let request = SourceFrameRequest {
        origin: OriginId {
            region: region(index),
            database_group: [index; 16].try_into().unwrap(),
            journal_generation: 1.into(),
            journal_partition: partitions().select(state.configuration.lineage, &key),
        },
        range: ClosedSourceInterval::try_from((after.into(), through.into())).unwrap(),
        membership: 1.into(),
        tables: vec![(state.configuration.table, state.configuration.lineage)],
    };
    let read_codec = Arc::clone(&codec);
    let frame = store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(
                    load_source_frame(&journal(index), attempt, (&request, &limits()), &read_codec)
                        .await
                        .unwrap(),
                )
            })
        })
        .await
        .unwrap();
    Arc::new(
        RegionalImport::try_new(
            (
                vec![RegionalTableBinding {
                    source_region: region(index),
                    source_table: state.configuration.table,
                    local_table: destination.table,
                    lineage: state.configuration.lineage,
                    membership: state.membership,
                    local_config: destination.config,
                }],
                frame.manifest().clone(),
                frame
                    .events()
                    .iter()
                    .map(|event| codec.decode_event(event.as_ref().bytes.as_ref()).unwrap())
                    .collect(),
            ),
            &*codec,
        )
        .unwrap(),
    )
}

pub(super) fn bounded_import(
    import: Arc<RegionalImport>,
) -> replication_provider::regional_application::import_admission::BoundedImport {
    replication_provider::regional_application::import_admission::BoundedImport::try_new(
        import,
        &limits(),
    )
    .unwrap()
}

pub(super) async fn deliver(
    store: &Store,
    index: u8,
    import: Arc<RegionalImport>,
) -> crate::import_receipt::CommittedImportPages {
    execute_import(
        store,
        journal(index),
        ImportExecution {
            import: bounded_import(import),
            codec: codec(),
        },
    )
    .await
    .unwrap()
}

pub(super) fn delivery_scope(import: &RegionalImport) -> crate::delivery::DeliveryScope {
    let binding = &import.bindings()[0];
    let index = if binding.source_region == region(1) {
        2
    } else {
        1
    };
    crate::delivery::DeliveryScope {
        origin: import.manifest().origin.clone(),
        table: binding.source_table,
        local_table: binding.source_table,
        lineage: binding.lineage,
        membership: binding.membership,
        destination: RegionalSourceBinding {
            region: region(index),
            database_group: [index; 16].try_into().unwrap(),
            generation: 1.into(),
            partitions: partitions(),
            table: binding.local_table,
            config: binding.local_config,
        },
    }
}

pub(super) async fn acknowledge(
    store: &Store,
    import: Arc<RegionalImport>,
    receipt: crate::transport::RemoteImportReceipt,
) -> replication_provider::delivery::coverage::DeliveryCoverage {
    crate::delivery::record_delivery(
        store,
        journal(1),
        Arc::new(crate::delivery::DeliveryExecution {
            scope: delivery_scope(&import),
            import,
            receipt: Arc::new(receipt),
            codec: codec(),
            limits: limits(),
        }),
    )
    .await
    .unwrap()
}
