use std::rc::Rc;

use crate::{common::*, constants::*, imports::*};

pub(crate) struct TableAtomicityWorkload {
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) operation_count: u64,
    pub(crate) key_count: u64,
    pub(crate) shared_key_count: u64,
    pub(crate) shared_operation_percent: u64,
    pub(crate) artifact_root: String,
    pub(crate) client_id: i32,
    pub(crate) client_count: i32,
    pub(crate) active_client_count: i32,
    pub(crate) history: OperationHistory,
    pub(crate) model: TableModel,
    pub(crate) possible_model: PossibleTableModel,
    pub(crate) table_binding: Option<Arc<storage_types::BoundTableInfo>>,
    pub(crate) anomalies: Vec<Anomaly>,
    pub(crate) shared_audit: Option<SharedKeyAudit>,
    pub(crate) gsi_build_job: [u8; 16],
    pub(crate) gsi_build_started: u64,
    pub(crate) gsi_start_request: Option<CreateTableRequest>,
    pub(crate) gsi_start_unknown: u64,
    pub(crate) gsi_start_recovered: u64,
    pub(crate) gsi_build_pages: u64,
    pub(crate) gsi_build_unknown: u64,
    pub(crate) gsi_build_writes: u64,
    pub(crate) gsi_build_active: u64,
    pub(crate) error_count: u64,
    pub(crate) audit_count: u64,
    pub(crate) gsi_audit_count: u64,
    pub(crate) trim_audit_count: u64,
    pub(crate) trim_execution_count: u64,
    pub(crate) stream_audit_count: u64,
    pub(crate) stream_entry_audit_count: u64,
    pub(crate) stream_image_chain_comparison_count: u64,
    pub(crate) shared_operation_count: u64,
    pub(crate) should_qualify_catalog_stream_reads: bool,
    pub(crate) should_cache_writes: bool,
    pub(crate) write_probe: Arc<crate::cached_write_qualification::WriteProbe>,
    pub(crate) context: Rc<WorkloadContext>,
    pub(crate) clock: Option<Arc<crate::simulation_clock::SimulationClock>>,
}

impl TableAtomicityWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 100_u64);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 100_u64);
        let key_count = option_or_default(&context, OPTION_KEY_COUNT, 16_u64).max(1);
        let shared_key_count = option_or_default(&context, OPTION_SHARED_KEY_COUNT, 0_u64);
        let shared_operation_percent =
            option_or_default(&context, OPTION_SHARED_OPERATION_PERCENT, 0_u64).min(100);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        let active_client_count =
            option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 2_i32).clamp(1, client_count);
        Self {
            name,
            profile,
            operation_count,
            key_count,
            shared_key_count,
            shared_operation_percent,
            artifact_root,
            client_id,
            client_count,
            active_client_count,
            history: OperationHistory::default(),
            model: TableModel::default(),
            possible_model: PossibleTableModel::default(),
            table_binding: None,
            anomalies: Vec::new(),
            shared_audit: None,
            gsi_build_job: storage_types::ambient::uuid_v4().into_bytes(),
            gsi_build_started: 0,
            gsi_start_request: None,
            gsi_start_unknown: 0,
            gsi_start_recovered: 0,
            gsi_build_pages: 0,
            gsi_build_unknown: 0,
            gsi_build_writes: 0,
            gsi_build_active: 0,
            error_count: 0,
            audit_count: 0,
            gsi_audit_count: 0,
            trim_audit_count: 0,
            trim_execution_count: 0,
            stream_audit_count: 0,
            stream_entry_audit_count: 0,
            stream_image_chain_comparison_count: 0,
            shared_operation_count: 0,
            should_qualify_catalog_stream_reads: option_or_default(
                &context,
                "qualifyCatalogStreamReads",
                false,
            ),
            should_cache_writes: option_or_default(&context, "cachedWrites", false),
            write_probe: Arc::default(),
            context: Rc::new(context),
            clock: None,
        }
    }

    pub(crate) fn driver(
        &self,
        db: SimDatabase,
    ) -> Result<kv_backend::foundationdb::FoundationDbBackend, String> {
        Ok(
            kv_backend::foundationdb::FoundationDbBackend::from_simulation(
                db,
                Some(b"aux-storage/fdb-chaos/table-atomicity/"),
                None,
                if self.should_cache_writes {
                    storage_types::ReadVersionPolicy::Cached
                } else {
                    storage_types::ReadVersionPolicy::Fresh
                },
                crate::simulation_clock::phase_clock(self.clock.as_ref())?,
            )
            .with_fault_probe(self.write_probe.clone()),
        )
    }

    pub(crate) async fn provider(
        &self,
        db: SimDatabase,
    ) -> Result<Arc<kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>>, String>
    {
        let clock = self
            .clock
            .as_ref()
            .ok_or("simulation phase clock is absent")?;
        let store = kv_backend::KvStore::initialize_format_with_clock(
            self.driver(db)?,
            storage_provider::StorageBackend::FoundationDb.default_initial_stream_lineages(),
            clock.clone(),
        )
        .await
        .map_err(|error| storage_error_detail(&error))?
        .with_metadata_cache_ttl(SIMULATION_METADATA_CACHE_TTL);
        Ok(Arc::new(store))
    }

    pub(crate) async fn capture_table_binding(
        &mut self,
        store: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> storage_types::StorageResult<()> {
        let binding = read_table_binding(
            store,
            self.table_name(),
            storage_types::AttemptPlan::new(
                storage_types::AttemptMode::ReadOnly,
                storage_types::DeadlineClass::PointRead,
            )
            .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
                store,
            )),
        )
        .await?;
        self.table_binding = Some(binding);
        Ok(())
    }

    pub(crate) fn table_name(&self) -> TableName {
        TableName::new("FdbChaosTableAtomicity")
    }

    pub(crate) fn create_table_request(&self) -> CreateTableRequest {
        let mut request = CreateTableRequest::new(
            self.table_name(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: GSI_CATEGORY_ATTR.to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: GSI_SCORE_ATTR.to_string(),
                    attribute_type: KeyAttributeType::N,
                },
            ],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        );
        request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
            index_name: IndexName::new(GSI_INDEX_NAME),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: GSI_CATEGORY_ATTR.to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: GSI_SCORE_ATTR.to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        request.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
        request.aux_stream_duration_hours = Some(
            StreamRetentionDuration::from_hours(i64::from(TABLE_STREAM_DURATION_HOURS))
                .expect("configured table retention"),
        );
        request
    }
}

/// Only the initial workload observation uses the existing bounded read retry.
/// Later build observations are scheduled by the bounded build caller instead.
pub(crate) async fn read_initial_table_binding<B: kv_backend::KvBackend + 'static>(
    store: &kv_backend::KvStore<B>,
    name: TableName,
    clock: &dyn storage_types::Clock,
) -> storage_types::StorageResult<Arc<storage_types::BoundTableInfo>> {
    storage_common::retry::retry_native_conflicts(
        clock,
        storage_provider::DYNAMO_WRITE_RETRY,
        storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadOnly,
            storage_types::DeadlineClass::PointRead,
        ),
        |attempt_request| read_table_binding(store, name.clone(), attempt_request),
    )
    .await
}

pub(crate) async fn read_table_binding<B: kv_backend::KvBackend + 'static>(
    store: &kv_backend::KvStore<B>,
    name: TableName,
    attempt_request: storage_types::AttemptRequest,
) -> storage_types::StorageResult<Arc<storage_types::BoundTableInfo>> {
    let cache = storage_provider::DynamoPort::metadata_cache(store);
    storage_provider::DynamoPort::with_attempt(store, attempt_request, move |attempt| {
        Box::pin(async move {
            storage_provider::resolve_table_in_attempt(attempt, &cache, &name)
                .await
                .map(|(table, _)| table)
        })
    })
    .await
}
