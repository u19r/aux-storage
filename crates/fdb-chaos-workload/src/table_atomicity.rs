use crate::{common::*, imports::*};

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
    pub(crate) gsi_model: GsiIndexModel,
    pub(crate) trim_model: TrimStateModel,
    pub(crate) anomalies: Vec<Anomaly>,
    pub(crate) shared_audit: Option<SharedKeyAudit>,
    pub(crate) gsi_seen_partitions: BTreeSet<String>,
    pub(crate) gsi_unclassified_partitions: BTreeSet<String>,
    pub(crate) error_count: u64,
    pub(crate) audit_count: u64,
    pub(crate) gsi_audit_count: u64,
    pub(crate) trim_audit_count: u64,
    pub(crate) trim_execution_count: u64,
    pub(crate) stream_audit_count: u64,
    pub(crate) direct_stream_pointer_audit_count: u64,
    pub(crate) direct_stream_pointer_decoupled_target_count: u64,
    pub(crate) shared_operation_count: u64,
    pub(crate) context: WorkloadContext,
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
        let mut trim_model = TrimStateModel::default();
        trim_model.expect_scope(TrimScopeExpectation::table(
            "FdbChaosTableAtomicity".to_string(),
        ));
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
            gsi_model: GsiIndexModel::default(),
            trim_model,
            anomalies: Vec::new(),
            shared_audit: None,
            gsi_seen_partitions: BTreeSet::new(),
            gsi_unclassified_partitions: BTreeSet::new(),
            error_count: 0,
            audit_count: 0,
            gsi_audit_count: 0,
            trim_audit_count: 0,
            trim_execution_count: 0,
            stream_audit_count: 0,
            direct_stream_pointer_audit_count: 0,
            direct_stream_pointer_decoupled_target_count: 0,
            shared_operation_count: 0,
            context,
        }
    }

    pub(crate) fn store(&self, db: SimDatabase) -> Result<FoundationDbKvStore, String> {
        FoundationDbKvStore::from_database(
            FoundationDbConfig {
                cluster_file_path: None,
                tenant_name: None,
                subspace_prefix: Some(b"aux-storage/fdb-chaos/table-atomicity/".to_vec()),
                cache_read_version_ms: 0,
                immediate_gsi_consistency: false,
                report_conflicting_keys: false,
            },
            db,
        )
        .map_err(|err| storage_error_detail(&err))
    }

    pub(crate) async fn provider(&self, db: SimDatabase) -> Result<Arc<FdbChaosProvider>, String> {
        let provider = Arc::new(
            SortedKvDbStorageProvider::new(self.store(db)?)
                .with_database_jobs_enabled(false)
                .with_immediate_gsi_consistency(true),
        );
        provider
            .initialize_storage()
            .await
            .map_err(|err| storage_error_detail(&err))?;
        Ok(provider)
    }

    pub(crate) async fn manager(&self, db: SimDatabase) -> Result<DatabaseManager, String> {
        DatabaseManager::new_with_mocks(self.provider(db).await?)
            .map_err(|error| storage_error_detail(&error))
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
        )
        .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
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
        }]));
        request.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
        request.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(
            TABLE_STREAM_DURATION_HOURS,
        ));
        request.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::Forever);
        request
    }
}
