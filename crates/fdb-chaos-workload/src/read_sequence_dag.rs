use crate::{common::*, imports::*};

mod constants;
mod ordinary;
mod runtime;
mod shape;

/// FoundationDB simulator workload for the production ReadSequence graph.
///
/// The fixture runs the ordinary provider path and the canonical Tuple mapped
/// lowering against the same planned request, then compares both with a small
/// deterministic result oracle.
pub(crate) struct ReadSequenceDagWorkload {
    name: String,
    profile: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    operation_count: u64,
    attempts: u64,
    published: u64,
    retries: u64,
    mismatches: u64,
    errors: u64,
    oracle_checks: u64,
    ordinary_attempts: u64,
    mapped_attempts: u64,
    mapped_selected: u64,
    mapped_fallbacks: u64,
    permuted_plans: u64,
    seed: u64,
    buggify: bool,
    context: WorkloadContext,
}

type NormalizedResult =
    std::collections::BTreeMap<String, Vec<Vec<std::collections::HashMap<String, AttributeValue>>>>;

impl ReadSequenceDagWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let _: String =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        let client_count = context.client_count();
        Self {
            name,
            profile: option_or_default_string(&context, OPTION_PROFILE, "read-sequence-dag"),
            client_id,
            client_count,
            active_client_count: option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32)
                .clamp(1, client_count),
            operation_count: option_or_default(&context, OPTION_OPERATION_COUNT, 1_u64),
            attempts: 0,
            published: 0,
            retries: 0,
            mismatches: 0,
            errors: 0,
            oracle_checks: 0,
            ordinary_attempts: 0,
            mapped_attempts: 0,
            mapped_selected: 0,
            mapped_fallbacks: 0,
            permuted_plans: 0,
            seed: option_or_default(&context, OPTION_SEED, 1_u64),
            buggify: option_or_default_string(&context, OPTION_BUGGIFY, "off") == "on",
            context,
        }
    }

    fn mapped_tenant(&self) -> Vec<u8> {
        format!(
            "aux-storage/fdb-chaos/read-sequence/{}/{}",
            self.seed, self.client_id
        )
        .into_bytes()
    }

    fn mapped_table_name(&self) -> TableName {
        TableName::new(&format!("ReadSequenceDag{}{}", self.seed, self.client_id))
    }

    fn mapped_index_name(&self) -> IndexName {
        IndexName::new(constants::MAPPED_INDEX_NAME)
    }

    async fn mapped_provider(&self, db: SimDatabase) -> Result<Arc<FdbChaosProvider>, String> {
        let store = FoundationDbKvStore::from_database(
            FoundationDbConfig {
                cluster_file_path: None,
                tenant_name: Some(self.mapped_tenant()),
                subspace_prefix: None,
                cache_read_version_ms: 0,
                immediate_gsi_consistency: true,
                report_conflicting_keys: false,
            },
            db,
        )
        .map_err(|error| storage_error_detail(&error))?;
        let provider = Arc::new(
            SortedKvDbStorageProvider::new(store)
                .with_database_jobs_enabled(false)
                .with_immediate_gsi_consistency(true),
        );
        provider
            .initialize_storage()
            .await
            .map_err(|error| storage_error_detail(&error))?;
        Ok(provider)
    }
}
