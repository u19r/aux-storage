use super::constants;
use crate::{common::*, constants::*, imports::*};

/// FoundationDB simulator workload for the production ReadSequence graph.
///
/// The fixture compares ordinary reads and shared graph execution against the
/// same deterministic oracle, including public continuation and node
/// permutation.
pub(crate) struct ReadSequenceDagWorkload {
    pub(super) name: String,
    pub(super) profile: String,
    pub(super) client_id: i32,
    pub(super) client_count: i32,
    pub(super) active_client_count: i32,
    pub(super) operation_count: u64,
    pub(super) attempts: u64,
    pub(super) published: u64,
    pub(super) retries: u64,
    pub(super) mismatches: u64,
    pub(super) errors: u64,
    pub(super) oracle_checks: u64,
    pub(super) ordinary_attempts: u64,
    pub(super) mapped_attempts: u64,
    pub(super) mapped_selected: u64,
    pub(super) mapped_fallbacks: u64,
    pub(super) permuted_plans: u64,
    pub(super) seed: u64,
    pub(super) buggify: bool,
    pub(super) context: WorkloadContext,
}

pub(super) type NormalizedResult =
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

    pub(super) fn mapped_tenant(&self) -> Vec<u8> {
        format!(
            "aux-storage/fdb-chaos/read-sequence/{}/{}",
            self.seed, self.client_id
        )
        .into_bytes()
    }

    pub(super) fn mapped_table_name(&self) -> TableName {
        TableName::new(&format!("ReadSequenceDag{}{}", self.seed, self.client_id))
    }

    pub(super) fn mapped_index_name(&self) -> IndexName {
        IndexName::new(constants::MAPPED_INDEX_NAME)
    }

    pub(super) async fn mapped_provider(
        &self,
        db: SimDatabase,
    ) -> Result<Arc<dyn storage_provider::DynamoOperations>, String> {
        let tenant = self.mapped_tenant();
        let driver =
            kv_backend::foundationdb::FoundationDbBackend::from_simulation(db, None, Some(&tenant));
        let store = kv_backend::KvStore::initialize_format(driver)
            .await
            .map_err(|error| storage_error_detail(&error))?;
        Ok(Arc::new(store))
    }
}
