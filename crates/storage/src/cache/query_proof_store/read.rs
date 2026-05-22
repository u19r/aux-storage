use storage_cache::{
    RuntimePreparedQueryProofPagePlan, runtime_query_proof::RuntimeQueryReadBlockReason,
};
use storage_types::{QueryTableRequest, StorageResult, StoredTableInfo, TableName};

use crate::{
    query_proof_request::ParsedQueryRequest,
    query_proof_store::{
        InMemoryQueryProofCacheState, PreparedQueryProofRead, QueryManifestEntry,
        QueryProofMaterializedPage,
    },
};

struct PreparedQueryReadContext<'a> {
    parsed_request: ParsedQueryRequest,
    matching_entries: Vec<&'a QueryManifestEntry>,
    prepared_page_plan: RuntimePreparedQueryProofPagePlan,
}

impl InMemoryQueryProofCacheState {
    pub(crate) fn prepare_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead> {
        let context = match self.prepare_query_read_context(table_name, table_info, request) {
            Ok(context) => context,
            Err(reason) => return Ok(storage_cache::blocked_runtime_query_proof_read(reason)),
        };

        let materialized_page = if let Some(page_shape) = context.prepared_page_plan.page_shape {
            let returned_entries = context
                .matching_entries
                .into_iter()
                .take(page_shape.returned_count)
                .collect::<Vec<_>>();
            let last_evaluated_key = if page_shape.needs_resume_token {
                let last_entry = returned_entries.last().ok_or_else(|| {
                    storage_types::StorageError::internal(
                        "query proof page requested resume token without entries",
                    )
                })?;
                Some(storage_cache::next_page_token_for_query_entry(
                    table_name,
                    table_info,
                    context.parsed_request.manifest_key.index_name.as_deref(),
                    &last_entry.primary_key,
                    &last_entry.query_space_key,
                )?)
            } else {
                None
            };

            Some(QueryProofMaterializedPage {
                primary_keys: returned_entries
                    .into_iter()
                    .map(|entry| entry.primary_key.clone())
                    .collect(),
                last_evaluated_key,
                page_complete: page_shape.page_complete,
            })
        } else {
            None
        };

        Ok(PreparedQueryProofRead {
            plan: context.prepared_page_plan.plan,
            materialized_page,
        })
    }

    fn prepare_query_read_context<'a>(
        &'a self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> Result<PreparedQueryReadContext<'a>, RuntimeQueryReadBlockReason> {
        if !self.is_enabled() {
            return Err(RuntimeQueryReadBlockReason::CacheDisabled);
        }
        if request.consistent_read {
            return Err(RuntimeQueryReadBlockReason::StrongReadBypass);
        }

        let parsed_request = match ParsedQueryRequest::from_request(table_name, table_info, request)
        {
            Ok(Some(parsed)) => parsed,
            Ok(None) | Err(_) => return Err(RuntimeQueryReadBlockReason::UnsupportedKeyCondition),
        };
        let Some(partition) = self.partitions.get(&parsed_request.manifest_key) else {
            return Err(RuntimeQueryReadBlockReason::MissingPartition);
        };
        if partition.coverage.continuity_broken {
            return Err(RuntimeQueryReadBlockReason::ContinuityBroken);
        }
        if partition.coverage.rebuilding {
            return Err(RuntimeQueryReadBlockReason::Rebuilding);
        }
        if partition.coverage.schema_fingerprint != parsed_request.schema_fingerprint {
            return Err(RuntimeQueryReadBlockReason::SchemaMismatch);
        }
        if !parsed_request.shape.coverage_semantics.coverage_supported
            || partition.coverage.covered_ranges.is_empty()
        {
            return Err(RuntimeQueryReadBlockReason::MissingCoverage);
        }

        let order_keys = partition.ordered_entry_keys.iter().collect::<Vec<_>>();
        let prepared_window = storage_cache::prepare_runtime_query_window(
            parsed_request.runtime_bounds(),
            parsed_request.shape.limit_option(),
            &order_keys
                .iter()
                .map(|entry| entry.sort_key_order_repr.as_deref())
                .collect::<Vec<_>>(),
            &storage_cache::borrow_runtime_coverage_ranges(&partition.coverage.covered_ranges),
            &storage_cache::borrow_runtime_coverage_ranges(
                &partition.coverage.current_schema_ranges,
            ),
            &partition.page_witnesses,
        );
        let Some(prepared_window) = prepared_window else {
            return Err(RuntimeQueryReadBlockReason::StartNotCovered);
        };
        let prepared_page_plan = storage_cache::prepare_runtime_query_proof_page_plan(
            None,
            prepared_window.matching_indexes.len(),
            prepared_window.witnessed_page_len,
            parsed_request.limit(),
            prepared_window.request_exhausted,
        );

        Ok(PreparedQueryReadContext {
            prepared_page_plan,
            matching_entries: prepared_window
                .matching_indexes
                .into_iter()
                .filter_map(|index| partition.entries.get(&order_keys[index].primary_key_json))
                .collect(),
            parsed_request,
        })
    }
}
