use std::collections::HashMap;

use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_types::{
    AttributeValue, DurablePointReadGuard, DurableTransactWriteGuard, StorageEnum, StorageResult,
    context::WrappedError,
};

use crate::{
    AuthoritativePointReadHit, AuthoritativePointReadPurpose, AuthoritativePointReadResult,
    PointReadGetRequest,
    cache_read_observability::{
        StorageGuardFallbackReason, record_authoritative_preimage_hit,
        record_authoritative_preimage_miss, record_guard_fallback,
    },
    database_manager::DatabaseManager,
};

pub(crate) struct CachedGuardedPreImage {
    pub(crate) item: Option<HashMap<String, AttributeValue>>,
    pub(crate) guard: DurablePointReadGuard,
}

impl CachedGuardedPreImage {
    pub(crate) fn into_transaction_guard(
        self,
        request: &PointReadGetRequest,
    ) -> DurableTransactWriteGuard {
        DurableTransactWriteGuard {
            table_name: request.table_name.clone(),
            key: request.key.clone(),
            guard: self.guard,
        }
    }
}

pub(crate) async fn authoritative_preimage(
    manager: &DatabaseManager,
    request: &PointReadGetRequest,
    purpose: AuthoritativePointReadPurpose,
) -> StorageResult<Option<CachedGuardedPreImage>> {
    let purpose_label = preimage_purpose_label(purpose);
    if !manager
        .cache_services
        .authoritative_write_preimages_enabled()
    {
        return Ok(None);
    }
    let result = manager
        .cache_services
        .get_authoritative_point_read(request, purpose)
        .await?;
    let hit = match result {
        AuthoritativePointReadResult::Hit(hit) => *hit,
        AuthoritativePointReadResult::Miss => {
            record_authoritative_preimage_miss(purpose_label);
            return Ok(None);
        }
    };
    match hit {
        AuthoritativePointReadHit::Present {
            item,
            revision: Some(revision),
        } => {
            record_authoritative_preimage_hit(purpose_label);
            Ok(Some(CachedGuardedPreImage {
                item: Some(item.into_attribute_map()?),
                guard: DurablePointReadGuard::Present(revision),
            }))
        }
        AuthoritativePointReadHit::Absent { proof: Some(proof) } => {
            record_authoritative_preimage_hit(purpose_label);
            Ok(Some(CachedGuardedPreImage {
                item: None,
                guard: DurablePointReadGuard::Absent(proof),
            }))
        }
        AuthoritativePointReadHit::Present { revision: None, .. }
        | AuthoritativePointReadHit::Absent { proof: None } => {
            record_authoritative_preimage_miss(purpose_label);
            Ok(None)
        }
    }
}

pub(crate) fn condition_matches(
    condition_expression: Option<&String>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    cached_item: &Option<HashMap<String, AttributeValue>>,
) -> StorageResult<bool> {
    let Some(condition_expression) = condition_expression else {
        return Ok(true);
    };
    let condition = parse_condition_expression(
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    )
    .map_err(|_| StorageEnum::ConditionalCheckFailed)?;
    Ok(evaluate_condition(
        cached_item.as_ref().unwrap_or(&HashMap::new()),
        &condition,
    ))
}

pub(crate) fn should_fallback(error: &storage_types::StorageError) -> bool {
    matches!(
        error.to_enum(),
        StorageEnum::GuardConflict { .. } | StorageEnum::Unsupported { .. }
    )
}

pub(crate) fn record_fallback(operation: &'static str, error: &storage_types::StorageError) {
    match error.to_enum() {
        StorageEnum::GuardConflict { .. } => {
            record_guard_fallback(operation, StorageGuardFallbackReason::GuardConflict);
        }
        StorageEnum::Unsupported { .. } => {
            record_guard_fallback(operation, StorageGuardFallbackReason::Unsupported);
        }
        _ => {}
    }
}

pub(crate) fn record_unsupported_fallback(operation: &'static str) {
    record_guard_fallback(operation, StorageGuardFallbackReason::Unsupported);
}

fn preimage_purpose_label(purpose: AuthoritativePointReadPurpose) -> &'static str {
    match purpose {
        AuthoritativePointReadPurpose::UpdatePreImage => "update",
        AuthoritativePointReadPurpose::ConditionalPutPreImage => "conditional_put",
        AuthoritativePointReadPurpose::ConditionalDeletePreImage => "conditional_delete",
        AuthoritativePointReadPurpose::TransactionPreImage => "transaction",
        AuthoritativePointReadPurpose::QueryProofPrewriteImage => "query_proof_prewrite",
        AuthoritativePointReadPurpose::StrongGet => "strong_get",
        AuthoritativePointReadPurpose::StrongBatchGet => "strong_batch_get",
    }
}
