//! Bounded point reads and exclusive ordered continuation on the caller
//! attempt.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    limits::InfrastructureLimits,
    page::{RecordPage, RecordPageRequest},
    record::{DurableRecord, DurableRecordKey},
};
use storage_common::point_results::try_place_value;
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ReadProtection, StorageResult,
};

use super::{codec, error::invalid_result, sql_statements};
use crate::{
    SqlAttempt, SqlLayout, SqlQueryPage, catalog::SqlCatalogAttempt,
    constants::INFRASTRUCTURE_POINT_LOAD_KEYS,
};

pub(super) async fn load_records<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    keys: &[DurableRecordKey],
    protection: ReadProtection,
    limits: InfrastructureLimits,
) -> StorageResult<Vec<Option<DurableRecord>>> {
    let mut used_bytes = limits.validate_keys(keys)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(keys.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for chunk in keys.chunks(INFRASTRUCTURE_POINT_LOAD_KEYS) {
        // SQL rows include current keys; their logical bytes are already
        // reserved.
        let chunk_bytes = limits.validate_keys(chunk)?;
        let budget =
            NonZeroUsize::new(limits.max_bytes().get().saturating_sub(used_bytes) + chunk_bytes)
                .unwrap_or(NonZeroUsize::MIN);
        let statement = sql_statements::load_batch(catalog.engine.engine(), chunk, limits)?;
        let page = catalog.attempt.query(statement, protection, budget).await?;
        records.extend(decode_load_page(page, chunk, limits, &mut used_bytes)?);
    }
    Ok(records)
}

fn decode_load_page(
    page: SqlQueryPage,
    keys: &[DurableRecordKey],
    limits: InfrastructureLimits,
    used_bytes: &mut usize,
) -> StorageResult<Vec<Option<DurableRecord>>> {
    if page.more {
        return Err(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::Bounds,
        }
        .into());
    }
    let mut records = Vec::new();
    records
        .try_reserve_exact(keys.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    records.resize_with(keys.len(), || None);
    for row in page.rows {
        let record = codec::decode_record(row)?;
        limits.validate_record(&record)?;
        let bytes = record.bytes.len();
        let count = try_place_value(keys, &mut records, |key, record| key == &record.key, record)
            .map_err(|error| match error.as_ref() {
            storage_types::StorageEnum::InfrastructureResult(
                InfrastructureResultKind::NativeCapacityRejected,
            ) => error,
            _ => invalid_result(),
        })?;
        let bytes = bytes.checked_mul(count).ok_or_else(invalid_result)?;
        *used_bytes = used_bytes.checked_add(bytes).ok_or_else(invalid_result)?;
    }
    limits.check_batch(keys.len(), *used_bytes)?;
    Ok(records)
}

pub(super) async fn scan_records<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    limits: InfrastructureLimits,
    request: RecordPageRequest,
) -> StorageResult<RecordPage> {
    request.validate(limits)?;
    // Keep the complete bounded lookahead available for decoding before it can
    // establish a continuation, even when the admitted prefix fills the page.
    let budget = limits
        .max_key_bytes()
        .get()
        .checked_add(limits.max_record_bytes().get())
        .and_then(|row| request.max_bytes.get().checked_add(row))
        .and_then(NonZeroUsize::new)
        .ok_or(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::RecordPage,
        })?;
    let statement = sql_statements::scan(catalog.engine.engine(), &request, limits)?;
    let page = catalog
        .attempt
        .query(statement, request.protection, budget)
        .await?;
    decode_page(page, limits, &request)
}

fn decode_page(
    page: SqlQueryPage,
    limits: InfrastructureLimits,
    request: &RecordPageRequest,
) -> StorageResult<RecordPage> {
    RecordPage::try_new(
        page.rows.into_iter().map(codec::decode_record),
        page.more,
        limits,
        request,
    )
}
