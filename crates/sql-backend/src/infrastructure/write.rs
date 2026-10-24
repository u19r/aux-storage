//! Every conditional batch has one savepoint and no committable partial result.
use infrastructure_provider::{
    batch::RecordBatch,
    record::{ConditionalWriteResult, RecordChange},
};
use storage_types::{InfrastructureResultKind, ReadProtection, Revision, StorageResult};

use super::{codec, error::invalid_result, sql_statements};
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

pub(super) async fn store_records<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    batch: &RecordBatch<'_>,
) -> StorageResult<ConditionalWriteResult> {
    if batch.as_ref().is_empty() {
        return Ok(ConditionalWriteResult::Applied);
    }
    let statements = prepare_statements(catalog.engine.engine(), batch.as_ref())?;
    catalog.attempt.execute(sql_statements::savepoint()).await?;
    let counts = catalog.attempt.execute_batch(statements).await?;
    let result = if let Some(index) = find_conflict(&counts, batch.as_ref().len())? {
        catalog.attempt.execute(sql_statements::rollback()).await?;
        let observed = load_conflicting_revision(catalog, &batch.as_ref()[index]).await?;
        ConditionalWriteResult::Conflict { observed }
    } else {
        ConditionalWriteResult::Applied
    };
    catalog.attempt.execute(sql_statements::release()).await?;
    Ok(result)
}

fn prepare_statements(
    engine: crate::SqlEngine,
    changes: &[RecordChange],
) -> StorageResult<Vec<crate::SqlStatement>> {
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(changes.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for change in changes {
        statements.push(sql_statements::change(engine, change)?);
    }
    Ok(statements)
}

fn find_conflict(counts: &[u64], expected: usize) -> StorageResult<Option<usize>> {
    if counts.len() != expected || counts.iter().any(|count| *count > 1) {
        return Err(invalid_result());
    }
    Ok(counts.iter().position(|count| *count == 0))
}

async fn load_conflicting_revision<A: SqlAttempt>(
    catalog: &mut SqlCatalogAttempt<'_, A, impl SqlLayout>,
    change: &RecordChange,
) -> StorageResult<Option<Revision>> {
    let statement = sql_statements::load_revision(catalog.engine.engine(), change.key())?;
    let budget =
        std::num::NonZeroUsize::new(std::mem::size_of::<u64>()).ok_or_else(invalid_result)?;
    let page = catalog
        .attempt
        .query(statement, ReadProtection::Conflict, budget)
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(invalid_result());
    }
    page.rows
        .into_iter()
        .next()
        .map(codec::decode_revision)
        .transpose()
}
