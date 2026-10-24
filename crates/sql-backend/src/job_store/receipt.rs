use scheduled_bg_jobs::store::CommitOperationId;
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{EstablishedCommit, EstablishedCommitOutcome, JobCommitError},
};

use super::sql_statements;
use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlStatement, SqlValue,
    constants::{
        JOB_RECEIPT_MAX_CONTENT_BYTES as MAX_CONTENT_BYTES,
        SCHEDULED_JOB_POINT_READ_BYTES as POINT_READ_BYTES,
    },
};

pub(super) async fn load<A: SqlAttempt>(
    attempt: &mut A,
    engine: SqlEngine,
    operation_id: CommitOperationId,
) -> Result<Option<EstablishedCommit>, JobCommitError> {
    let page = attempt
        .query(
            sql_statements::read_receipt(engine, operation_id)?,
            ReadProtection::Conflict,
            POINT_READ_BYTES,
        )
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(corrupt().into());
    }
    page.rows.into_iter().next().map(decode).transpose()
}

pub(super) fn save(
    engine: SqlEngine,
    operation_id: CommitOperationId,
    content: &[u8],
    outcome: EstablishedCommitOutcome,
) -> Result<SqlStatement, JobCommitError> {
    if content.is_empty() || content.len() > MAX_CONTENT_BYTES {
        return Err(corrupt().into());
    }
    sql_statements::save_receipt(engine, operation_id, content, outcome).map_err(Into::into)
}

fn decode(row: crate::SqlRow) -> Result<EstablishedCommit, JobCommitError> {
    let columns: [SqlValue; 2] = row.into_values().try_into().map_err(|_| corrupt())?;
    let [SqlValue::Integer(outcome), SqlValue::Blob(content)] = columns else {
        return Err(corrupt().into());
    };
    let outcome = match outcome {
        0 => EstablishedCommitOutcome::Committed,
        1 => EstablishedCommitOutcome::Conflict,
        _ => return Err(corrupt().into()),
    };
    if content.is_empty() || content.len() > MAX_CONTENT_BYTES {
        return Err(corrupt().into());
    }
    Ok(EstablishedCommit {
        transaction_content: content,
        outcome,
    })
}

fn corrupt() -> RawOrderedJobStoreError {
    RawOrderedJobStoreError::Corrupt("scheduled job commit receipt")
}
