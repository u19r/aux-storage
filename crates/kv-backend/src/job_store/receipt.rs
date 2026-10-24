use scheduled_bg_jobs::store::CommitOperationId;
use storage_provider::{
    RawOrderedJobStoreError,
    job_store::{EstablishedCommit, EstablishedCommitOutcome, JobCommitError},
};

use crate::{
    KvAttempt, KvMutation, ReadProtection,
    constants::{
        JOB_RECEIPT_CHUNK_BYTES as CHUNK_BYTES, JOB_RECEIPT_MAX_CONTENT_BYTES as MAX_CONTENT_BYTES,
        JOB_RECEIPT_METADATA_BYTES as METADATA_BYTES, JOB_RECEIPT_PREFIX as RECEIPT_PREFIX,
        JOB_RECEIPT_VERSION as VERSION,
    },
};

pub(super) async fn load<A: KvAttempt>(
    attempt: &A,
    operation_id: CommitOperationId,
) -> Result<Option<EstablishedCommit>, ReceiptError> {
    let call = attempt.settlement().begin_call()?;
    let result = async {
        let Some(metadata) = attempt
            .get_admitted(
                &metadata_key(operation_id)?,
                ReadProtection::Conflict,
                crate::KvValueAllowance::new(METADATA_BYTES, METADATA_BYTES),
            )
            .await?
        else {
            return Ok(None);
        };
        let (outcome, content_bytes, chunk_count) = decode_metadata(&metadata)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(chunk_count)
            .map_err(|_| rejected())?;
        for index in 0..chunk_count {
            keys.push(chunk_key(operation_id, index)?);
        }
        let chunks = attempt
            .get_batch_admitted(
                keys,
                ReadProtection::Conflict,
                crate::KvValueAllowance::new(CHUNK_BYTES, content_bytes),
            )
            .await?;
        if chunks.len() != chunk_count {
            return Err(corrupt());
        }
        let mut content = Vec::new();
        content
            .try_reserve_exact(content_bytes)
            .map_err(|_| rejected())?;
        for chunk in chunks {
            let chunk = chunk.ok_or_else(corrupt)?;
            if chunk.is_empty()
                || chunk.len() > CHUNK_BYTES
                || content
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|bytes| bytes > content_bytes)
            {
                return Err(corrupt());
            }
            content.extend_from_slice(&chunk);
        }
        if content.len() != content_bytes {
            return Err(corrupt());
        }
        Ok(Some(EstablishedCommit {
            transaction_content: content,
            outcome,
        }))
    }
    .await;
    if let Err(error) = &result {
        match error {
            ReceiptError::Storage(error) => attempt.settlement().reject_error(error),
            ReceiptError::Corrupt(_) => attempt.settlement().reject(),
        }
    }
    call.complete(Ok(()))?;
    result
}

pub(super) fn encode(
    operation_id: CommitOperationId,
    content: &[u8],
    outcome: EstablishedCommitOutcome,
) -> Result<Vec<KvMutation>, ReceiptError> {
    if content.is_empty() || content.len() > MAX_CONTENT_BYTES {
        return Err(corrupt());
    }
    let chunks = content.chunks(CHUNK_BYTES);
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(METADATA_BYTES)
        .map_err(|_| rejected())?;
    metadata.push(VERSION);
    metadata.push(match outcome {
        EstablishedCommitOutcome::Committed => 0,
        EstablishedCommitOutcome::Conflict => 1,
    });
    metadata.extend_from_slice(&(content.len() as u32).to_be_bytes());
    metadata.extend_from_slice(&(chunks.len() as u32).to_be_bytes());
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(chunks.len() + 1)
        .map_err(|_| rejected())?;
    changes.push(KvMutation::Put {
        key: metadata_key(operation_id)?,
        value: metadata,
    });
    for (index, chunk) in chunks.enumerate() {
        let mut value = Vec::new();
        value
            .try_reserve_exact(chunk.len())
            .map_err(|_| rejected())?;
        value.extend_from_slice(chunk);
        changes.push(KvMutation::Put {
            key: chunk_key(operation_id, index)?,
            value,
        });
    }
    Ok(changes)
}

fn decode_metadata(
    metadata: &[u8],
) -> Result<(EstablishedCommitOutcome, usize, usize), ReceiptError> {
    if metadata.len() != METADATA_BYTES || metadata[0] != VERSION {
        return Err(corrupt());
    }
    let outcome = match metadata[1] {
        0 => EstablishedCommitOutcome::Committed,
        1 => EstablishedCommitOutcome::Conflict,
        _ => return Err(corrupt()),
    };
    let content_bytes =
        u32::from_be_bytes(metadata[2..6].try_into().map_err(|_| corrupt())?) as usize;
    let chunk_count =
        u32::from_be_bytes(metadata[6..10].try_into().map_err(|_| corrupt())?) as usize;
    if content_bytes == 0
        || content_bytes > MAX_CONTENT_BYTES
        || chunk_count != content_bytes.div_ceil(CHUNK_BYTES)
    {
        return Err(corrupt());
    }
    Ok((outcome, content_bytes, chunk_count))
}

fn metadata_key(operation_id: CommitOperationId) -> storage_types::StorageResult<Vec<u8>> {
    let mut key = base_key(operation_id)?;
    key.push(0);
    Ok(key)
}

fn chunk_key(
    operation_id: CommitOperationId,
    index: usize,
) -> storage_types::StorageResult<Vec<u8>> {
    let mut key = base_key(operation_id)?;
    key.push(1);
    key.extend_from_slice(&(index as u32).to_be_bytes());
    Ok(key)
}

fn base_key(operation_id: CommitOperationId) -> storage_types::StorageResult<Vec<u8>> {
    let mut key = Vec::new();
    key.try_reserve_exact(RECEIPT_PREFIX.len() + CommitOperationId::BYTE_LENGTH + 5)
        .map_err(|_| rejected())?;
    key.extend_from_slice(RECEIPT_PREFIX);
    key.extend_from_slice(operation_id.as_bytes());
    Ok(key)
}

pub(super) enum ReceiptError {
    Storage(storage_types::StorageError),
    Corrupt(RawOrderedJobStoreError),
}

impl From<storage_types::StorageError> for ReceiptError {
    fn from(error: storage_types::StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<ReceiptError> for JobCommitError {
    fn from(error: ReceiptError) -> Self {
        match error {
            ReceiptError::Storage(error) => Self::Storage(error),
            ReceiptError::Corrupt(error) => Self::Contract(error),
        }
    }
}

fn corrupt() -> ReceiptError {
    ReceiptError::Corrupt(RawOrderedJobStoreError::Corrupt(
        "scheduled job commit receipt",
    ))
}

fn rejected() -> storage_types::StorageError {
    storage_types::InfrastructureResultKind::NativeCapacityRejected.into()
}
