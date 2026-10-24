//! Idempotent creation retains the actual protected native incarnation.

use storage_types::StorageResult;

use crate::{Queue, QueueAttempt, QueueCatalogRecord, QueueError, QueueResult};

pub enum QueueCreation {
    Created(QueueCatalogRecord),
    Existing(QueueCatalogRecord),
}

impl From<QueueCreation> for Queue {
    fn from(value: QueueCreation) -> Self {
        match value {
            QueueCreation::Created(record) | QueueCreation::Existing(record) => record.queue,
        }
    }
}

pub async fn create_queue_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    proposed: QueueCatalogRecord,
) -> StorageResult<QueueResult<QueueCreation>> {
    if let Some(existing) = attempt
        .read_queue_by_name(&proposed.queue.queue_name)
        .await?
    {
        if existing.queue.queue_url == proposed.queue.queue_url
            && existing.queue.attributes == proposed.queue.attributes
        {
            return Ok(Ok(QueueCreation::Existing(existing)));
        }
        return Ok(Err(queue_exists(proposed.queue.queue_name)));
    }
    if crate::queue_catalog::read_queue_by_url(attempt, &proposed.queue.queue_url)
        .await?
        .is_some()
    {
        return Ok(Err(queue_exists(proposed.queue.queue_name)));
    }
    attempt.write_queue_record(proposed.clone(), None).await?;
    Ok(Ok(QueueCreation::Created(proposed)))
}

fn queue_exists(name: String) -> QueueError {
    QueueError::ResourceExists {
        resource_type: "queue",
        resource_id: name,
    }
}
