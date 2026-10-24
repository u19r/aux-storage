use std::collections::HashMap;

use queue_provider::{MessageId, Queue, QueueMessage, ReceiptHandle};
use storage_types::{DurationSeconds, StorageError, StorageResult, TimestampMillis};

use super::{StatefulTestBackend, models::QueueReclamationFixtureTarget};

const FIRST_BODY: &str = "queue-cleanup-incarnation-a";

impl StatefulTestBackend {
    pub async fn establish_deleted_queue_payload(&self, queue_name: &str) -> StorageResult<()> {
        let provider = self
            .database
            .as_ref()
            .expect("fixture database is open")
            .initialization_queue_provider()
            .ok_or_else(|| StorageError::unsupported("queue provider was not selected"))?;
        let queue = provider
            .create_queue(Queue {
                queue_name: queue_name.to_owned(),
                queue_url: format!("https://queue.example.test/000000000000/{queue_name}"),
                attributes: HashMap::new(),
                created_at: TimestampMillis::now(),
            })
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let message_id = MessageId::from_bytes([7; 12]);
        provider
            .send_message(queue_message(&queue.queue_url, message_id, FIRST_BODY))
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let received = provider
            .receive_messages(
                &queue.queue_url,
                1,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        let message = received
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::internal("queue fixture message was not received"))?;
        provider
            .delete_message(
                &queue.queue_url,
                ReceiptHandle::from(message.receipt_handle.as_str()),
            )
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        *self
            .queue_reclamation_target
            .lock()
            .expect("queue reclamation target lock") = Some(QueueReclamationFixtureTarget {
            queue_url: queue.queue_url,
            message_id,
        });
        Ok(())
    }

    pub async fn begin_queue_message_cleanup(&self) -> StorageResult<()> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .drive_queue_cleanup_once()
            .await
    }

    pub async fn reincarnate_queue_payload(&self, body: &str) -> StorageResult<()> {
        let (queue_url, message_id) = {
            let target = self
                .queue_reclamation_target
                .lock()
                .expect("queue reclamation target lock");
            let target = target
                .as_ref()
                .expect("queue reclamation target is configured");
            (target.queue_url.clone(), target.message_id)
        };
        self.database
            .as_ref()
            .expect("fixture database is open")
            .initialization_queue_provider()
            .ok_or_else(|| StorageError::unsupported("queue provider was not selected"))?
            .send_message(queue_message(&queue_url, message_id, body))
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        Ok(())
    }

    pub async fn finish_queue_message_cleanup(&self) -> StorageResult<()> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .drive_queue_cleanup_until_idle()
            .await
    }

    pub async fn receive_queue_payload(&self) -> StorageResult<Option<String>> {
        let queue_url = self
            .queue_reclamation_target
            .lock()
            .expect("queue reclamation target lock")
            .as_ref()
            .expect("queue reclamation target is configured")
            .queue_url
            .clone();
        let mut received = self
            .database
            .as_ref()
            .expect("fixture database is open")
            .initialization_queue_provider()
            .ok_or_else(|| StorageError::unsupported("queue provider was not selected"))?
            .receive_messages(
                &queue_url,
                1,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .map_err(|error| StorageError::internal(&error.to_string()))?;
        Ok(received.pop().map(|message| message.body))
    }
}

fn queue_message(queue_url: &str, message_id: MessageId, body: &str) -> QueueMessage {
    let now = TimestampMillis::now();
    QueueMessage {
        receive_count: 0,
        message_id,
        queue_url: queue_url.to_owned(),
        body: body.to_owned(),
        message_attributes: None,
        receipt_handle: None,
        created_at: now,
        visibility_timestamp: Some(now),
    }
}
