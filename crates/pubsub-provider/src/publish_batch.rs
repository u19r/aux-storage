//! `PublishBatch` acceptance (W3 design 8, AWS run 2 A7): one Durable
//! attempt reads the topic and its subscriptions once and writes one
//! publication with its own snapshot generation for each valid entry. An
//! invalid entry is a `Failed` member; a storage error fails the request.

use std::sync::Arc;

use storage_types::StorageResult;

use crate::{
    AcceptedPublication, PublishRequest, PubsubAttempt, PubsubError, PubsubMessageId, PubsubPort,
    PubsubResult, PubsubValidationKind, SubscriptionSnapshot, TopicArn,
    constants::{PUBLISH_WRITE, PUBSUB_SNAPSHOT_CHUNK_SIZE},
    publication_acceptance::{PublishAcceptanceInput, collect_subscriptions},
    pubsub_port::read_topic,
};

/// One decoded entry of a `PublishBatch` request.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct PublishBatchEntry {
    pub id: String,
    pub request: PublishRequest,
    /// Not supported; an entry that sets it fails.
    pub message_structure: Option<String>,
}

/// The result of one entry, in request order.
#[derive(Debug)]
pub enum PublishBatchResult {
    Successful {
        id: String,
        message_id: PubsubMessageId,
    },
    Failed {
        id: String,
        error: PubsubError,
    },
}

impl PublishBatchEntry {
    /// The checks of `Publish` for one entry, and the AWS texts for
    /// `MessageStructure`.
    fn validate(&mut self) -> PubsubResult<()> {
        if let Some(structure) = &self.message_structure {
            if structure == "json"
                && serde_json::from_str::<serde_json::Value>(&self.request.message).is_err()
            {
                return Err(PubsubError::validation_with_detail(
                    PubsubValidationKind::InvalidAttributeValue,
                    "Message Structure - JSON message body failed to parse",
                ));
            }
            return Err(PubsubError::validation_with_detail(
                PubsubValidationKind::UnsupportedAttribute,
                "MessageStructure",
            ));
        }
        self.request.validate()
    }
}

struct BatchInput {
    topic_arn: TopicArn,
    accepted: Vec<PublishAcceptanceInput>,
}

pub async fn accept_publish_batch_on_port<P: PubsubPort>(
    port: &P,
    topic_arn: TopicArn,
    entries: Vec<PublishBatchEntry>,
    custom_sender_enabled: bool,
) -> PubsubResult<Vec<PublishBatchResult>> {
    let accepted_at = port.now();
    let mut results = Vec::with_capacity(entries.len());
    let mut accepted = Vec::new();
    for mut entry in entries {
        entry.request.topic_arn = topic_arn.clone();
        match entry.validate() {
            Ok(()) => {
                let message_id = PubsubMessageId::new();
                accepted.push(PublishAcceptanceInput::new(
                    entry.request,
                    message_id.clone(),
                    accepted_at,
                    custom_sender_enabled,
                ));
                results.push(PublishBatchResult::Successful {
                    id: entry.id,
                    message_id,
                });
            }
            Err(error) => results.push(PublishBatchResult::Failed {
                id: entry.id,
                error,
            }),
        }
    }
    let input = Arc::new(BatchInput {
        topic_arn,
        accepted,
    });
    let found = port
        .run(PUBLISH_WRITE, Arc::clone(&input), |attempt, input, _| {
            Box::pin(write_batch(
                attempt,
                input.topic_arn.clone(),
                input.accepted.clone(),
            ))
        })
        .await
        .map_err(PubsubError::from)?;
    if !found {
        return Err(PubsubError::topic_not_found(input.topic_arn.to_string()));
    }
    Ok(results)
}

/// Returns false when the topic does not exist; nothing is written then.
async fn write_batch<A: PubsubAttempt>(
    attempt: &mut A,
    topic_arn: TopicArn,
    accepted: Vec<PublishAcceptanceInput>,
) -> StorageResult<bool> {
    let Some(topic) = read_topic(attempt, &topic_arn).await? else {
        return Ok(false);
    };
    let subscriptions = collect_subscriptions(attempt, topic.incarnation).await?;
    for acceptance in accepted {
        // Each publication has its own generation: chunks are deleted by
        // generation, so two publications cannot share a snapshot.
        let snapshot = SubscriptionSnapshot::with_generation(
            topic.topic.topic_arn.clone(),
            subscriptions.clone(),
            PUBSUB_SNAPSHOT_CHUNK_SIZE,
            acceptance.snapshot_generation,
        )
        .map_err(|_| {
            storage_types::StorageError::internal("publish snapshot exceeds supported size")
        })?;
        let publication = AcceptedPublication::new(snapshot, acceptance);
        publication.validate()?;
        attempt.write_publication(publication).await?;
    }
    Ok(true)
}
