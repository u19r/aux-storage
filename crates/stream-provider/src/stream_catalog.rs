//! User-stream catalog decisions shared by the SQL and KV stores.

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, DurationSeconds, SemanticDecision, StorageResult, StreamName, UserStreamName,
};

use crate::{
    Stream, StreamAttempt, StreamError, StreamPartitioningMode, StreamPartitioningSupport,
    StreamPort, StreamResult,
    constants::{STREAM_WRITE_RETRY, USER_STREAM_PREFIX},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct StreamCatalogRecord {
    pub stream: Stream,
    pub deleted: bool,
}

#[derive(Clone, Copy)]
pub enum StreamCatalogLookup<'a> {
    Name(&'a str),
    Id(&'a StreamName),
}

impl StreamCatalogLookup<'_> {
    #[must_use]
    pub fn matches(self, record: &StreamCatalogRecord) -> bool {
        match self {
            Self::Name(name) => record.stream.name.as_str() == name,
            Self::Id(id) => record.stream.internal_id == *id,
        }
    }
}

pub(crate) async fn create_stream<P: StreamPort>(
    port: &P,
    name: UserStreamName,
    ttl_seconds: Option<DurationSeconds>,
    partitioning_mode: StreamPartitioningMode,
) -> StreamResult<StreamName> {
    let partitioning_mode = match (partitioning_mode, port.partitioning_support()) {
        (StreamPartitioningMode::KeyOrdered, StreamPartitioningSupport::NormalizeToSingle) => {
            StreamPartitioningMode::Single
        }
        (StreamPartitioningMode::KeyOrdered, StreamPartitioningSupport::KeyOrdered) => {
            StreamPartitioningMode::KeyOrdered
        }
        (StreamPartitioningMode::Single, _) => StreamPartitioningMode::Single,
    };
    let internal_id = new_stream_id();
    let stream = Stream {
        name,
        internal_id: internal_id.clone(),
        ttl_seconds,
        partitioning_mode,
        created_at: port.now(),
    };
    let decision = retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let stream = stream.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { create_stream_in_attempt(attempt, stream).await })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)?;
    decision
        .into_result()
        .map_err(StreamError::from_storage_error)?;
    Ok(internal_id)
}

pub async fn create_stream_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    stream: Stream,
) -> StorageResult<SemanticDecision<()>> {
    let mut settlement = PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let name = stream.name.as_str().to_owned();
        if attempt
            .read_stream_record(StreamCatalogLookup::Name(&name))
            .await?
            .is_some_and(|record| !record.deleted)
        {
            return Ok(SemanticDecision::Rejected(
                StreamError::stream_already_exists(name).into_storage_error(),
            ));
        }
        attempt
            .write_stream_record(
                &name,
                StreamCatalogRecord {
                    stream,
                    deleted: false,
                },
            )
            .await?;
        Ok(SemanticDecision::Accepted(()))
    }
    .await;
    settlement.complete(result)
}

pub async fn load_user_stream_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    name: &UserStreamName,
) -> StorageResult<SemanticDecision<Stream>> {
    let mut settlement = PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        Ok(attempt
            .read_stream_record(StreamCatalogLookup::Name(name.as_str()))
            .await?
            .filter(|record| !record.deleted)
            .map(|record| SemanticDecision::Accepted(record.stream))
            .unwrap_or_else(|| {
                SemanticDecision::Rejected(
                    StreamError::stream_not_found(name.as_str()).into_storage_error(),
                )
            }))
    }
    .await;
    settlement.complete(result)
}

pub(crate) async fn get_stream<P: StreamPort>(
    port: &P,
    name: UserStreamName,
) -> StreamResult<Option<Stream>> {
    let name = name.as_str().to_owned();
    port.with_stream_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            Ok(attempt
                .read_stream_record(StreamCatalogLookup::Name(&name))
                .await?
                .filter(|record| !record.deleted)
                .map(|record| record.stream))
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

pub(crate) async fn delete_stream<P: StreamPort>(
    port: &P,
    name: UserStreamName,
) -> StreamResult<()> {
    let decision = retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let name = name.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                match load_user_stream_in_attempt(attempt, &name).await? {
                    SemanticDecision::Accepted(stream) => {
                        delete_stream_in_attempt(attempt, &name, stream).await?;
                        Ok(SemanticDecision::Accepted(()))
                    }
                    SemanticDecision::Rejected(error) => Ok(SemanticDecision::Rejected(error)),
                }
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)?;
    decision
        .into_result()
        .map_err(StreamError::from_storage_error)
}

/// Delete the catalog incarnation observed on this attempt without reloading
/// it.
pub async fn delete_stream_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    name: &UserStreamName,
    stream: Stream,
) -> StorageResult<()> {
    let mut settlement = PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        attempt
            .write_stream_record(
                name.as_str(),
                StreamCatalogRecord {
                    stream,
                    deleted: true,
                },
            )
            .await
    }
    .await;
    settlement.complete(result)
}

fn new_stream_id() -> StreamName {
    let mut id = USER_STREAM_PREFIX.to_vec();
    id.extend_from_slice(uuid::Uuid::now_v7().as_bytes());
    StreamName::from(id)
}
