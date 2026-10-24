//! Queue identity, idempotent creation, and catalog paging are shared policy.

use std::{collections::HashMap, num::NonZeroU32};

use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    Queue, QueueAttempt, QueueError, QueueMessageCounts, QueueOperation, QueuePort, QueueResult,
    constants::{CATALOG_PAGE_ITEMS, QUEUE_WRITE_RETRY},
};

/// A fresh incarnation makes old messages unreachable after delete/recreate.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueueIncarnation([u8; 16]);

impl Default for QueueIncarnation {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueIncarnation {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7().into_bytes())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl TryFrom<&[u8]> for QueueIncarnation {
    type Error = StorageError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes = value
            .try_into()
            .map_err(|_| StorageError::internal("queue incarnation has invalid length"))?;
        Ok(Self(bytes))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueueCatalogRecord {
    pub queue: Queue,
    pub incarnation: QueueIncarnation,
}

#[derive(Clone)]
pub struct QueueCatalogRequest {
    pub prefix: Option<String>,
    pub exclusive_start: Option<String>,
    pub limit: NonZeroU32,
}

pub struct QueueCatalogPage {
    pub rows: Vec<QueueCatalogRecord>,
    pub more: bool,
}

pub async fn read_queue_by_url<A: QueueAttempt>(
    attempt: &mut A,
    url: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    let [record]: [_; 1] = attempt
        .read_queues_by_url(vec![url.to_owned()])
        .await?
        .try_into()
        .map_err(|_| StorageError::internal("queue URL batch count differs"))?;
    Ok(record)
}

pub(crate) async fn create_queue<P: QueuePort>(port: &P, queue: Queue) -> QueueResult<Queue> {
    let proposed = QueueCatalogRecord {
        queue,
        incarnation: QueueIncarnation::new(),
    };
    let created = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let proposed = proposed.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { crate::create_queue_in_attempt(attempt, proposed).await })
        })
    })
    .await?;
    created.map(Queue::from)
}

pub(crate) async fn get_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<Option<Queue>> {
    let url = url.to_owned();
    port.with_queue_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            Ok(read_queue_by_url(attempt, &url)
                .await?
                .map(|record| record.queue))
        })
    })
    .await
    .map_err(QueueError::from)
}

pub(crate) async fn get_queue_by_name<P: QueuePort>(
    port: &P,
    name: &str,
) -> QueueResult<Option<Queue>> {
    let name = name.to_owned();
    port.with_queue_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            Ok(attempt
                .read_queue_by_name(&name)
                .await?
                .map(|record| record.queue))
        })
    })
    .await
    .map_err(QueueError::from)
}

pub(crate) async fn list_queues<P: QueuePort>(
    port: &P,
    prefix: Option<&str>,
) -> QueueResult<Vec<Queue>> {
    let prefix = prefix.map(str::to_owned);
    port.with_queue_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            let mut result = Vec::new();
            let mut exclusive_start = None;
            loop {
                let request = QueueCatalogRequest {
                    prefix: prefix.clone(),
                    exclusive_start,
                    limit: CATALOG_PAGE_ITEMS,
                };
                let (queues, next) = attempt.read_queue_page(&request).await?.finish(&request)?;
                result.extend(queues);
                let Some(cursor) = next else {
                    return Ok(result);
                };
                exclusive_start = Some(cursor);
            }
        })
    })
    .await
    .map_err(QueueError::from)
}

pub(crate) async fn delete_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<()> {
    let url = url.to_owned();
    retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let url = url.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                if let Some(catalog) = QueueOperation::load(attempt, &url).await? {
                    catalog.delete().await?;
                }
                Ok(())
            })
        })
    })
    .await
    .map_err(QueueError::from)
}

pub(crate) async fn purge_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<()> {
    let url = url.to_owned();
    let next_incarnation = QueueIncarnation::new();
    let found = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let url = url.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(catalog) = QueueOperation::load(attempt, &url).await? else {
                    return Ok(false);
                };
                catalog.purge(next_incarnation).await?;
                Ok(true)
            })
        })
    })
    .await?;
    if found {
        Ok(())
    } else {
        Err(QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: url,
        })
    }
}

pub(crate) async fn get_queue_with_message_counts<P: QueuePort>(
    port: &P,
    url: &str,
) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
    let url = url.to_owned();
    let captured_at = port.now();
    port.with_queue_attempt(AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move {
            let Some(record) = read_queue_by_url(attempt, &url).await? else {
                return Ok(None);
            };
            let counts = attempt
                .count_queue_messages(record.incarnation, captured_at)
                .await?;
            Ok(Some((record.queue, counts)))
        })
    })
    .await
    .map_err(QueueError::from)
}

pub(crate) async fn set_queue_attributes<P: QueuePort>(
    port: &P,
    url: &str,
    attributes: HashMap<String, String>,
) -> QueueResult<()> {
    let url = url.to_owned();
    let found = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let url = url.clone();
        let attributes = attributes.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(catalog) = QueueOperation::load(attempt, &url).await? else {
                    return Ok(false);
                };
                catalog.set_attributes(attributes).await?;
                Ok(true)
            })
        })
    })
    .await?;
    if found {
        Ok(())
    } else {
        Err(QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: url,
        })
    }
}

impl QueueCatalogPage {
    fn finish(
        mut self,
        request: &QueueCatalogRequest,
    ) -> StorageResult<(Vec<Queue>, Option<String>)> {
        let bound = request.limit.get() as usize + 1;
        if self.rows.len() > bound || self.more && self.rows.is_empty() {
            return Err(StorageError::internal(
                "queue catalog page has invalid bounds",
            ));
        }
        let mut previous = request.exclusive_start.as_deref();
        for row in &self.rows {
            let name = row.queue.queue_name.as_str();
            if previous.is_some_and(|previous| name <= previous)
                || request
                    .prefix
                    .as_deref()
                    .is_some_and(|prefix| !name.starts_with(prefix))
            {
                return Err(StorageError::internal(
                    "queue catalog page has invalid order",
                ));
            }
            previous = Some(name);
        }
        let has_more = self.more || self.rows.len() > request.limit.get() as usize;
        self.rows.truncate(request.limit.get() as usize);
        let next = if has_more {
            self.rows.last().map(|row| row.queue.queue_name.clone())
        } else {
            None
        };
        Ok((self.rows.into_iter().map(|row| row.queue).collect(), next))
    }
}
