//! Queue identity, idempotent creation, and catalog paging are shared policy.

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU32,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageError, StorageResult, TimestampMillis};

use crate::{
    ListDeadLetterSourceQueuesResponse, Queue, QueueAttempt, QueueError, QueueOperation, QueuePort,
    QueueResult, QueueValidationKind, TagRuleViolation, apply_tag_change,
    constants::{
        CATALOG_PAGE_ITEMS, CATALOG_POINT_READ, CATALOG_RANGE_READ, CATALOG_WRITE,
        DEAD_LETTER_SOURCE_SCAN_ROWS,
    },
    queue_message::queue_not_found,
    queue_redrive::{RedrivePolicy, find_redrive_target, normalize_redrive_policy},
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
        Self(storage_types::ambient::uuid_now_v7().into_bytes())
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

/// The identity of one queue creation. Unlike the incarnation, a purge keeps
/// it; a delete and re-create gives the same name a new one. Receipt handles
/// name it.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueId([u8; 16]);

impl QueueId {
    #[must_use]
    pub fn new() -> Self {
        Self(storage_types::ambient::uuid_now_v7().into_bytes())
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        uuid::Uuid::try_parse(text)
            .ok()
            .map(|id| Self(id.into_bytes()))
    }
}

impl Default for QueueId {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueCatalogRecord {
    /// `<queue id>:<queue name>`, the part of a receipt handle that binds it
    /// to this queue creation.
    pub(crate) fn receipt_scope(&self) -> String {
        format!(
            "{}:{}",
            uuid::Uuid::from_bytes(self.id.0).simple(),
            self.queue.queue_name
        )
    }
}

/// SQS `ApproximateNumberOfMessages*` values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueMessageCounts {
    pub visible: u64,
    pub not_visible: u64,
    pub delayed: u64,
}

/// The physical facts of stored messages that select a count bucket. A
/// backend reports them; `QueueMessageCounts::add` gives them their meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueCountFacts {
    /// The visibility time is at or before the count time.
    pub due: bool,
    /// The row has a receipt.
    pub leased: bool,
    /// The retention time is after the count time.
    pub live: bool,
}

impl QueueMessageCounts {
    /// The one definition of the buckets: an expired row is in none; a due
    /// row is visible; a later row is not visible when it has a receipt and
    /// delayed when it has none.
    pub fn add(&mut self, facts: QueueCountFacts, rows: u64) {
        let bucket = match facts {
            QueueCountFacts { live: false, .. } => return,
            QueueCountFacts { due: true, .. } => &mut self.visible,
            QueueCountFacts { leased: true, .. } => &mut self.not_visible,
            QueueCountFacts { .. } => &mut self.delayed,
        };
        *bucket = bucket.saturating_add(rows);
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueCatalogRecord {
    pub queue: Queue,
    pub id: QueueId,
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

/// The queue name that a queue URL or a bare name addresses: its last path
/// segment. A URL is never a storage key, so every node finds a queue from
/// any URL form that ends in its name (review finding 2).
#[must_use]
pub fn queue_name_of(queue_url: &str) -> &str {
    queue_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default()
}

/// The catalog row of the queue that `url` (a URL or a bare name) names.
pub async fn read_queue_by_url<A: QueueAttempt>(
    attempt: &mut A,
    url: &str,
    protection: ReadProtection,
) -> StorageResult<Option<QueueCatalogRecord>> {
    attempt
        .read_queue_by_name(queue_name_of(url), protection)
        .await
}

struct CreateQueueInput {
    proposed: QueueCatalogRecord,
    target: Option<RedrivePolicy>,
}

/// A `RedrivePolicy` is stored in normal form, and the target of a new
/// queue's policy must exist in the same attempt (design D10). An empty
/// policy is no policy.
pub(crate) async fn create_queue<P: QueuePort>(port: &P, mut queue: Queue) -> QueueResult<Queue> {
    let target = normalize_redrive_policy(&mut queue.attributes)?;
    queue.attributes.retain(|_, value| !value.is_empty());
    queue_settings_size(&queue).map_err(QueueError::from)?;
    let proposed = QueueCatalogRecord {
        queue,
        id: QueueId::new(),
        incarnation: QueueIncarnation::new(),
    };
    let input = Arc::new(CreateQueueInput { proposed, target });
    let created = port
        .run(CATALOG_WRITE, input, |attempt, input, _| {
            Box::pin(crate::create_queue_in_attempt(
                attempt,
                input.proposed.clone(),
                input.target.as_ref(),
            ))
        })
        .await?;
    created.map(Queue::from)
}

/// Catalog reads run under the native retry like writes: a read can fail
/// with a native conflict (FoundationDB: a read version that is too old or
/// in the future), and the client must not see it.
pub(crate) async fn get_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<Option<Queue>> {
    get_queue_by_name(port, queue_name_of(url)).await
}

pub(crate) async fn get_queue_by_name<P: QueuePort>(
    port: &P,
    name: &str,
) -> QueueResult<Option<Queue>> {
    port.run(
        CATALOG_POINT_READ,
        Arc::new(name.to_owned()),
        |attempt, name, _| Box::pin(queue_named_in_attempt(attempt, name)),
    )
    .await
    .map_err(QueueError::from)
}

async fn queue_named_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    name: &str,
) -> StorageResult<Option<Queue>> {
    Ok(attempt
        .read_queue_by_name(name, ReadProtection::Snapshot)
        .await?
        .map(|record| record.queue))
}

struct ListQueuesInput {
    prefix: Option<String>,
    after: Option<String>,
    limit: NonZeroU32,
}

/// One page of queues in name order after `after`, at most `limit`, and the
/// last name when more remain. One read-only attempt; it normally reads one
/// catalog page of `limit + 1` rows. A KV page also stops at its byte bound,
/// so the attempt reads more pages until it has `limit` rows or the end.
pub(crate) async fn list_queue_page<P: QueuePort>(
    port: &P,
    prefix: Option<&str>,
    after: Option<&str>,
    limit: NonZeroU32,
) -> QueueResult<(Vec<Queue>, Option<String>)> {
    let input = Arc::new(ListQueuesInput {
        prefix: prefix.map(str::to_owned),
        after: after.map(str::to_owned),
        limit,
    });
    port.run(CATALOG_RANGE_READ, input, |attempt, input, _| {
        Box::pin(read_queue_rows(
            attempt,
            input.prefix.clone(),
            input.after.clone(),
            input.limit,
        ))
    })
    .await
    .map_err(QueueError::from)
}

async fn read_queue_rows<A: QueueAttempt>(
    attempt: &mut A,
    prefix: Option<String>,
    mut exclusive_start: Option<String>,
    limit: NonZeroU32,
) -> StorageResult<(Vec<Queue>, Option<String>)> {
    let mut queues = Vec::new();
    loop {
        let remaining = limit.get() - u32::try_from(queues.len()).unwrap_or(u32::MAX);
        let request = QueueCatalogRequest {
            prefix: prefix.clone(),
            exclusive_start,
            limit: NonZeroU32::new(remaining).unwrap_or(NonZeroU32::MIN),
        };
        let (page, next) = attempt.read_queue_page(&request).await?.finish(&request)?;
        queues.extend(page);
        match next {
            Some(cursor) if queues.len() < limit.get() as usize => exclusive_start = Some(cursor),
            next => return Ok((queues, next)),
        }
    }
}

struct TagChangeInput {
    url: String,
    added: BTreeMap<String, String>,
    removed: Vec<String>,
}

/// `TagQueue` and `UntagQueue`: one read-write catalog attempt that reads
/// the row, applies the merged change under the shared tag rule, and writes
/// the row. A rule violation is 400 `InvalidParameterValue` (text not probed).
pub(crate) async fn change_queue_tags<P: QueuePort>(
    port: &P,
    url: &str,
    added: BTreeMap<String, String>,
    removed: Vec<String>,
) -> QueueResult<()> {
    let input = Arc::new(TagChangeInput {
        url: url.to_owned(),
        added,
        removed,
    });
    port.run(CATALOG_WRITE, input, |attempt, input, _| {
        Box::pin(change_tags_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)?
}

async fn change_tags_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &TagChangeInput,
) -> StorageResult<QueueResult<()>> {
    let Some(mut catalog) = QueueOperation::load(attempt, &input.url).await? else {
        return Ok(Err(queue_not_found(input.url.clone())));
    };
    if let Err(violation) = apply_tag_change(
        &mut catalog.record.queue.tags,
        input.added.clone(),
        &input.removed,
    ) {
        return Ok(Err(tag_violation(
            violation,
            &catalog.record.queue.queue_name,
        )));
    }
    queue_settings_size(&catalog.record.queue)?;
    let incarnation = catalog.record.incarnation;
    catalog
        .attempt
        .write_queue_record(catalog.record, Some(incarnation))
        .await?;
    Ok(Ok(()))
}

/// The serialized attributes and tags of one catalog row are at most
/// `MAX_QUEUE_ROW_SETTINGS_BYTES` (not probed): a FoundationDB value is at
/// most 100 KB, and a listing reads whole rows in one attempt.
pub(crate) fn queue_settings_size(queue: &Queue) -> StorageResult<()> {
    let size = serde_json::to_vec(&(&queue.attributes, &queue.tags))
        .map_err(|error| StorageError::internal(&error.to_string()))?
        .len();
    if size > crate::constants::MAX_QUEUE_ROW_SETTINGS_BYTES {
        return Err(StorageError::validation(format!(
            "Queue attributes and tags must be at most {} bytes.",
            crate::constants::MAX_QUEUE_ROW_SETTINGS_BYTES
        )));
    }
    Ok(())
}

/// Suite scripts: the text of a tag key outside 1 to 128 characters.
pub(crate) const TAG_KEY_LENGTH_MESSAGE: &str =
    "Tag keys must be between 1 and 128 characters in length.";

/// The SQS error of a tag rule violation (texts from the suite scripts).
pub(crate) fn tag_violation(violation: TagRuleViolation, queue_name: &str) -> QueueError {
    let detail = match violation {
        TagRuleViolation::TooMany => format!("Too many tags added for queue {queue_name}."),
        TagRuleViolation::InvalidKey => TAG_KEY_LENGTH_MESSAGE.to_owned(),
        TagRuleViolation::ReservedPrefix => {
            "Caller is an end user and not allowed to mutate system tags".to_owned()
        }
        TagRuleViolation::InvalidValue => {
            "Tag values must be between 0 and 256 characters in length.".to_owned()
        }
    };
    QueueError::validation_with_detail(QueueValidationKind::InvalidParameterValue, detail)
}

/// Probe 6: deleting a missing queue is `QueueDoesNotExist`, not success.
pub(crate) async fn delete_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<()> {
    let is_deleted = port
        .run(
            CATALOG_WRITE,
            Arc::new(url.to_owned()),
            |attempt, url, _| Box::pin(delete_in_attempt(attempt, url)),
        )
        .await
        .map_err(QueueError::from)?;
    if is_deleted {
        Ok(())
    } else {
        Err(queue_not_found(url.to_owned()))
    }
}

async fn delete_in_attempt<A: QueueAttempt>(attempt: &mut A, url: &str) -> StorageResult<bool> {
    let Some(catalog) = QueueOperation::load(attempt, url).await? else {
        return Ok(false);
    };
    catalog.delete().await?;
    Ok(true)
}

struct PurgeInput {
    url: String,
    next_incarnation: QueueIncarnation,
}

pub(crate) async fn purge_queue<P: QueuePort>(port: &P, url: &str) -> QueueResult<()> {
    let input = Arc::new(PurgeInput {
        url: url.to_owned(),
        next_incarnation: QueueIncarnation::new(),
    });
    let found = port
        .run(CATALOG_WRITE, Arc::clone(&input), |attempt, input, _| {
            Box::pin(purge_in_attempt(attempt, input))
        })
        .await?;
    if found {
        Ok(())
    } else {
        Err(QueueError::ResourceNotFound {
            resource_type: "queue",
            resource_id: input.url.clone(),
        })
    }
}

async fn purge_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &PurgeInput,
) -> StorageResult<bool> {
    let Some(catalog) = QueueOperation::load(attempt, &input.url).await? else {
        return Ok(false);
    };
    catalog.purge(input.next_incarnation).await?;
    Ok(true)
}

/// The read is `Durable`: the catalog attributes in the answer never show a
/// `SetQueueAttributes`, create or delete that a crash can remove. The counts
/// stay approximate (SQS `ApproximateNumberOf*`): each region stops at
/// `QUEUE_COUNT_SCAN_ENTRIES`, and a concurrent claim or send changes them.
pub(crate) async fn get_queue_with_message_counts<P: QueuePort>(
    port: &P,
    url: &str,
) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
    get_queue_with_bounded_counts(port, url, crate::constants::QUEUE_COUNT_SCAN_ENTRIES).await
}

struct CountsInput {
    url: String,
    captured_at: TimestampMillis,
    scan_limit: NonZeroU32,
}

/// `GetQueueAttributes` counts with an explicit region bound, for tests and
/// tools; the API uses `QUEUE_COUNT_SCAN_ENTRIES`.
pub async fn get_queue_with_bounded_counts<P: QueuePort>(
    port: &P,
    url: &str,
    scan_limit: NonZeroU32,
) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
    let input = Arc::new(CountsInput {
        url: url.to_owned(),
        captured_at: port.now(),
        scan_limit,
    });
    port.run(CATALOG_RANGE_READ, input, |attempt, input, _| {
        Box::pin(counts_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)
}

async fn counts_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &CountsInput,
) -> StorageResult<Option<(Queue, QueueMessageCounts)>> {
    let Some(record) = read_queue_by_url(attempt, &input.url, ReadProtection::Snapshot).await?
    else {
        return Ok(None);
    };
    let counts = attempt
        .count_queue_messages(record.incarnation, input.captured_at, input.scan_limit)
        .await?;
    Ok(Some((record.queue, counts)))
}

struct SetAttributesInput {
    url: String,
    attributes: HashMap<String, String>,
    target: Option<RedrivePolicy>,
    updated_at: TimestampMillis,
}

/// A `RedrivePolicy` is stored in normal form and its target must exist in
/// the same attempt; an empty value removes the policy (design D10).
pub(crate) async fn set_queue_attributes<P: QueuePort>(
    port: &P,
    url: &str,
    mut attributes: HashMap<String, String>,
) -> QueueResult<()> {
    let target = normalize_redrive_policy(&mut attributes)?;
    let input = Arc::new(SetAttributesInput {
        url: url.to_owned(),
        attributes,
        target,
        updated_at: port.now(),
    });
    port.run(CATALOG_WRITE, input, |attempt, input, _| {
        Box::pin(set_attributes_in_attempt(attempt, input))
    })
    .await?
}

async fn set_attributes_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &SetAttributesInput,
) -> StorageResult<QueueResult<()>> {
    let Some(catalog) = QueueOperation::load(attempt, &input.url).await? else {
        return Ok(Err(queue_not_found(input.url.clone())));
    };
    if let Some(target) = &input.target {
        let source = catalog.record.queue.queue_name.clone();
        if let Err(error) = find_redrive_target(&mut *catalog.attempt, target, &source).await? {
            return Ok(Err(error));
        }
    }
    catalog
        .set_attributes(input.attributes.clone(), input.updated_at)
        .await?;
    Ok(Ok(()))
}

struct DeadLetterSourcesInput {
    target_url: String,
    exclusive_start_name: Option<String>,
    max_results: NonZeroU32,
}

/// One page of the queues whose redrive policy names `target_url` as their
/// dead-letter queue, in name order. `None` means that the target does not
/// exist. One read-only attempt reads the target, then catalog pages after
/// `exclusive_start_name`, and stops at `max_results` matches or after
/// `DEAD_LETTER_SOURCE_SCAN_ROWS` catalog rows. `next_token` is the name of
/// the last scanned row whenever rows remain, also when fewer rows matched,
/// so the work of one call is bounded on a large catalog. `queue_urls` holds
/// queue names; the manager turns them into URLs of its endpoint.
pub(crate) async fn list_dead_letter_source_queues<P: QueuePort>(
    port: &P,
    target_url: &str,
    exclusive_start_name: Option<&str>,
    max_results: NonZeroU32,
) -> QueueResult<Option<ListDeadLetterSourceQueuesResponse>> {
    let input = Arc::new(DeadLetterSourcesInput {
        target_url: target_url.to_owned(),
        exclusive_start_name: exclusive_start_name.map(str::to_owned),
        max_results,
    });
    port.run(CATALOG_RANGE_READ, input, |attempt, input, _| {
        Box::pin(dead_letter_sources_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)
}

async fn dead_letter_sources_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &DeadLetterSourcesInput,
) -> StorageResult<Option<ListDeadLetterSourceQueuesResponse>> {
    let Some(target) =
        read_queue_by_url(attempt, &input.target_url, ReadProtection::Snapshot).await?
    else {
        return Ok(None);
    };
    scan_dead_letter_sources(
        attempt,
        &target.queue,
        input.exclusive_start_name.clone(),
        input.max_results,
    )
    .await
    .map(Some)
}

/// The bounded catalog scan of `list_dead_letter_source_queues`.
async fn scan_dead_letter_sources<A: QueueAttempt>(
    attempt: &mut A,
    target: &Queue,
    mut exclusive_start: Option<String>,
    max_results: NonZeroU32,
) -> StorageResult<ListDeadLetterSourceQueuesResponse> {
    let mut queue_urls = Vec::new();
    let mut scanned = 0_u32;
    loop {
        let budget = DEAD_LETTER_SOURCE_SCAN_ROWS.get() - scanned;
        let request = QueueCatalogRequest {
            prefix: None,
            exclusive_start: exclusive_start.clone(),
            limit: NonZeroU32::new(budget.min(CATALOG_PAGE_ITEMS.get())).unwrap_or(NonZeroU32::MIN),
        };
        let (queues, next) = attempt.read_queue_page(&request).await?.finish(&request)?;
        let page_len = queues.len();
        for (index, queue) in queues.into_iter().enumerate() {
            scanned += 1;
            if RedrivePolicy::of(&queue).is_some_and(|policy| policy.target().names(target)) {
                queue_urls.push(queue.queue_name.clone());
            }
            let is_last_row = index + 1 == page_len && next.is_none();
            if !is_last_row
                && (queue_urls.len() == max_results.get() as usize
                    || scanned == DEAD_LETTER_SOURCE_SCAN_ROWS.get())
            {
                return Ok(ListDeadLetterSourceQueuesResponse {
                    queue_urls,
                    next_token: Some(queue.queue_name),
                });
            }
        }
        let Some(cursor) = next else {
            return Ok(ListDeadLetterSourceQueuesResponse {
                queue_urls,
                next_token: None,
            });
        };
        exclusive_start = Some(cursor);
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
