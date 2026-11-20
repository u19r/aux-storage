use std::{num::NonZeroU32, time::Duration};

use storage_types::{
    AttemptDurability, AttemptMode, AttemptPlan, DeadlineClass, NativeWork, WriteRetryPolicy,
};

pub(crate) const COMPAT_PRODUCT_NAME: &str = concat!("Amazon ", "SN", "S");
pub(crate) const SNS_NOT_FOUND_ERROR_TYPE: &str = "NotFound";
pub(crate) const SNS_RESOURCE_NOT_FOUND_ERROR_TYPE: &str = "ResourceNotFound";
pub(crate) const SNS_INVALID_PARAMETER_ERROR_TYPE: &str = "InvalidParameter";
pub(crate) const SNS_INTERNAL_ERROR_TYPE: &str = "InternalError";

pub const PUBLISH_INTENT_LEASE_MILLIS: i64 = 30_000;

pub(crate) const PUBSUB_PAGE_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(100 - 1);
pub(crate) const PUBSUB_SNAPSHOT_CHUNK_SIZE: usize = 10;
/// Due-index entries that an ordered-key store reads with a snapshot read
/// before it selects the records of one claim (design 7.1).
pub(crate) const PUBSUB_CLAIM_WINDOW: NonZeroU32 = NonZeroU32::MIN.saturating_add(256 - 1);
pub(crate) const PUBSUB_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(5, Duration::from_millis(2));

/// Topic and subscription changes: protected read and write.
pub(crate) const CATALOG_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::SchemaChange)
        .retrying(PUBSUB_WRITE_RETRY);
/// One topic or subscription row.
pub(crate) const CATALOG_POINT_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).retrying(PUBSUB_WRITE_RETRY);
/// GetTopicAttributes: the topic row and every subscription page of it. The
/// point-read budget of today; admitted as the range read it is.
pub(crate) const TOPIC_ATTRIBUTES_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
        .retrying(PUBSUB_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// ListTopics and ListSubscriptions pages.
pub(crate) const CATALOG_RANGE_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
        .retrying(PUBSUB_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// AWS deletes a subscription that is still pending confirmation 48 hours
/// after `Subscribe`.
pub const PENDING_SUBSCRIPTION_LIFETIME: Duration = Duration::from_secs(48 * 60 * 60);
/// Pending-subscription index entries that one sweep turn reads.
pub const STALE_SUBSCRIPTION_PAGE: NonZeroU32 = NonZeroU32::MIN.saturating_add(100 - 1);
/// One page of the pending-subscription index, read-only.
pub(crate) const PENDING_SUBSCRIPTION_SCAN: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY)
        .work(NativeWork::RangeRead);
/// The delete of one stale pending subscription: protected read and delete.
pub(crate) const STALE_SUBSCRIPTION_DELETE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY);
/// A publication under a fresh message ID (Publish, PublishBatch, a persisted
/// acceptance).
pub(crate) const PUBLISH_WRITE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).retrying(PUBSUB_WRITE_RETRY);
/// The chunk claim and the chunk commit are `Relaxed` (decision D7): a lost
/// claim makes the intent due again; a lost commit leaves the intent at the
/// old chunk (written Durable at publish) and the chunk is materialized
/// again with the same record IDs. They read intents and chunks, which the
/// Durable publish and these Relaxed attempts write; no Durable reader
/// depends on them (rule DUR-F6).
pub(crate) const CHUNK_ITEMS: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);
/// The next publish due time; a hint, so Relaxed.
pub(crate) const PUBLISH_DUE_PROBE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::LongPollProbe)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);
/// A claim attempt is `Relaxed` (decision D7): a lost claim returns the
/// record when its lease ends, and the per-claim nonce keeps an attempt of
/// the lost claim from settling a later claim. It reads only delivery rows,
/// which only Relaxed attempts write (rule DUR-F6).
pub(crate) const DELIVERY_CLAIM: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);
/// One page of destination heads, read-only. A hint: a wrong answer costs one
/// claim or one sweep period (rule DUR-F6).
pub(crate) const DESTINATION_HEADS_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed)
        .work(NativeWork::RangeRead);
/// The state and payload of one delivery (tests and workloads only).
pub(crate) const DELIVERY_READ: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::BackgroundPage)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);
/// Settlement of one delivery, or of one local queue batch with its deliveries.
pub(crate) const DELIVERY_SETTLE: AttemptPlan =
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
        .retrying(PUBSUB_WRITE_RETRY)
        .with_durability(AttemptDurability::Relaxed);

pub const SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES: usize = 256 * 1024;
pub(crate) const SNS_SUBJECT_CHARACTER_LIMIT: usize = 100;
/// The most characters of a topic `DisplayName`.
pub(crate) const DISPLAY_NAME_MAX_CHARS: usize = 100;

pub(crate) const SNS_MAX_ATTRIBUTE_NAME_BYTES: usize = 256;
/// How far an undecodable due row moves ahead, so later rows are claimed
/// (review D4).
pub(crate) const POISON_DEFERRAL: Duration = Duration::from_secs(3_600);
/// The most records of one local SQS batch (finding 4).
pub const LOCAL_BATCH_MAX_RECORDS: usize = 32;
/// The most body bytes of one local SQS batch (finding 4).
pub const LOCAL_BATCH_MAX_BODY_BYTES: usize = 1024 * 1024;
