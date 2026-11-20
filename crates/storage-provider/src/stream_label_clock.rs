//! Each stream enable takes a store-wide stamp above every earlier one.

use std::future::Future;

use storage_types::{StorageResult, TimestampMillis};

/// The store's last issued stream-enable stamp. Nodes' clocks are skewed and
/// a table name can be recreated within one millisecond; one monotone stamp
/// keeps every enable's ARN distinct while its label stays the AWS ISO time.
pub trait StreamLabelClockAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    /// Read with conflict protection: concurrent enables serialize on it.
    fn load_stream_label_clock(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<TimestampMillis>>> + Send;

    fn store_stream_label_clock(
        &mut self,
        stamp: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// The stamp of a stream enable at `now`: the store clock unless the store
/// or this table already issued that millisecond or a later one.
pub(crate) async fn issue_stream_enable_stamp<A: StreamLabelClockAttempt + ?Sized>(
    attempt: &mut A,
    now: TimestampMillis,
    previous: Option<TimestampMillis>,
) -> StorageResult<TimestampMillis> {
    let issued = attempt.load_stream_label_clock().await?.max(previous);
    let stamp = issued.map_or(now, |issued| now.max(issued + 1));
    attempt.store_stream_label_clock(stamp).await?;
    Ok(stamp)
}
