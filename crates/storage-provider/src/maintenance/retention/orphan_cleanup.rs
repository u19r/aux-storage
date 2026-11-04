//! A disabled stream schedules key-only discovery for pointerless anchors.

use serde::{Deserialize, Serialize};
use storage_types::{
    ItemHistoryAddress, ItemHistoryPublication, StorageResult, StreamRetentionDuration,
    TimestampMillis,
};
use stream_provider::HistoryRowDelete;

use super::{
    StreamRetentionPort, StreamTrimStateWrite,
    history_reclaim::{HistoryCleanupRead, load_reclaim_decisions},
    table_cleanup::TableCleanupWork,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ItemHistorySweep {
    after: Option<Vec<u8>>,
    retry_at: Option<TimestampMillis>,
    pub(super) not_before: TimestampMillis,
}

impl StreamTrimStateWrite {
    pub(crate) fn schedule_item_history_sweep(&mut self, now: TimestampMillis) {
        self.state.frontier.item_history_sweep = Some(Box::new(ItemHistorySweep {
            after: None,
            retry_at: None,
            not_before: now,
        }));
        self.state.next_due_at = Some(now);
        self.next_marker = self.state.scheduled_marker();
    }
}

pub(super) async fn advance_item_history_sweeps(
    attempt: &mut impl StreamRetentionPort,
    works: &mut [TableCleanupWork],
    now: TimestampMillis,
) -> StorageResult<()> {
    for work in works {
        // Pointer cleanup may have removed a retired event's predecessor in
        // this wave. Finish that prefix before interpreting remaining event
        // bodies.
        if work.state.frontier.has_pending_cleanup()? {
            continue;
        }
        let Some(mut sweep) = work.state.frontier.item_history_sweep.take() else {
            continue;
        };
        if sweep.not_before > now {
            work.state.frontier.item_history_sweep = Some(sweep);
            continue;
        }
        let namespace = attempt
            .load_table_history_namespace(work.table.history_id, sweep.after.as_deref())
            .await?;
        let Some(namespace) = namespace else {
            work.state.frontier.item_history_sweep = sweep.restart(now);
            continue;
        };
        let repeat = match ItemHistoryAddress::try_from_physical_stream(
            &work.table,
            &namespace.stream_name,
        )? {
            Some(history) => sweep.clean_namespace(attempt, work, history).await?,
            None => false,
        };
        if !repeat {
            sweep.after = Some(namespace.cursor);
        }
        sweep.not_before = now + i64::from(crate::constants::TABLE_CLEANUP_YIELD_MILLIS);
        work.state.frontier.item_history_sweep = Some(sweep);
    }
    Ok(())
}

impl ItemHistorySweep {
    async fn clean_namespace(
        &mut self,
        attempt: &mut impl StreamRetentionPort,
        work: &mut TableCleanupWork,
        history: ItemHistoryAddress,
    ) -> StorageResult<bool> {
        let mut pages = attempt
            .load_item_history_metadata_pages(std::slice::from_ref(&history))
            .await?;
        if pages.len() != 1 {
            return Err(storage_types::StorageError::internal(
                "History sweep page count differs",
            ));
        }
        let page = pages.remove(0);
        let frontier = work.state.frontier.clone();
        self.observe_retry(&page, work)?;
        let read = HistoryCleanupRead {
            work: 0,
            history,
            page,
            frontier,
        };
        let decisions =
            load_reclaim_decisions(attempt, std::slice::from_ref(work), vec![read]).await?;
        let mut repeat = false;
        for decision in decisions {
            if let Some(reference) = decision.delete {
                attempt
                    .delete_history_rows(&[HistoryRowDelete::Item(reference)])
                    .await?;
                work.deleted_rows += 1;
                repeat |= !decision.complete;
            }
        }
        Ok(repeat)
    }

    fn observe_retry(
        &mut self,
        page: &super::ItemHistoryMetadataPage,
        work: &TableCleanupWork,
    ) -> StorageResult<()> {
        let StreamRetentionDuration::FiniteHours(hours) = work.state.retention else {
            return Ok(());
        };
        for entry in &page.entries {
            if let ItemHistoryPublication::Event { created_at, .. } = entry.publication()
                && !work.state.frontier.is_retired(entry)?
            {
                let due = *created_at + i64::from(hours) * 3600 * 1000;
                self.retry_at = Some(self.retry_at.map_or(due, |current| current.min(due)));
            }
        }
        Ok(())
    }

    fn restart(mut self: Box<Self>, now: TimestampMillis) -> Option<Box<Self>> {
        let due = self.retry_at?;
        self.after = None;
        self.retry_at = None;
        self.not_before = due.max(now + i64::from(crate::constants::TABLE_CLEANUP_YIELD_MILLIS));
        Some(self)
    }
}
