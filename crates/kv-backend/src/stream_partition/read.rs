//! Read keyed stream rows from one protected partition family snapshot.

use std::num::NonZeroUsize;

use storage_types::{StorageError, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    ReadDirection, StreamItem, StreamReadRequest, StreamRowPage, verify_stream_item_identity,
};

use super::{
    key_layout::{decode_route, index_key, row_prefix},
    state::load_state,
};
use crate::{
    KvAttempt, KvRange, KvRangePage, ReadProtection,
    constants::STREAM_READ_BYTES,
    stream_read::{range::read_range_for_prefix, row::decode_stream_row},
};

struct PartitionScan {
    prefix: Vec<u8>,
    range: KvRange,
    last_id: Option<StreamItemId>,
    more: bool,
}

struct PartitionReadState<'a> {
    stream_name: &'a StreamName,
    request: &'a StreamReadRequest,
    protection: ReadProtection,
    scans: Vec<PartitionScan>,
    rows: Vec<StreamItem>,
}

pub(crate) async fn read_stream_rows<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    stream_name: &StreamName,
    request: &StreamReadRequest,
) -> StorageResult<StreamRowPage> {
    PartitionReadState {
        stream_name,
        request,
        protection,
        scans: Vec::new(),
        rows: Vec::new(),
    }
    .load_scans(attempt, std::convert::identity)
    .await?
    .read(attempt)
    .await
}

pub(crate) async fn read_user_page_through<A: KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    stream_name: &StreamName,
    request: &StreamReadRequest,
    through: StreamItemId,
) -> StorageResult<StreamRowPage> {
    PartitionReadState {
        stream_name,
        request,
        protection: catalog.protection,
        scans: Vec::new(),
        rows: Vec::new(),
    }
    .load_scans(catalog.attempt, |sealed| {
        Some(sealed.map_or(through, |id| id.min(through)))
    })
    .await?
    .read(catalog.attempt)
    .await
}

impl PartitionReadState<'_> {
    async fn load_scans<A: KvAttempt>(
        mut self,
        attempt: &mut A,
        upper_bound: impl Fn(Option<StreamItemId>) -> Option<StreamItemId>,
    ) -> StorageResult<Self> {
        let family = load_state(attempt, self.stream_name, self.protection).await?;
        for partition in family
            .partitions
            .iter()
            .filter(|partition| partition.is_readable())
        {
            let prefix = row_prefix(
                self.stream_name,
                partition.placement_slot,
                partition.partition_id,
            )?;
            let sealed = upper_bound(partition.sealed_after_id);
            if let Some((prefix, range)) =
                read_range_for_prefix(self.request, prefix, partition.opened_after_id, sealed)?
            {
                self.scans.push(PartitionScan {
                    prefix,
                    range,
                    last_id: None,
                    more: true,
                });
            }
        }
        Ok(self)
    }

    async fn read<A: KvAttempt>(mut self, attempt: &mut A) -> StorageResult<StreamRowPage> {
        loop {
            self.sort_rows();
            let pending = self.pending_scans();
            if pending.is_empty() {
                return Ok(self.finish());
            }
            self.refill(attempt, pending).await?;
        }
    }

    fn sort_rows(&mut self) {
        self.rows
            .sort_unstable_by(|left, right| match self.request.direction {
                ReadDirection::Forward => left.id.cmp(&right.id),
                ReadDirection::Backward => right.id.cmp(&left.id),
            });
    }

    fn pending_scans(&self) -> Vec<usize> {
        let cutoff = self.rows.get(self.max_rows() - 1).map(|row| row.id);
        // A byte-limited page can hide a row before the global cutoff.
        self.scans
            .iter()
            .enumerate()
            .filter_map(|(index, scan)| {
                (scan.more
                    && cutoff.is_none_or(|cutoff| {
                        scan.last_id
                            .is_none_or(|last| precedes(last, cutoff, self.request.direction))
                    }))
                .then_some(index)
            })
            .collect()
    }

    async fn refill<A: KvAttempt>(
        &mut self,
        attempt: &mut A,
        pending: Vec<usize>,
    ) -> StorageResult<()> {
        let count = NonZeroUsize::new(pending.len())
            .ok_or_else(|| StorageError::internal("pending stream scans are empty"))?;
        let budget = NonZeroUsize::new((STREAM_READ_BYTES.get() / count.get()).max(1))
            .ok_or_else(|| StorageError::internal("pending stream scans have no byte budget"))?;
        let ranges = pending
            .iter()
            .map(|&index| {
                let mut range = self.scans[index].range.clone();
                range.max_bytes = budget;
                range
            })
            .collect();
        let pages = attempt.scan_batch(ranges, self.protection).await?;
        if pages.len() != pending.len() {
            return Err(StorageError::internal(
                "partitioned stream batch returned the wrong page count",
            ));
        }
        for (index, page) in pending.into_iter().zip(pages) {
            self.scans[index].absorb_page(page, self.stream_name, self.request, &mut self.rows)?;
        }
        Ok(())
    }

    fn max_rows(&self) -> usize {
        self.request.limit.get() as usize + 1
    }

    fn finish(mut self) -> StreamRowPage {
        let more = self.rows.len() > self.max_rows() || self.scans.iter().any(|scan| scan.more);
        self.rows.truncate(self.max_rows());
        StreamRowPage {
            rows: self.rows,
            more,
        }
    }
}

impl PartitionScan {
    fn absorb_page(
        &mut self,
        page: KvRangePage,
        stream_name: &StreamName,
        request: &StreamReadRequest,
        rows: &mut Vec<StreamItem>,
    ) -> StorageResult<()> {
        if page.more && page.entries.is_empty() {
            return Err(StorageError::internal(
                "partitioned stream page cannot advance",
            ));
        }
        self.more = page.more;
        for entry in page.entries {
            let row = decode_stream_row(&self.prefix, entry)?;
            if row.stream_name.as_ref() != Some(stream_name) {
                return Err(StorageError::internal(
                    "partitioned stream row has another stream name",
                ));
            }
            self.last_id = Some(row.id);
            rows.push(row);
        }
        if let Some(last) = self.last_id {
            let mut edge = self.prefix.clone();
            edge.extend_from_slice(last.as_bytes());
            match request.direction {
                ReadDirection::Forward => {
                    edge.push(0);
                    self.range.begin = edge;
                }
                ReadDirection::Backward => self.range.end = edge,
            }
        }
        Ok(())
    }
}

fn precedes(left: StreamItemId, right: StreamItemId, direction: ReadDirection) -> bool {
    match direction {
        ReadDirection::Forward => left < right,
        ReadDirection::Backward => left > right,
    }
}

pub(crate) async fn read_stream_item<A: KvAttempt>(
    attempt: &mut A,
    protection: ReadProtection,
    stream_name: &StreamName,
    item_id: StreamItemId,
) -> StorageResult<Option<StreamItem>> {
    let Some(route) = attempt
        .get_admitted(
            &index_key(stream_name, item_id)?,
            protection,
            crate::KvValueAllowance::new(
                crate::constants::STREAM_ROUTE_BYTES,
                crate::constants::STREAM_ROUTE_BYTES,
            ),
        )
        .await?
    else {
        return Ok(None);
    };
    let (placement_slot, partition_id) = decode_route(route)?;
    let prefix = row_prefix(stream_name, placement_slot, partition_id)?;
    let mut key = prefix.clone();
    key.extend_from_slice(item_id.as_bytes());
    let value = attempt
        .get(&key, protection)
        .await?
        .ok_or_else(|| StorageError::internal("partitioned stream index has no row"))?;
    let row = decode_stream_row(&prefix, crate::KvEntry::new(key, value))?;
    verify_stream_item_identity(row, stream_name, item_id).map(Some)
}
