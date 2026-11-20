//! Protected SQL item observations for a native write.

use storage_backfill::GsiProjectionLifecycle;
use storage_common::TtlIndexWriteBatch;
use storage_provider::{
    AcceptedDeleteItem, AcceptedUpsert, BoundPutItem, CATALOG_READ_BYTES, ItemWriteAttempt,
    ItemWriteObservation, ItemWriteObservationRead, LiveItemMetadata, combine_write_observations,
    lifecycle_tables_for_writes,
};
use storage_types::{BoundTableInfo, StorageError, StorageResult, StoredWireItem};

use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlRow, SqlValue,
    catalog::SqlCatalogAttempt,
    item_observation::{
        WriteObservationReads, decode_write_observations, match_observation_rows,
        prepare_observation_reads,
    },
    stream_history,
    write_preparation::SqlPreparedWrites,
};

impl<A: SqlAttempt, L: SqlLayout> ItemWriteAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_item_write_prerequisites(
        &mut self,
        _table: &BoundTableInfo,
        prerequisites: storage_provider::ItemWritePrerequisites,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            // Later members reuse this attempt's history observation.
            if !prerequisites.needs_history || catalog.dynamo_history.is_some() {
                return Ok(());
            }
            // Routing metadata uses the attempt snapshot without row locks.
            let page = catalog
                .attempt
                .query_batch_protected(
                    vec![(
                        stream_history::history_statement(
                            catalog.engine,
                            stream_history::HistoryRead::Writer,
                        ),
                        ReadProtection::Snapshot,
                    )],
                    CATALOG_READ_BYTES,
                )
                .await?
                .pop()
                .ok_or_else(|| {
                    StorageError::internal("SQL history prerequisite page is missing")
                })?;
            if catalog.engine.engine() == SqlEngine::Postgres {
                catalog.dynamo_positions = crate::catalog::DynamoStreamPositions::Native(
                    crate::stream_allocation::StreamEventNumbers::default(),
                    crate::stream_history::transaction_offset(&page)?,
                );
            }
            catalog.dynamo_history =
                Some(stream_history::decode_history_page(page, catalog.engine)?);
            Ok(())
        }
        .await;
        operation.complete(result)
    }

    async fn observe_for_writes(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
    ) -> StorageResult<storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let WriteObservationReads {
                statements,
                live_groups,
                marker_groups,
            } = prepare_observation_reads(catalog.engine, reads)?;
            if statements.is_empty() {
                return Ok(storage_types::Exact::empty());
            }
            let mut window = Vec::new();
            let prefix = catalog.catalog_lock.item_lock_prefix(
                reads.iter().map(ItemWriteObservationRead::target),
                &mut window,
            )?;
            window
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            window.extend(statements);
            let max_bytes = CATALOG_READ_BYTES
                .get()
                .checked_mul(reads.len().max(1))
                .and_then(std::num::NonZeroUsize::new)
                .ok_or(storage_types::AttemptFailure::CAPACITY)?;
            let pages = catalog
                .attempt
                .query_batch_protected_exact(window, max_bytes)
                .await?;
            let mut pages = pages.into_iter();
            catalog
                .catalog_lock
                .finish_lock_prefix(prefix, &mut pages)?;
            catalog.decode_observation_pages(reads, &live_groups, &marker_groups, pages.collect())
        }
        .await;
        operation.complete(result)
    }

    async fn prepare_primary_upsert(
        &mut self,
        item: &AcceptedUpsert,
        effects: &LiveItemMetadata<'_>,
        _writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let mut prepared =
                std::mem::replace(&mut catalog.prepared_writes, Ok(Default::default()))?;
            let image = match prepared
                .primary
                .iter()
                .position(|write| write.matches(item))
            {
                Some(position) => prepared.primary.swap_remove(position),
                None => crate::write_preparation::PreparedPrimaryWrite::try_new(
                    catalog.engine,
                    item.into(),
                )?,
            };
            catalog.prepared_writes = Ok(prepared);
            catalog.stage_primary_upsert(image.prepare(effects)?)?;
            Ok(())
        }
        .await;
        operation.complete(result)
    }

    fn stage_primary_delete(
        &mut self,
        item: &AcceptedDeleteItem,
        _writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            catalog.stage_primary_delete(crate::write_preparation::encode_primary_delete(
                catalog.engine,
                &item.table,
                &item.key,
            )?)
        })();
        operation.complete(result)
    }

    type PreparedWrites = SqlPreparedWrites;
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>) {
        if let Err(error) = &writes {
            self.attempt.reject_settlement_error(error);
        }
        self.prepared_writes = writes;
    }

    fn prepare_put_items(&mut self, items: &[&BoundPutItem]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        catalog.prepared_writes = Ok(Default::default());
        let result = SqlPreparedWrites::try_new(catalog.engine, items).map(|writes| {
            catalog.prepared_writes = Ok(writes);
        });
        operation.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    pub(crate) async fn read_combined_write_observations(
        &mut self,
        batch: storage_provider::WriteObservationBatch<'_>,
    ) -> StorageResult<storage_provider::WriteObservationBatchResult> {
        let mut window = Vec::new();
        let token_prefix = batch
            .receipt_key
            .map(|slot| self.catalog_lock.token_lock_prefix(slot, &mut window));
        let receipt_slot = batch
            .receipt_key
            .map(|slot| -> StorageResult<usize> {
                let index = window.len();
                window.push((
                    crate::transact_write::receipt_statement(self.engine, slot)?,
                    ReadProtection::Snapshot,
                ));
                Ok(index)
            })
            .transpose()?;
        let names = batch
            .tables
            .iter()
            .map(|(name, _)| (*name).clone())
            .collect::<Vec<_>>();
        let catalog_prefix = self.catalog_lock.catalog_prefix(
            &names.iter().collect::<Vec<_>>(),
            None,
            &mut window,
        )?;
        let item_prefix = self.catalog_lock.item_lock_prefix(
            batch.reads.iter().map(ItemWriteObservationRead::target),
            &mut window,
        )?;
        let stamp_slot = window.len();
        window.push((
            crate::catalog::terminal_catalog_statement(self.engine, &names),
            ReadProtection::Snapshot,
        ));
        let history_slot = if batch.prerequisites.needs_history && self.dynamo_history.is_none() {
            let slot = window.len();
            let statement =
                stream_history::history_statement(self.engine, stream_history::HistoryRead::Writer);
            window.push((
                crate::item_observation::guard_observation_statement(
                    self.engine,
                    statement,
                    batch.tables,
                )?,
                ReadProtection::Snapshot,
            ));
            Some(slot)
        } else {
            None
        };
        let WriteObservationReads {
            statements,
            live_groups,
            marker_groups,
        } = prepare_observation_reads(self.engine, batch.reads)?;
        let item_slot = window.len();
        for (statement, protection) in statements {
            window.push((
                crate::item_observation::guard_observation_statement(
                    self.engine,
                    statement,
                    batch.tables,
                )?,
                protection,
            ));
        }
        let expected = window.len();
        let max_bytes = CATALOG_READ_BYTES
            .get()
            .checked_mul(batch.reads.len().max(batch.tables.len()).max(1))
            .and_then(std::num::NonZeroUsize::new)
            .ok_or(storage_types::AttemptFailure::CAPACITY)?;
        let mut result = self.attempt.query_batch_open(window, max_bytes).await?;
        let receipt = match (batch.receipt_key, receipt_slot) {
            (Some(slot), Some(index)) if index < result.pages.len() => {
                Some(crate::transact_write::decode_receipt_page(
                    slot,
                    std::mem::take(&mut result.pages[index]),
                )?)
            }
            _ => None,
        };
        let stamps = result
            .pages
            .get_mut(stamp_slot)
            .map(|page| crate::catalog::decode_terminal_catalog(&names, std::mem::take(page)))
            .transpose()?;
        let matches = stamps.as_ref().is_some_and(|stamps| {
            batch
                .tables
                .iter()
                .zip(stamps)
                .all(|((_, candidate), actual)| {
                    candidate.map(|candidate| candidate.stamp) == *actual
                })
        });
        if let Some(failure) = result.failure {
            let mut candidates: Vec<usize> = failure
                .statement_index
                .checked_sub(item_slot)
                .and_then(|slot| live_groups.get(slot))
                .map(|group| {
                    group
                        .iter()
                        .filter_map(|index| {
                            let table = batch.reads[*index].target().0;
                            batch.tables.iter().position(|(_, candidate)| {
                                candidate.is_some_and(|candidate| {
                                    candidate.table.history_id == table.history_id
                                })
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            candidates.sort_unstable();
            candidates.dedup();
            return Ok(storage_provider::WriteObservationBatchResult {
                receipt,
                catalog_version: None,
                stamps,
                observations: Err(storage_provider::WriteObservationFailure {
                    candidates,
                    error: failure.error,
                }),
                publications: Vec::new(),
            });
        }
        let mut pages = storage_types::Exact::try_new(expected, result.pages)?.into_iter();
        if let Some(prefix) = token_prefix {
            self.catalog_lock.finish_lock_prefix(prefix, &mut pages)?;
            pages.next();
        }
        self.catalog_lock
            .finish_lock_prefix(catalog_prefix, &mut pages)?;
        self.catalog_lock
            .finish_lock_prefix(item_prefix, &mut pages)?;
        pages.next();
        let observations = if matches {
            if history_slot.is_some() {
                let page = pages.next().ok_or_else(|| {
                    StorageError::internal("SQL history prerequisite page is missing")
                })?;
                self.dynamo_positions = crate::catalog::DynamoStreamPositions::Native(
                    crate::stream_allocation::StreamEventNumbers::default(),
                    stream_history::transaction_offset(&page)?,
                );
                self.dynamo_history = Some(stream_history::decode_history_page(page, self.engine)?);
            }
            Some(self.decode_observation_pages(
                batch.reads,
                &live_groups,
                &marker_groups,
                pages.collect(),
            )?)
        } else {
            None
        };
        Ok(storage_provider::WriteObservationBatchResult {
            receipt,
            catalog_version: None,
            stamps,
            observations: Ok(observations),
            publications: Vec::new(),
        })
    }

    fn decode_observation_pages(
        &mut self,
        reads: &[ItemWriteObservationRead<'_>],
        live_groups: &[Vec<usize>],
        marker_groups: &[Vec<usize>],
        pages: Vec<crate::SqlQueryPage>,
    ) -> StorageResult<storage_types::Exact<(ItemWriteObservation, GsiProjectionLifecycle)>> {
        let mut pages = pages.into_iter();
        let mut live_rows = (0..reads.len()).map(|_| None).collect::<Vec<_>>();
        for group in live_groups {
            let page = pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL live observation page is missing"))?;
            if page.more {
                return Err(StorageError::internal(
                    "SQL live observation exceeded its page bound",
                ));
            }
            let mut matched = match_observation_rows(group.len(), page.rows)?;
            for (position, index) in group.iter().enumerate() {
                live_rows[*index] = matched[position].take();
            }
        }
        let mut markers = if marker_groups.is_empty() {
            Vec::new()
        } else {
            (0..reads.len())
                .map(|_| None)
                .collect::<Vec<Option<storage_types::RegionalMarker>>>()
        };
        for group in marker_groups {
            let page = pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL marker observation page is missing"))?;
            if page.more {
                return Err(StorageError::internal(
                    "SQL marker observation exceeded its page bound",
                ));
            }
            let mut matched = match_observation_rows(group.len(), page.rows)?;
            for (position, index) in group.iter().enumerate() {
                markers[*index] = matched[position]
                    .take()
                    .map(crate::regional::decode_marker_row)
                    .transpose()?;
            }
        }
        // GSI phases come from the confirmed catalog binding.
        let lifecycles = lifecycle_tables_for_writes(reads)?
            .into_iter()
            .map(|table| {
                (
                    table.info.table_name.clone(),
                    GsiProjectionLifecycle::from_binding(table),
                )
            })
            .collect::<Vec<_>>();
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(reads.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for (read, row) in reads.iter().zip(&live_rows) {
            requests.push((
                read.target().0,
                row.as_ref()
                    .map(|row| row.value(crate::ColumnIndex::new(0)))
                    .transpose()?,
                read.target().0.max_indexers,
            ));
        }
        let bound = reads
            .len()
            .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
            .ok_or_else(|| StorageError::internal("Primary observation bound overflow"))?;
        let hydrated = crate::item_read::decode_item_images(self, requests, bound)?;
        for (read, item) in reads.iter().zip(&hydrated) {
            if let Some(item) = item
                && !item.item.has_verified_keys(read.target().1)?
            {
                return Err(StorageError::internal(
                    "SQL observation content differs from physical key",
                ));
            }
        }
        let mut items = decode_write_observations(live_rows, hydrated)?;
        for (item, marker) in items.iter_mut().zip(markers) {
            *item = std::mem::replace(item, ItemWriteObservation::Absent).with_marker(marker)?;
        }
        combine_write_observations(
            reads,
            storage_types::Exact::try_new(reads.len(), items)?,
            lifecycles,
        )
    }
}

pub(crate) fn decode_live(
    row: SqlRow,
    stored: StoredWireItem,
) -> StorageResult<ItemWriteObservation> {
    let [_value, provenance, ttl_epoch] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL item observation width is invalid"))?;
    let provenance = decode_provenance(provenance)?;
    let ttl_epoch = match ttl_epoch {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value),
        _ => {
            return Err(StorageError::internal(
                "SQL stored TTL epoch has invalid type",
            ));
        }
    };
    Ok(ItemWriteObservation::Live {
        stored: Box::new(stored),
        provenance,
        ttl_epoch,
    })
}

fn decode_provenance(value: SqlValue) -> StorageResult<storage_types::ItemProvenance> {
    let SqlValue::Blob(bytes) = value else {
        return Err(StorageError::internal(
            "SQL stored provenance has invalid type",
        ));
    };
    storage_types::ItemProvenance::decode(&bytes)
}
