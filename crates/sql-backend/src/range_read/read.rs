//! One SQL range lowerer for SQLite, Turso, and Postgres.

use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, PrimaryStateRow, PrimaryStateScan,
    PrimaryStateScanAttempt, RangeReadItem, RangeReadPage,
};
use storage_types::{BoundTableInfo, StorageResult};

use super::{
    decode::{RangeDecode, decode_range_rows},
    projection::{PrimaryState, PublicImage, RangeProjection},
    sql_statements::{GsiRowShape, RangeStatementInput, ordered_columns, range_statement},
};
use crate::{
    SqlAttempt, SqlLayout,
    catalog::SqlCatalogAttempt,
    constants::{RANGE_PAGE_READ_BYTES, TERMINAL_RANGE_READ_BYTES},
};

impl<A: SqlAttempt, L: SqlLayout> PrimaryStateScanAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn scan_primary_state(
        &mut self,
        scan: &PrimaryStateScan,
    ) -> StorageResult<RangeReadPage<PrimaryStateRow>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let bound = scan.bind()?;
            let page = scan.page(&bound)?;
            catalog
                .read_projected_range::<PrimaryState>(scan.table(), &bound, &page, None)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    pub(crate) async fn read_combined_range(
        &mut self,
        candidate: storage_provider::TableReadCandidate<'_>,
        range: Option<(&BoundRangeReadPlan<'_>, &BoundRangeReadPage<'_>)>,
    ) -> StorageResult<storage_provider::ReadObservationResult<Option<RangeReadPage<RangeReadItem>>>>
    {
        let names = [candidate.table.info.table_name.clone()];
        let mut statements = vec![(
            crate::catalog::terminal_catalog_statement(self.engine, &names),
            crate::ReadProtection::Snapshot,
        )];
        if let Some((bound, page)) = range {
            let columns = ordered_columns(candidate.table, bound)?;
            let gsi = bound
                .index
                .as_ref()
                .map(|index| GsiRowShape::for_index(candidate.table, index));
            let input = RangeStatementInput {
                table: candidate.table,
                bound,
                page,
                columns: &columns,
                gsi: gsi.as_ref(),
            };
            let statement = range_statement::<PublicImage>(self.engine, &input)?;
            let guarded = crate::item_observation::guard_observation_statement(
                self.engine,
                statement,
                &[(&names[0], Some(candidate))],
            )?;
            statements.push((guarded, crate::ReadProtection::Snapshot));
        }
        // The first range page and COMMIT share one wave. A page that needs
        // more physical rows ends early with a continuation key.
        let observation = statements.remove(0).0;
        let observed = self
            .attempt
            .finish_read_batch(observation, statements, TERMINAL_RANGE_READ_BYTES)
            .await?;
        let stamps = observed
            .observation
            .map(|page| crate::catalog::decode_terminal_catalog(&names, page))
            .transpose()?;
        let matches = stamps
            .as_ref()
            .is_some_and(|stamps| *stamps == [Some(candidate.stamp)]);
        let result = match observed.reads {
            Err(failure) => Err(if range.is_some() && failure.statement_index == 1 {
                failure.error
            } else {
                failure.outside_candidates()
            }),
            Ok(pages) => {
                if matches {
                    match range {
                        Some((bound, page)) => self
                            .read_projected_range::<PublicImage>(
                                candidate.table,
                                bound,
                                page,
                                pages.into_single(),
                            )
                            .await
                            .map(Some),
                        None => Ok(None),
                    }
                } else {
                    Ok(None)
                }
            }
        };
        Ok(storage_provider::ReadObservationResult {
            catalog_version: None,
            stamps,
            result,
        })
    }

    /// `terminal`: the first page came from a read that committed the
    /// attempt, so no further page can be read in it.
    pub(crate) async fn read_projected_range<P: RangeProjection>(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
        mut first: Option<crate::SqlQueryPage>,
    ) -> StorageResult<RangeReadPage<P::Item>> {
        let terminal = first.is_some();
        let columns = ordered_columns(table, bound)?;
        let gsi = bound
            .index
            .as_ref()
            .map(|index| GsiRowShape::for_index(table, index));
        let mut result = RangeReadPage::<P::Item>::default();
        let mut start = page.start.clone();
        loop {
            let physical_page = BoundRangeReadPage {
                condition: page.condition,
                start,
                budget: page.budget,
                forward: page.forward,
            };
            let input = RangeStatementInput {
                table,
                bound,
                page: &physical_page,
                columns: &columns,
                gsi: gsi.as_ref(),
            };
            let fetched = match first.take() {
                Some(fetched) => fetched,
                None => {
                    self.attempt
                        .query(
                            range_statement::<P>(self.engine, &input)?,
                            self.protection,
                            RANGE_PAGE_READ_BYTES,
                        )
                        .await?
                }
            };
            let bytes = if terminal {
                TERMINAL_RANGE_READ_BYTES
            } else {
                RANGE_PAGE_READ_BYTES
            };
            let decoded =
                self.decode_range_batch::<P>(&input, fetched, bytes.get(), &mut result)?;
            if matches!(decoded, RangeDecode::Exhausted) {
                return result.finish();
            }
            if matches!(decoded, RangeDecode::BudgetStop)
                || result.progress.evaluated_count >= page.budget.max_items
                || terminal
            {
                return result.finish_with_more();
            }
            start = Some(result.progress.last_evaluated_key.clone().ok_or_else(|| {
                storage_types::StorageError::internal(
                    "SQL range continuation lacks an admitted row",
                )
            })?);
        }
    }

    fn decode_range_batch<P: RangeProjection>(
        &mut self,
        input: &RangeStatementInput<'_, '_>,
        mut fetched: crate::SqlQueryPage,
        max_bytes: usize,
        result: &mut RangeReadPage<P::Item>,
    ) -> StorageResult<RangeDecode> {
        let image = match input.gsi {
            Some(shape) => shape.reads_image().then_some(input.columns.len()),
            None => Some(input.columns.len() + 1),
        };
        let hydrated = if let Some(offset) = image {
            crate::item_read::select_image_prefix(input.table, &mut fetched, offset, max_bytes)?;
            let capacity = input.table.max_indexers;
            let requests = fetched
                .rows
                .iter()
                .map(|row| {
                    Ok((
                        input.table,
                        Some(row.value(crate::ColumnIndex::new(offset))?),
                        capacity,
                    ))
                })
                .collect::<StorageResult<Vec<_>>>()?;
            crate::item_read::decode_item_images(self, requests, max_bytes)?
        } else {
            fetched.rows.iter().map(|_| None).collect()
        };
        decode_range_rows::<P>(self.engine, input, (fetched, hydrated), result)
    }
}
