//! Primary item point reads over the one persisted SQL row layout.

use std::fmt::Write as _;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_provider::{
    BoundRangeReadPage, BoundRangeReadPlan, CATALOG_READ_BYTES, ItemReadAttempt, RangeReadItem,
    RangeReadPage,
};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, Exact, KeyAttributeType, KeyAttributes,
    KeySchemaElement, StorageError, StorageResult, WireItem,
};

use crate::{
    ColumnIndex, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt, range_read::PublicImage,
};

impl<A: SqlAttempt, L: SqlLayout> ItemReadAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn finish_primary_read(
        &mut self,
        batch: storage_provider::PrimaryReadBatch<'_>,
    ) -> StorageResult<storage_provider::ReadObservationResult<Exact<Option<WireItem>>>> {
        // Only PostgreSQL currently supports combined authority and item
        // settlement. Other SQL engines keep their existing snapshot and
        // settlement behavior.
        if self.engine.engine() != SqlEngine::Postgres {
            return storage_provider::finish_primary_read_default(self, batch).await;
        }
        batch.check_pairs()?;
        if batch.tables.len() != 1 || batch.reads.len() != 1 {
            return finish_primary_batch(self, batch).await;
        }
        let candidate = &batch.tables[0];
        let (table, key) = batch.reads[0];
        let statement = combined_point_statement(self.engine, candidate.stamp, table, key)?;
        let result = self
            .attempt
            .finish_read_batch(statement, Vec::new(), CATALOG_READ_BYTES)
            .await?;
        let mut stamps = None;
        let mut item = Ok(None);
        if let Some(page) = result.observation {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "terminal SQL Get returned multiple rows",
                ));
            }
            if let Some(row) = page.rows.first() {
                let stamp = decode_nullable_stamp(row)?;
                stamps = Some(Exact::try_new(1, vec![stamp])?);
                if stamp == Some(candidate.stamp) {
                    item = (|| {
                        let value = row.value(ColumnIndex::new(2))?;
                        let value = (!matches!(value, SqlValue::Null)).then_some(value);
                        let item = decode_item_images(
                            self,
                            vec![(candidate.table, value, candidate.table.max_indexers)],
                            storage_types::MaterializedItemRecord::encoded_length_bound(),
                        )?
                        .pop()
                        .flatten()
                        .map(|stored| stored.item);
                        if let Some(item) = &item
                            && !item.has_verified_keys(batch.reads[0].1)?
                        {
                            return Err(StorageError::internal(
                                "SQL primary content differs from physical key",
                            ));
                        }
                        Ok(item)
                    })();
                }
            }
        }
        let items = match result.reads {
            Ok(_) => {
                // `finish_read_batch` proved the empty statement list's
                // result count; only the observation can be absent.
                if stamps.is_none() {
                    return Err(StorageError::internal(
                        "terminal SQL Get returned no catalog observation",
                    ));
                }
                item.and_then(|item| Exact::try_new(1, vec![item]))
            }
            Err(failure) => {
                if failure.statement_index != 0 {
                    return Err(StorageError::internal(
                        "terminal SQL Get failure position differs",
                    ));
                }
                Err(failure.error)
            }
        };
        Ok(storage_provider::ReadObservationResult {
            catalog_version: None,
            stamps,
            result: items,
        })
    }

    async fn read_primary_item(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<WireItem>> {
        self.read_primary_pairs(&[(table, key)])
            .await?
            .into_single()
            .ok_or_else(|| StorageError::internal("Missing primary point-read result"))
    }

    async fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> StorageResult<Exact<Option<WireItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut requests = Vec::new();
            let mut results = Vec::new();
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            results
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            let rows = load_primary_rows(catalog, reads, "item_value").await?;
            for ((table, _), row) in reads.iter().zip(&rows) {
                requests.push((
                    *table,
                    row.as_ref()
                        .map(|row| row.value(ColumnIndex::new(0)))
                        .transpose()?,
                    table.max_indexers,
                ));
            }
            let bound = reads
                .len()
                .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
                .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
            let hydrated = decode_item_images(catalog, requests, bound)?;
            for (item, (_, key)) in hydrated.into_iter().zip(reads) {
                if let Some(item) = &item
                    && !item.item.has_verified_keys(key)?
                {
                    return Err(StorageError::internal(
                        "SQL primary content differs from physical key",
                    ));
                }
                results.push(item.map(|item| item.item));
            }
            Exact::try_new(reads.len(), results)
        }
        .await;
        settlement.complete(result)
    }

    async fn read_range_observation(
        &mut self,
        candidate: storage_provider::TableReadCandidate<'_>,
        range: Option<(&BoundRangeReadPlan<'_>, &BoundRangeReadPage<'_>)>,
    ) -> StorageResult<storage_provider::ReadObservationResult<Option<RangeReadPage<RangeReadItem>>>>
    {
        if self.engine.engine() != crate::SqlEngine::Postgres {
            return storage_provider::read_range_observation_default(self, candidate, range).await;
        }
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = catalog.read_combined_range(candidate, range).await;
        operation.complete(result)
    }

    async fn read_range_page(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
    ) -> StorageResult<RangeReadPage<RangeReadItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .read_projected_range::<PublicImage>(table, bound, page, None)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

async fn finish_primary_batch<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    batch: storage_provider::PrimaryReadBatch<'_>,
) -> StorageResult<storage_provider::ReadObservationResult<Exact<Option<WireItem>>>> {
    let names = batch
        .tables
        .iter()
        .map(|candidate| candidate.table.info.table_name.clone())
        .collect::<Vec<_>>();
    let prefix = crate::catalog::terminal_catalog_statement(catalog.engine, &names);
    let statements = batch
        .reads
        .iter()
        .map(|(table, key)| {
            let candidate = batch
                .tables
                .iter()
                .find(|candidate| std::ptr::eq(candidate.table, *table))
                .ok_or_else(|| StorageError::internal("point-read table has no candidate"))?;
            guarded_point_statement(catalog.engine, candidate.stamp, table, key)
                .map(|statement| (statement, crate::ReadProtection::Snapshot))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let result = catalog
        .attempt
        .finish_read_batch(
            prefix,
            statements,
            crate::catalog::catalog_batch_bytes(names.len())?,
        )
        .await?;
    let stamps = result
        .observation
        .map(|page| crate::catalog::decode_terminal_catalog(&names, page))
        .transpose()?;
    let items = match result.reads {
        Err(failure) => {
            // The prefix contains no candidate item schema. Global item indexes
            // start at one and remain global across native windows.
            Err(
                if failure
                    .statement_index
                    .checked_sub(1)
                    .is_some_and(|index| index < batch.reads.len())
                {
                    failure.error
                } else {
                    failure.outside_candidates()
                },
            )
        }
        Ok(pages) => decode_primary_batch(catalog, batch.reads, pages),
    };
    Ok(storage_provider::ReadObservationResult {
        catalog_version: None,
        stamps,
        result: items,
    })
}

fn decode_primary_batch<A: SqlAttempt, L: SqlLayout>(
    catalog: &SqlCatalogAttempt<'_, A, L>,
    reads: &[(&BoundTableInfo, &KeyAttributes)],
    pages: storage_types::Exact<SqlQueryPage>,
) -> StorageResult<Exact<Option<WireItem>>> {
    let rows = pages
        .into_iter()
        .map(decode_point_page)
        .collect::<StorageResult<Vec<_>>>()?;
    let requests = reads
        .iter()
        .zip(&rows)
        .map(|((table, _), row)| {
            let value = row
                .as_ref()
                .map(|row| row.value(ColumnIndex::new(0)))
                .transpose()?;
            Ok((
                *table,
                value.filter(|value| !matches!(value, SqlValue::Null)),
                table.max_indexers,
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let bound = reads
        .len()
        .checked_mul(storage_types::MaterializedItemRecord::encoded_length_bound())
        .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
    let items = decode_item_images(catalog, requests, bound)?;
    let items = items
        .into_iter()
        .zip(reads)
        .map(|(item, (_, key))| {
            if let Some(item) = &item
                && !item.item.has_verified_keys(key)?
            {
                return Err(StorageError::internal(
                    "SQL primary content differs from physical key",
                ));
            }
            Ok(item.map(|item| item.item))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Exact::try_new(reads.len(), items)
}

async fn load_primary_rows<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    reads: &[(&BoundTableInfo, &KeyAttributes)],
    columns: &str,
) -> StorageResult<Vec<Option<crate::SqlRow>>> {
    let statements = primary_read_statements(catalog.engine, reads, columns)?;
    let mut protected = Vec::new();
    let mut rows = Vec::new();
    rows.try_reserve_exact(reads.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    let prefix = (catalog.protection != crate::ReadProtection::Snapshot)
        .then(|| {
            catalog
                .catalog_lock
                .item_lock_prefix(reads.iter().copied(), &mut protected)
        })
        .transpose()?;
    protected
        .try_reserve_exact(statements.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    protected.extend(
        statements
            .into_iter()
            .map(|statement| (statement, catalog.protection)),
    );
    let pages = catalog
        .attempt
        .query_batch_protected_exact(protected, CATALOG_READ_BYTES)
        .await?;
    let mut pages = pages.into_iter();
    if let Some(prefix) = prefix {
        catalog
            .catalog_lock
            .finish_lock_prefix(prefix, &mut pages)?;
    }
    for page in pages {
        rows.push(decode_point_page(page)?);
    }
    Ok(rows)
}

fn primary_read_statements(
    engine: impl SqlLayout,
    reads: &[(&BoundTableInfo, &KeyAttributes)],
    columns: &str,
) -> StorageResult<Vec<SqlStatement>> {
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(reads.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for (table, key) in reads {
        statements.push(point_read_statement(engine, table, key, columns)?);
    }
    Ok(statements)
}

fn decode_point_page(page: SqlQueryPage) -> StorageResult<Option<crate::SqlRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL point read returned multiple rows",
        ));
    }
    Ok(page.rows.into_iter().next())
}

pub(crate) fn decode_item_images<A: SqlAttempt, L: SqlLayout>(
    catalog: &SqlCatalogAttempt<'_, A, L>,
    reads: Vec<(
        &BoundTableInfo,
        Option<&SqlValue>,
        storage_types::MaxIndexers,
    )>,
    maximum: usize,
) -> StorageResult<Vec<Option<storage_types::StoredWireItem>>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let probe = catalog.attempt.fault_probe();
    let mut budget = storage_types::PageByteBudget::try_new(maximum)?;
    reads
        .into_iter()
        .map(|(table, value, capacity)| {
            value
                .map(|value| {
                    let record = decode_item_value(table, value)?;
                    if !budget.admit(record.decoded_bytes()) {
                        return Err(storage_types::AttemptFailure::CAPACITY.into());
                    }
                    let item = record.into_stored(capacity)?;
                    let Some(probe) = probe else {
                        return Ok(item);
                    };
                    Ok(storage_types::StoredWireItem {
                        item: probe.alter_item_image(catalog.protection, item.item),
                        indexers: item.indexers,
                    })
                })
                .transpose()
        })
        .collect()
}

pub(crate) fn decode_item_value<'a>(
    table: &BoundTableInfo,
    value: &'a SqlValue,
) -> StorageResult<storage_types::MaterializedItemRecord<'a>> {
    let SqlValue::Blob(value) = value else {
        return Err(StorageError::internal("SQL primary value has invalid type"));
    };
    storage_types::MaterializedItemRecord::decode(
        value,
        table.history_id,
        storage_types::MaterializedItemRecord::encoded_length_bound(),
    )
}

pub(crate) fn select_image_prefix(
    table: &BoundTableInfo,
    page: &mut SqlQueryPage,
    offset: usize,
    maximum: usize,
) -> StorageResult<()> {
    let mut budget = storage_types::PageByteBudget::try_new(maximum)?;
    let mut count = 0;
    for row in &page.rows {
        let record = decode_item_value(table, row.value(ColumnIndex::new(offset))?)?;
        if !budget.admit(record.decoded_bytes()) {
            break;
        }
        count += 1;
    }
    if count < page.rows.len() {
        page.rows.truncate(count);
        page.more = true;
    }
    Ok(())
}

pub(crate) fn point_read_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    columns: &str,
) -> StorageResult<SqlStatement> {
    let (predicate, binds) = primary_key_predicate(engine, table, key)?;
    let physical = engine.item_table_name(table.history_id);
    Ok(SqlStatement::new(
        format!("SELECT {columns} FROM \"{physical}\" WHERE {predicate} LIMIT 1"),
        binds,
    ))
}

pub(crate) fn primary_key_predicate(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<(String, Vec<SqlValue>)> {
    let capacity = table
        .key_schema
        .iter()
        .map(|element| element.attribute_name.len() + "\"\" = CAST($2 AS TEXT)::NUMERIC AND ".len())
        .sum();
    let mut predicate = String::with_capacity(capacity);
    let mut binds = Vec::with_capacity(table.info.key_schema.len());
    for element in KeySchemaElement::ordered(&table.info.key_schema) {
        let value = key
            .get(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let column = AttributeName::new(&element.attribute_name);
        let key_type = table
            .info
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == element.attribute_name)
            .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
        let placeholder = match (engine.engine(), key_type) {
            (SqlEngine::Postgres, KeyAttributeType::N) => {
                engine.engine().numeric_placeholder(binds.len() + 1)
            }
            _ => engine.engine().placeholder(binds.len() + 1),
        };
        if !binds.is_empty() {
            predicate.push_str(" AND ");
        }
        let _ = write!(predicate, "\"{}\" = {placeholder}", column.sanitized());
        binds.push(encode_key_value(engine, value)?);
    }
    Ok((predicate, binds))
}

pub(crate) fn encode_key_value(
    engine: impl SqlLayout,
    value: &AttributeValue,
) -> StorageResult<SqlValue> {
    match value {
        AttributeValue::S(raw) => Ok(SqlValue::Text(raw.clone())),
        AttributeValue::N(raw) if engine.engine() == SqlEngine::Postgres => {
            Ok(SqlValue::Text(raw.clone()))
        }
        AttributeValue::N(raw) => storage_types::encode_dynamodb_number_key(raw)
            .map(SqlValue::Text)
            .map_err(StorageError::validation),
        AttributeValue::B(raw) => storage_types::dynamodb_binary::decode_base64_string(raw)
            .map(SqlValue::Blob)
            .map_err(|error| {
                StorageError::validation(format!("binary key must be base64: {error}"))
            }),
        _ => Err(StorageError::validation(
            "key attribute must be an S, N, or B scalar",
        )),
    }
}

pub(crate) fn decode_key_value(
    engine: impl SqlLayout,
    kind: KeyAttributeType,
    value: &SqlValue,
) -> StorageResult<AttributeValue> {
    match (kind, value) {
        (KeyAttributeType::S, SqlValue::Text(value)) => Ok(AttributeValue::S(value.clone())),
        (KeyAttributeType::N, SqlValue::Text(value)) if engine.engine() == SqlEngine::Postgres => {
            Ok(AttributeValue::N(value.clone()))
        }
        (KeyAttributeType::N, SqlValue::Text(value)) => {
            storage_types::decode_dynamodb_number_key(value)
                .map(AttributeValue::N)
                .map_err(|_| StorageError::internal("SQL range row has an invalid numeric key"))
        }
        (KeyAttributeType::B, SqlValue::Blob(value)) => {
            Ok(AttributeValue::B(STANDARD.encode(value)))
        }
        _ => Err(StorageError::internal(
            "SQL range row has an invalid key type",
        )),
    }
}

fn decode_nullable_stamp(
    row: &crate::SqlRow,
) -> StorageResult<Option<storage_provider::CatalogStamp>> {
    match (
        row.value(ColumnIndex::new(0))?,
        row.value(ColumnIndex::new(1))?,
    ) {
        (SqlValue::Null, SqlValue::Null) => Ok(None),
        (SqlValue::Null, _) | (_, SqlValue::Null) => {
            Err(StorageError::internal("partial SQL catalog stamp"))
        }
        _ => crate::catalog::decode_stamp(row).map(Some),
    }
}

pub(crate) fn combined_point_statement(
    engine: impl SqlLayout,
    stamp: storage_provider::CatalogStamp,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<SqlStatement> {
    render_authority_point_statement(
        engine,
        stamp,
        table,
        key,
        "SELECT authority.id, authority.catalog_revision, item.item_value FROM (VALUES (1)) AS \
         anchor(value) LEFT JOIN authority ON TRUE LEFT",
    )
}

pub(crate) fn guarded_point_statement(
    engine: impl SqlLayout,
    stamp: storage_provider::CatalogStamp,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<SqlStatement> {
    // The separate prefix already carries authority. Missing items need no
    // anchor row; the existing point decoder accepts an empty result.
    render_authority_point_statement(
        engine,
        stamp,
        table,
        key,
        "SELECT item.item_value FROM authority",
    )
}

fn render_authority_point_statement(
    engine: impl SqlLayout,
    stamp: storage_provider::CatalogStamp,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    select: &str,
) -> StorageResult<SqlStatement> {
    let (predicate, mut binds) = primary_key_predicate(engine, table, key)?;
    let physical = engine.item_table_name(table.history_id);
    let name = binds.len() + 1;
    let history = name + 1;
    let revision = history + 1;
    binds.push(SqlValue::Text(table.info.table_name.to_string()));
    binds.push(SqlValue::Text(
        uuid::Uuid::from_bytes(*stamp.history_id.as_ref()).to_string(),
    ));
    binds.push(SqlValue::Integer(
        i64::try_from(stamp.revision.get())
            .map_err(|_| StorageError::internal("catalog revision exceeds SQL integer"))?,
    ));
    Ok(SqlStatement::new(
        format!(
            "WITH authority AS (SELECT id, catalog_revision FROM {} WHERE table_name = ${name} \
             AND table_status <> 'DELETING') {select} JOIN LATERAL (SELECT item_value FROM \
             \"{physical}\" WHERE authority.id = ${history} AND authority.catalog_revision = \
             ${revision} AND {predicate} LIMIT 1) AS item ON TRUE",
            engine.relation_name("tables")
        ),
        binds,
    ))
}
