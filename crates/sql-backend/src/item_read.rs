//! Primary item point reads over the one persisted SQL row layout.

use std::fmt::Write as _;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_provider::{CATALOG_READ_BYTES, GetItemAttempt};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, KeyAttributeType, KeyAttributes,
    KeySchemaElement, StorageError, StorageResult, WireItem,
};

use crate::{
    ColumnIndex, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
};

impl<A: SqlAttempt, L: SqlLayout> GetItemAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn read_primary_item(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<WireItem>> {
        self.read_primary_pairs(&[(table, key)])
            .await?
            .pop()
            .ok_or_else(|| StorageError::internal("Missing primary point-read result"))
    }

    async fn read_primary_pairs(
        &mut self,
        reads: &[(&BoundTableInfo, &KeyAttributes)],
    ) -> StorageResult<Vec<Option<WireItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut requests = Vec::new();
            let mut results = Vec::new();
            requests
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            results
                .try_reserve_exact(reads.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let rows = load_primary_rows(catalog, reads).await?;
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
                .checked_mul(CATALOG_READ_BYTES.get())
                .ok_or_else(|| StorageError::internal("Primary batch bound overflow"))?;
            let hydrated = hydrate_item_images(catalog, requests, bound).await?;
            if hydrated.len() != reads.len() {
                return Err(StorageError::internal(
                    "SQL primary hydration count differs",
                ));
            }
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
            Ok(results)
        }
        .await;
        settlement.complete(result)
    }
}

async fn load_primary_rows<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    reads: &[(&BoundTableInfo, &KeyAttributes)],
) -> StorageResult<Vec<Option<crate::SqlRow>>> {
    let is_locked = catalog.engine.engine() == SqlEngine::Postgres
        && catalog.protection != crate::ReadProtection::Snapshot
        && !reads.is_empty();
    let statements = primary_read_statements(catalog.engine, reads, is_locked)?;
    let expected = statements.len();
    let mut protected = Vec::new();
    let mut rows = Vec::new();
    protected
        .try_reserve_exact(expected)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    rows.try_reserve_exact(reads.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    protected.extend(
        statements
            .into_iter()
            .enumerate()
            .map(|(index, statement)| {
                let protection = if is_locked && index == 0 {
                    crate::ReadProtection::Snapshot
                } else {
                    catalog.protection
                };
                (statement, protection)
            }),
    );
    let mut pages = catalog
        .attempt
        .query_batch_protected(protected, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != expected {
        return Err(StorageError::internal(
            "SQL point-read batch returned the wrong count",
        ));
    }
    if is_locked {
        pages.remove(0);
    }
    for page in pages {
        rows.push(decode_point_page(page)?);
    }
    Ok(rows)
}

fn primary_read_statements(
    engine: impl SqlLayout,
    reads: &[(&BoundTableInfo, &KeyAttributes)],
    is_locked: bool,
) -> StorageResult<Vec<SqlStatement>> {
    let count = reads
        .len()
        .checked_add(usize::from(is_locked))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    if is_locked {
        statements.push(crate::item_lock::item_lock_statement(
            reads.iter().copied(),
        )?);
    }
    for (table, key) in reads {
        statements.push(point_read_statement(engine, table, key, "item_descriptor")?);
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

pub(crate) async fn hydrate_item_images<A: SqlAttempt, L: SqlLayout>(
    attempt: &mut SqlCatalogAttempt<'_, A, L>,
    reads: Vec<(
        &BoundTableInfo,
        Option<&SqlValue>,
        storage_types::MaxIndexers,
    )>,
    maximum: usize,
) -> StorageResult<Vec<Option<storage_types::StoredWireItem>>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let count = reads.iter().filter(|(_, value, _)| value.is_some()).count();
        let mut present = Vec::new();
        let mut records = Vec::new();
        let mut results = Vec::new();
        present
            .try_reserve_exact(reads.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        records
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        results
            .try_reserve_exact(reads.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (table, value, capacity) in reads {
            present.push(value.is_some());
            if let Some(value) = value {
                records.push((
                    table,
                    decode_item_descriptor(table, value, maximum)?,
                    capacity,
                ));
            }
        }
        if records.is_empty() {
            results.extend(present.into_iter().map(|_| None));
            return Ok(results);
        }
        let hydrated =
            stream_provider::retained_image::RetainedItemRecord::hydrate(attempt, records, maximum)
                .await?;
        #[cfg(feature = "fault-injection")]
        let hydrated = match attempt.attempt.fault_probe() {
            Some(probe) => hydrated
                .into_iter()
                .map(|mut item| {
                    item.item = probe.alter_item_image(attempt.protection, item.item);
                    item
                })
                .collect::<Vec<_>>(),
            None => hydrated,
        };
        let mut items = hydrated.into_iter();
        for present in present {
            results.push(if present {
                Some(
                    items.next().ok_or_else(|| {
                        StorageError::internal("Missing primary hydration result")
                    })?,
                )
            } else {
                None
            });
        }
        if items.next().is_some() {
            return Err(StorageError::internal("Extra primary hydration result"));
        }
        Ok(results)
    }
    .await;
    settlement.complete(result)
}

pub(crate) fn decode_item_descriptor(
    table: &BoundTableInfo,
    value: &SqlValue,
    maximum: usize,
) -> StorageResult<stream_provider::retained_image::RetainedItemRecord> {
    let SqlValue::Blob(value) = value else {
        return Err(StorageError::internal(
            "SQL primary descriptor has invalid type",
        ));
    };
    stream_provider::retained_image::RetainedItemRecord::decode(value, table.history_id, maximum)
}

pub(crate) fn select_image_prefix(
    table: &BoundTableInfo,
    page: &mut SqlQueryPage,
    offset: usize,
    maximum: usize,
) -> StorageResult<()> {
    let mut budget = stream_provider::retained_image::RetainedImagePageBudget::try_new(maximum)?;
    let mut count = 0;
    for row in &page.rows {
        let record = decode_item_descriptor(table, row.value(ColumnIndex::new(offset))?, maximum)?;
        if !budget.admit(record.read_work_bytes(maximum)?)? {
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
    let physical = engine.item_table_name(&table.info.table_name, table.history_id);
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
