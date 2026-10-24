//! Bounded ordered SystemStream record scans on the caller's SQL attempt.

use std::future::Future;

use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    StorageEnum, StorageError, StorageResult, StreamItemId, StreamName, SystemStreamEffectDigest,
    SystemStreamPosition, TimestampMillis, storage_serde,
};
use stream_provider::{
    DynamoStreamEffectRow, MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, PointerOwner, StreamPointerReference,
    SystemStreamTrimAttempt, SystemStreamTrimOperationId, SystemStreamTrimPageReceipt,
    SystemStreamTrimPageRequest, SystemStreamTrimRecord,
};

use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
};

impl<A: SqlAttempt, L: SqlLayout> SystemStreamTrimAttempt for SqlCatalogAttempt<'_, A, L> {
    fn read_trim_receipts(
        &mut self,
        request: &SystemStreamTrimPageRequest,
    ) -> impl Future<
        Output = StorageResult<(
            Option<SystemStreamTrimPageReceipt>,
            Option<SystemStreamTrimPageReceipt>,
        )>,
    > + Send {
        read_receipts(self.attempt, self.engine, request)
    }

    fn load_trim_boundaries(
        &mut self,
        request: &SystemStreamTrimPageRequest,
    ) -> impl Future<Output = StorageResult<Vec<SystemStreamPosition>>> + Send {
        crate::stream_history::load_trim_boundaries(self.attempt, self.engine, request)
    }

    fn scan_trim_records(
        &mut self,
        after: Option<&SystemStreamPosition>,
    ) -> impl Future<Output = StorageResult<(Vec<SystemStreamTrimRecord>, bool)>> + Send {
        scan_records(self.attempt, self.engine, after)
    }

    fn apply_trim_page(
        &mut self,
        selected: &[SystemStreamTrimRecord],
        receipt: &SystemStreamTrimPageReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        apply_page(self.attempt, self.engine, selected, receipt)
    }
}

pub(crate) async fn initialize_trim_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(
            format!(
                "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_trim_receipts} \
                 (operation_key {} NOT NULL, input_sequence {} NOT NULL, receipt_blob {} NOT \
                 NULL, PRIMARY KEY (operation_key, input_sequence)){}",
                engine.engine().blob_type(),
                engine.engine().integer_type(),
                engine.engine().blob_type(),
                engine.engine().table_suffix(),
                relation_sys_system_stream_trim_receipts =
                    engine.relation_name("sys_system_stream_trim_receipts"),
            ),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn read_receipts<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    request: &SystemStreamTrimPageRequest,
) -> StorageResult<(
    Option<SystemStreamTrimPageReceipt>,
    Option<SystemStreamTrimPageReceipt>,
)> {
    let sequence = request.continuation().map_or(0, |cursor| cursor.sequence());
    let mut statements = vec![receipt_read_statement(
        engine,
        request.operation_id(),
        sequence,
    )?];
    if let Some(previous) = sequence.checked_sub(1) {
        statements.push(receipt_read_statement(
            engine,
            request.operation_id(),
            previous,
        )?);
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != if sequence == 0 { 1 } else { 2 } {
        return Err(invalid_receipt());
    }
    let mut pages = pages.into_iter();
    let current = decode_receipt(pages.next().ok_or_else(invalid_receipt)?, request, sequence)?;
    let predecessor = if sequence == 0 {
        None
    } else {
        decode_receipt(
            pages.next().ok_or_else(invalid_receipt)?,
            request,
            sequence - 1,
        )?
    };
    Ok((current, predecessor))
}

pub(crate) async fn write_receipt<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    let changed = attempt
        .execute(receipt_insert_statement(engine, receipt)?)
        .await?;
    if changed != 1 {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream trim page was issued concurrently".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn receipt_insert_statement(
    engine: impl SqlLayout,
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<SqlStatement> {
    let sequence = i64::try_from(receipt.input_sequence).map_err(|_| invalid_receipt())?;
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_system_stream_trim_receipts} (operation_key, \
             input_sequence, receipt_blob) VALUES ({}) ON CONFLICT DO NOTHING",
            engine.engine().placeholders(3),
            relation_sys_system_stream_trim_receipts =
                engine.relation_name("sys_system_stream_trim_receipts"),
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(&receipt.operation_id)?),
            SqlValue::Integer(sequence),
            SqlValue::Blob(storage_serde::to_bytes(receipt)?),
        ],
    ))
}

fn receipt_read_statement(
    engine: impl SqlLayout,
    operation: &SystemStreamTrimOperationId,
    sequence: u64,
) -> StorageResult<SqlStatement> {
    let sequence = i64::try_from(sequence).map_err(|_| invalid_receipt())?;
    Ok(SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_system_stream_trim_receipts} WHERE \
             operation_key = {} AND input_sequence = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_system_stream_trim_receipts =
                engine.relation_name("sys_system_stream_trim_receipts"),
        ),
        vec![
            SqlValue::Blob(storage_serde::to_bytes(operation)?),
            SqlValue::Integer(sequence),
        ],
    ))
}

fn decode_receipt(
    page: crate::SqlQueryPage,
    request: &SystemStreamTrimPageRequest,
    sequence: u64,
) -> StorageResult<Option<SystemStreamTrimPageReceipt>> {
    if page.more || page.rows.len() > 1 {
        return Err(invalid_receipt());
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let [SqlValue::Blob(bytes)]: [SqlValue; 1] = row
                .into_values()
                .try_into()
                .map_err(|_| invalid_receipt())?
            else {
                return Err(invalid_receipt());
            };
            let receipt: SystemStreamTrimPageReceipt = storage_serde::from_bytes(&bytes)?;
            if receipt.operation_id != *request.operation_id() || receipt.input_sequence != sequence
            {
                return Err(invalid_receipt());
            }
            Ok(receipt)
        })
        .transpose()
}

fn invalid_receipt() -> StorageError {
    StorageError::internal("SQL SystemStream trim receipt is invalid")
}

pub(crate) async fn scan_records<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    after: Option<&SystemStreamPosition>,
) -> StorageResult<(Vec<SystemStreamTrimRecord>, bool)> {
    let statement = crate::stream_read::scan_trim_records(engine, after)?;
    // The caller's write attempt is serializable. Its joined scan participates
    // in predicate conflicts; Postgres cannot row-lock the nullable pointer
    // side. apply_page rechecks selected records and owners with conflict
    // protection.
    let page = attempt
        .query(statement, ReadProtection::Snapshot, CATALOG_READ_BYTES)
        .await?;
    let more = page.more || page.rows.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS;
    let mut pointers = Vec::new();
    let records = page
        .rows
        .into_iter()
        .take(MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS)
        .map(|row| {
            let [shard, lineage, order, offset, effect, pointer, created_at, descriptor, pointer_blob]: [SqlValue; 9] =
                row.into_values().try_into().map_err(|_| invalid_record())?;
            let (
                SqlValue::Text(shard),
                SqlValue::Text(lineage),
                SqlValue::Blob(order),
                SqlValue::Text(offset),
                SqlValue::Text(effect),
                SqlValue::Blob(pointer),
                SqlValue::Integer(created_at),
                SqlValue::Blob(descriptor),
                SqlValue::Blob(pointer_blob),
            ) = (shard, lineage, order, offset, effect, pointer, created_at, descriptor, pointer_blob)
            else {
                return Err(invalid_record());
            };
            let order: [u8; 8] = order.try_into().map_err(|_| invalid_record())?;
            let position = SystemStreamPosition::try_new(shard, lineage, offset)
                .map_err(|_| invalid_record())?;
            if position.offset().order().map_err(|_| invalid_record())? != u64::from_be_bytes(order)
            {
                return Err(invalid_record());
            }
            let descriptor: stream_provider::retained_image::system_stream::RetainedSystemStreamRecord = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::decode(&descriptor, crate::constants::STREAM_READ_BYTES.get())?;
            if descriptor.position != position { return Err(invalid_record()); }
            let length = descriptor.read_work_bytes(CATALOG_READ_BYTES.get())?;
            let pointer_id = StreamItemId::try_from(pointer.as_slice()).map_err(|_| invalid_record())?;
            pointers.push(stream_provider::retained_image::stream_row::StoredStreamRow::decode(pointer_id, &pointer_blob, crate::constants::STREAM_READ_BYTES.get())?);
            Ok((length, SystemStreamTrimRecord {
                position,
                pointer_item_id: pointer_id,
                effect_id: SystemStreamEffectDigest::try_new(effect)
                    .map_err(|_| invalid_record())?,
                created_at: TimestampMillis::from_timestamp(created_at),
            }))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let (selected, excluded) = stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::select_trim_prefix(records, pointers, CATALOG_READ_BYTES.get())?;
    Ok((selected, more || excluded))
}

fn invalid_record() -> StorageError {
    StorageError::internal("SQL SystemStream trim record is invalid")
}

async fn apply_page<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    selected: &[SystemStreamTrimRecord],
    receipt: &SystemStreamTrimPageReceipt,
) -> StorageResult<()> {
    if selected.is_empty() {
        return write_receipt(attempt, engine, receipt).await;
    }
    let system_stream = StreamName::system_table_stream();
    let mut statements = Vec::with_capacity(selected.len() * 4);
    for candidate in selected {
        statements.push(SqlStatement::new(
            format!(
                "SELECT receipt_blob, created_at FROM {relation_sys_dynamo_stream_effects} WHERE \
                 effect_id = {}",
                engine.engine().placeholder(1),
                relation_sys_dynamo_stream_effects =
                    engine.relation_name("sys_dynamo_stream_effects"),
            ),
            vec![SqlValue::Text(candidate.effect_id.as_str().to_owned())],
        ));
        statements.push(SqlStatement::new(
            format!(
                "SELECT record_blob, pointer_item_id, effect_id, created_at FROM \
                 {relation_sys_dynamo_stream_records} WHERE shard_id = {} AND lineage_id = {} AND \
                 offset_order = {} AND offset_text = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                relation_sys_dynamo_stream_records =
                    engine.relation_name("sys_dynamo_stream_records"),
            ),
            position_binds(&candidate.position)?,
        ));
        statements.push(SqlStatement::new(
            format!(
                "SELECT row_blob, created_at FROM {relation_sys_dynamo_stream_items} WHERE \
                 stream_name = {} AND item_id = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
            ),
            vec![
                SqlValue::Blob(system_stream.as_ref().to_vec()),
                SqlValue::Blob(candidate.pointer_item_id.as_bytes().to_vec()),
            ],
        ));
        statements.push(SqlStatement::new(
            format!(
                "SELECT item_stream, item_item_id, owner_kind FROM \
                 {relation_sys_dynamo_stream_pointer_owners} WHERE owner_stream = {} AND \
                 owner_item_id = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                relation_sys_dynamo_stream_pointer_owners =
                    engine.relation_name("sys_dynamo_stream_pointer_owners"),
            ),
            vec![
                SqlValue::Blob(system_stream.as_ref().to_vec()),
                SqlValue::Blob(candidate.pointer_item_id.as_bytes().to_vec()),
            ],
        ));
    }
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != selected.len() * 4 {
        return Err(invalid_record());
    }
    let mut pages = pages.into_iter();
    let mut descriptors = Vec::with_capacity(selected.len());
    let mut pointers = Vec::with_capacity(selected.len());
    let mut observations = Vec::with_capacity(selected.len());
    for candidate in selected {
        let [effect_blob, effect_created]: [SqlValue; 2] =
            single_row(pages.next().ok_or_else(invalid_record)?)?
                .into_values()
                .try_into()
                .map_err(|_| invalid_record())?;
        let [record_blob, record_pointer, record_effect, record_created]: [SqlValue; 4] =
            single_row(pages.next().ok_or_else(invalid_record)?)?
                .into_values()
                .try_into()
                .map_err(|_| invalid_record())?;
        let [pointer_blob, pointer_created]: [SqlValue; 2] =
            single_row(pages.next().ok_or_else(invalid_record)?)?
                .into_values()
                .try_into()
                .map_err(|_| invalid_record())?;
        let [owner_stream, owner_item_id, owner_kind]: [SqlValue; 3] =
            single_row(pages.next().ok_or_else(invalid_record)?)?
                .into_values()
                .try_into()
                .map_err(|_| invalid_record())?;
        let (
            SqlValue::Blob(effect_blob),
            SqlValue::Integer(effect_created),
            SqlValue::Blob(record_blob),
            SqlValue::Blob(record_pointer),
            SqlValue::Text(record_effect),
            SqlValue::Integer(record_created),
            SqlValue::Blob(pointer_blob),
            SqlValue::Integer(pointer_created),
            SqlValue::Blob(owner_stream),
            SqlValue::Blob(owner_item_id),
            SqlValue::Integer(owner_kind),
        ) = (
            effect_blob,
            effect_created,
            record_blob,
            record_pointer,
            record_effect,
            record_created,
            pointer_blob,
            pointer_created,
            owner_stream,
            owner_item_id,
            owner_kind,
        )
        else {
            return Err(invalid_record());
        };
        if effect_created != candidate.created_at.timestamp_millis()
            || record_created != effect_created
            || pointer_created != effect_created
            || record_pointer != candidate.pointer_item_id.as_bytes()
            || record_effect != candidate.effect_id.as_str()
            || owner_kind != PointerOwner::System as i64
        {
            return Err(invalid_record());
        }
        let effect: DynamoStreamEffectRow = storage_serde::from_bytes(&effect_blob)?;
        let descriptor: stream_provider::retained_image::system_stream::RetainedSystemStreamRecord =
            stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::decode(
                &record_blob,
                crate::constants::STREAM_READ_BYTES.get(),
            )?;
        if descriptor.position != candidate.position || descriptor.history() != effect.history_id {
            return Err(invalid_record());
        }
        let pointer = stream_provider::retained_image::stream_row::StoredStreamRow::decode(
            candidate.pointer_item_id,
            &pointer_blob,
            crate::constants::STREAM_READ_BYTES.get(),
        )?;
        pointer.require_address(&system_stream, candidate.pointer_item_id)?;
        pointer.require_pointer_effect(&candidate.effect_id)?;
        let reference = StreamPointerReference {
            owner: PointerOwner::try_from(u8::try_from(owner_kind).map_err(|_| invalid_record())?)?,
            owner_stream: system_stream.clone(),
            owner_item_id: candidate.pointer_item_id,
            item_stream: StreamName::new(&owner_stream),
            item_item_id: Some(
                StreamItemId::try_from(owner_item_id.as_slice()).map_err(|_| invalid_record())?,
            )
            .filter(|id| *id != StreamItemId::default()),
        };
        descriptors.push(descriptor);
        pointers.push(pointer);
        observations.push((effect, reference));
    }
    let mut catalog =
        SqlCatalogAttempt::new(attempt, engine, storage_types::ReadProtection::Conflict);
    stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::admit_trim_work(
        &descriptors,
        &pointers,
        crate::constants::STREAM_READ_BYTES.get(),
    )?;
    let mut retiring =
        stream_provider::retained_image::stream_row::StoredStreamRow::retirement_receipts(
            &pointers,
        )?;
    retiring.extend(stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::retirement_receipts(&descriptors)?);
    let pointers = stream_provider::retained_image::stream_row::StoredStreamRow::hydrate(
        &mut catalog,
        pointers,
        crate::constants::STREAM_READ_BYTES.get(),
    )
    .await?;
    let retained =
        stream_provider::retained_image::system_stream::RetainedSystemStreamRecord::hydrate(
            &mut catalog,
            descriptors,
            crate::constants::STREAM_READ_BYTES.get(),
        )
        .await?;
    if retained.len() != selected.len() || pointers.len() != selected.len() {
        return Err(invalid_record());
    }
    stream_provider::retained_image::RetainedOwnerRecord::release_stream_batch(
        &mut catalog,
        retiring,
    )
    .await?;
    let trimmed_rows = selected
        .iter()
        .zip(observations)
        .zip(retained)
        .zip(pointers)
        .map(|(((candidate, (effect, reference)), record), pointer)| {
            effect.accept_trim(candidate, &record, &pointer, &reference)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    if engine.engine() == SqlEngine::Postgres {
        crate::stream_allocation::store_retired_offsets(attempt, engine, selected).await?;
    }
    let mut statements = Vec::with_capacity(selected.len() * 4 + 1);
    for (candidate, trimmed) in selected.iter().zip(trimmed_rows) {
        statements.push(SqlStatement::new(
            format!(
                "DELETE FROM {relation_sys_dynamo_stream_pointer_owners} WHERE owner_stream = {} \
                 AND owner_item_id = {} AND owner_kind = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                relation_sys_dynamo_stream_pointer_owners =
                    engine.relation_name("sys_dynamo_stream_pointer_owners"),
            ),
            vec![
                SqlValue::Blob(system_stream.as_ref().to_vec()),
                SqlValue::Blob(candidate.pointer_item_id.as_bytes().to_vec()),
                SqlValue::Integer(PointerOwner::System as i64),
            ],
        ));
        statements.push(SqlStatement::new(
            format!(
                "DELETE FROM {relation_sys_dynamo_stream_items} WHERE stream_name = {} AND \
                 item_id = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
            ),
            vec![
                SqlValue::Blob(system_stream.as_ref().to_vec()),
                SqlValue::Blob(candidate.pointer_item_id.as_bytes().to_vec()),
            ],
        ));
        statements.push(SqlStatement::new(
            format!(
                "DELETE FROM {relation_sys_dynamo_stream_records} WHERE shard_id = {} AND \
                 lineage_id = {} AND offset_order = {} AND offset_text = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                relation_sys_dynamo_stream_records =
                    engine.relation_name("sys_dynamo_stream_records"),
            ),
            position_binds(&candidate.position)?,
        ));
        statements.push(SqlStatement::new(
            format!(
                "UPDATE {relation_sys_dynamo_stream_effects} SET receipt_blob = {} WHERE \
                 effect_id = {} AND created_at = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                relation_sys_dynamo_stream_effects =
                    engine.relation_name("sys_dynamo_stream_effects"),
            ),
            vec![
                SqlValue::Blob(storage_serde::to_bytes(&trimmed)?),
                SqlValue::Text(candidate.effect_id.as_str().to_owned()),
                SqlValue::Integer(candidate.created_at.timestamp_millis()),
            ],
        ));
    }
    statements.push(receipt_insert_statement(engine, receipt)?);
    let counts = attempt.execute_batch(statements).await?;
    if counts.len() != selected.len() * 4 + 1 || counts.into_iter().any(|count| count != 1) {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream trim changed while deleting its page".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn position_binds(position: &SystemStreamPosition) -> StorageResult<Vec<SqlValue>> {
    Ok(vec![
        SqlValue::Text(position.shard_id().as_str().to_owned()),
        SqlValue::Text(position.lineage_id().as_str().to_owned()),
        SqlValue::Blob(
            position
                .offset()
                .order()
                .map_err(|_| invalid_record())?
                .to_be_bytes()
                .to_vec(),
        ),
        SqlValue::Text(position.offset().as_str().to_owned()),
    ])
}

fn single_row(page: crate::SqlQueryPage) -> StorageResult<crate::SqlRow> {
    if page.more || page.rows.len() != 1 {
        return Err(invalid_record());
    }
    page.rows.into_iter().next().ok_or_else(invalid_record)
}
