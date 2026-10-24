//! Render the actual physical grammar before qualifying native statement
//! limits.

use std::sync::Arc;

use infrastructure_provider::{
    limits::InfrastructureLimits,
    page::{BoundedRecordRange, RecordPageRequest},
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange, RecordExpectation},
};
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ReadProtection, Revision, StorageResult,
};

use super::sql_statements;
use crate::SqlEngine;

pub(crate) struct RecordStatementShape {
    pub sql_bytes: usize,
    pub parameters: usize,
    pub initialization_bind_bytes: usize,
}

impl RecordStatementShape {
    pub(crate) fn try_new(engine: SqlEngine, limits: InfrastructureLimits) -> StorageResult<Self> {
        let key: DurableRecordKey = RecordAddress {
            namespace: "x".into(),
            resource: vec![1],
            generation: u64::MAX,
            suffix: vec![0],
        }
        .try_into()?;
        let last = RecordAddress {
            namespace: "x".into(),
            resource: vec![1],
            generation: u64::MAX,
            suffix: vec![1],
        }
        .try_into()?;
        let record = DurableRecord {
            key: key.clone(),
            revision: Revision::INITIAL,
            bytes: Arc::from([]),
        };
        let request = RecordPageRequest {
            range: BoundedRecordRange::try_new(key.clone(), last, limits.max_records())?,
            after: Some(key.clone()),
            max_bytes: limits.max_bytes(),
            protection: ReadProtection::Conflict,
        };
        let count = limits
            .max_records()
            .get()
            .min(crate::constants::INFRASTRUCTURE_POINT_LOAD_KEYS);
        let keys = vec![key.clone(); count];
        let initialization = sql_statements::initialize_version(engine, &[0; 16]);
        let initialization_bind_bytes = initialization.input_shape()?.value_bytes;
        let statements = [
            sql_statements::render_format_schema(engine),
            sql_statements::render_record_schema(engine),
            initialization,
            sql_statements::load_version(engine),
            sql_statements::existing_record(),
            sql_statements::existing_namespace_record(engine, "x")?,
            sql_statements::load_batch(engine, &keys, limits)?,
            sql_statements::load_revision(engine, &key)?,
            sql_statements::scan(engine, &request, limits)?,
            sql_statements::savepoint(),
            sql_statements::rollback(),
            sql_statements::release(),
            sql_statements::change(
                engine,
                &RecordChange::Store {
                    record: record.clone(),
                    expected: RecordExpectation::Absent,
                },
            )?,
            sql_statements::change(
                engine,
                &RecordChange::Store {
                    record,
                    expected: RecordExpectation::Revision(Revision::INITIAL),
                },
            )?,
            sql_statements::delete(engine, &key, Revision::INITIAL)?,
        ];
        let mut shape = Self {
            sql_bytes: 0,
            parameters: 0,
            initialization_bind_bytes,
        };
        for statement in statements {
            let input = statement.input_shape()?;
            shape.sql_bytes = shape.sql_bytes.max(input.sql_bytes);
            shape.parameters = shape.parameters.max(input.parameters);
        }
        Ok(shape)
    }

    /// Serialized aggregate row/statement data only; never a transaction
    /// guarantee.
    pub(crate) fn bound_batch_data(
        &self,
        limits: InfrastructureLimits,
        framing: (usize, usize),
    ) -> StorageResult<std::num::NonZeroUsize> {
        let invalid = || InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::NativeCapacity,
        };
        let records = limits.max_records().get();
        let key = limits.max_key_bytes().get();
        let commands = records
            .checked_add(records.div_ceil(crate::constants::INFRASTRUCTURE_POINT_LOAD_KEYS))
            .and_then(|count| {
                count.checked_add(crate::constants::INFRASTRUCTURE_BATCH_CONTROL_STATEMENTS)
            })
            .and_then(|count| count.checked_add(crate::constants::INFRASTRUCTURE_LAYOUT_STATEMENTS))
            .ok_or_else(invalid)?;
        // Point results, range results and mutations each admit one aggregate
        // payload. The maximum range lookahead remains an independent row.
        let payloads = limits
            .max_bytes()
            .get()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(key))
            .and_then(|bytes| bytes.checked_add(limits.max_record_bytes().get()))
            .and_then(|bytes| {
                bytes.checked_add(crate::constants::INFRASTRUCTURE_FORMAT_PAYLOAD_BYTES)
            })
            .and_then(|bytes| bytes.checked_add(self.initialization_bind_bytes))
            .ok_or_else(invalid)?;
        // Two result batches, mutations, lookahead and conflict-revision row;
        // initialization also has format insert/read and occupancy
        // observations.
        let rows = records
            .checked_mul(3)
            .and_then(|count| count.checked_add(5))
            .ok_or_else(invalid)?;
        let row_frames = framing.0.checked_mul(rows).ok_or_else(invalid)?;
        // Expected mutation revisions and the conflict revision response.
        let revisions = records
            .checked_add(1)
            .and_then(|count| count.checked_mul(std::mem::size_of::<u64>()))
            .ok_or_else(invalid)?;
        let point_keys = records
            .checked_mul(key)
            .ok_or_else(invalid)?
            .min(limits.max_bytes().get());
        let reads = key
            .checked_mul(crate::constants::INFRASTRUCTURE_BATCH_READ_KEYS)
            .and_then(|bytes| bytes.checked_add(point_keys))
            .ok_or_else(invalid)?;
        let statements = self
            .sql_bytes
            .checked_add(framing.1)
            .and_then(|bytes| bytes.checked_mul(commands))
            .ok_or_else(invalid)?;
        let bytes = payloads
            .checked_add(row_frames)
            .and_then(|bytes| bytes.checked_add(revisions))
            .and_then(|bytes| bytes.checked_add(reads))
            .and_then(|bytes| bytes.checked_add(statements))
            .ok_or_else(invalid)?;
        std::num::NonZeroUsize::new(bytes).ok_or_else(|| invalid().into())
    }

    #[cfg(feature = "sqlite")]
    pub(crate) fn require_limits(
        &self,
        sql_bytes: usize,
        parameters: usize,
        columns: usize,
    ) -> StorageResult<()> {
        let required_columns = crate::constants::INFRASTRUCTURE_RECORD_COLUMNS
            .len()
            .max(crate::constants::INFRASTRUCTURE_FORMAT_COLUMNS);
        if self.sql_bytes > sql_bytes || self.parameters > parameters || required_columns > columns
        {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::NativeCapacity,
            }
            .into());
        }
        Ok(())
    }
}
