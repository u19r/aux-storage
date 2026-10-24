//! Connection-derived limits apply to rows and statements, not entire
//! transactions.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    physical_store::{NativeTransactionByteLimit, PhysicalRecordCapacity},
};
use rusqlite::limits::Limit;
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

pub(super) struct SqliteRecordLimits {
    row_bytes: usize,
    sql_bytes: usize,
    parameters: usize,
    columns: usize,
}

impl SqliteRecordLimits {
    pub(super) async fn load(connections: &[tokio_rusqlite::Connection]) -> StorageResult<Self> {
        let mut qualified: Option<Self> = None;
        for connection in connections {
            let limits = connection
                .call(|connection| Ok(Self::load_connection(connection)?))
                .await
                .map_err(super::error::map_worker_error)?;
            qualified = Some(match qualified {
                None => limits,
                Some(previous) => Self {
                    row_bytes: previous.row_bytes.min(limits.row_bytes),
                    sql_bytes: previous.sql_bytes.min(limits.sql_bytes),
                    parameters: previous.parameters.min(limits.parameters),
                    columns: previous.columns.min(limits.columns),
                },
            });
        }
        qualified.ok_or_else(|| {
            InfrastructureResultKind::Unavailable {
                context: InfrastructureContext::NativeCapacity,
            }
            .into()
        })
    }

    pub(super) fn load_connection(connection: &rusqlite::Connection) -> rusqlite::Result<Self> {
        Ok(Self {
            row_bytes: load_limit(connection, Limit::SQLITE_LIMIT_LENGTH)?,
            sql_bytes: load_limit(connection, Limit::SQLITE_LIMIT_SQL_LENGTH)?,
            parameters: load_limit(connection, Limit::SQLITE_LIMIT_VARIABLE_NUMBER)?,
            columns: load_limit(connection, Limit::SQLITE_LIMIT_COLUMN)?,
        })
    }

    pub(super) fn require_statement(&self, statement: &crate::SqlStatement) -> StorageResult<()> {
        statement
            .input_shape()?
            .require_limits(self.sql_bytes, self.parameters, self.row_bytes)
    }

    pub(super) fn record_capacity(
        &self,
        limits: InfrastructureLimits,
    ) -> StorageResult<PhysicalRecordCapacity> {
        // Six row fields: one header-size varint plus six serial-type varints,
        // each at most nine bytes. Logical key bytes include
        // generation/revision.
        let row = limits
            .max_key_bytes()
            .get()
            .checked_add(limits.max_record_bytes().get())
            .and_then(|bytes| bytes.checked_add(crate::constants::SQLITE_RECORD_HEADER_BOUND));
        let format = crate::constants::INFRASTRUCTURE_FORMAT_PAYLOAD_BYTES
            + crate::constants::SQLITE_FORMAT_HEADER_BOUND;
        if row.is_none_or(|bytes| bytes.max(format) > self.row_bytes) {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::NativeCapacity,
            }
            .into());
        }
        let shape =
            crate::infrastructure::RecordStatementShape::try_new(crate::SqlEngine::Sqlite, limits)?;
        shape.require_limits(self.sql_bytes, self.parameters, self.columns)?;
        let data =
            shape.bound_batch_data(limits, (crate::constants::SQLITE_RECORD_HEADER_BOUND, 0))?;
        Ok(PhysicalRecordCapacity::new(
            NativeTransactionByteLimit::Unknown,
            Some(data),
        ))
    }
}

fn load_limit(connection: &rusqlite::Connection, limit: Limit) -> rusqlite::Result<usize> {
    usize::try_from(connection.limit(limit)?).map_err(|_| rusqlite::Error::InvalidQuery)
}
