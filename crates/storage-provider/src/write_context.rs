use storage_backfill::GsiProjectionLifecycle;
use storage_common::TtlConfigRecord;
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    TableName, TimestampMillis,
};

mod operation_contexts;

pub use operation_contexts::{
    BatchWriteContexts, OrderedOperationContexts, PhysicalWrite, TransactOperationContext,
    validate_transact_item_count,
};

fn validate_physical_table(table_name: &TableName) -> StorageResult<()> {
    if table_name.as_ref().trim().is_empty() {
        return Err(StorageError::validation(
            "ttl physical table name must not be empty",
        ));
    }
    Ok(())
}

/// Required internal sidecar for a physical TTL write. It is not part of a
/// public DynamoDB request and has no context-free fallback for physical data.
///
/// Construction is intentionally limited to the constructors below. The
/// representation is private so a caller cannot pair a physical table with a
/// different logical route or forge the stored write authority by matching on
/// an enum variant.
#[derive(Clone, PartialEq, Eq)]
pub struct TtlWriteContext {
    physical_table: TableName,
    state: TtlWriteContextState,
    table_metadata_prevalidated: bool,
}

#[derive(Clone, PartialEq, Eq)]
enum TtlWriteContextState {
    Direct,
    System,
}

impl TtlWriteContext {
    pub fn direct(physical_table: TableName) -> StorageResult<Self> {
        validate_physical_table(&physical_table)?;
        Ok(Self {
            physical_table,
            state: TtlWriteContextState::Direct,
            table_metadata_prevalidated: false,
        })
    }

    /// Construct the explicit context reserved for system-table writes.
    /// System authority is never inferred from a physical table name, so a
    /// caller must opt into this constructor even for a table whose name is a
    /// system identity.
    pub fn system(physical_table: TableName) -> StorageResult<Self> {
        validate_physical_table(&physical_table)?;
        Ok(Self {
            physical_table,
            state: TtlWriteContextState::System,
            table_metadata_prevalidated: false,
        })
    }

    /// Mark metadata resolution performed by the database manager as current
    /// for this write. Providers may trust their matching metadata cache entry
    /// and avoid repeating the durable table-name lookup.
    #[must_use]
    pub fn with_table_metadata_prevalidated(mut self) -> Self {
        self.table_metadata_prevalidated = true;
        self
    }

    #[must_use]
    pub fn table_metadata_prevalidated(&self) -> bool {
        self.table_metadata_prevalidated
    }

    #[must_use]
    pub fn is_direct(&self) -> bool {
        matches!(
            &self.state,
            TtlWriteContextState::Direct | TtlWriteContextState::System
        )
    }

    #[must_use]
    pub fn is_system(&self) -> bool {
        matches!(&self.state, TtlWriteContextState::System)
    }

    #[must_use]
    pub fn physical_table(&self) -> &TableName {
        &self.physical_table
    }

    #[must_use]
    pub fn logical_table_name(&self) -> &TableName {
        &self.physical_table
    }
}

/// Backend-agnostic stream inputs for planning one table
/// write. None of these fields touch key-value specifics; every backend
/// provider plans the same stream identity and GSI consistency inputs before
/// lowering to its own mutation or SQL statement shape.
///
/// Owned and `Send + 'static` by construction (clones/moves at `prepare_*`
/// time) so blocking backends (SQLite `with_transaction`) capture the batch
/// directly without mirror structs. Shared snapshots ride `Arc` (cache
/// creates the `Arc`, batch clones the pointer); per-write scalars move.
#[derive(Clone)]
pub struct TableStreamContext {
    pub item_revision: Option<storage_types::ItemRevision>,
    pub stream_item_id: Option<StreamItemId>,
    pub target_item_stream_version: Option<ItemStreamVersion>,
    pub created_at: Option<TimestampMillis>,
    pub immediate_gsi_consistency: bool,
    pub gsi_lifecycle: Option<std::sync::Arc<GsiProjectionLifecycle>>,
}

/// Backend-agnostic inputs for planning one table update on top of the
/// shared stream context. Like [`TableStreamContext`], every field is
/// provider-neutral; backends differ only in how they lower the plan.
#[derive(Clone)]
pub struct TableUpdateContext {
    pub stream: TableStreamContext,
    pub index: usize,
    pub ttl_config: Option<TtlConfigRecord>,
    pub ttl_context: TtlWriteContext,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
}
