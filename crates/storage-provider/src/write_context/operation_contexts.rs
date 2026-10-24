use storage_types::{
    BatchWriteItemRequest, StorageError, StorageResult, TableName, TransactEncodeItem,
    TransactWriteItem, transact_encode_item_table_name, transact_item_table_name,
};

use super::{TtlWriteContext, validate_physical_table};

/// An operation and its required physical sidecar, retaining the source
/// ordinal used to align pre-read results with writes.
#[derive(Clone, PartialEq, Eq)]
pub struct PhysicalWrite<R, C> {
    ordinal: usize,
    physical_table: TableName,
    request: R,
    context: C,
}

impl<R, C> PhysicalWrite<R, C> {
    fn from_parts(ordinal: usize, physical_table: TableName, request: R, context: C) -> Self {
        Self {
            ordinal,
            physical_table,
            request,
            context,
        }
    }
}

impl<R, C> PhysicalWrite<R, C> {
    #[must_use]
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    #[must_use]
    pub fn physical_table(&self) -> &TableName {
        &self.physical_table
    }

    #[must_use]
    pub fn request(&self) -> &R {
        &self.request
    }

    #[must_use]
    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn try_map_request<R2, F>(self, map: F) -> StorageResult<PhysicalWrite<R2, C>>
    where F: FnOnce(R) -> StorageResult<R2> {
        let Self {
            ordinal,
            physical_table,
            request,
            context,
        } = self;
        Ok(PhysicalWrite::from_parts(
            ordinal,
            physical_table,
            map(request)?,
            context,
        ))
    }

    /// Consume the carrier without cloning its request or context. Shared
    /// preparation uses this when it already owns the member vector for a
    /// native attempt.
    pub fn into_parts(self) -> (usize, TableName, R, C) {
        let Self {
            ordinal,
            physical_table,
            request,
            context,
        } = self;
        (ordinal, physical_table, request, context)
    }
}

impl<R> PhysicalWrite<R, TtlWriteContext> {
    pub fn try_new(
        ordinal: usize,
        physical_table: TableName,
        request: R,
        context: TtlWriteContext,
    ) -> StorageResult<Self> {
        validate_physical_table(&physical_table)?;
        if context.physical_table() != &physical_table {
            return Err(StorageError::validation(
                "ttl write context physical table does not match operation table",
            ));
        }
        Ok(Self::from_parts(ordinal, physical_table, request, context))
    }
}

/// Exact operation-aligned contexts for a batch write.
#[derive(Clone, PartialEq, Eq)]
pub struct BatchWriteContexts {
    contexts: Vec<TtlWriteContext>,
}

impl BatchWriteContexts {
    pub fn try_new(contexts: Vec<TtlWriteContext>) -> StorageResult<Self> {
        for context in &contexts {
            validate_physical_table(context.physical_table())?;
        }
        Ok(Self { contexts })
    }

    pub fn validate_exact_alignment<R>(
        &self,
        writes: &[PhysicalWrite<R, TtlWriteContext>],
    ) -> StorageResult<()> {
        if self.contexts.len() != writes.len() {
            return Err(StorageError::validation(
                "ttl batch contexts must exactly align with writes",
            ));
        }
        for (ordinal, (expected, write)) in self.contexts.iter().zip(writes).enumerate() {
            if write.ordinal() != ordinal || expected != write.context() {
                return Err(StorageError::validation(
                    "ttl batch contexts have an invalid operation ordinal or value",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn contexts(&self) -> &[TtlWriteContext] {
        &self.contexts
    }

    /// Validate batch request/context alignment: same count, every write
    /// table-bound, ordinals contiguous, physical tables paired. Shared by
    /// KV and SQL batch paths so alignment rejects identically. Stays `pub`:
    /// KV and SQL backends are separate crates and both call it.
    pub fn validate_batch_request(&self, request: &BatchWriteItemRequest) -> StorageResult<()> {
        let writes = request
            .request_items
            .iter()
            .flat_map(|(table_name, write_requests)| {
                write_requests.iter().map(move |_| table_name.clone())
            })
            .enumerate()
            .map(|(ordinal, table_name)| {
                let context = self.contexts().get(ordinal).ok_or_else(|| {
                    StorageError::validation("ttl batch contexts must exactly align with writes")
                })?;
                PhysicalWrite::try_new(ordinal, table_name, (), context.clone())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        self.validate_exact_alignment(&writes)
    }
}

/// One transaction operation's immutable context and source ordinal.
#[derive(Clone, PartialEq, Eq)]
pub struct TransactOperationContext {
    ordinal: usize,
    physical_table: TableName,
    context: TtlWriteContext,
}

impl TransactOperationContext {
    pub fn try_new(
        ordinal: usize,
        physical_table: TableName,
        context: TtlWriteContext,
    ) -> StorageResult<Self> {
        validate_physical_table(&physical_table)?;
        if context.physical_table() != &physical_table {
            return Err(StorageError::validation(
                "ttl transaction context physical table does not match operation table",
            ));
        }
        Ok(Self {
            ordinal,
            physical_table,
            context,
        })
    }

    #[must_use]
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    #[must_use]
    pub fn physical_table(&self) -> &TableName {
        &self.physical_table
    }

    #[must_use]
    pub fn context(&self) -> &TtlWriteContext {
        &self.context
    }
}

/// Ordered contexts for transaction, resolved-apply, and import operations.
/// Construction and pre-read validation reject any missing, extra, or
/// reordered sidecar before a provider performs a read or condition check.
#[derive(Clone, PartialEq, Eq)]
pub struct OrderedOperationContexts {
    operations: Vec<TransactOperationContext>,
}

impl OrderedOperationContexts {
    pub fn try_new(operations: Vec<TransactOperationContext>) -> StorageResult<Self> {
        for (ordinal, operation) in operations.iter().enumerate() {
            if operation.ordinal() != ordinal {
                return Err(StorageError::validation(
                    "ttl operation contexts must use contiguous source ordinals",
                ));
            }
        }
        Ok(Self { operations })
    }

    pub fn validate_exact_alignment<R>(
        &self,
        writes: &[PhysicalWrite<R, TtlWriteContext>],
    ) -> StorageResult<()> {
        if self.operations.len() != writes.len() {
            return Err(StorageError::validation(
                "ttl operation contexts must exactly align with writes",
            ));
        }
        for (ordinal, (expected, write)) in self.operations.iter().zip(writes).enumerate() {
            if expected.ordinal() != ordinal
                || write.ordinal() != ordinal
                || expected.physical_table() != write.physical_table()
                || expected.context() != write.context()
            {
                return Err(StorageError::validation(
                    "ttl operation contexts have an invalid operation alignment",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn operations(&self) -> &[TransactOperationContext] {
        &self.operations
    }

    /// Validate transact request/context alignment: same count, every item
    /// table-bound, ordinals contiguous, physical tables paired. Shared by
    /// KV and SQL transact paths so alignment rejects identically.
    pub fn validate_transact_items(&self, items: &[TransactWriteItem]) -> StorageResult<()> {
        let writes = items
            .iter()
            .enumerate()
            .map(|(ordinal, item)| {
                let table_name = transact_item_table_name(item).cloned().ok_or_else(|| {
                    StorageError::validation("Invalid Transact Write Item request")
                })?;
                let context = self.operations().get(ordinal).ok_or_else(|| {
                    StorageError::validation(
                        "ttl operation contexts must exactly align with writes",
                    )
                })?;
                PhysicalWrite::try_new(ordinal, table_name, (), context.context().clone())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        self.validate_exact_alignment(&writes)
    }

    /// Wire-encoded variant of
    /// [`OrderedOperationContexts::validate_transact_items`]. Shared by KV
    /// and SQL encode transact paths.
    pub fn validate_transact_encode_items(
        &self,
        items: &[TransactEncodeItem],
    ) -> StorageResult<()> {
        let writes = items
            .iter()
            .enumerate()
            .map(|(ordinal, item)| {
                let table_name =
                    transact_encode_item_table_name(item)
                        .cloned()
                        .ok_or_else(|| {
                            StorageError::validation("Invalid Transact Write Item request")
                        })?;
                let context = self.operations().get(ordinal).ok_or_else(|| {
                    StorageError::validation(
                        "ttl operation contexts must exactly align with writes",
                    )
                })?;
                PhysicalWrite::try_new(ordinal, table_name, (), context.context().clone())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        self.validate_exact_alignment(&writes)
    }
}

/// Validate a transact item count bound once for every backend. KV encode,
/// SQLite, and Postgres paths shared three identical copies; the wording
/// stays byte-identical here so no backend changes its observable message.
pub fn validate_transact_item_count(count: usize) -> StorageResult<()> {
    if count == 0 {
        return Err(StorageError::validation(
            "Transaction request must contain at least one item",
        ));
    }
    if count > 100 {
        return Err(StorageError::validation(
            "Transaction request cannot contain more than 100 items",
        ));
    }
    Ok(())
}
