//! Pure TransactGetItems validation and normalization (phase 5, §3.2).
//!
//! `TryFrom` runs the existing wire-level request validation, canonicalizes
//! table names with the current `dynamodb_resource_name` rule, retains the
//! original spelling for consumed capacity, and preserves request member
//! order. Tables are stored once; members map to them by index instead of
//! cloning a table schema per key. No database access.

use storage_types::{
    DynamoRequestValidate, ReturnConsumedCapacity, StorageError, StorageResult, StoredTableInfo,
    TableName, TransactGetItem, TransactGetItemsRequest,
};

use super::prepared::PreparedTransactGet;

/// Pure TransactGetItems request plan: canonical table names, original
/// response names, and stable table/member ordinals.
pub struct TransactGetPlan {
    items: Vec<TransactGetItem>,
    tables: Vec<TableName>,
    table_indexes: Vec<usize>,
    response_tables: Vec<TableName>,
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
}

impl TryFrom<TransactGetItemsRequest> for TransactGetPlan {
    type Error = StorageError;

    fn try_from(request: TransactGetItemsRequest) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        let mut tables = Vec::new();
        let mut table_indexes = Vec::with_capacity(request.transact_items.len());
        let mut response_tables = Vec::with_capacity(request.transact_items.len());
        let mut items = Vec::with_capacity(request.transact_items.len());
        for item in request.transact_items {
            let canonical = TableName::new(item.get.table_name.dynamodb_resource_name());
            let table_index = match tables.iter().position(|table| table == &canonical) {
                Some(index) => index,
                None => {
                    tables.push(canonical.clone());
                    tables.len() - 1
                }
            };
            table_indexes.push(table_index);
            response_tables.push(item.get.table_name.clone());
            items.push(TransactGetItem {
                get: storage_types::TransactGetRequest {
                    table_name: canonical,
                    ..item.get
                },
            });
        }
        Ok(Self {
            items,
            tables,
            table_indexes,
            response_tables,
            return_consumed_capacity: request.return_consumed_capacity,
        })
    }
}

impl TransactGetPlan {
    /// Distinct requested tables in first-occurrence order.
    pub fn table_names(&self) -> &[TableName] {
        &self.tables
    }

    /// Bind one metadata result per [`TransactGetPlan::table_names`] entry,
    /// in that order, then validate member keys in original request order.
    /// Members are walked once in request order: a table resolution failure
    /// surfaces when its first member is walked, a top-level key failure
    /// surfaces at its own member, recorded key reasons collapse after the
    /// walk, and duplicates come last — exactly the manager order this owner
    /// replaces.
    pub fn bind(
        &self,
        tables: Vec<StorageResult<StoredTableInfo>>,
    ) -> StorageResult<PreparedTransactGet<'_>> {
        if tables.len() != self.tables.len() {
            return Err(StorageError::internal(
                "transact-get metadata carrier length must match distinct tables",
            ));
        }
        let mut slots: Vec<Option<StorageResult<StoredTableInfo>>> =
            tables.into_iter().map(Some).collect();
        let mut infos: Vec<Option<StoredTableInfo>> =
            std::iter::repeat_with(|| None).take(slots.len()).collect();
        let mut preflights = Vec::with_capacity(self.items.len());
        for (member, table_index) in self.items.iter().zip(&self.table_indexes) {
            let table_info = self.resolve_member_table(&mut slots, &mut infos, *table_index)?;
            preflights.push(
                storage_types::preflight_transact_get_item_key_with_table_info(member, table_info)?,
            );
        }
        if let Some(error) = storage_types::transaction_canceled_for_preflights(&preflights) {
            return Err(error);
        }
        storage_types::validate_no_duplicate_transact_item_keys(&preflights)?;
        let infos = infos
            .into_iter()
            .map(|info| {
                info.ok_or_else(|| StorageError::internal("transact-get table was not observed"))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(PreparedTransactGet::bound(self, infos))
    }

    /// Resolve one distinct table observation on first-member encounter.
    /// Owns the resolve-once, miss-mapping, and metadata-identity
    /// invariants: the carrier slot is taken exactly once, a miss maps to
    /// the public not-found contract, and a surviving observation must
    /// match the requested canonical name.
    fn resolve_member_table<'slots>(
        &self,
        slots: &mut [Option<StorageResult<StoredTableInfo>>],
        infos: &'slots mut [Option<StoredTableInfo>],
        table_index: usize,
    ) -> StorageResult<&'slots StoredTableInfo> {
        if infos[table_index].is_none() {
            let slot = slots[table_index]
                .take()
                .ok_or_else(|| StorageError::internal("transact-get table resolved twice"))?;
            match slot {
                Err(error) => {
                    return Err(transact_get_table_not_found_as_resource_not_found(error));
                }
                Ok(info) => {
                    if info.table_name != self.tables[table_index] {
                        return Err(StorageError::internal(
                            "transact-get metadata identity must match the requested table",
                        ));
                    }
                    infos[table_index] = Some(info);
                }
            }
        }
        infos[table_index]
            .as_ref()
            .ok_or_else(|| StorageError::internal("transact-get table was not resolved"))
    }

    pub(super) fn member(&self, ordinal: usize) -> &TransactGetItem {
        &self.items[ordinal]
    }

    pub(super) fn member_table(&self, ordinal: usize) -> usize {
        self.table_indexes[ordinal]
    }

    pub(super) fn member_response_table(&self, ordinal: usize) -> &TableName {
        &self.response_tables[ordinal]
    }

    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    pub(super) const fn return_consumed_capacity(&self) -> Option<ReturnConsumedCapacity> {
        self.return_consumed_capacity
    }
}

/// Map a failed table resolution to the public not-found contract. Other
/// resolution errors pass through unchanged.
fn transact_get_table_not_found_as_resource_not_found(error: StorageError) -> StorageError {
    use storage_types::{StorageEnum, context::WrappedError as _};
    if let StorageEnum::TableNotFound { name, .. } = error.to_enum() {
        return StorageError::from(StorageEnum::ResourceNotFound {
            resource_type: "table",
            resource_id: name.clone(),
        });
    }
    error
}
