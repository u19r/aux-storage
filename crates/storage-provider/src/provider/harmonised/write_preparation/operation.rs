//! Shared write preparation owner (phase 4, §4.1).
//!
//! Borrows table snapshots plus typed members and converts them into the
//! existing prepared batch through the existing [`TableWriteRepository`]
//! rules. Synchronous, performs no I/O.

use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use storage_common::TtlConfigRecord;
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};

use super::{
    super::{
        models::{
            SingleUpdateOptions, TableCheckInput, TableDeleteInput, TablePutInput,
            TableUpdateInput, TableWriteBatchInput, ValidationSnapshot,
        },
        table_finish::PreparedTableBatch,
        table_writes::TableWriteRepository,
    },
    member::TableWriteMember,
};

/// Borrowed conversion-and-validation owner for table write members.
pub struct TableWritePreparation<'a, T: Borrow<StoredTableInfo>> {
    tables: &'a HashMap<TableName, T>,
    members: &'a [TableWriteMember],
    immediate_gsi_consistency: bool,
}

impl<'a, T: Borrow<StoredTableInfo>> TableWritePreparation<'a, T> {
    /// Borrow snapshots, members and the immediate-GSI choice.
    pub fn new(
        tables: &'a HashMap<TableName, T>,
        members: &'a [TableWriteMember],
        immediate_gsi_consistency: bool,
    ) -> Self {
        Self {
            tables,
            members,
            immediate_gsi_consistency,
        }
    }

    /// Borrowed table names in member order with duplicates, including
    /// checks. Allocates nothing; native adapters deduplicate reads.
    pub fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.members.iter().map(TableWriteMember::table_name)
    }

    /// Convert members, then call `prepare_table_batch` once and
    /// `validate_prepared_batch` once. A missing required TTL observation
    /// is an internal construction error, never a guessed disabled policy.
    pub fn prepare(
        self,
        repository: &impl TableWriteRepository,
        configs: &HashMap<TableName, Option<TtlConfigRecord>>,
    ) -> StorageResult<PreparedTableBatch> {
        let shared_tables = self.shared_tables();
        let input = self.convert_members(&shared_tables, configs)?;
        let prepared = repository.prepare_table_batch(input)?;
        repository.validate_prepared_batch(&prepared, &ValidationSnapshot {})?;
        Ok(prepared)
    }

    /// Prepare an owned member vector without cloning request carriers.
    ///
    /// Native adapters already own their framed members when they enter this
    /// boundary. Consuming those members keeps one request representation
    /// through preparation; the borrowed [`Self::prepare`] path remains for
    /// callers that need to retain the input for response assembly or retry
    /// setup.
    pub fn prepare_owned(
        repository: &impl TableWriteRepository,
        tables: &HashMap<TableName, T>,
        members: Vec<TableWriteMember>,
        immediate_gsi_consistency: bool,
        configs: &HashMap<TableName, Option<TtlConfigRecord>>,
    ) -> StorageResult<PreparedTableBatch> {
        let shared_tables = tables
            .iter()
            .map(|(name, table)| (name.clone(), Arc::new(table.borrow().clone())))
            .collect::<HashMap<_, _>>();
        let input = Self::convert_owned_members(
            members,
            &shared_tables,
            immediate_gsi_consistency,
            configs,
        )?;
        let prepared = repository.prepare_table_batch(input)?;
        repository.validate_prepared_batch(&prepared, &ValidationSnapshot {})?;
        Ok(prepared)
    }

    /// Convert the borrowed members through the same consuming owner used by
    /// native adapters. The borrowed boundary is retained for callers that
    /// need the original request carriers, while conversion policy has one
    /// implementation.
    fn convert_members(
        &self,
        tables: &HashMap<TableName, Arc<StoredTableInfo>>,
        configs: &HashMap<TableName, Option<TtlConfigRecord>>,
    ) -> StorageResult<TableWriteBatchInput> {
        Self::convert_owned_members(
            self.members.to_vec(),
            tables,
            self.immediate_gsi_consistency,
            configs,
        )
    }

    fn convert_owned_members(
        members: Vec<TableWriteMember>,
        tables: &HashMap<TableName, Arc<StoredTableInfo>>,
        immediate_gsi_consistency: bool,
        configs: &HashMap<TableName, Option<TtlConfigRecord>>,
    ) -> StorageResult<TableWriteBatchInput> {
        let mut puts = Vec::new();
        let mut updates = Vec::new();
        let mut deletes = Vec::new();
        let mut checks = Vec::new();
        for member in members {
            match member {
                TableWriteMember::Put(write) => {
                    let table = tables.get(write.physical_table()).ok_or_else(|| {
                        StorageError::internal("write stage misses its table snapshot")
                    })?;
                    let config = configs
                        .get(write.physical_table())
                        .ok_or_else(|| {
                            StorageError::internal("write preparation misses its TTL snapshot")
                        })?
                        .clone();
                    puts.push(TablePutInput::for_single_put(
                        write,
                        Arc::clone(table),
                        immediate_gsi_consistency,
                        config,
                    ));
                }
                TableWriteMember::Update(write) => {
                    let table = tables.get(write.physical_table()).ok_or_else(|| {
                        StorageError::internal("write stage misses its table snapshot")
                    })?;
                    let config = configs
                        .get(write.physical_table())
                        .ok_or_else(|| {
                            StorageError::internal("write preparation misses its TTL snapshot")
                        })?
                        .clone();
                    updates.push(TableUpdateInput::for_single_update(
                        write,
                        Arc::clone(table),
                        SingleUpdateOptions {
                            immediate_gsi_consistency,
                            ttl_config: config,
                        },
                    ));
                }
                TableWriteMember::Delete(write) => {
                    let table = tables.get(write.physical_table()).ok_or_else(|| {
                        StorageError::internal("write stage misses its table snapshot")
                    })?;
                    let config = configs
                        .get(write.physical_table())
                        .ok_or_else(|| {
                            StorageError::internal("write preparation misses its TTL snapshot")
                        })?
                        .clone();
                    deletes.push(TableDeleteInput::for_single_delete(
                        write,
                        Arc::clone(table),
                        immediate_gsi_consistency,
                        config,
                    ));
                }
                TableWriteMember::Check(check) => {
                    let (ordinal, request) = check.into_parts();
                    let table = tables.get(&request.table_name).ok_or_else(|| {
                        StorageError::internal("write stage misses its table snapshot")
                    })?;
                    let storage_types::TransactConditionCheckRequest {
                        table_name,
                        key,
                        condition_expression,
                        expression_attribute_names,
                        expression_attribute_values,
                        return_values_on_condition_check_failure,
                    } = request;
                    checks.push(TableCheckInput {
                        ordinal,
                        table_name,
                        table: Arc::clone(table),
                        key,
                        condition_expression,
                        expression_attribute_names,
                        expression_attribute_values,
                        return_values_on_condition_check_failure,
                    });
                }
            }
        }
        Ok(TableWriteBatchInput {
            puts,
            updates,
            deletes,
            checks,
        })
    }

    fn shared_tables(&self) -> HashMap<TableName, Arc<StoredTableInfo>> {
        self.tables
            .iter()
            .map(|(name, table)| (name.clone(), Arc::new(table.borrow().clone())))
            .collect()
    }
}
