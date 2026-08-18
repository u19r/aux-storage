use std::collections::HashMap;

use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalBackfillTombstone, LogicalImportApplyCase,
    LogicalImportApplyDecision, LogicalImportRecordKind, plan_logical_import_apply,
};
use storage_common::ttl::TtlConfigRecord;
use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{
    AttributeValue, ItemStreamVersion, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    TableName,
};

use super::{
    SQLiteStorageProvider,
    logical_backfill::{payload_i64, payload_optional_string, payload_string},
    logical_backfill_gsi::{import_gsi_backfill_record, import_physical_gsi_record},
    logical_backfill_stream::{
        import_stream_cursor_record, import_stream_format_record, import_stream_item_record,
        import_user_stream_record,
    },
};
use crate::{
    error_handler::map_sqlite_error,
    stream_writer::{SqliteWriteStreamEntriesInput, write_stream_entries},
    transaction_manager::with_transaction,
    utils::SqliteConn,
};

pub(super) async fn import_logical_records(
    provider: &SQLiteStorageProvider,
    records: Vec<LogicalBackfillRecord>,
) -> StorageResult<()> {
    let immediate_gsi_consistency = provider.immediate_gsi_consistency;
    with_transaction(&provider.connection, move |sqlite| {
        let mut context = SyncImportContext::default();
        for record in records {
            match record {
                LogicalBackfillRecord::PresentItem {
                    table_name,
                    key_json: _,
                    item_json,
                    indexers,
                    item_stream_version,
                } => {
                    let table_name = TableName::new(&table_name);
                    let item = serde_json::from_str::<HashMap<String, AttributeValue>>(&item_json)?;
                    SQLiteStorageProvider::import_present_item_with_context(
                        &table_name,
                        ImportPresentItemInput {
                            item,
                            indexers: &indexers,
                            old_item_source: OldItemSource::ReadLocal,
                            item_stream_version,
                            immediate_gsi_consistency,
                        },
                        &mut context,
                        sqlite,
                    )?;
                }
                LogicalBackfillRecord::Tombstone(tombstone) => {
                    SQLiteStorageProvider::import_tombstone_with_context(
                        tombstone,
                        OldItemSource::ReadLocal,
                        immediate_gsi_consistency,
                        &mut context,
                        sqlite,
                    )?;
                }
                LogicalBackfillRecord::DomainRecord {
                    domain,
                    payload_json,
                    ..
                } => match domain {
                    LogicalBackfillDomain::TableMetadata => {
                        SQLiteStorageProvider::import_table_metadata_record(&payload_json, sqlite)?;
                    }
                    LogicalBackfillDomain::DurableRevisions => {
                        SQLiteStorageProvider::import_durable_revision_record(
                            &payload_json,
                            sqlite,
                        )?;
                    }
                    LogicalBackfillDomain::TtlRecords => {
                        SQLiteStorageProvider::import_ttl_record(&payload_json, sqlite)?;
                    }
                    LogicalBackfillDomain::StreamRecords => {
                        SQLiteStorageProvider::import_stream_record(&payload_json, sqlite)?;
                    }
                    LogicalBackfillDomain::GsiRecords => {
                        SQLiteStorageProvider::import_gsi_record(&payload_json, sqlite)?;
                    }
                    LogicalBackfillDomain::ItemRecords
                    | LogicalBackfillDomain::Tombstones
                    | LogicalBackfillDomain::StorageControlPlane
                    | LogicalBackfillDomain::BackgroundJobs
                    | LogicalBackfillDomain::SyncControlPlane => {
                        return Err(StorageError::validation(format!(
                            "sqlite logical import received unexpected domain record for \
                             {domain:?}"
                        )));
                    }
                },
                LogicalBackfillRecord::StreamRecord { .. } => {
                    return Err(StorageError::validation(
                        "sqlite logical import currently imports stream rows through domain \
                         records",
                    ));
                }
            }
        }
        Ok(())
    })
    .await
}

#[derive(Default)]
pub(super) struct SyncImportContext {
    table_infos: HashMap<TableName, StoredTableInfo>,
    ttl_configs: HashMap<TableName, Option<TtlConfigRecord>>,
}

impl SyncImportContext {
    fn table_info(
        &mut self,
        table_name: &TableName,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<StoredTableInfo> {
        if !self.table_infos.contains_key(table_name) {
            self.table_infos.insert(
                table_name.clone(),
                SQLiteStorageProvider::do_get_table_info(table_name, sqlite)?,
            );
        }
        self.table_infos.get(table_name).cloned().ok_or_else(|| {
            StorageError::internal(&format!(
                "sync import table metadata missing for {table_name}"
            ))
        })
    }

    fn ttl_config(
        &mut self,
        table_name: &TableName,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if !self.ttl_configs.contains_key(table_name) {
            self.ttl_configs.insert(
                table_name.clone(),
                SQLiteStorageProvider::load_ttl_config_txn(sqlite, table_name)?,
            );
        }
        self.ttl_configs.get(table_name).cloned().ok_or_else(|| {
            StorageError::internal(&format!("sync import TTL config missing for {table_name}"))
        })
    }
}

pub(super) enum OldItemSource<'a> {
    ReadLocal,
    Resolved {
        item_json: Option<&'a str>,
        indexers: Option<&'a [String]>,
    },
}

pub(super) struct ImportPresentItemInput<'a> {
    pub(super) item: HashMap<String, AttributeValue>,
    pub(super) indexers: &'a [String],
    pub(super) old_item_source: OldItemSource<'a>,
    pub(super) item_stream_version: ItemStreamVersion,
    pub(super) immediate_gsi_consistency: bool,
}

type OptionalLogicalItemWithIndexers =
    (Option<HashMap<String, AttributeValue>>, Option<Vec<String>>);

impl OldItemSource<'_> {
    fn load(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<OptionalLogicalItemWithIndexers> {
        match self {
            Self::ReadLocal => SQLiteStorageProvider::do_get_item_with_indexers(
                table_name, key, sqlite,
            )
            .map(|old| {
                old.map_or((None, None), |(item, indexers)| {
                    (Some(item), Some(indexers))
                })
            }),
            Self::Resolved {
                item_json: Some(old_item_json),
                indexers,
            } => Ok((
                Some(serde_json::from_str(old_item_json)?),
                indexers.map(<[_]>::to_vec),
            )),
            Self::Resolved {
                item_json: None, ..
            } => Ok((None, None)),
        }
    }
}

impl SQLiteStorageProvider {
    pub(super) fn import_present_item_with_context(
        table_name: &TableName,
        input: ImportPresentItemInput<'_>,
        context: &mut SyncImportContext,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let ImportPresentItemInput {
            item,
            indexers,
            old_item_source,
            item_stream_version,
            immediate_gsi_consistency,
        } = input;
        let table_info = context.table_info(table_name, sqlite)?;
        let split = split_item_into_key_and_attributes_sync(item, &table_info)?;
        let (old_item, old_indexers) =
            old_item_source.load(table_name, &split.key_attributes, sqlite)?;
        let current_version =
            Self::current_item_stream_version(table_name, &split.key_attributes, sqlite)?;
        let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
            current_version,
            item_stream_version,
            LogicalImportRecordKind::PresentItem,
        ));
        if !matches!(decision, LogicalImportApplyDecision::ApplyPresentItem) {
            return Ok(());
        }

        let payload =
            crate::utils::main_table_payload(&split.key_attributes, &split.non_key_attributes);
        let indexed = crate::indexed_item::SqlIndexedItem::extract(
            &split.all_attributes,
            payload.as_ref(),
            Some(indexers),
            table_info.max_indexers,
        )?;
        crate::backends::sqlite::put_item_impl::execute_put_item(
            sqlite,
            table_name.sanitized_name().as_ref(),
            &split.key_attributes,
            &indexed,
            table_info.max_indexers,
        )?;
        Self::do_set_item_revision(
            table_name,
            &split.key_attributes,
            item_stream_version,
            sqlite,
        )?;
        write_stream_entries(
            sqlite,
            &table_info,
            &split.all_attributes,
            SqliteWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                indexers,
                old_indexers: old_indexers.as_deref(),
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )?;
        if immediate_gsi_consistency {
            Self::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                old_item.as_ref(),
                Some(&split.all_attributes),
                indexers,
                item_stream_version,
            )?;
        }
        let ttl_config = context.ttl_config(table_name, sqlite)?;
        Self::update_ttl_index_entries(
            sqlite,
            &table_info,
            ttl_config.as_ref(),
            old_item.as_ref(),
            Some(&split.all_attributes),
        )
    }

    pub(super) fn import_tombstone_with_context(
        tombstone: LogicalBackfillTombstone,
        old_item_source: OldItemSource<'_>,
        immediate_gsi_consistency: bool,
        context: &mut SyncImportContext,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let table_name = TableName::new(&tombstone.table_name);
        let key = serde_json::from_str::<KeyAttributes>(&tombstone.key_json)?;
        let table_info = context.table_info(&table_name, sqlite)?;
        let (existing_item, existing_indexers) = old_item_source.load(&table_name, &key, sqlite)?;
        let current_version = Self::current_item_stream_version(&table_name, &key, sqlite)?;
        let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
            current_version,
            tombstone.item_stream_version,
            LogicalImportRecordKind::Tombstone,
        ));
        if !matches!(decision, LogicalImportApplyDecision::ApplyTombstone) {
            return Ok(());
        }

        Self::delete_imported_item(&table_name, &key, sqlite)?;
        Self::do_set_item_revision(&table_name, &key, tombstone.item_stream_version, sqlite)?;
        let key_item = key.to_attribute_map();
        write_stream_entries(
            sqlite,
            &table_info,
            &key_item,
            SqliteWriteStreamEntriesInput {
                old_item: existing_item.as_ref(),
                indexers: &[],
                old_indexers: existing_indexers.as_deref(),
                is_deleted: true,
                item_stream_version: tombstone.item_stream_version,
                replication: None,
            },
        )?;
        if immediate_gsi_consistency {
            Self::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                existing_item.as_ref(),
                None,
                &[],
                tombstone.item_stream_version,
            )?;
        }
        let ttl_config = context.ttl_config(&table_name, sqlite)?;
        Self::update_ttl_index_entries(
            sqlite,
            &table_info,
            ttl_config.as_ref(),
            existing_item.as_ref(),
            None,
        )
    }

    fn current_item_stream_version(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<Option<ItemStreamVersion>> {
        let revision = Self::do_get_item_revision(table_name, key, sqlite)?;
        if revision == 0 {
            Ok(None)
        } else {
            Ok(Some(ItemStreamVersion::try_from(revision)?))
        }
    }

    fn delete_imported_item(
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let where_clause = key
            .iter()
            .enumerate()
            .map(|(index, (attr_name, _))| format!("{attr_name} = ?{}", index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let values = key
            .iter()
            .map(|(_, value)| {
                value.inner_string().map_err(|err| {
                    StorageError::validation(format!("key attribute must be scalar: {err}"))
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let sql = format!(
            "DELETE FROM \"table_{}\" WHERE {where_clause}",
            table_name.sanitized_name()
        );
        sqlite
            .execute(&sql, rusqlite::params_from_iter(values.iter()))
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn import_durable_revision_record(
        payload_json: &str,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let payload: serde_json::Value = serde_json::from_str(payload_json)?;
        let table_name = payload_string(&payload, "table_name")?;
        let key_json = payload_string(&payload, "key_json")?;
        let revision = payload_i64(&payload, "revision")?;
        sqlite
            .execute(
                r"INSERT INTO item_revisions (table_name, key_json, revision)
                  VALUES (?1, ?2, ?3)
                  ON CONFLICT(table_name, key_json)
                  DO UPDATE SET revision = excluded.revision",
                (table_name.as_str(), key_json.as_str(), revision),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn import_table_metadata_record(
        payload_json: &str,
        sqlite: &SqliteConn<'_>,
    ) -> StorageResult<()> {
        let payload: serde_json::Value = serde_json::from_str(payload_json)?;
        let id = payload_string(&payload, "id")?;
        let table_name = payload_string(&payload, "table_name")?;
        let table_status = payload_string(&payload, "table_status")?;
        let created_at = payload_i64(&payload, "created_at")?;
        let attribute_definitions = payload_string(&payload, "attribute_definitions")?;
        let key_schema = payload_string(&payload, "key_schema")?;
        let max_indexers = payload_i64(&payload, "max_indexers")?;
        storage_types::MaxIndexers::try_new(u8::try_from(max_indexers).map_err(|_| {
            StorageError::validation("table metadata max_indexers is outside the supported range")
        })?)?;
        let global_secondary_indexes =
            payload_optional_string(&payload, "global_secondary_indexes")?;
        let table_size_bytes = payload_i64(&payload, "table_size_bytes")?;
        let item_count = payload_i64(&payload, "item_count")?;
        let stream_specification = payload_optional_string(&payload, "stream_specification")?;
        let deletion_protection_enabled = payload
            .get("deletion_protection_enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let table_stream_duration_hours = payload
            .get("table_stream_duration_hours")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(72);
        let default_item_stream_duration_hours = payload
            .get("default_item_stream_duration_hours")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(table_stream_duration_hours);
        sqlite
            .execute(
                r"INSERT INTO tables (
                    id, table_name, table_status, created_at, attribute_definitions, key_schema,
                    max_indexers, global_secondary_indexes, table_size_bytes, item_count, stream_specification,
                    deletion_protection_enabled, table_stream_duration_hours,
                    default_item_stream_duration_hours
                  )
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                  ON CONFLICT(table_name)
                  DO UPDATE SET
                    id = excluded.id,
                    table_status = excluded.table_status,
                    created_at = excluded.created_at,
                    attribute_definitions = excluded.attribute_definitions,
                    key_schema = excluded.key_schema,
                    max_indexers = excluded.max_indexers,
                    global_secondary_indexes = excluded.global_secondary_indexes,
                    table_size_bytes = excluded.table_size_bytes,
                    item_count = excluded.item_count,
                    stream_specification = excluded.stream_specification,
                    deletion_protection_enabled = excluded.deletion_protection_enabled,
                    table_stream_duration_hours = excluded.table_stream_duration_hours,
                    default_item_stream_duration_hours = excluded.default_item_stream_duration_hours",
                (
                    id.as_str(),
                    table_name.as_str(),
                    table_status.as_str(),
                    created_at,
                    attribute_definitions.as_str(),
                    key_schema.as_str(),
                    max_indexers,
                    global_secondary_indexes.as_deref(),
                    table_size_bytes,
                    item_count,
                    stream_specification.as_deref(),
                    if deletion_protection_enabled {
                        1i64
                    } else {
                        0i64
                    },
                    table_stream_duration_hours,
                    default_item_stream_duration_hours,
                ),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn import_ttl_record(payload_json: &str, sqlite: &SqliteConn<'_>) -> StorageResult<()> {
        let payload: serde_json::Value = serde_json::from_str(payload_json)?;
        let table_name = payload_string(&payload, "table_name")?;
        let config_blob = payload
            .get("config_blob")
            .cloned()
            .ok_or_else(|| StorageError::validation("ttl record missing config_blob"))
            .and_then(|value| serde_json::from_value::<Vec<u8>>(value).map_err(Into::into))?;
        sqlite
            .execute(
                r"INSERT INTO sys_ttl_configs (table_name, config_blob)
                  VALUES (?1, ?2)
                  ON CONFLICT(table_name)
                  DO UPDATE SET config_blob = excluded.config_blob",
                (table_name.as_str(), config_blob),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    fn import_stream_record(payload_json: &str, sqlite: &SqliteConn<'_>) -> StorageResult<()> {
        let payload: serde_json::Value = serde_json::from_str(payload_json)?;
        match payload_string(&payload, "stream_table")?.as_str() {
            "sys_stream_format_metadata" => import_stream_format_record(&payload, sqlite),
            "sys_user_streams" => import_user_stream_record(&payload, sqlite),
            "sys_stream_items" => import_stream_item_record(&payload, sqlite),
            "sys_stream_cursors" => import_stream_cursor_record(&payload, sqlite),
            stream_table => Err(StorageError::validation(format!(
                "unsupported stream logical table {stream_table}"
            ))),
        }
    }

    fn import_gsi_record(payload_json: &str, sqlite: &SqliteConn<'_>) -> StorageResult<()> {
        let payload: serde_json::Value = serde_json::from_str(payload_json)?;
        match payload_string(&payload, "gsi_record_type")?.as_str() {
            "backfill_state" => import_gsi_backfill_record(&payload, sqlite),
            "physical_row" => import_physical_gsi_record(&payload, sqlite),
            record_type => Err(StorageError::validation(format!(
                "unsupported gsi logical record type {record_type}"
            ))),
        }
    }
}
