use crate::backends::turso::provider::core::*;

impl TursoStorageProvider {
    pub(crate) async fn load_table_info_cached(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        if let Some(cached) = self.table_info_cache.read().await.get(table_name).cloned() {
            return Ok((*cached).clone());
        }

        let conn = self.connect().await?;
        let rows = self
            .query_rows(
                &conn,
                sql_statements::get_table_info(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;

        let Some(row) = rows.into_iter().next() else {
            return Err(StorageError::table_not_found(table_name));
        };

        let info = row_to_table_info(&row)?;
        self.table_info_cache
            .write()
            .await
            .insert(table_name.clone(), Arc::new(info.clone()));
        Ok(info)
    }

    pub(crate) async fn load_table_info_uncached<C>(
        &self,
        conn: &C,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                sql_statements::get_table_info(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;

        let Some(row) = rows.into_iter().next() else {
            return Err(StorageError::table_not_found(table_name));
        };

        row_to_table_info(&row)
    }

    pub(crate) async fn table_exists_conn<C>(
        &self,
        conn: &C,
        table_name: &TableName,
    ) -> StorageResult<bool>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                sql_statements::table_exists(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;
        let count = rows
            .first()
            .and_then(|row| row.get("count"))
            .map(value_to_i64)
            .transpose()?
            .unwrap_or_default();
        Ok(count > 0)
    }

    pub(crate) async fn invalidate_table_cache(&self, table_name: &TableName) {
        self.table_info_cache.write().await.remove(table_name);
    }

    pub(crate) async fn get_item_map_by_key<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        if key.is_empty() {
            return Ok(None);
        }

        let table_name_safe = table_info.table_name.sanitized_name();
        let (where_clause, mut params) = build_key_where_clause(key, &table_info.key_schema)?;
        let sql = sql_statements::select_main_row(&table_name_safe, &where_clause);
        let rows = self
            .query_rows(conn, &sql, std::mem::take(&mut params))
            .await?;

        rows.into_iter()
            .next()
            .map(|row| row_to_item_map_main(&row, table_info))
            .transpose()
    }

    pub(crate) async fn get_item_map_with_indexers_by_key<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<(HashMap<String, AttributeValue>, Vec<String>)>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        if key.is_empty() {
            return Ok(None);
        }

        let table_name_safe = table_info.table_name.sanitized_name();
        let (where_clause, params) = build_key_where_clause(key, &table_info.key_schema)?;
        let sql = sql_statements::select_main_row(&table_name_safe, &where_clause);
        let rows = self.query_rows(conn, &sql, params).await?;

        rows.into_iter()
            .next()
            .map(|row| {
                let decoded = row_to_decoded_item_main(&row, table_info)?;
                decoded
                    .item
                    .into_attribute_map()
                    .map(|item| (item, decoded.indexers))
            })
            .transpose()
    }

    pub(crate) async fn put_item_txn<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        input: TursoPutItemTxnInput<'_>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let TursoPutItemTxnInput {
            item,
            indexers,
            condition,
            return_old_on_condition_failure,
            item_stream_ttl_hours: aux_item_stream_ttl_hours,
        } = input;
        let mut item = item.clone();
        normalize_attribute_map_numbers_for_write(&mut item);
        let SplitDynamoItem {
            key_attributes,
            non_key_attributes,
            all_attributes,
        } = split_item_into_key_and_attributes_sync(item, table_info)?;
        let payload = crate::utils::main_table_payload(&key_attributes, &non_key_attributes);

        if is_key_absence_condition(condition, table_info) && !return_old_on_condition_failure {
            self.insert_main_row(
                conn,
                table_info,
                &key_attributes,
                &all_attributes,
                payload.as_ref(),
                indexers,
            )
            .await?;
            let item_stream_version = storage_types::ItemStreamVersion::try_from(
                self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                    .await?,
            )?;
            self.write_stream_entries_for_item_change(
                conn,
                table_info,
                &all_attributes,
                TursoWriteStreamEntriesInput {
                    old_item: None,
                    indexers: indexers.unwrap_or_default(),
                    old_indexers: None,
                    is_deleted: false,
                    item_stream_version,
                    replication: None,
                },
            )
            .await?;
            self.apply_item_stream_duration(
                conn,
                table_info,
                &key_attributes,
                aux_item_stream_ttl_hours,
            )
            .await?;
            if self.immediate_gsi_consistency {
                self.apply_gsi_rows_for_item_change(
                    conn,
                    table_info,
                    None,
                    Some(&all_attributes),
                    indexers.unwrap_or_default(),
                )
                .await?;
            }
            return Ok(None);
        }

        let (old_item, old_indexers) = self
            .get_item_map_with_indexers_by_key(conn, table_info, &key_attributes)
            .await?
            .map_or_else(
                || (None, Vec::new()),
                |(item, indexers)| (Some(item), indexers),
            );

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(crate::provider_core::write::conditional_failure(
                old_item.as_ref(),
                return_old_on_condition_failure,
            ));
        }

        self.upsert_main_row(
            conn,
            table_info,
            &key_attributes,
            &all_attributes,
            payload.as_ref(),
            indexers,
        )
        .await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &all_attributes,
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                indexers: indexers.unwrap_or_default(),
                old_indexers: old_item.as_ref().map(|_| old_indexers.as_slice()),
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        self.apply_item_stream_duration(
            conn,
            table_info,
            &key_attributes,
            aux_item_stream_ttl_hours,
        )
        .await?;
        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(
                conn,
                table_info,
                old_item.as_ref(),
                Some(&all_attributes),
                indexers.unwrap_or_default(),
            )
            .await?;
        }

        Ok(old_item)
    }

    pub(crate) async fn overwrite_item_txn<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        input: TursoOverwriteItemInput<'_>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let TursoOverwriteItemInput {
            item,
            old_item,
            indexers,
            old_indexers,
            replication,
            item_stream_ttl_hours,
        } = input;
        let mut item = item.clone();
        normalize_attribute_map_numbers_for_write(&mut item);
        let SplitDynamoItem {
            key_attributes,
            non_key_attributes,
            all_attributes,
        } = split_item_into_key_and_attributes_sync(item, table_info)?;
        let payload = crate::utils::main_table_payload(&key_attributes, &non_key_attributes);

        self.upsert_main_row(
            conn,
            table_info,
            &key_attributes,
            &all_attributes,
            payload.as_ref(),
            Some(indexers),
        )
        .await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &all_attributes,
            TursoWriteStreamEntriesInput {
                old_item,
                indexers,
                old_indexers,
                is_deleted: false,
                item_stream_version,
                replication,
            },
        )
        .await?;
        self.apply_item_stream_duration(conn, table_info, &key_attributes, item_stream_ttl_hours)
            .await?;
        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(
                conn,
                table_info,
                old_item,
                Some(&all_attributes),
                indexers,
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn delete_item_txn_with_replication<C>(
        &self,
        conn: &C,
        input: TursoDeleteItemInput<'_>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let TursoDeleteItemInput {
            table_info,
            key,
            condition,
            return_old_on_condition_failure,
            replication,
            old_indexers: declared_old_indexers,
            item_stream_ttl_hours,
        } = input;
        let (old_item, old_indexers) = self
            .get_item_map_with_indexers_by_key(conn, table_info, key)
            .await?
            .map_or_else(
                || (None, Vec::new()),
                |(item, indexers)| (Some(item), indexers),
            );
        if old_item.is_none() {
            return Ok(None);
        }

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(crate::provider_core::write::conditional_failure(
                old_item.as_ref(),
                return_old_on_condition_failure,
            ));
        }

        let table_name_safe = table_info.table_name.sanitized_name();
        let (where_clause, params) = build_key_where_clause(key, &table_info.key_schema)?;
        let delete_sql = sql_statements::delete_main_row(&table_name_safe, &where_clause);
        let _ = self.execute(conn, &delete_sql, params).await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, key)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &key.to_attribute_map(),
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                indexers: &[],
                old_indexers: declared_old_indexers.or(Some(&old_indexers)),
                is_deleted: true,
                item_stream_version,
                replication,
            },
        )
        .await?;
        self.apply_item_stream_duration(conn, table_info, key, item_stream_ttl_hours)
            .await?;

        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(conn, table_info, old_item.as_ref(), None, &[])
                .await?;
        }

        Ok(old_item)
    }

    pub(crate) async fn write_stream_entries_for_item_change<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        item_data: &HashMap<String, AttributeValue>,
        input: TursoWriteStreamEntriesInput<'_>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let TursoWriteStreamEntriesInput {
            old_item,
            indexers,
            old_indexers,
            is_deleted,
            item_stream_version,
            replication,
        } = input;
        if !crate::stream_writer::should_write_stream_entries_for_gsi_mode(
            table_info,
            self.immediate_gsi_consistency,
        ) {
            return Ok(());
        }

        let stream_tables_exist = self
            .query_rows(
                conn,
                sql_statements::stream_items_table_exists(),
                Vec::new(),
            )
            .await
            .is_ok_and(|rows| !rows.is_empty());
        if !stream_tables_exist {
            return Ok(());
        }

        let created_at = TimestampMillis::now();
        let item_key = ItemKey::from_key_schema(
            table_info.table_name.clone(),
            &table_info.key_schema,
            item_data,
        )
        .map_err(|err| StorageError::internal(&format!("stream item key error: {err}")))?;
        let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
            .map_err(|err| StorageError::internal(&format!("stream name error: {err}")))?;
        let item_stream_name = String::from(&item_stream);

        let data = storage_types::storage_serde::to_bytes(item_data)?;
        let old_bytes = old_item
            .filter(|old| !old.is_empty())
            .map(storage_types::storage_serde::to_bytes)
            .transpose()?;
        let embedded_bytes = old_bytes.as_ref().map_or(0, Vec::len) + data.len();
        let data_type = if is_deleted {
            StreamDataType::DeleteMarker
        } else {
            StreamDataType::DynamoDbJson
        };

        self.insert_stream_row(
            conn,
            &item_stream,
            storage_types::StreamItemId::from(item_stream_version),
            data.clone(),
            created_at,
            data_type,
        )
        .await?;

        let stored_pointer = if embedded_bytes <= STREAM_EMBEDDED_MAX_BYTES {
            let mut items = Vec::with_capacity(1 + usize::from(old_bytes.is_some()));
            items.push(EmbeddedStreamItem {
                data: data.clone(),
                data_type,
            });
            if let Some(old) = old_bytes {
                items.push(EmbeddedStreamItem {
                    data: old,
                    data_type: StreamDataType::DynamoDbJson,
                });
            }
            StoredStreamPointer::embedded(
                item_stream,
                table_info.table_name.clone(),
                item_stream_version,
                items,
            )
        } else {
            StoredStreamPointer::pointer(
                item_stream,
                table_info.table_name.clone(),
                item_stream_version,
            )
        };
        let stored_pointer = stored_pointer
            .with_indexers(indexers.to_vec())
            .with_old_indexers(old_indexers.map(<[_]>::to_vec));
        let stored_pointer = if let Some(replication) = replication.cloned() {
            stored_pointer.with_replication_metadata(replication)
        } else {
            stored_pointer
        };
        let pointer_data = storage_types::storage_serde::to_bytes(&stored_pointer)?;

        let table_pointer_stream_item_id = StreamItemId::from(Uuid::now_v7());
        self.insert_stream_row(
            conn,
            &StreamName::table_stream(&table_info.table_name),
            table_pointer_stream_item_id,
            pointer_data.clone(),
            created_at,
            StreamDataType::StreamPointer,
        )
        .await?;
        let system_pointer_stream_item_id = StreamItemId::from(Uuid::now_v7());
        self.insert_stream_row(
            conn,
            &StreamName::system_table_stream(),
            system_pointer_stream_item_id,
            pointer_data,
            created_at,
            StreamDataType::StreamPointer,
        )
        .await?;
        self.insert_stream_pointer_index(
            conn,
            TursoStreamPointerIndexEntry {
                table_name: &table_info.table_name,
                item_stream_name: &item_stream_name,
                item_stream_version,
                table_stream_item_id: table_pointer_stream_item_id,
                system_stream_item_id: system_pointer_stream_item_id,
                created_at,
            },
        )
        .await?;
        self.insert_change_index_marker(conn, table_info, table_pointer_stream_item_id, created_at)
            .await?;

        Ok(())
    }
}
