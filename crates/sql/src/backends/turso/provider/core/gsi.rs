use crate::backends::turso::provider::core::*;

impl TursoStorageProvider {
    pub(crate) async fn process_gsi_updates(&self) -> StorageResult<bool> {
        let cursor_name: CursorName = "gsi-update-cursor".to_string().into();
        let stream_name = StreamName::system_table_stream();
        let mut cursor_position = self
            .ensure_gsi_update_cursor(&stream_name, &cursor_name)
            .await?;
        self.refresh_gsi_update_lag(&stream_name, cursor_position)
            .await?;
        let mut did_work = false;
        let mut table_infos: HashMap<TableName, Option<StoredTableInfo>> = HashMap::new();

        loop {
            let records_result = self
                .get_items_from_pointer_stream(
                    stream_name.clone(),
                    cursor_position,
                    Some(crate::constants::GSI_UPDATE_STREAM_FETCH_LIMIT),
                )
                .await
                .map_err(|error| {
                    StorageError::internal(&format!("turso gsi stream read failed: {error}"))
                })?;

            let had_more = records_result.has_more;
            let last_item = records_result.last_evaluated_key.or_else(|| {
                records_result.last_scanned_key.or_else(|| {
                    records_result
                        .records
                        .last()
                        .map(|(pointer, _)| pointer.stream_item_id)
                })
            });
            let records = records_result.records;

            if records.is_empty() {
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                return Ok(did_work);
            }

            let batch_infos = table_infos.clone();
            let this = self.clone();
            let batch_did_work = self
                .with_exclusive_transaction(true, move |conn| {
                    let this = this.clone();
                    let records = records.clone();
                    let mut table_infos = batch_infos.clone();
                    Box::pin(async move {
                        let mut batch_did_work = false;
                        for (pointer, stream_items) in records {
                            let filtered_info =
                                if let Some(cached) = table_infos.get(&pointer.table_name) {
                                    cached.clone()
                                } else {
                                    let loaded = this
                                        .get_table_info(&pointer.table_name)
                                        .await
                                        .ok()
                                        .and_then(|info| turso_user_gsi_table_info(&info));
                                    table_infos.insert(pointer.table_name.clone(), loaded.clone());
                                    loaded
                                };
                            let Some(table_info) = filtered_info.as_ref() else {
                                continue;
                            };

                            let (old_item, new_item) = turso_gsi_images(&stream_items);
                            if old_item.is_some() || new_item.is_some() {
                                this.apply_gsi_rows_for_item_change(
                                    conn,
                                    table_info,
                                    old_item.as_ref(),
                                    new_item.as_ref(),
                                )
                                .await?;
                                batch_did_work = true;
                            }
                        }
                        Ok((batch_did_work, table_infos))
                    })
                })
                .await?;
            did_work |= batch_did_work.0;
            table_infos = batch_did_work.1;

            let Some(last_item) = last_item else {
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                return Ok(did_work);
            };
            self.advance_cursor(stream_name.clone(), cursor_name.clone(), last_item)
                .await
                .map_err(|error| {
                    StorageError::internal(&format!("turso advance gsi cursor failed: {error}"))
                })?;
            cursor_position = Some(last_item);
            self.refresh_gsi_update_lag(&stream_name, cursor_position)
                .await?;

            if !had_more {
                return Ok(did_work);
            }
        }
    }

    async fn ensure_gsi_update_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
    ) -> StorageResult<Option<StreamItemId>> {
        let cursor_position = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso get gsi cursor failed: {error}"))
            })?
            .map(|cursor| cursor.position);

        if cursor_position.is_none() {
            self.create_cursor(
                stream_name.clone(),
                cursor_name.clone(),
                CursorPosition::Head,
            )
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso create gsi cursor failed: {error}"))
            })?;
        }

        Ok(cursor_position)
    }

    async fn refresh_gsi_update_lag(
        &self,
        stream_name: &StreamName,
        cursor_position: Option<StreamItemId>,
    ) -> StorageResult<()> {
        let page = self
            .read_forward(stream_name.clone(), cursor_position, 1)
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso gsi lag read failed: {error}"))
            })?;
        storage_common::observe_gsi_lag(
            &self.gsi_propagation_governor,
            page.items.first().map(|item| item.created_at),
            current_ms_u64(),
        );
        Ok(())
    }
}

fn current_ms_u64() -> u64 {
    u64::try_from(*TimestampMillis::now()).unwrap_or(0)
}

fn turso_user_gsi_table_info(table_info: &StoredTableInfo) -> Option<StoredTableInfo> {
    let mut filtered = table_info.clone();
    filtered.global_secondary_indexes = table_info.global_secondary_indexes.as_ref().map(|gsis| {
        gsis.iter()
            .filter(|gsi| !storage_common::ttl::is_ttl_index(&gsi.index_name))
            .cloned()
            .collect::<Vec<_>>()
    });
    filtered
        .global_secondary_indexes
        .as_ref()
        .filter(|gsis| !gsis.is_empty())?;
    Some(filtered)
}

type TursoGsiImage = Option<HashMap<String, AttributeValue>>;

fn turso_gsi_images(stream_items: &[StreamItem]) -> (TursoGsiImage, TursoGsiImage) {
    let Some(first) = stream_items.first() else {
        return (None, None);
    };

    if first.data_type == StreamDataType::DeleteMarker {
        let old_item = stream_items
            .last()
            .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
        return (old_item, None);
    }

    let new_item = storage_types::storage_serde::from_bytes(&first.data).ok();
    let old_item = stream_items
        .get(1)
        .filter(|item| item.data_type != StreamDataType::DeleteMarker)
        .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
    (old_item, new_item)
}
