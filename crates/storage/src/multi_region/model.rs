use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use storage_backfill::{
    LogicalBackfillId, LogicalBootstrapPreflightCase, LogicalBootstrapPreflightDecision,
    plan_logical_bootstrap_preflight,
};
use storage_types::{
    AttributeValue, ItemStreamVersion, MultiRegionConsistency, PutItemRequest, ReplicaDescription,
    ReplicaStatus, ReplicaUpdate, ReplicationMutation, ReplicationWriteSource, StorageError,
    StorageResult, StreamItemId, StreamName, StreamRecord, TableName, TimestampMillis,
};
use stream::{StoredStreamPointer, StreamDataType, StreamError, StreamItem, StreamPage};

use crate::{DatabaseManager, DeleteItemInput, PutItemInput, ScanTableInput, Tables};

const PAYLOAD_ATTR: &str = "payload";
const PK_ATTR: &str = "pk";
const SK_ATTR: &str = "sk";
const TABLE_CONFIG_SK: &str = "config";
const CHECKPOINT_SK: &str = "cursor";
const STATUS_SK: &str = "status";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableReplicationConfigRecord {
    pub table_name: TableName,
    pub multi_region_consistency: MultiRegionConsistency,
    pub replica_epoch: u64,
    pub replicas: Vec<ReplicaDescription>,
    pub updated_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerCheckpointRecord {
    pub peer_region: String,
    pub last_system_stream_cursor: Option<StreamItemId>,
    pub updated_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerReplicationStatusRecord {
    pub peer_region: String,
    pub last_inbound_heartbeat_at: Option<TimestampMillis>,
    pub last_heartbeat_rtt_ms: Option<u64>,
    pub clock_offset_estimate_ms: Option<i64>,
    pub clock_offset_uncertainty_ms: Option<u64>,
    pub last_received_source_commit_ts: Option<TimestampMillis>,
    pub last_received_commit_ts: Option<TimestampMillis>,
    pub last_inbound_apply_at: Option<TimestampMillis>,
    pub sender_queue_depth: Option<u64>,
    pub last_outbound_apply_at: Option<TimestampMillis>,
    pub last_outbound_commit_ts: Option<TimestampMillis>,
    pub last_remote_applied_commit_ts: Option<TimestampMillis>,
    pub last_auth_failure_at: Option<TimestampMillis>,
    pub updated_at: TimestampMillis,
}

impl PeerReplicationStatusRecord {
    #[must_use]
    pub fn new(peer_region: impl Into<String>) -> Self {
        Self {
            peer_region: peer_region.into(),
            last_inbound_heartbeat_at: None,
            last_heartbeat_rtt_ms: None,
            clock_offset_estimate_ms: None,
            clock_offset_uncertainty_ms: None,
            last_received_source_commit_ts: None,
            last_received_commit_ts: None,
            last_inbound_apply_at: None,
            sender_queue_depth: None,
            last_outbound_apply_at: None,
            last_outbound_commit_ts: None,
            last_remote_applied_commit_ts: None,
            last_auth_failure_at: None,
            updated_at: TimestampMillis::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableBootstrapCursorRecord {
    pub table_name: TableName,
    pub peer_region: String,
    #[serde(default)]
    pub protected_stream_cursor: Option<StreamItemId>,
    pub last_system_stream_cursor: Option<StreamItemId>,
    #[serde(default)]
    pub activation_cursor: Option<StreamItemId>,
    #[serde(default)]
    pub session_started_at: Option<TimestampMillis>,
    #[serde(default)]
    pub logical_backfill_manifest_id: Option<String>,
    #[serde(default)]
    pub logical_backfill_domain: Option<String>,
    #[serde(default)]
    pub logical_backfill_cursor: Option<String>,
    pub updated_at: TimestampMillis,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboundReplicationMutationRecord {
    pub system_stream_cursor: StreamItemId,
    pub mutation: ReplicationMutation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboundReplicationBatch {
    pub records: Vec<OutboundReplicationMutationRecord>,
    pub checkpoint_cursor: Option<StreamItemId>,
    pub reached_end: bool,
}

impl DatabaseManager {
    pub async fn get_multi_region_table_state(
        &self,
        table_name: &TableName,
    ) -> StorageResult<(
        Option<Vec<ReplicaDescription>>,
        Option<MultiRegionConsistency>,
    )> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok((None, None));
        }

        let config = self.get_table_replication_config(table_name).await?;
        Ok(match config {
            Some(config) => (Some(config.replicas), Some(config.multi_region_consistency)),
            None => (None, None),
        })
    }

    pub async fn get_table_replication_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TableReplicationConfigRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(None);
        }

        let item = self
            .get_item_map(
                Tables::sys_storage_replication(),
                control_plane_key(table_config_pk(table_name), TABLE_CONFIG_SK),
            )
            .await?;
        item.as_ref().map(decode_payload).transpose()
    }

    pub async fn put_table_replication_config(
        &self,
        record: &TableReplicationConfigRecord,
    ) -> StorageResult<()> {
        self.ensure_multi_region_control_plane_table().await?;
        self.put_item(
            PutItemInput::builder()
                .table_name(Tables::sys_storage_replication())
                .item(payload_item(
                    table_config_pk(&record.table_name),
                    TABLE_CONFIG_SK,
                    record,
                )?)
                .build(),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_table_replication_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<()> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        self.delete_item(
            DeleteItemInput::builder()
                .table_name(Tables::sys_storage_replication())
                .key(control_plane_key(
                    table_config_pk(table_name),
                    TABLE_CONFIG_SK,
                ))
                .build(),
        )
        .await?;
        Ok(())
    }

    pub async fn get_peer_checkpoint(
        &self,
        peer_region: &str,
    ) -> StorageResult<Option<PeerCheckpointRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(None);
        }

        validate_region_name(peer_region)?;
        let item = self
            .get_item_map(
                Tables::sys_storage_replication(),
                control_plane_key(peer_checkpoint_pk(peer_region), CHECKPOINT_SK),
            )
            .await?;
        item.as_ref().map(decode_payload).transpose()
    }

    pub async fn put_peer_checkpoint(&self, record: &PeerCheckpointRecord) -> StorageResult<()> {
        self.ensure_multi_region_control_plane_table().await?;
        self.put_item(put_item_input_from_request(peer_checkpoint_put_request(
            record,
        )?))
        .await?;
        Ok(())
    }

    pub async fn delete_peer_checkpoint(&self, peer_region: &str) -> StorageResult<()> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        validate_region_name(peer_region)?;
        self.delete_item(
            DeleteItemInput::builder()
                .table_name(Tables::sys_storage_replication())
                .key(control_plane_key(
                    peer_checkpoint_pk(peer_region),
                    CHECKPOINT_SK,
                ))
                .build(),
        )
        .await?;
        Ok(())
    }

    pub async fn get_peer_replication_status(
        &self,
        peer_region: &str,
    ) -> StorageResult<Option<PeerReplicationStatusRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(None);
        }

        validate_region_name(peer_region)?;
        let item = self
            .get_item_map(
                Tables::sys_storage_replication(),
                control_plane_key(peer_status_pk(peer_region), STATUS_SK),
            )
            .await?;
        item.as_ref().map(decode_payload).transpose()
    }

    pub async fn put_peer_replication_status(
        &self,
        record: &PeerReplicationStatusRecord,
    ) -> StorageResult<()> {
        validate_region_name(&record.peer_region)?;
        self.ensure_multi_region_control_plane_table().await?;
        self.put_item(
            PutItemInput::builder()
                .table_name(Tables::sys_storage_replication())
                .item(payload_item(
                    peer_status_pk(&record.peer_region),
                    STATUS_SK,
                    record,
                )?)
                .build(),
        )
        .await?;
        Ok(())
    }

    pub async fn list_peer_replication_statuses(
        &self,
    ) -> StorageResult<Vec<PeerReplicationStatusRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(Vec::new());
        }

        let (items, _) = self
            .scan_table_map(
                ScanTableInput::builder()
                    .table_name(Tables::sys_storage_replication())
                    .build(),
            )
            .await?;
        let mut statuses: Vec<PeerReplicationStatusRecord> = Vec::new();
        for item in items {
            let Some(pk) = item.get(PK_ATTR).and_then(|value| value.inner_str().ok()) else {
                continue;
            };
            if !pk.starts_with("peer-status#") {
                continue;
            }
            statuses.push(decode_payload(&item)?);
        }
        statuses.sort_by(|left, right| left.peer_region.cmp(&right.peer_region));
        Ok(statuses)
    }

    pub async fn update_peer_replication_status<F>(
        &self,
        peer_region: &str,
        update: F,
    ) -> StorageResult<PeerReplicationStatusRecord>
    where
        F: FnOnce(&mut PeerReplicationStatusRecord),
    {
        self.ensure_multi_region_replication_control_plane_supported()?;
        validate_region_name(peer_region)?;
        let mut record = self
            .get_peer_replication_status(peer_region)
            .await?
            .unwrap_or_else(|| PeerReplicationStatusRecord::new(peer_region));
        update(&mut record);
        record.updated_at = TimestampMillis::now();
        self.put_peer_replication_status(&record).await?;
        Ok(record)
    }

    pub async fn get_table_bootstrap_cursor(
        &self,
        table_name: &TableName,
        peer_region: &str,
    ) -> StorageResult<Option<TableBootstrapCursorRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(None);
        }

        validate_region_name(peer_region)?;
        let item = self
            .get_item_map(
                Tables::sys_storage_replication(),
                control_plane_key(bootstrap_cursor_pk(table_name, peer_region), CHECKPOINT_SK),
            )
            .await?;
        item.as_ref().map(decode_payload).transpose()
    }

    pub async fn put_table_bootstrap_cursor(
        &self,
        record: &TableBootstrapCursorRecord,
    ) -> StorageResult<()> {
        self.ensure_multi_region_control_plane_table().await?;
        self.put_item(put_item_input_from_request(
            table_bootstrap_cursor_put_request(record)?,
        ))
        .await?;
        Ok(())
    }

    pub async fn ensure_logical_bootstrap_destination_preflight(
        &self,
        table_name: &TableName,
        source_region: &str,
        manifest_id: &LogicalBackfillId,
    ) -> StorageResult<LogicalBootstrapPreflightDecision> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        let marker_peer_region =
            bootstrap_preflight_marker_peer_region(source_region, manifest_id.as_str());
        if self
            .get_table_bootstrap_cursor(table_name, &marker_peer_region)
            .await?
            .is_some()
        {
            return Ok(plan_logical_bootstrap_preflight(
                LogicalBootstrapPreflightCase {
                    destination_empty: false,
                    preflight_marker_present: true,
                },
            ));
        }

        let (items, _) = self
            .scan_table_map(
                ScanTableInput::builder()
                    .table_name(table_name.clone())
                    .limit(1_u32)
                    .consistent_read(true)
                    .build(),
            )
            .await?;
        let decision = plan_logical_bootstrap_preflight(LogicalBootstrapPreflightCase {
            destination_empty: items.is_empty(),
            preflight_marker_present: false,
        });
        if matches!(
            decision,
            LogicalBootstrapPreflightDecision::AllowEmptyDestination
        ) {
            self.put_table_bootstrap_cursor(&TableBootstrapCursorRecord {
                table_name: table_name.clone(),
                peer_region: marker_peer_region,
                protected_stream_cursor: None,
                last_system_stream_cursor: None,
                activation_cursor: None,
                session_started_at: Some(TimestampMillis::now()),
                logical_backfill_manifest_id: Some(manifest_id.as_str().to_string()),
                logical_backfill_domain: Some("destination_preflight".to_string()),
                logical_backfill_cursor: Some("complete".to_string()),
                updated_at: TimestampMillis::now(),
            })
            .await?;
        }
        Ok(decision)
    }

    pub async fn delete_table_bootstrap_cursor(
        &self,
        table_name: &TableName,
        peer_region: &str,
    ) -> StorageResult<()> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        validate_region_name(peer_region)?;
        self.delete_item(
            DeleteItemInput::builder()
                .table_name(Tables::sys_storage_replication())
                .key(control_plane_key(
                    bootstrap_cursor_pk(table_name, peer_region),
                    CHECKPOINT_SK,
                ))
                .build(),
        )
        .await?;
        Ok(())
    }

    pub async fn ensure_multi_region_control_plane_table(&self) -> StorageResult<()> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        Tables::create_sys_storage_replication_table(self).await
    }

    pub async fn apply_replica_updates(
        &self,
        table_name: &TableName,
        updates: &[ReplicaUpdate],
    ) -> StorageResult<TableReplicationConfigRecord> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        let actions = parse_replica_actions(updates)?;
        let bootstrap_regions = actions
            .iter()
            .filter(|action| action.kind == ReplicaActionKind::Create)
            .map(|action| action.region_name.clone())
            .collect::<Vec<_>>();
        let mut config = self
            .get_table_replication_config(table_name)
            .await?
            .unwrap_or(TableReplicationConfigRecord {
                table_name: table_name.clone(),
                multi_region_consistency: MultiRegionConsistency::Eventual,
                replica_epoch: 0,
                replicas: Vec::new(),
                updated_at: TimestampMillis::now(),
            });

        for action in actions {
            apply_replica_action(&mut config, action)?;
        }

        config.replica_epoch = config.replica_epoch.saturating_add(1);
        config.updated_at = TimestampMillis::now();
        config
            .replicas
            .sort_by(|left, right| left.region_name.cmp(&right.region_name));
        self.put_table_replication_config(&config).await?;
        self.ensure_bootstrap_sessions_for_created_replicas(
            table_name,
            &bootstrap_regions,
            TimestampMillis::now(),
        )
        .await?;
        Ok(config)
    }

    async fn ensure_bootstrap_sessions_for_created_replicas(
        &self,
        table_name: &TableName,
        peer_regions: &[String],
        now: TimestampMillis,
    ) -> StorageResult<()> {
        if peer_regions.is_empty() {
            return Ok(());
        }

        let protected_cursor = self.latest_system_stream_cursor().await?;
        for peer_region in peer_regions {
            if self
                .get_table_bootstrap_cursor(table_name, peer_region)
                .await?
                .is_some()
            {
                continue;
            }
            self.put_table_bootstrap_cursor(&TableBootstrapCursorRecord {
                table_name: table_name.clone(),
                peer_region: peer_region.clone(),
                protected_stream_cursor: protected_cursor,
                last_system_stream_cursor: protected_cursor,
                activation_cursor: None,
                session_started_at: Some(now),
                logical_backfill_manifest_id: None,
                logical_backfill_domain: None,
                logical_backfill_cursor: None,
                updated_at: now,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn latest_system_stream_cursor(&self) -> StorageResult<Option<StreamItemId>> {
        let page = self
            .database_trait_provider()
            .read_backward(StreamName::system_table_stream(), None, 1)
            .await
            .map_err(StreamError::into_storage_enum)?;
        Ok(page.items.into_iter().next().map(|item| item.id))
    }

    pub async fn list_table_replication_configs(
        &self,
    ) -> StorageResult<Vec<TableReplicationConfigRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(Vec::new());
        }

        let (items, _) = self
            .scan_table_map(
                ScanTableInput::builder()
                    .table_name(Tables::sys_storage_replication())
                    .build(),
            )
            .await?;
        let mut configs: Vec<TableReplicationConfigRecord> = Vec::new();
        for item in items {
            let Some(pk) = item.get(PK_ATTR).and_then(|value| value.inner_str().ok()) else {
                continue;
            };
            if !pk.starts_with("table#") {
                continue;
            }
            configs.push(decode_payload(&item)?);
        }
        configs.sort_by(|left, right| left.table_name.as_ref().cmp(right.table_name.as_ref()));
        Ok(configs)
    }

    pub async fn list_table_bootstrap_cursors_for_peer(
        &self,
        peer_region: &str,
    ) -> StorageResult<Vec<TableBootstrapCursorRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(Vec::new());
        }

        validate_region_name(peer_region)?;
        let (items, _) = self
            .scan_table_map(
                ScanTableInput::builder()
                    .table_name(Tables::sys_storage_replication())
                    .build(),
            )
            .await?;
        let mut cursors: Vec<TableBootstrapCursorRecord> = Vec::new();
        for item in items {
            let Some(pk) = item.get(PK_ATTR).and_then(|value| value.inner_str().ok()) else {
                continue;
            };
            if !pk.starts_with("bootstrap#") || !pk.ends_with(&format!("#{peer_region}")) {
                continue;
            }
            cursors.push(decode_payload(&item)?);
        }
        cursors.sort_by(|left, right| left.table_name.as_ref().cmp(right.table_name.as_ref()));
        Ok(cursors)
    }

    pub async fn list_table_bootstrap_cursors(
        &self,
    ) -> StorageResult<Vec<TableBootstrapCursorRecord>> {
        if !self.supports_multi_region_replication_control_plane() {
            return Ok(Vec::new());
        }

        let (items, _) = self
            .scan_table_map(
                ScanTableInput::builder()
                    .table_name(Tables::sys_storage_replication())
                    .build(),
            )
            .await?;
        let mut cursors: Vec<TableBootstrapCursorRecord> = Vec::new();
        for item in items {
            let Some(pk) = item.get(PK_ATTR).and_then(|value| value.inner_str().ok()) else {
                continue;
            };
            if !pk.starts_with("bootstrap#") {
                continue;
            }
            cursors.push(decode_payload(&item)?);
        }
        cursors.sort_by(|left, right| {
            left.peer_region
                .cmp(&right.peer_region)
                .then_with(|| left.table_name.as_ref().cmp(right.table_name.as_ref()))
        });
        Ok(cursors)
    }

    pub async fn mark_replica_active(
        &self,
        table_name: &TableName,
        peer_region: &str,
    ) -> StorageResult<TableReplicationConfigRecord> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        validate_region_name(peer_region)?;
        let mut config = self
            .get_table_replication_config(table_name)
            .await?
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "replication config for table '{}' does not exist",
                    table_name
                ))
            })?;
        let replica = config
            .replicas
            .iter_mut()
            .find(|replica| replica.region_name == peer_region)
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "replica '{}' does not exist for table '{}'",
                    peer_region, table_name
                ))
            })?;
        replica.replica_status = ReplicaStatus::Active;
        replica.replica_status_description = Some("Replica catchup completed".to_string());
        replica.replica_inaccessible_date_time = None;
        config.updated_at = TimestampMillis::now();
        self.put_table_replication_config(&config).await?;
        Ok(config)
    }

    pub async fn read_outbound_replication_batch(
        &self,
        source_region: &str,
        start_after: Option<StreamItemId>,
        include_tables: &[TableName],
        exclude_tables: &[TableName],
        mutation_limit: usize,
        byte_limit: usize,
    ) -> StorageResult<OutboundReplicationBatch> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        if mutation_limit == 0 {
            return Err(StorageError::validation(
                "multi-region outbound mutation limit must be greater than zero",
            ));
        }
        if byte_limit == 0 {
            return Err(StorageError::validation(
                "multi-region outbound byte limit must be greater than zero",
            ));
        }
        if include_tables.is_empty() {
            return Ok(OutboundReplicationBatch {
                records: Vec::new(),
                checkpoint_cursor: start_after,
                reached_end: true,
            });
        }

        let include_tables = include_tables.iter().cloned().collect::<HashSet<_>>();
        let exclude_tables = exclude_tables.iter().cloned().collect::<HashSet<_>>();
        let mut table_schemas = HashMap::new();
        for table_name in &include_tables {
            let table_info = self.get_table_info_arc(table_name).await?;
            table_schemas.insert(table_name.clone(), table_info.key_schema.clone());
        }

        let mut records = Vec::new();
        let mut last_safe_cursor = start_after;
        let mut cursor = start_after;
        let mut buffered_bytes = 0usize;
        let system_stream_name = StreamName::system_table_stream();

        let reached_end = loop {
            let StreamPage {
                items,
                last_evaluated_key,
                ..
            } = self
                .database_trait_provider()
                .read_forward(system_stream_name.clone(), cursor, 1_000)
                .await
                .map_err(StreamError::into_storage_enum)?;
            if items.is_empty() {
                break true;
            }

            for pointer_item in items {
                let next_safe_cursor = Some(pointer_item.id);
                let Some(record) = self
                    .decode_outbound_replication_record(
                        source_region,
                        pointer_item,
                        &include_tables,
                        &exclude_tables,
                        &table_schemas,
                    )
                    .await?
                else {
                    last_safe_cursor = next_safe_cursor;
                    cursor = next_safe_cursor;
                    continue;
                };
                let record_bytes = serde_json::to_vec(&record.mutation).map_err(|error| {
                    StorageError::internal(&format!(
                        "serialize outbound replication mutation for byte accounting: {error}"
                    ))
                })?;
                let would_exceed_mutations = records.len() >= mutation_limit;
                let would_exceed_bytes = !records.is_empty()
                    && buffered_bytes.saturating_add(record_bytes.len()) > byte_limit;
                if would_exceed_mutations || would_exceed_bytes {
                    return Ok(OutboundReplicationBatch {
                        records,
                        checkpoint_cursor: last_safe_cursor,
                        reached_end: false,
                    });
                }

                buffered_bytes = buffered_bytes.saturating_add(record_bytes.len());
                last_safe_cursor = next_safe_cursor;
                cursor = next_safe_cursor;
                records.push(record);
            }

            if last_evaluated_key.is_none() {
                break true;
            }
        };

        Ok(OutboundReplicationBatch {
            records,
            checkpoint_cursor: last_safe_cursor,
            reached_end,
        })
    }
}

pub fn peer_checkpoint_put_request(record: &PeerCheckpointRecord) -> StorageResult<PutItemRequest> {
    validate_region_name(&record.peer_region)?;
    Ok(PutItemRequest::new(
        Tables::sys_storage_replication(),
        payload_item(
            peer_checkpoint_pk(&record.peer_region),
            CHECKPOINT_SK,
            record,
        )?,
    ))
}

pub fn table_bootstrap_cursor_put_request(
    record: &TableBootstrapCursorRecord,
) -> StorageResult<PutItemRequest> {
    validate_region_name(&record.peer_region)?;
    Ok(PutItemRequest::new(
        Tables::sys_storage_replication(),
        payload_item(
            bootstrap_cursor_pk(&record.table_name, &record.peer_region),
            CHECKPOINT_SK,
            record,
        )?,
    ))
}

fn put_item_input_from_request(request: PutItemRequest) -> PutItemInput {
    PutItemInput {
        table_name: request.table_name,
        item: request.item.into(),
        condition_expression: request.condition_expression,
        expression_attribute_names: request.expression_attribute_names,
        expression_attribute_values: request.expression_attribute_values,
        return_values: request.return_values,
        return_old_on_condition_failure: false,
        aux_item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
    }
}

pub(crate) fn validate_replica_updates(updates: &[ReplicaUpdate]) -> StorageResult<()> {
    let _ = parse_replica_actions(updates)?;
    Ok(())
}

impl DatabaseManager {
    async fn decode_outbound_replication_record(
        &self,
        source_region: &str,
        pointer_item: StreamItem,
        include_tables: &HashSet<TableName>,
        exclude_tables: &HashSet<TableName>,
        table_schemas: &HashMap<TableName, Vec<storage_types::KeySchemaElement>>,
    ) -> StorageResult<Option<OutboundReplicationMutationRecord>> {
        if pointer_item.data_type != StreamDataType::StreamPointer {
            return Ok(None);
        }

        let stored_pointer = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(
            pointer_item.data.as_slice(),
        )
        .map_err(|error| {
            StorageError::internal(&format!(
                "decode outbound multi-region system stream pointer '{}': {error}",
                pointer_item.id
            ))
        })?;
        let table_name = stored_pointer.table_name().clone();
        if !include_tables.contains(&table_name) || exclude_tables.contains(&table_name) {
            return Ok(None);
        }

        let replication = stored_pointer
            .replication_metadata()
            .cloned()
            .unwrap_or_else(|| synthesize_local_replication_metadata(source_region, &pointer_item));
        if replication.write_source != ReplicationWriteSource::Local {
            return Ok(None);
        }

        let key_schema = table_schemas.get(&table_name).ok_or_else(|| {
            StorageError::internal(&format!(
                "missing cached key schema for outbound replication table '{}'",
                table_name
            ))
        })?;
        let stream_record = self
            .decode_stream_record_from_pointer(&stored_pointer, &pointer_item, key_schema)
            .await?;
        let Some(stream_record) = stream_record else {
            return Ok(None);
        };

        Ok(Some(OutboundReplicationMutationRecord {
            system_stream_cursor: pointer_item.id,
            mutation: ReplicationMutation {
                table_name,
                key: stream_record.keys.into(),
                new_image: stream_record.new_image,
                old_image: stream_record.old_image,
                metadata: replication,
            },
        }))
    }

    async fn decode_stream_record_from_pointer(
        &self,
        stored_pointer: &StoredStreamPointer,
        pointer_item: &StreamItem,
        key_schema: &[storage_types::KeySchemaElement],
    ) -> StorageResult<Option<StreamRecord>> {
        let item_images = match stored_pointer {
            StoredStreamPointer::Embedded { items, .. } => items
                .iter()
                .map(|item| StreamItem {
                    id: StreamItemId::from(stored_pointer.target_item_stream_version()),
                    stream_name: None,
                    data: item.data.clone(),
                    data_type: item.data_type,
                    created_at: pointer_item.created_at,
                })
                .collect::<Vec<_>>(),
            StoredStreamPointer::Pointer {
                stream_name,
                item_stream_version,
                ..
            } => {
                let exclusive_start_version =
                    item_stream_version.checked_increment().ok_or_else(|| {
                        StorageError::internal("outbound replication item stream version overflow")
                    })?;
                self.database_trait_provider()
                    .read_item_stream_backward_from_version(
                        stream_name.clone(),
                        exclusive_start_version,
                        2,
                    )
                    .await
                    .map_err(StreamError::into_storage_enum)?
                    .items
            }
        };

        decode_stream_record_from_item_images(item_images, key_schema)
    }
}

pub(crate) fn table_config_pk(table_name: &TableName) -> String {
    format!("table#{table_name}")
}

pub(crate) fn peer_checkpoint_pk(peer_region: &str) -> String {
    format!("peer#{peer_region}")
}

pub(crate) fn peer_status_pk(peer_region: &str) -> String {
    format!("peer-status#{peer_region}")
}

pub(crate) fn bootstrap_cursor_pk(table_name: &TableName, peer_region: &str) -> String {
    format!("bootstrap#{table_name}#{peer_region}")
}

fn validate_region_name(region_name: &str) -> StorageResult<()> {
    if region_name.trim().is_empty() {
        return Err(StorageError::validation(
            "region name must not be empty for multi-region control-plane records",
        ));
    }
    Ok(())
}

fn bootstrap_preflight_marker_peer_region(source_region: &str, manifest_id: &str) -> String {
    fn marker_safe(value: &str) -> String {
        value
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                    ch
                } else {
                    '_'
                }
            })
            .collect()
    }

    format!(
        "__bootstrap_preflight_{}_{}",
        marker_safe(source_region),
        marker_safe(manifest_id)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaActionKind {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicaActionRequest {
    kind: ReplicaActionKind,
    region_name: String,
}

fn parse_replica_actions(updates: &[ReplicaUpdate]) -> StorageResult<Vec<ReplicaActionRequest>> {
    let mut actions = Vec::with_capacity(updates.len());
    let mut seen_regions = std::collections::BTreeSet::new();

    for update in updates {
        let action = replica_action_from_update(update)?;
        validate_region_name(&action.region_name)?;
        if !seen_regions.insert(action.region_name.clone()) {
            return Err(StorageError::validation(format!(
                "duplicate replica update for region '{}'",
                action.region_name
            )));
        }
        actions.push(action);
    }

    Ok(actions)
}

fn replica_action_from_update(update: &ReplicaUpdate) -> StorageResult<ReplicaActionRequest> {
    let mut action = None;

    if let Some(create) = update.create.as_ref() {
        action = Some(ReplicaActionRequest {
            kind: ReplicaActionKind::Create,
            region_name: create.region_name.trim().to_string(),
        });
    }
    if let Some(update_action) = update.update.as_ref() {
        if action.is_some() {
            return Err(StorageError::validation(
                "each replica update must contain exactly one of Create, Update, or Delete",
            ));
        }
        action = Some(ReplicaActionRequest {
            kind: ReplicaActionKind::Update,
            region_name: update_action.region_name.trim().to_string(),
        });
    }
    if let Some(delete) = update.delete.as_ref() {
        if action.is_some() {
            return Err(StorageError::validation(
                "each replica update must contain exactly one of Create, Update, or Delete",
            ));
        }
        action = Some(ReplicaActionRequest {
            kind: ReplicaActionKind::Delete,
            region_name: delete.region_name.trim().to_string(),
        });
    }

    action.ok_or_else(|| {
        StorageError::validation(
            "each replica update must contain exactly one of Create, Update, or Delete",
        )
    })
}

fn apply_replica_action(
    config: &mut TableReplicationConfigRecord,
    action: ReplicaActionRequest,
) -> StorageResult<()> {
    match action.kind {
        ReplicaActionKind::Create => {
            if config
                .replicas
                .iter()
                .any(|replica| replica.region_name == action.region_name)
            {
                return Err(StorageError::validation(format!(
                    "replica '{}' already exists for table '{}'",
                    action.region_name, config.table_name
                )));
            }
            config.replicas.push(ReplicaDescription {
                region_name: action.region_name,
                replica_status: ReplicaStatus::Creating,
                replica_status_description: Some("Replica creation requested".to_string()),
                replica_inaccessible_date_time: None,
            });
        }
        ReplicaActionKind::Update => {
            let replica = config
                .replicas
                .iter_mut()
                .find(|replica| replica.region_name == action.region_name)
                .ok_or_else(|| {
                    StorageError::validation(format!(
                        "replica '{}' does not exist for table '{}'",
                        action.region_name, config.table_name
                    ))
                })?;
            replica.replica_status = ReplicaStatus::Updating;
            replica.replica_status_description = Some("Replica update requested".to_string());
            replica.replica_inaccessible_date_time = None;
        }
        ReplicaActionKind::Delete => {
            let replica = config
                .replicas
                .iter_mut()
                .find(|replica| replica.region_name == action.region_name)
                .ok_or_else(|| {
                    StorageError::validation(format!(
                        "replica '{}' does not exist for table '{}'",
                        action.region_name, config.table_name
                    ))
                })?;
            replica.replica_status = ReplicaStatus::Deleting;
            replica.replica_status_description = Some("Replica deletion requested".to_string());
            replica.replica_inaccessible_date_time = None;
        }
    }

    Ok(())
}

fn control_plane_key(
    pk: impl Into<String>,
    sk: impl Into<String>,
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        (PK_ATTR.to_string(), AttributeValue::S(pk.into())),
        (SK_ATTR.to_string(), AttributeValue::S(sk.into())),
    ])
}

fn payload_item<T: Serialize>(
    pk: impl Into<String>,
    sk: impl Into<String>,
    payload: &T,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let payload = serde_json::to_string(payload).map_err(|error| {
        StorageError::internal(&format!("serialize multi-region payload: {error}"))
    })?;
    Ok(HashMap::from([
        (PK_ATTR.to_string(), AttributeValue::S(pk.into())),
        (SK_ATTR.to_string(), AttributeValue::S(sk.into())),
        (PAYLOAD_ATTR.to_string(), AttributeValue::S(payload)),
    ]))
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<T> {
    let payload = item
        .get(PAYLOAD_ATTR)
        .ok_or_else(|| StorageError::internal("multi-region payload missing payload attribute"))?;
    let payload = payload.inner_str().map_err(|error| {
        StorageError::internal(&format!("multi-region payload is invalid: {error}"))
    })?;
    serde_json::from_str(payload).map_err(|error| {
        StorageError::internal(&format!("decode multi-region payload json: {error}"))
    })
}

pub(super) fn decode_stream_record_from_item_images(
    item_images: Vec<StreamItem>,
    key_schema: &[storage_types::KeySchemaElement],
) -> StorageResult<Option<StreamRecord>> {
    let new_image = item_images.first();
    let old_image = item_images.get(1);
    let Some(new_image) = new_image else {
        return Ok(None);
    };

    let decoded_new_image: Option<HashMap<String, AttributeValue>> =
        if new_image.data_type == StreamDataType::DeleteMarker {
            None
        } else {
            Some(
                storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(
                    new_image.data.as_slice(),
                )
                .map_err(|error| {
                    StorageError::internal(&format!(
                        "decode outbound replication new image '{}': {error}",
                        new_image.id
                    ))
                })?,
            )
        };
    let delete_marker_new_key = decode_delete_marker_key(new_image)?;
    let decoded_old_image: Option<HashMap<String, AttributeValue>> =
        if let Some(old_image) = old_image {
            if old_image.data_type == StreamDataType::DeleteMarker {
                None
            } else {
                Some(
                    storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(
                        old_image.data.as_slice(),
                    )
                    .map_err(|error| {
                        StorageError::internal(&format!(
                            "decode outbound replication old image '{}': {error}",
                            old_image.id
                        ))
                    })?,
                )
            }
        } else {
            None
        };
    let delete_marker_old_key = old_image
        .map(decode_delete_marker_key)
        .transpose()?
        .flatten();

    let item_for_key = decoded_old_image
        .as_ref()
        .or(decoded_new_image.as_ref())
        .or(delete_marker_old_key.as_ref())
        .or(delete_marker_new_key.as_ref())
        .ok_or_else(|| {
            StorageError::internal(
                "outbound replication pointer had no decodable old or new image for key extraction",
            )
        })?;
    let keys = item_for_key
        .iter()
        .filter(|(attribute_name, _)| {
            key_schema
                .iter()
                .any(|schema| schema.attribute_name == **attribute_name)
        })
        .map(|(attribute_name, attribute_value)| (attribute_name.clone(), attribute_value.clone()))
        .collect::<HashMap<_, _>>();

    if keys.len() != key_schema.len() {
        return Err(StorageError::internal(
            "outbound replication pointer was missing one or more key attributes",
        ));
    }

    Ok(Some(StreamRecord {
        cursor: None,
        keys,
        sequence_number: ItemStreamVersion::from(new_image.id).to_string(),
        old_image: decoded_old_image,
        new_image: decoded_new_image,
    }))
}

fn decode_delete_marker_key(
    item: &StreamItem,
) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
    if item.data_type != StreamDataType::DeleteMarker || item.data.is_empty() {
        return Ok(None);
    }

    storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(
        item.data.as_slice(),
    )
    .map(Some)
    .map_err(|error| {
        StorageError::internal(&format!(
            "decode outbound replication delete marker key '{}': {error}",
            item.id
        ))
    })
}

fn synthesize_local_replication_metadata(
    source_region: &str,
    pointer_item: &StreamItem,
) -> storage_types::ReplicationEventMetadata {
    storage_types::ReplicationEventMetadata {
        origin_region: source_region.to_string(),
        origin_sequence: pointer_item.id,
        origin_hlc: storage_types::ReplicationHybridLogicalClock {
            physical_ms: pointer_item.created_at,
            logical: 0,
        },
        origin_commit_ts: pointer_item.created_at,
        table_replica_epoch: 0,
        write_source: ReplicationWriteSource::Local,
    }
}
