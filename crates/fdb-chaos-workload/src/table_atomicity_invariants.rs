use crate::{common::*, constants::*, imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) async fn read_current_item(
        &self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
        key: &str,
    ) -> storage_types::StorageResult<Option<storage_types::AttributeMap>> {
        let mut request = GetItemRequest::new(self.table_name(), self.key_attributes(key));
        request.consistent_read = Some(true);
        let response = storage_common::retry::retry_native_conflicts(
            provider,
            storage_provider::DYNAMO_WRITE_RETRY,
            || provider.get_item(request.clone()),
        )
        .await?;
        response
            .item
            .as_ref()
            .map(WireItem::to_attribute_map)
            .transpose()
    }

    pub(crate) async fn check_gsi_exactness(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        let partitions = self.gsi_seen_partitions.iter().cloned().collect::<Vec<_>>();
        self.trace_check_progress("gsi_snapshot", "start", 0);
        for (partition_ordinal, partition) in partitions.into_iter().enumerate() {
            if self.gsi_unclassified_partitions.contains(&partition) {
                continue;
            }
            self.trace_check_progress("gsi_partition_read", "start", partition_ordinal as u64);
            let request = QueryRequest::builder()
                .table_name(self.table_name())
                .index_name(Some(IndexName::new(GSI_INDEX_NAME)))
                .key_condition_expression(format!("{GSI_CATEGORY_ATTR} = :category"))
                .expression_attribute_values(Some(
                    HashMap::from([(
                        ":category".to_string(),
                        AttributeValue::S(partition.clone()),
                    )])
                    .into(),
                ))
                .build();
            let page = storage_common::retry::retry_native_conflicts(
                provider,
                storage_provider::DYNAMO_WRITE_RETRY,
                || provider.query(request.clone()),
            )
            .await
            .map_err(|err| storage_error_detail(&err))?;
            let items = page.items.unwrap_or_default();
            self.trace_check_progress("gsi_partition_read", "finish", partition_ordinal as u64);
            let mut actual = BTreeSet::new();
            for item in &items {
                actual.insert(
                    self.gsi_entry_from_item(
                        &item
                            .to_attribute_map()
                            .map_err(|error| storage_error_detail(&error))?,
                    )?,
                );
            }
            let expected = self.gsi_model.entries_for_partition(&partition);
            if actual == expected {
                self.gsi_audit_count += 1;
                continue;
            }
            let key = format!("{GSI_INDEX_NAME}/{partition}");
            self.record_anomaly(
                AnomalyKind::AuditValueMismatch,
                key.clone(),
                Some(format!("{expected:?}")),
                Some(format!("{actual:?}")),
                "GSI query result differs from expected indexed table model".to_string(),
            );
            return Err(format!(
                "GSI mismatch partition={partition} expected_count={} actual_count={}",
                expected.len(),
                actual.len()
            ));
        }
        self.trace_check_progress("gsi_snapshot", "finish", self.gsi_audit_count);
        Ok(())
    }

    pub(crate) async fn check_trim_state_exactness(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        self.trace_check_progress("trim_state", "start", self.trim_audit_count);
        let due_before = TimestampMillis::now() + (73 * 60 * 60 * 1000);
        let markers = storage_common::retry::retry_native_conflicts(
            provider,
            storage_provider::DYNAMO_WRITE_RETRY,
            || StreamRetentionRepository::list_due_stream_trim_markers(provider, due_before, 1_000),
        )
        .await
        .map_err(|err| storage_error_detail(&err))?;
        let table_name = self.table_name();
        let mut current_table_scopes = BTreeSet::new();
        for marker in markers
            .into_iter()
            .filter(|marker| marker.scope.table_name == table_name)
        {
            let mut states = storage_common::retry::retry_native_conflicts(
                provider,
                storage_provider::DYNAMO_WRITE_RETRY,
                || {
                    StreamRetentionRepository::load_stream_trim_states(
                        provider,
                        std::slice::from_ref(&marker.scope),
                    )
                },
            )
            .await
            .map_err(|err| storage_error_detail(&err))?;
            let state = states.pop().flatten().ok_or_else(|| {
                format!(
                    "stream trim marker has no state scope={} policy_version={}",
                    marker.scope.scope_id, marker.policy_version
                )
            })?;
            if !state.marker_matches(&marker) {
                continue;
            }
            if !state.has_finite_due_work() {
                return Err(format!(
                    "current stream trim marker points at non-finite state scope={} \
                     policy_version={}",
                    marker.scope.scope_id, marker.policy_version
                ));
            }
            current_table_scopes.insert(marker.scope.scope_id);
        }
        self.write_trim_provider_snapshot(&current_table_scopes)?;
        let expected = self
            .table_binding
            .iter()
            .map(|table| storage_provider::table_retention_scope_id(table.history_id))
            .collect::<BTreeSet<_>>();
        if current_table_scopes != expected {
            return Err(format!(
                "table retention scopes differ: expected={expected:?} \
                 actual={current_table_scopes:?}"
            ));
        }
        self.trim_audit_count += current_table_scopes.len() as u64;
        self.trace_check_progress("trim_state", "finish", self.trim_audit_count);
        Ok(())
    }

    pub(crate) async fn run_pre_expiry_stream_trim(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        self.trace_check_progress("trim_job", "start", self.trim_execution_count);
        let now = TimestampMillis::now();
        StreamDurationTrimWorker::new(provider, StreamDurationTrimConfig::default())
            .run_due_page(now, now)
            .await
            .map_err(|err| storage_error_detail(&err))?;
        self.trim_execution_count = self.trim_execution_count.saturating_add(1);
        self.trace_check_progress("trim_job", "finish", self.trim_execution_count);
        Ok(())
    }

    pub(crate) async fn check_stream_records(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        self.trace_check_progress("stream_records", "start", 0);
        let expected = self
            .history
            .events()
            .iter()
            .filter(|event| event.outcome == OperationOutcome::Committed)
            .filter(|event| {
                matches!(
                    event.kind,
                    OperationKind::Put
                        | OperationKind::PutIfAbsent
                        | OperationKind::Update
                        | OperationKind::TransactWrite
                )
            })
            .filter(|event| self.owned_key_index(&event.key).is_some())
            .filter_map(|event| {
                event
                    .value
                    .as_ref()
                    .map(|value| (event.key.clone(), value.clone()))
            })
            .collect::<BTreeSet<_>>();
        if expected.is_empty() {
            self.trace_check_progress("stream_records", "finish", 0);
            return Ok(());
        }

        let mut actual = BTreeSet::new();
        let mut page_token = None;
        let mut page_count = 0_u64;
        loop {
            page_count += 1;
            if page_count > 100 {
                return Err("stream audit exceeded 100 pages".to_string());
            }
            let request = storage_types::GetStreamRecordsRequest::table(
                self.table_name(),
                page_token,
                Some(1_000),
            );
            // An audit needs a completed observation. Retry a known-aborted
            // read with a fresh snapshot; never accept an absent observation.
            let response = storage_common::retry::retry_native_conflicts(
                provider,
                storage_provider::DYNAMO_WRITE_RETRY,
                || provider.get_stream_records(request.clone()),
            )
            .await
            .map_err(|err| storage_error_detail(&err))?;
            // Active streams keep a qualified tail checkpoint on empty pages.
            // Emptiness completes this finite replay; token presence does not.
            if response.records().is_empty() {
                break;
            }
            for record in response.records() {
                let Some(new_image) = record.item.as_ref() else {
                    continue;
                };
                let key = string_attr(new_image, "pk")?;
                if self.owned_key_index(&key).is_none() {
                    continue;
                }
                let value = string_attr(new_image, "payload")?;
                actual.insert((key, value));
            }
            let Some(next_page_token) = response.last_evaluated_key().map(str::to_owned) else {
                break;
            };
            page_token = Some(next_page_token);
        }

        if !expected.is_subset(&actual) {
            self.record_anomaly(
                AnomalyKind::AuditValueMismatch,
                format!("{}/stream-records", self.table_name()),
                Some(format!("{expected:?}")),
                Some(format!("{actual:?}")),
                "committed owned writes are not all visible through table stream records"
                    .to_string(),
            );
            return Err(format!(
                "stream record mismatch missing_owned_write_count={}",
                expected.difference(&actual).count()
            ));
        }

        self.stream_audit_count += expected.len() as u64;
        self.trace_check_progress("stream_records", "finish", self.stream_audit_count);
        Ok(())
    }

    pub(crate) async fn check_direct_stream_pointers(
        &mut self,
        db: SimDatabase,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        use storage_provider::DynamoPort;
        let table = self
            .table_binding
            .as_ref()
            .ok_or("captured table binding is absent")?;
        let target = storage_types::TableHistoryTarget {
            table_name: self.table_name(),
            history_id: table.history_id,
        };
        let (rows, decoupled) = super::table_stream_audit::audit_stream_pointers(
            self.driver(db),
            target,
            provider.metadata_cache(),
            provider,
        )
        .await
        .map_err(|error| storage_error_detail(&error))?;
        self.direct_stream_pointer_audit_count += rows;
        self.direct_stream_pointer_decoupled_target_count += decoupled;
        Ok(())
    }
}
