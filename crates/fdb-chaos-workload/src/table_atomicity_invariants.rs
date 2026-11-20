use crate::{common::*, constants::*, imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) async fn read_current_item(
        &self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
        key: &str,
    ) -> storage_types::StorageResult<Option<storage_types::AttributeMap>> {
        let mut request = GetItemRequest::new(self.table_name(), self.key_attributes(key));
        request.consistent_read = Some(true);
        let response = provider.get_item(request).await?;
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
        // All clients finish start before check; no item writers remain. Read
        // every page, including partitions affected by unknown commit outcomes.
        let mut projections = Vec::new();
        for index in [None, Some(IndexName::new(GSI_INDEX_NAME))] {
            let is_index = index.is_some();
            let mut request: storage_types::ScanRequest = serde_json::from_value(
                serde_json::json!({"TableName": self.table_name(), "Limit": 7}),
            )
            .expect("valid audit scan");
            request.index_name = index;
            let mut items = std::collections::BTreeMap::new();
            loop {
                let page = provider
                    .scan(request.clone())
                    .await
                    .map_err(|error| storage_error_detail(&error))?;
                for item in page.items.unwrap_or_default() {
                    let attributes = item
                        .to_attribute_map()
                        .map_err(|error| storage_error_detail(&error))?;
                    if !is_index
                        && (!attributes.contains_key(GSI_CATEGORY_ATTR)
                            || !attributes.contains_key(GSI_SCORE_ATTR))
                    {
                        continue;
                    }
                    let key = string_attr(&attributes, "pk")?;
                    if items.insert(key, attributes).is_some() {
                        return Err("exact GSI scan duplicated a primary key".to_owned());
                    }
                }
                request.exclusive_start_key = page
                    .last_evaluated_key
                    .map(storage_types::ExclusiveStartKey::Key);
                if request.exclusive_start_key.is_none() {
                    break;
                }
            }
            projections.push(items);
        }
        if projections[0] != projections[1] {
            return Err(format!(
                "exact GSI/table projection mismatch expected={} actual={}",
                projections[0].len(),
                projections[1].len()
            ));
        }
        self.gsi_audit_count += 1;
        self.trace_check_progress("gsi_snapshot", "finish", self.gsi_audit_count);
        Ok(())
    }

    pub(crate) async fn check_trim_state_exactness(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        self.trace_check_progress("trim_state", "start", self.trim_audit_count);
        let due_before = TimestampMillis::now() + (73 * 60 * 60 * 1000);
        let markers =
            MaintenanceOperations::list_due_stream_trim_markers(provider, due_before, 1_000)
                .await
                .map_err(|err| storage_error_detail(&err))?;
        let table_name = self.table_name();
        let mut current_table_scopes = BTreeSet::new();
        for marker in markers
            .into_iter()
            .filter(|marker| marker.scope.table_name == table_name)
        {
            let mut states = MaintenanceOperations::load_stream_trim_states(
                provider,
                std::slice::from_ref(&marker.scope),
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
            // The provider retries a known-aborted read with a fresh snapshot;
            // an unknown outcome stays an error.
            let response = provider
                .get_stream_records(request)
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

    pub(crate) async fn check_canonical_stream_entries(
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
        let (rows, comparisons) = super::table_stream_audit::audit_stream_entries(
            self.driver(db)?,
            target,
            provider.metadata_cache(),
            provider,
        )
        .await
        .map_err(|error| storage_error_detail(&error))?;
        if rows == 0 || comparisons == 0 {
            return Err("canonical stream audit has no entry/image-chain witnesses".into());
        }
        self.stream_entry_audit_count += rows;
        self.stream_image_chain_comparison_count += comparisons;
        Ok(())
    }
}
