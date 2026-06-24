use crate::{common::*, imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) async fn check_gsi_exactness(
        &mut self,
        manager: &DatabaseManager,
    ) -> Result<(), String> {
        let partitions = self.gsi_seen_partitions.iter().cloned().collect::<Vec<_>>();
        for partition in partitions {
            if self.gsi_unclassified_partitions.contains(&partition) {
                continue;
            }
            let (items, _) = manager
                .query_index_map(
                    QueryIndexInput::builder()
                        .table_name(self.table_name())
                        .index_name(IndexName::new(GSI_INDEX_NAME))
                        .key_condition_expression(format!("{GSI_CATEGORY_ATTR} = :category"))
                        .expression_attribute_values(HashMap::from([(
                            ":category".to_string(),
                            AttributeValue::S(partition.clone()),
                        )]))
                        .build(),
                )
                .await
                .map_err(|err| storage_error_detail(&err))?;
            let mut actual = BTreeSet::new();
            for item in &items {
                actual.insert(self.gsi_entry_from_item(item)?);
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
        Ok(())
    }

    pub(crate) async fn check_trim_state_exactness(
        &mut self,
        provider: &FdbChaosProvider,
    ) -> Result<(), String> {
        let due_before = TimestampMillis::now() + (73 * 60 * 60 * 1000);
        let markers =
            StreamDurationTrimBackend::list_due_stream_trim_markers(provider, due_before, 1_000)
                .await
                .map_err(|err| storage_error_detail(&err))?;
        let table_name = self.table_name();
        let mut current_table_scopes = BTreeSet::new();
        let mut current_item_scopes = BTreeSet::new();
        for marker in markers
            .into_iter()
            .filter(|marker| marker.scope.table_name == table_name)
        {
            let state = StreamDurationTrimBackend::load_stream_trim_state(provider, &marker.scope)
                .await
                .map_err(|err| storage_error_detail(&err))?
                .ok_or_else(|| {
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
            match marker.scope.kind {
                storage_provider::StreamTrimScopeKind::Table => {
                    current_table_scopes.insert(marker.scope.scope_id);
                }
                storage_provider::StreamTrimScopeKind::Item => {
                    current_item_scopes.insert(marker.scope.scope_id);
                }
            }
        }
        self.write_trim_provider_snapshot(&current_table_scopes, &current_item_scopes)?;

        let classified = self.trim_model.classified_scopes();
        let expected_table_count = classified
            .iter()
            .filter(|scope| scope.kind == TrimScopeKind::Table)
            .count();
        let expected_item_scopes = classified
            .iter()
            .filter(|scope| scope.kind == TrimScopeKind::Item)
            .map(|scope| scope.id.clone())
            .collect::<BTreeSet<_>>();
        let unclassified_item_scopes = self
            .trim_model
            .unclassified_scopes()
            .into_iter()
            .filter(|scope| scope.kind == TrimScopeKind::Item)
            .map(|scope| scope.id)
            .collect::<BTreeSet<_>>();
        let unowned_item_budget =
            self.active_client_count.saturating_sub(1) as usize * self.key_count as usize;
        let unexpected_item_scopes = current_item_scopes
            .difference(&expected_item_scopes)
            .filter(|scope| !unclassified_item_scopes.contains(*scope))
            .count();
        if current_table_scopes.len() != expected_table_count
            || !expected_item_scopes.is_subset(&current_item_scopes)
            || unexpected_item_scopes > unowned_item_budget
        {
            self.record_anomaly(
                AnomalyKind::AuditValueMismatch,
                format!("{}/stream-trim", table_name),
                Some(format!(
                    "table_count={expected_table_count}, item_scopes={expected_item_scopes:?}"
                )),
                Some(format!(
                    "table_scopes={current_table_scopes:?}, item_scopes={current_item_scopes:?}"
                )),
                "stream trim due-marker/state counts differ from expected model".to_string(),
            );
            return Err(format!(
                "stream trim mismatch expected_table_count={} actual_table_count={} \
                 missing_owned_item_count={} unexpected_item_count={unexpected_item_scopes}",
                expected_table_count,
                current_table_scopes.len(),
                expected_item_scopes
                    .difference(&current_item_scopes)
                    .count(),
            ));
        }

        self.trim_audit_count += (current_table_scopes.len() + expected_item_scopes.len()) as u64;
        Ok(())
    }

    pub(crate) async fn run_pre_expiry_stream_trim(
        &mut self,
        provider: &FdbChaosProvider,
    ) -> Result<(), String> {
        provider
            .run_job(STREAM_TRIM_JOB)
            .await
            .map_err(|err| storage_error_detail(&err))?;
        self.trim_execution_count = self.trim_execution_count.saturating_add(1);
        Ok(())
    }

    pub(crate) async fn check_stream_records(
        &mut self,
        manager: &DatabaseManager,
    ) -> Result<(), String> {
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
            let response = manager
                .get_stream_records_for_table_name(
                    &self.table_name(),
                    page_token.as_deref(),
                    Some(1_000),
                )
                .await
                .map_err(|err| storage_error_detail(&err))?;
            for record in response.records {
                let Some(new_image) = record.new_image else {
                    continue;
                };
                let key = string_attr(&new_image, "pk")?;
                if self.owned_key_index(&key).is_none() {
                    continue;
                }
                let value = string_attr(&new_image, "payload")?;
                actual.insert((key, value));
            }
            let Some(next_page_token) = response.last_evaluated_key else {
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
        Ok(())
    }

    pub(crate) async fn check_direct_stream_pointers(
        &mut self,
        provider: &FdbChaosProvider,
    ) -> Result<(), String> {
        let audit = provider
            .audit_table_stream_pointer_integrity(&self.table_name(), 10_000)
            .await
            .map_err(|err| storage_error_detail(&err))?;
        if audit.anomaly_count() > 0 {
            self.record_anomaly(
                AnomalyKind::AuditValueMismatch,
                format!("{}/direct-stream-pointers", self.table_name()),
                Some(
                    "missing_system=0, missing_table_pointer=0, missing_item_stream=0, \
                     missing_item_pointer=0, orphaned_table_pointer=0"
                        .to_string(),
                ),
                Some(format!("{audit:?}")),
                "direct compact stream pointer scan found missing pointer side effects".to_string(),
            );
            return Err(format!(
                "direct stream pointer mismatch missing_system={} missing_table_pointer={} \
                 missing_item_stream={} missing_item_pointer={} orphaned_table_pointer={}",
                audit.missing_system_rows,
                audit.missing_table_pointer_indexes,
                audit.missing_item_stream_rows,
                audit.missing_item_pointer_indexes,
                audit.orphaned_table_pointer_indexes,
            ));
        }
        self.direct_stream_pointer_audit_count += audit.table_stream_rows;
        self.direct_stream_pointer_decoupled_target_count += audit.decoupled_pointer_target_rows;
        Ok(())
    }
}
