use crate::{
    common::*, constants::*, imports::*, table_atomicity::*,
    table_atomicity_recording::FailedOperation,
};

impl TableAtomicityWorkload {
    pub(crate) async fn setup_phase(&mut self, db: SimDatabase) {
        self.trace_phase("setup");
        if self.client_id != 0 {
            return;
        }
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_invariant_error("setup", err);
                return;
            }
        };
        let mut request = self.create_table_request();
        request.global_secondary_indexes = None;
        request
            .attribute_definitions
            .retain(|definition| definition.attribute_name == "pk");
        match provider.create_table(request).await {
            Ok(_) => {}
            Err(err) if matches!(err.as_ref(), StorageEnum::TableAlreadyExists { .. }) => {}
            Err(err) => {
                self.trace_invariant_error("setup", storage_error_detail(&err));
            }
        }
        // Reserved keys do not belong to any client history. They guarantee a
        // nonempty source and multiple protected pages before ordinary writes.
        for ordinal in 0..16 {
            let item: storage_types::PutItemRequest = serde_json::from_value(serde_json::json!({
                "TableName": self.table_name(),
                "Item": {"pk": {"S": format!("build-seed/{ordinal:03}")},
                    "payload": {"S": "seed"},
                    GSI_CATEGORY_ATTR: {"S": "build-seed"}, GSI_SCORE_ATTR: {"N": ordinal.to_string()}}
            })).expect("valid build seed");
            if let Err(error) = provider.put_item(item).await {
                self.trace_invariant_error("seed_build", storage_error_detail(&error));
                return;
            }
        }
        if self.should_cache_writes
            && let Err(error) = self.qualify_cached_write(&provider).await
        {
            self.trace_invariant_error("cached_write", storage_error_detail(&error));
            return;
        }
        if self.should_qualify_catalog_stream_reads
            && let Err(error) = self.qualify_catalog_stream_reads(db, &provider).await
        {
            self.trace_invariant_error("catalog_stream_reads", storage_error_detail(&error));
        }
    }

    pub(crate) async fn start_phase(&mut self, db: SimDatabase) {
        self.trace_phase("start");
        if !self.is_active_client() {
            if let Err(err) = self.write_artifacts() {
                self.trace_invariant_error("write_artifacts", err);
            }
            return;
        }
        let store = match self.provider(db).await {
            Ok(store) => store,
            Err(error) => {
                self.trace_invariant_error("start", error);
                return;
            }
        };
        let clock = self.clock.as_ref().expect("simulation phase clock");
        match read_initial_table_binding(&store, self.table_name(), clock.as_ref()).await {
            Ok(binding) => self.table_binding = Some(binding),
            Err(error) => {
                self.trace_invariant_error("start", storage_error_detail(&error));
                return;
            }
        }
        let provider = store;
        if self.client_id == 0 {
            let expected = self.create_table_request();
            self.gsi_start_request = Some(expected.clone());
            match submit_index(&provider, expected).await {
                Ok(()) => {
                    self.gsi_build_started = 1;
                    self.gsi_start_request = None;
                }
                Err(error) if error.is_commit_unknown() => {
                    self.gsi_start_unknown += 1;
                    self.trace_check_progress(
                        "online_build_start_unknown",
                        "unconfirmed",
                        self.gsi_start_unknown,
                    );
                }
                Err(error) if is_known_abort(&error) => {
                    self.trace_check_progress("online_build_conflict", "start", 1);
                }
                Err(error) => {
                    self.trace_invariant_error("start_build", storage_error_detail(&error));
                    return;
                }
            }
            if let Err(error) = self.advance_online_build(&provider).await {
                self.trace_invariant_error("advance_build", error);
                return;
            }
        }

        for sequence in 0..self.operation_count {
            let key =
                self.operation_key(u64::from(self.context.rnd()), u64::from(self.context.rnd()));
            let value = self.value(sequence);
            let committed_before = self.history.committed_count();
            match self.context.rnd() % 6 {
                0 => {
                    let started_at_sim_us = self.sim_time_us();
                    let result = provider
                        .put_item(
                            storage_types::PutItemRequest::builder()
                                .table_name(self.table_name())
                                .item(
                                    WireItem::try_from(self.item(&key, &value))
                                        .expect("valid workload item"),
                                )
                                .condition_expression(None)
                                .build(),
                        )
                        .await;
                    match result {
                        Ok(_) => self.record_success(
                            started_at_sim_us,
                            sequence,
                            OperationKind::Put,
                            key,
                            Some(value),
                        ),
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::Put,
                                key,
                                value: Some(value),
                                error: err,
                            });
                        }
                    }
                }
                1 => {
                    let started_at_sim_us = self.sim_time_us();
                    let result = provider
                        .put_item(
                            storage_types::PutItemRequest::builder()
                                .table_name(self.table_name())
                                .item(
                                    WireItem::try_from(self.item(&key, &value))
                                        .expect("valid workload item"),
                                )
                                .condition_expression(Some("attribute_not_exists(pk)".to_string()))
                                .build(),
                        )
                        .await;
                    match result {
                        Ok(_) => self.record_success(
                            started_at_sim_us,
                            sequence,
                            OperationKind::PutIfAbsent,
                            key,
                            Some(value),
                        ),
                        Err(err) if is_condition_failure(&err) => {
                            self.record_condition_failed(
                                started_at_sim_us,
                                sequence,
                                key,
                                value,
                                storage_error_detail(&err),
                            );
                        }
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::PutIfAbsent,
                                key,
                                value: Some(value),
                                error: err,
                            });
                        }
                    }
                }
                2 => {
                    let next = format!("{value}-updated");
                    let started_at_sim_us = self.sim_time_us();
                    let mut expression_attribute_values =
                        HashMap::from([(":payload".to_string(), AttributeValue::S(next.clone()))]);
                    let update_expression =
                        if let Some((category, score)) = self.gsi_projection(&key, &next) {
                            expression_attribute_values
                                .insert(":category".to_string(), AttributeValue::S(category));
                            expression_attribute_values
                                .insert(":score".to_string(), AttributeValue::N(score));
                            format!(
                                "SET payload = :payload, {GSI_CATEGORY_ATTR} = :category, \
                                 {GSI_SCORE_ATTR} = :score"
                            )
                        } else {
                            "SET payload = :payload".to_string()
                        };
                    let result = provider
                        .update_item(
                            storage_types::UpdateItemRequest::builder()
                                .table_name(self.table_name())
                                .key(self.key_attributes(&key))
                                .update_expression(update_expression)
                                .expression_attribute_values(Some(
                                    expression_attribute_values.into(),
                                ))
                                .build(),
                        )
                        .await;
                    match result {
                        Ok(_) => self.record_success(
                            started_at_sim_us,
                            sequence,
                            OperationKind::Update,
                            key,
                            Some(next),
                        ),
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::Update,
                                key,
                                value: Some(next),
                                error: err,
                            });
                        }
                    }
                }
                3 => {
                    let started_at_sim_us = self.sim_time_us();
                    let result = provider
                        .delete_item(storage_types::DeleteItemRequest::new(
                            self.table_name(),
                            self.key_attributes(&key),
                        ))
                        .await;
                    match result {
                        Ok(_) => self.record_success(
                            started_at_sim_us,
                            sequence,
                            OperationKind::Delete,
                            key,
                            None,
                        ),
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::Delete,
                                key,
                                value: None,
                                error: err,
                            });
                        }
                    }
                }
                4 => {
                    let started_at_sim_us = self.sim_time_us();
                    let result = self.read_current_item(&provider, &key).await;
                    match result {
                        Ok(item) => {
                            let actual = match self.payload_from_item(&key, item) {
                                Ok(actual) => actual,
                                Err(err) => {
                                    self.record_anomaly(
                                        AnomalyKind::AuditValueMismatch,
                                        key,
                                        None,
                                        None,
                                        err,
                                    );
                                    self.trace_invariant_error(
                                        "read",
                                        "invalid read payload shape".to_string(),
                                    );
                                    return;
                                }
                            };
                            let expected = self.model.get(&key).map(str::to_string);
                            self.record_success(
                                started_at_sim_us,
                                sequence,
                                OperationKind::Read,
                                key.clone(),
                                actual.clone(),
                            );
                            if !key.starts_with("shared/")
                                && !self.possible_model.allows(&key, actual.as_deref())
                            {
                                let kind = match (expected.as_ref(), actual.as_ref()) {
                                    (Some(_), None) => AnomalyKind::AuditMissing,
                                    (None, Some(_)) => AnomalyKind::AuditUnexpected,
                                    (Some(_), Some(_)) => AnomalyKind::AuditValueMismatch,
                                    (None, None) => AnomalyKind::AuditValueMismatch,
                                };
                                self.record_anomaly(
                                    kind,
                                    key.clone(),
                                    expected.clone(),
                                    actual.clone(),
                                    format!(
                                        "read-your-writes possible-state model and \
                                         request-surface read differ; possible_values={}",
                                        self.possible_model.describe_key(&key)
                                    ),
                                );
                                self.trace_invariant_error(
                                    "read",
                                    format!(
                                        "model mismatch key={key} expected_present={} \
                                         actual_present={}",
                                        expected.is_some(),
                                        actual.is_some()
                                    ),
                                );
                                return;
                            }
                        }
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::Read,
                                key,
                                value: None,
                                error: err,
                            });
                        }
                    }
                }
                _ => {
                    let second_key = self.transact_side_effect_key(&key, self.client_id, sequence);
                    let started_at_sim_us = self.sim_time_us();
                    let result = provider
                        .transact_write_items(TransactWriteItemsRequest {
                            transact_items: vec![
                                TransactWriteItem {
                                    put: Some(TransactPutRequest {
                                        table_name: self.table_name(),
                                        item: WireItem::try_from(self.item(&key, &value))
                                            .expect("valid workload item"),
                                        indexers: None,
                                        condition_expression: None,
                                        expression_attribute_names: None,
                                        expression_attribute_values: None,
                                        return_values_on_condition_check_failure: None,
                                    }),
                                    update: None,
                                    delete: None,
                                    condition_check: None,
                                },
                                TransactWriteItem {
                                    put: Some(TransactPutRequest {
                                        table_name: self.table_name(),
                                        item: WireItem::try_from(self.item(&second_key, &value))
                                            .expect("valid workload item"),
                                        indexers: None,
                                        condition_expression: None,
                                        expression_attribute_names: None,
                                        expression_attribute_values: None,
                                        return_values_on_condition_check_failure: None,
                                    }),
                                    update: None,
                                    delete: None,
                                    condition_check: None,
                                },
                            ],
                            client_request_token: None,
                            return_consumed_capacity: None,
                            return_item_collection_metrics: None,
                        })
                        .await;
                    match result {
                        Ok(_) => {
                            self.record_success(
                                started_at_sim_us,
                                sequence,
                                OperationKind::TransactWrite,
                                key,
                                Some(value),
                            );
                        }
                        Err(err) => {
                            self.record_failure(FailedOperation {
                                started_at_sim_us,
                                sequence,
                                kind: OperationKind::TransactWrite,
                                key,
                                value: Some(value),
                                error: err,
                            });
                        }
                    }
                }
            }
            if self.client_id == 0 && self.gsi_build_active == 0 {
                if self.gsi_build_started > 0
                    && self.history.committed_count() > committed_before
                    && self.history.events().last().is_some_and(|event| {
                        event.value.is_some()
                            && matches!(
                                event.kind,
                                OperationKind::Put
                                    | OperationKind::PutIfAbsent
                                    | OperationKind::Update
                                    | OperationKind::TransactWrite
                            )
                    })
                {
                    self.gsi_build_writes += 1;
                }
                // Keep the build unfinished until an ordinary write commits.
                if (self.gsi_build_started == 0 || self.gsi_build_writes > 0)
                    && let Err(error) = self.advance_online_build(&provider).await
                {
                    self.trace_invariant_error("advance_build", error);
                    return;
                }
            }
        }
        if self.client_id == 0 {
            // Each operation can add a primary row and a transaction side
            // effect. Include setup rows and lifecycle transitions.
            let page_bound = self
                .operation_count
                .saturating_mul(self.active_client_count as u64)
                .saturating_mul(2)
                .saturating_add(20);
            for _ in 0..page_bound {
                if self.gsi_build_active > 0 {
                    break;
                }
                if let Err(error) = self.advance_online_build(&provider).await {
                    self.trace_invariant_error("finish_build", error);
                    return;
                }
            }
            if self.gsi_build_active == 0 || self.gsi_build_pages == 0 || self.gsi_build_writes == 0
            {
                self.trace_invariant_error(
                    "build_witness",
                    "online build lacks required witnesses".to_owned(),
                );
            }
        }
    }

    async fn advance_online_build(
        &mut self,
        provider: &kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>,
    ) -> Result<(), String> {
        use storage_provider::maintenance::GsiBuildRequest;
        use storage_types::maintenance::StepResult;
        // A known-aborted observation or page consumes this invocation. The
        // existing bounded caller schedules fresh observations and decisions.
        if self.gsi_build_started == 0 {
            let expected = self
                .gsi_start_request
                .clone()
                .ok_or("unconfirmed start request is absent")?;
            let original = self
                .table_binding
                .as_ref()
                .ok_or("original table binding is absent")?;
            match start_index(provider, original, expected).await {
                Ok(result) => {
                    self.gsi_build_started = 1;
                    self.gsi_start_request = None;
                    if self.gsi_start_unknown > 0 {
                        self.gsi_start_recovered = 1;
                        self.trace_check_progress(
                            "online_build_start_recovered",
                            match result {
                                IndexStartResult::Existing => "existing",
                                IndexStartResult::Submitted => "submitted",
                            },
                            1,
                        );
                    }
                }
                Err(error) if error.is_commit_unknown() => {
                    self.gsi_start_unknown += 1;
                    self.trace_check_progress(
                        "online_build_start_unknown",
                        "unconfirmed",
                        self.gsi_start_unknown,
                    );
                    return Ok(());
                }
                Err(error) if is_known_abort(&error) => {
                    self.trace_check_progress("online_build_conflict", "start", 1);
                    return Ok(());
                }
                Err(error) => return Err(storage_error_detail(&error)),
            }
        }
        if let Err(error) = self.capture_table_binding(provider).await {
            if is_known_abort(&error) {
                self.trace_check_progress("online_build_conflict", "before_binding", 1);
                return Ok(());
            }
            return Err(storage_error_detail(&error));
        }
        let before = self
            .table_binding
            .as_ref()
            .and_then(|table| {
                table
                    .binding
                    .gsi(&IndexName::new(GSI_INDEX_NAME))
                    .map(|index| index.phase)
            })
            .ok_or("online build index binding is absent")?;
        let request = GsiBuildRequest {
            destination: b"aux-storage:simulation:gsi:v1"
                .to_vec()
                .try_into()
                .expect("destination"),
            job: self.gsi_build_job.into(),
            fence: std::num::NonZeroU64::MIN,
            page_limit: 1usize.try_into().expect("page limit"),
            captured_at: TimestampMillis::now(),
        };
        match provider.advance_gsi_build(request).await {
            Ok(StepResult::Advanced(())) => {
                if before == storage_types::GsiPhase::Filling {
                    self.gsi_build_pages += 1;
                }
            }
            Ok(StepResult::Complete) => {}
            Ok(StepResult::Superseded) => {
                return Err("simulation build authority superseded".to_owned());
            }
            Err(error) if error.is_commit_unknown() => {
                // The next scheduled invocation reads durable progress and
                // source rows again. An ambiguous page never reuses its writes.
                self.gsi_build_unknown += 1;
            }
            Err(error) if is_known_abort(&error) => {
                self.trace_check_progress("online_build_conflict", "advance", 1);
                return Ok(());
            }
            Err(error) => return Err(storage_error_detail(&error)),
        }
        if let Err(error) = self.capture_table_binding(provider).await {
            if is_known_abort(&error) {
                self.trace_check_progress("online_build_conflict", "after_binding", 1);
                return Ok(());
            }
            return Err(storage_error_detail(&error));
        }
        if self.table_binding.as_ref().and_then(|table| {
            table
                .binding
                .gsi(&IndexName::new(GSI_INDEX_NAME))
                .map(|index| index.phase)
        }) == Some(storage_types::GsiPhase::Active)
        {
            self.gsi_build_active = 1;
        }
        self.trace_check_progress("online_build_pages", "finish", self.gsi_build_pages);
        Ok(())
    }
}

/// Observe durable metadata before resubmitting an unconfirmed index start.
/// The workload has one metadata writer, so a matching index in the original
/// table incarnation is the result of its intended request.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IndexStartResult {
    Existing,
    Submitted,
}

pub(crate) async fn start_index<B: kv_backend::KvBackend + 'static>(
    provider: &kv_backend::KvStore<B>,
    original: &storage_types::BoundTableInfo,
    expected: storage_types::CreateTableRequest,
) -> storage_types::StorageResult<IndexStartResult> {
    let cache = storage_provider::TableMetadataCache::new();
    let name = expected.table_name.clone();
    let current = storage_provider::DynamoPort::with_attempt(
        provider,
        storage_types::AttemptPlan::new(
            storage_types::AttemptMode::ReadOnly,
            storage_types::DeadlineClass::PointRead,
        )
        .start(storage_types::AttemptDeadlineSource::attempt_deadlines(
            provider,
        )),
        move |attempt| {
            Box::pin(async move {
                storage_provider::resolve_table_in_attempt(attempt, &cache, &name)
                    .await
                    .map(|(table, _)| table)
            })
        },
    )
    .await?;
    if current.history_id != original.history_id || current.key_schema != original.key_schema {
        return Err(storage_types::StorageError::internal(
            "index start table incarnation changed",
        ));
    }
    let desired = expected
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| indexes.first())
        .expect("workload declares one index");
    let definition = current
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| {
            indexes
                .iter()
                .find(|index| index.index_name == desired.index_name)
        });
    let binding = current.binding.gsi(&desired.index_name);
    match (definition, binding) {
        (Some(actual), Some(_))
            if actual.key_schema == desired.key_schema
                && actual.projection == desired.projection
                && expected
                    .attribute_definitions
                    .iter()
                    .all(|wanted| current.attribute_definitions.contains(wanted)) =>
        {
            Ok(IndexStartResult::Existing)
        }
        (None, None) => submit_index(provider, expected)
            .await
            .map(|()| IndexStartResult::Submitted),
        _ => Err(storage_types::StorageError::internal(
            "index start durable definition differs from request",
        )),
    }
}

pub(crate) async fn submit_index<B: kv_backend::KvBackend + 'static>(
    provider: &kv_backend::KvStore<B>,
    expected: storage_types::CreateTableRequest,
) -> storage_types::StorageResult<()> {
    let mut request: storage_types::UpdateTableRequest =
        serde_json::from_value(serde_json::json!({"TableName": expected.table_name}))
            .expect("valid table update");
    request.attribute_definitions = Some(expected.attribute_definitions);
    request.global_secondary_index_updates = expected.global_secondary_indexes.map(|indexes| {
        indexes
            .into_iter()
            .map(|create| storage_types::GlobalSecondaryIndexUpdate {
                create: Some(create),
                update: None,
                delete: None,
            })
            .collect()
    });
    provider.update_table(request).await.map(|_| ())
}

/// A conflict or an expired operation deadline before commit submission
/// leaves no effects; the bounded caller schedules fresh observations.
fn is_known_abort(error: &storage_types::StorageError) -> bool {
    matches!(
        error.attempt_kind(),
        Some(
            storage_types::AttemptKind::Conflict
                | storage_types::AttemptKind::Unavailable
                | storage_types::AttemptKind::Deadline
        )
    )
}
