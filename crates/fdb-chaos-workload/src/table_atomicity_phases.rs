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
        let provider = match self.provider(db).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_invariant_error("setup", err);
                return;
            }
        };
        match provider.create_table(self.create_table_request()).await {
            Ok(_) => {}
            Err(err) if matches!(err.as_ref(), StorageEnum::TableAlreadyExists { .. }) => {}
            Err(err) => {
                self.trace_invariant_error("setup", storage_error_detail(&err));
            }
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
        if let Err(error) = self.capture_table_binding(&store).await {
            self.trace_invariant_error("start", error);
            return;
        }
        let provider = store;

        for sequence in 0..self.operation_count {
            let key =
                self.operation_key(u64::from(self.context.rnd()), u64::from(self.context.rnd()));
            let value = self.value(sequence);
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
        }
    }
}
