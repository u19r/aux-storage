use crate::{common::*, imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) async fn check_phase(&mut self, db: SimDatabase) {
        self.trace_phase("check");
        if !self.is_active_client() {
            if let Err(err) = self.write_artifacts() {
                self.trace_invariant_error("write_artifacts", err);
            }
            return;
        }
        if let Err(err) = self.write_artifacts() {
            self.trace_invariant_error("write_artifacts", err);
            return;
        }
        let provider = match self.provider(db).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_invariant_error("check", err);
                return;
            }
        };
        let manager = match DatabaseManager::new_with_mocks(Arc::clone(&provider)) {
            Ok(manager) => manager,
            Err(error) => {
                self.trace_invariant_error("check", storage_error_detail(&error));
                return;
            }
        };
        for key_index in 0..self.key_count {
            let key = self.owned_key(key_index);
            let expected = self.model.get(&key).map(str::to_string);
            let actual_item = match manager
                .get_item_map(self.table_name(), self.key_attributes(&key))
                .await
            {
                Ok(actual) => actual,
                Err(err) => {
                    self.record_anomaly(
                        AnomalyKind::OperationFailed,
                        key,
                        None,
                        None,
                        storage_error_detail(&err),
                    );
                    self.trace_invariant_error("check", storage_error_detail(&err));
                    return;
                }
            };
            let actual = match self.payload_from_item(&key, actual_item) {
                Ok(actual) => actual,
                Err(err) => {
                    self.record_anomaly(AnomalyKind::AuditValueMismatch, key, expected, None, err);
                    self.trace_invariant_error("check", "invalid item payload shape".to_string());
                    return;
                }
            };
            if self.possible_model.allows(&key, actual.as_deref()) {
                self.audit_count += 1;
            } else {
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
                        "possible-state model and request-surface read differ; possible_values={}",
                        self.possible_model.describe_key(&key)
                    ),
                );
                self.trace_invariant_error(
                    "check",
                    format!(
                        "model mismatch key={key} expected_present={} actual_present={}",
                        expected.is_some(),
                        actual.is_some()
                    ),
                );
                return;
            }
        }
        if let Err(err) = self.check_gsi_exactness(&manager).await {
            self.trace_invariant_error("check_gsi", err);
            return;
        }
        if self.client_id == 0
            && let Err(err) = self.check_trim_state_exactness(&provider).await
        {
            self.trace_invariant_error("check_trim", err);
            return;
        }
        if self.client_id == 0
            && let Err(err) = self.run_pre_expiry_stream_trim(&provider).await
        {
            self.record_anomaly(
                AnomalyKind::OperationFailed,
                format!("{}/stream-trim-job", self.table_name()),
                None,
                None,
                err.clone(),
            );
            self.trace_invariant_error("run_stream_trim", err);
            return;
        }
        if self.client_id == 0
            && let Err(err) = self.check_trim_state_exactness(&provider).await
        {
            self.trace_invariant_error("check_trim_after_execution", err);
            return;
        }
        if self.client_id == 0
            && let Err(err) = self.check_stream_records(&manager).await
        {
            self.trace_invariant_error("check_stream", err);
            return;
        }
        if self.client_id == 0
            && let Err(err) = self.check_direct_stream_pointers(&provider).await
        {
            self.trace_invariant_error("check_direct_stream_pointers", err);
            return;
        }
        for event in self.history.events().to_vec() {
            if event.kind != OperationKind::TransactWrite {
                continue;
            }
            let side_effect_key =
                self.transact_side_effect_key(&event.key, event.client_id, event.sequence);
            let expected = event.value.clone();
            let actual_item = match manager
                .get_item_map(self.table_name(), self.key_attributes(&side_effect_key))
                .await
            {
                Ok(actual) => actual,
                Err(err) => {
                    self.record_anomaly(
                        AnomalyKind::OperationFailed,
                        side_effect_key,
                        expected,
                        None,
                        storage_error_detail(&err),
                    );
                    self.trace_invariant_error("check", storage_error_detail(&err));
                    return;
                }
            };
            let actual = match self.payload_from_item(&side_effect_key, actual_item) {
                Ok(actual) => actual,
                Err(err) => {
                    self.record_anomaly(
                        AnomalyKind::AuditValueMismatch,
                        side_effect_key,
                        expected,
                        None,
                        err,
                    );
                    self.trace_invariant_error(
                        "check",
                        "invalid transaction side-effect payload shape".to_string(),
                    );
                    return;
                }
            };
            let side_effect_ok = match &event.outcome {
                OperationOutcome::Committed => actual == expected,
                OperationOutcome::Unknown { .. } => actual.is_none() || actual == expected,
                OperationOutcome::Failed { .. } | OperationOutcome::ConditionFailed { .. } => {
                    actual.is_none()
                }
            };
            if side_effect_ok {
                self.audit_count += 1;
                continue;
            }
            let kind = match (expected.as_ref(), actual.as_ref()) {
                (Some(_), None) => AnomalyKind::AuditMissing,
                (None, Some(_)) => AnomalyKind::AuditUnexpected,
                (Some(_), Some(_)) => AnomalyKind::AuditValueMismatch,
                (None, None) => AnomalyKind::AuditValueMismatch,
            };
            self.record_anomaly(
                kind,
                side_effect_key.clone(),
                expected.clone(),
                actual.clone(),
                match &event.outcome {
                    OperationOutcome::Committed => "committed transaction side-effect read differs",
                    OperationOutcome::Unknown { .. } => {
                        "unknown transaction side-effect must be absent or the expected value"
                    }
                    OperationOutcome::Failed { .. } | OperationOutcome::ConditionFailed { .. } => {
                        "failed transaction produced a side-effect"
                    }
                }
                .to_string(),
            );
            self.trace_invariant_error(
                "check",
                format!(
                    "transaction side-effect mismatch key={side_effect_key} expected_present={} \
                     actual_present={}",
                    expected.is_some(),
                    actual.is_some()
                ),
            );
            return;
        }
        if self.shared_key_count > 0 {
            let mut reads = Vec::new();
            for key_index in 0..self.shared_key_count {
                let key = self.shared_key(key_index);
                let actual_item = match manager
                    .get_item_map(self.table_name(), self.key_attributes(&key))
                    .await
                {
                    Ok(actual) => actual,
                    Err(err) => {
                        self.record_anomaly(
                            AnomalyKind::OperationFailed,
                            key,
                            None,
                            None,
                            storage_error_detail(&err),
                        );
                        self.trace_invariant_error("check", storage_error_detail(&err));
                        return;
                    }
                };
                let actual = match self.payload_from_item(&key, actual_item) {
                    Ok(actual) => actual,
                    Err(err) => {
                        self.record_anomaly(AnomalyKind::AuditValueMismatch, key, None, None, err);
                        self.trace_invariant_error(
                            "check",
                            "invalid shared-key payload shape".to_string(),
                        );
                        return;
                    }
                };
                reads.push(SharedKeyRead { key, actual });
            }
            self.shared_audit = Some(SharedKeyAudit::new(self.client_id, reads));
        }
        if let Err(err) = self.write_artifacts() {
            self.trace_invariant_error("write_artifacts", err);
        }
    }
}
