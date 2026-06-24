use crate::{imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosTableAtomicityPhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
                "OperationCount" => self.operation_count,
                "KeyCount" => self.key_count,
                "SharedKeyCount" => self.shared_key_count,
                "SharedOperationPercent" => self.shared_operation_percent,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    pub(crate) fn record(&mut self, event: HistoryEvent) {
        self.model.apply(&event);
        self.possible_model.apply(&event);
        self.apply_gsi_model(&event);
        self.apply_trim_model(&event);
        self.history.push(event);
    }

    pub(crate) fn apply_gsi_model(&mut self, event: &HistoryEvent) {
        match &event.outcome {
            OperationOutcome::Committed => match event.kind {
                OperationKind::Put
                | OperationKind::PutIfAbsent
                | OperationKind::Update
                | OperationKind::TransactWrite => {
                    if let Some(value) = &event.value
                        && let Some((partition, sort)) = self.gsi_projection(&event.key, value)
                    {
                        self.gsi_seen_partitions.insert(partition.clone());
                        self.gsi_model.put(
                            partition,
                            GsiEntry::new(event.key.clone(), sort, value.clone()),
                        );
                    }
                }
                OperationKind::Delete => {
                    self.gsi_model.delete(&event.key);
                }
                OperationKind::Read => {}
            },
            OperationOutcome::Unknown { .. } => {
                if let Some(value) = &event.value
                    && let Some((partition, _)) = self.gsi_projection(&event.key, value)
                {
                    self.gsi_seen_partitions.insert(partition.clone());
                    self.gsi_unclassified_partitions.insert(partition);
                }
            }
            OperationOutcome::ConditionFailed { .. } | OperationOutcome::Failed { .. } => {}
        }
    }

    pub(crate) fn apply_trim_model(&mut self, event: &HistoryEvent) {
        match &event.outcome {
            OperationOutcome::Committed => match event.kind {
                OperationKind::Put
                | OperationKind::PutIfAbsent
                | OperationKind::Update
                | OperationKind::TransactWrite => {
                    if let Some(scope) = self.item_trim_scope(&event.key) {
                        self.trim_model.expect_scope(scope);
                    }
                }
                OperationKind::Delete | OperationKind::Read => {}
            },
            OperationOutcome::Unknown { .. } => {
                if let Some(scope) = self.item_trim_scope(&event.key) {
                    self.trim_model.unclassify(scope);
                }
            }
            OperationOutcome::ConditionFailed { .. } | OperationOutcome::Failed { .. } => {}
        }
    }

    pub(crate) fn sim_time_us(&self) -> u64 {
        let now = self.context.now();
        if !now.is_finite() || now < 0.0 {
            return 0;
        }
        let micros = (now * 1_000_000.0).round();
        if micros >= u64::MAX as f64 {
            u64::MAX
        } else {
            (micros as u64).max(1)
        }
    }

    pub(crate) fn history_event(
        &self,
        started_at_sim_us: u64,
        sequence: u64,
        kind: OperationKind,
        key: String,
        value: Option<String>,
        outcome: OperationOutcome,
    ) -> HistoryEvent {
        HistoryEvent::with_sim_interval(
            sequence,
            self.client_id,
            started_at_sim_us,
            self.sim_time_us(),
            kind,
            key,
            value,
            outcome,
        )
    }

    pub(crate) fn record_success(
        &mut self,
        started_at_sim_us: u64,
        sequence: u64,
        kind: OperationKind,
        key: String,
        value: Option<String>,
    ) {
        let event = self.history_event(
            started_at_sim_us,
            sequence,
            kind,
            key,
            value,
            OperationOutcome::Committed,
        );
        self.record(event);
    }

    pub(crate) fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    pub(crate) fn record_failure(
        &mut self,
        started_at_sim_us: u64,
        sequence: u64,
        kind: OperationKind,
        key: String,
        value: Option<String>,
        error: String,
    ) {
        let outcome = classify_operation_error(&error);
        if matches!(outcome, OperationOutcome::Unknown { .. }) {
            self.trace_unknown_commit(sequence, &kind, &error);
        } else {
            self.error_count += 1;
            self.anomalies.push(Anomaly {
                kind: AnomalyKind::OperationFailed,
                client_id: self.client_id,
                key: key.clone(),
                expected: None,
                actual: None,
                detail: error.clone(),
            });
        }
        let event = self.history_event(started_at_sim_us, sequence, kind, key, value, outcome);
        self.record(event);
    }

    pub(crate) fn trace_unknown_commit(&self, sequence: u64, kind: &OperationKind, error: &str) {
        let kind = format!("{kind:?}");
        self.context.trace(
            Severity::Warn,
            "AuxStorageFdbChaosTableAtomicityUnknownCommit",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "ClientId" => self.client_id,
                "Sequence" => sequence,
                "OperationKind" => kind,
                "Error" => error,
            ],
        );
    }

    pub(crate) fn record_condition_failed(
        &mut self,
        started_at_sim_us: u64,
        sequence: u64,
        key: String,
        value: String,
        error: String,
    ) {
        if !key.starts_with("shared/") && !self.possible_model.allows_present(&key) {
            let detail = format!(
                "maybe_committed: conditional failure is not explained by owned-key model; \
                 original_error={error}"
            );
            self.trace_unknown_commit(sequence, &OperationKind::PutIfAbsent, &detail);
            let event = self.history_event(
                started_at_sim_us,
                sequence,
                OperationKind::PutIfAbsent,
                key,
                Some(value),
                OperationOutcome::Unknown { error: detail },
            );
            self.record(event);
            return;
        }

        let event = self.history_event(
            started_at_sim_us,
            sequence,
            OperationKind::PutIfAbsent,
            key,
            Some(value),
            OperationOutcome::ConditionFailed { error },
        );
        self.record(event);
    }

    pub(crate) fn record_anomaly(
        &mut self,
        kind: AnomalyKind,
        key: String,
        expected: Option<String>,
        actual: Option<String>,
        detail: String,
    ) {
        self.anomalies.push(Anomaly {
            kind,
            client_id: self.client_id,
            key,
            expected,
            actual,
            detail,
        });
        self.error_count += 1;
    }
}
