use crate::{imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) fn client_artifact_root(&self) -> PathBuf {
        PathBuf::from(&self.artifact_root).join(format!("client-{}", self.client_id))
    }

    pub(crate) fn trim_scope_report(&self) -> TrimScopeReport {
        TrimScopeReport::new(
            self.client_id,
            self.trim_model.classified_scopes(),
            self.trim_model.unclassified_scopes(),
        )
    }

    pub(crate) fn write_artifacts(&mut self) -> Result<(), String> {
        let root = self.client_artifact_root();
        fs::create_dir_all(&root).map_err(|err| {
            format!(
                "failed to create workload artifact directory {}: {err}",
                root.display()
            )
        })?;
        let history = self
            .history
            .to_json_lines()
            .map_err(|err| format!("failed to serialize history: {err}"))?;
        fs::write(root.join("history.jsonl"), history)
            .map_err(|err| format!("failed to write history artifact: {err}"))?;
        let report = AnomalyReport::new(self.name.clone(), self.client_id, self.anomalies.clone());
        let report_json = serde_json::to_string_pretty(&report)
            .map_err(|err| format!("failed to serialize anomaly report: {err}"))?;
        fs::write(root.join("anomaly-report.json"), format!("{report_json}\n"))
            .map_err(|err| format!("failed to write anomaly report: {err}"))?;
        if let Some(shared_audit) = &self.shared_audit {
            let audit_json = serde_json::to_string_pretty(shared_audit)
                .map_err(|err| format!("failed to serialize shared-key audit: {err}"))?;
            fs::write(
                root.join("shared-key-audit.json"),
                format!("{audit_json}\n"),
            )
            .map_err(|err| format!("failed to write shared-key audit: {err}"))?;
        }
        let trim_report_json = serde_json::to_string_pretty(&self.trim_scope_report())
            .map_err(|err| format!("failed to serialize trim scope report: {err}"))?;
        fs::write(
            root.join("trim-scopes.json"),
            format!("{trim_report_json}\n"),
        )
        .map_err(|err| format!("failed to write trim scope report: {err}"))?;
        Ok(())
    }

    pub(crate) fn write_trim_provider_snapshot(
        &self,
        table_scopes: &BTreeSet<String>,
        item_scopes: &BTreeSet<String>,
    ) -> Result<(), String> {
        let root = self.client_artifact_root();
        fs::create_dir_all(&root).map_err(|err| {
            format!(
                "failed to create workload artifact directory {}: {err}",
                root.display()
            )
        })?;
        let snapshot = TrimProviderSnapshot::new(
            self.client_id,
            table_scopes.iter().cloned().collect(),
            item_scopes.iter().cloned().collect(),
        );
        let snapshot_json = serde_json::to_string_pretty(&snapshot)
            .map_err(|err| format!("failed to serialize trim provider snapshot: {err}"))?;
        fs::write(
            root.join("trim-provider-snapshot.json"),
            format!("{snapshot_json}\n"),
        )
        .map_err(|err| format!("failed to write trim provider snapshot: {err}"))
    }

    pub(crate) fn trace_invariant_error(&mut self, phase: &'static str, error: String) {
        let artifact_error = self.write_artifacts().err();
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosTableAtomicityError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
                "ArtifactError" => artifact_error.unwrap_or_default(),
            ],
        );
    }
}
