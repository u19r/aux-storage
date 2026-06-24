use crate::{common::*, imports::*, partition_family::*};

impl RustWorkload for PartitionFamilyWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if self.client_id != 0 {
            return;
        }
        let provider = match self.provider(db).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("setup", err);
                return;
            }
        };

        self.trace_operation("setup", "create_key_ordered_stream");
        let stream_name = match provider
            .create_stream(
                self.stream_user_name(),
                None,
                StreamPartitioningMode::KeyOrdered,
            )
            .await
        {
            Ok(stream_name) => stream_name,
            Err(err) => {
                self.trace_error("create_key_ordered_stream", err.to_string());
                return;
            }
        };
        self.stream_name = Some(stream_name);
    }

    async fn start(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() {
            return;
        }
        let Some(stream_name) = self.stream_name.clone() else {
            self.trace_error("start", "missing setup stream name".to_string());
            return;
        };
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("start", err);
                return;
            }
        };

        let hot_keys = self.hot_partition_keys(0, 8);
        if self.operation_count > 0 {
            let payload = vec![b'x'; 512];
            self.trace_operation("start", "append_hot_partition_items");
            for index in 0..self.operation_count {
                let key_index = usize::try_from(index % hot_keys.len() as u64).unwrap_or(0);
                let item_id = match provider
                    .append_item(stream_name.clone(), &payload, Some(&hot_keys[key_index]))
                    .await
                {
                    Ok(item_id) => item_id,
                    Err(err) => {
                        self.trace_error("append_hot_partition_items", err.to_string());
                        return;
                    }
                };
                self.expected_item_ids.push(item_id);
                self.append_count += 1;
            }
        }

        self.trace_operation("start", "write_hot_load_sample");
        if let Err(err) = self
            .write_hot_load_sample(db, &stream_name, &hot_keys)
            .await
        {
            self.trace_error("write_hot_load_sample", err);
            return;
        }

        self.trace_operation("start", "partition_reconcile");
        let family_component = ordered_log_family_component(&stream_name);
        for _ in 0..3 {
            let (lease_key, commit_at_ms) =
                self.begin_partition_reconcile_lease_resume(&stream_name);
            if let Err(err) = provider
                .run_ordered_log_partition_reconcile_once(&family_component)
                .await
            {
                self.trace_error("partition_reconcile", storage_error_detail(&err));
                if let Err(artifact_err) = self.write_background_lease_artifacts() {
                    self.trace_error("background_lease_artifacts", artifact_err);
                }
                return;
            }
            self.reconcile_count += 1;
            self.record_partition_reconcile_commit(lease_key, commit_at_ms);
        }
        if let Err(err) = self.write_background_lease_artifacts() {
            self.trace_error("background_lease_artifacts", err);
            return;
        }

        if self.operation_count > 0 {
            self.trace_operation("start", "append_after_reconcile");
            let Some(after_key) = hot_keys.first() else {
                self.trace_error("append_after_reconcile", "missing hot key".to_string());
                return;
            };
            match provider
                .append_item(stream_name.clone(), b"after-reconcile", Some(after_key))
                .await
            {
                Ok(item_id) => {
                    self.expected_item_ids.push(item_id);
                    self.append_count += 1;
                }
                Err(err) => {
                    self.trace_error("append_after_reconcile", err.to_string());
                }
            }
        }
    }

    async fn check(&mut self, db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if self.client_id != 0 {
            return;
        }
        let Some(stream_name) = self.stream_name.clone() else {
            self.trace_error("check", "missing stream name".to_string());
            return;
        };
        let provider = match self.provider(db.clone()).await {
            Ok(provider) => provider,
            Err(err) => {
                self.trace_error("check", err);
                return;
            }
        };

        if !self.expected_item_ids.is_empty() {
            self.trace_operation("check", "read_stream_order");
            let limit =
                u32::try_from(self.expected_item_ids.len().saturating_add(8)).unwrap_or(u32::MAX);
            let page = match provider
                .read_forward(stream_name.clone(), None, limit)
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    self.trace_error("read_stream_order", err.to_string());
                    return;
                }
            };
            let actual_ids = page.items.iter().map(|item| item.id).collect::<Vec<_>>();
            if actual_ids != self.expected_item_ids {
                self.trace_error(
                    "read_stream_order",
                    format!(
                        "partitioned stream order mismatch expected_count={} actual_count={}",
                        self.expected_item_ids.len(),
                        actual_ids.len()
                    ),
                );
                return;
            }
            self.read_back_count += actual_ids.len() as u64;
        }

        self.trace_operation("check", "direct_partition_info_scan");
        let partitions = match self.direct_partition_infos(db, &stream_name).await {
            Ok(partitions) => partitions,
            Err(err) => {
                self.trace_error("direct_partition_info_scan", err);
                return;
            }
        };
        self.direct_scan_count += partitions.len() as u64;
        self.split_count = partitions
            .iter()
            .filter(|partition| partition.state == PartitionState::WriteClosed)
            .count() as u64;
        if self.split_count == 0 {
            self.trace_error(
                "direct_partition_info_scan",
                "expected at least one write-closed parent after reconcile".to_string(),
            );
            return;
        }
        if partitions
            .iter()
            .any(|partition| partition.state == PartitionState::Retired)
        {
            self.trace_error(
                "direct_partition_info_scan",
                "ordered-log partition family unexpectedly has retired partitions".to_string(),
            );
            return;
        }
        if let Err(err) = self.check_writable_ranges(&partitions) {
            self.trace_error("direct_partition_info_scan", err);
            return;
        }
        self.range_check_count += 1;
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64("aux_storage_partition_family_setup_count", self.setup_count),
            metric_val_u64("aux_storage_partition_family_start_count", self.start_count),
            metric_val_u64("aux_storage_partition_family_check_count", self.check_count),
            metric_val_u64(
                "aux_storage_partition_family_append_count",
                self.append_count,
            ),
            metric_val_u64(
                "aux_storage_partition_family_reconcile_count",
                self.reconcile_count,
            ),
            metric_val_u64(
                "aux_storage_partition_family_direct_scan_count",
                self.direct_scan_count,
            ),
            metric_val_u64("aux_storage_partition_family_split_count", self.split_count),
            metric_val_u64(
                "aux_storage_partition_family_range_check_count",
                self.range_check_count,
            ),
            metric_val_u64(
                "aux_storage_partition_family_read_back_count",
                self.read_back_count,
            ),
            metric_val_u64(
                "aux_storage_partition_family_background_lease_event_count",
                self.background_lease_event_count,
            ),
            metric_val_u64("aux_storage_partition_family_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        120.0
    }
}
