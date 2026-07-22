use super::*;

impl FoundationDbKvStore {
    pub(crate) async fn load_partition_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let Some(config_bytes) = trx
            .get(&config_key, false)
            .await
            .map_err(|err| map_fdb_error("read partition family config", err))?
        else {
            return Ok(None);
        };
        let config = parse_partition_family_config(&config_bytes)?;

        let partition_prefix = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let partition_entries = Self::read_key_prefix(trx, &partition_prefix, 1024)
            .await
            .map_err(|err| map_fdb_error("read partition family partitions", err))?;
        let mut partitions = Vec::with_capacity(partition_entries.len());
        for (_key, value) in partition_entries {
            partitions.push(parse_partition_info(&value)?);
        }
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });

        Ok(Some(ResolvedPartitionFamily { config, partitions }))
    }

    pub(crate) async fn load_partition_family_state_tx_retryable(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> Result<Option<ResolvedPartitionFamily>, FdbTransactionAttemptError> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        let Some(config_bytes) = trx
            .get(&config_key, false)
            .await
            .map_err(|err| FdbTransactionAttemptError::fdb("read partition family config", err))?
        else {
            return Ok(None);
        };
        let config = parse_partition_family_config(&config_bytes)?;

        let partition_prefix = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_info_prefix(family_kind, family_component),
        );
        let partition_entries = Self::read_key_prefix(trx, &partition_prefix, 1024)
            .await
            .map_err(|err| {
                FdbTransactionAttemptError::fdb("read partition family partitions", err)
            })?;
        let mut partitions = Vec::with_capacity(partition_entries.len());
        for (_key, value) in partition_entries {
            partitions.push(parse_partition_info(&value)?);
        }
        partitions.sort_unstable_by(|left, right| {
            left.hash_start_inclusive
                .cmp(&right.hash_start_inclusive)
                .then_with(|| left.partition_id.cmp(&right.partition_id))
        });

        Ok(Some(ResolvedPartitionFamily { config, partitions }))
    }

    pub(crate) fn save_partition_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        family: &ResolvedPartitionFamily,
    ) -> StorageResult<()> {
        let config_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_config_key(family_kind, family_component),
        );
        trx.set(
            &config_key,
            &partition_family_config_bytes(family_component, &family.config)?,
        );
        let epoch_key = Self::prefix_bytes(
            prefix,
            &crate::partition_family::partition_family_epoch_key(family_kind, family_component),
        );
        trx.set(&epoch_key, &partition_family_epoch_bytes(&family.config));
        for partition in &family.partitions {
            let partition_key = Self::prefix_bytes(
                prefix,
                &crate::partition_family::partition_info_key(
                    family_kind,
                    family_component,
                    partition.partition_id,
                ),
            );
            trx.set(&partition_key, &partition_info_bytes(partition)?);
        }
        Ok(())
    }

    pub(crate) async fn ensure_ordered_log_family_state_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
    ) -> StorageResult<ResolvedPartitionFamily> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(existing) = Self::load_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
        )
        .await?
        {
            return Ok(existing);
        }

        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::OrderedLog,
                DEFAULT_ORDERED_LOG_PARTITION_COUNT,
            ),
            partitions: initial_partition_infos(DEFAULT_ORDERED_LOG_PARTITION_COUNT),
        };
        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
            &family,
        )?;
        Ok(family)
    }

    pub(crate) async fn ensure_ordered_log_family_state_tx_retryable(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
    ) -> Result<ResolvedPartitionFamily, FdbTransactionAttemptError> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(existing) = Self::load_partition_family_state_tx_retryable(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
        )
        .await?
        {
            return Ok(existing);
        }

        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::OrderedLog,
                DEFAULT_ORDERED_LOG_PARTITION_COUNT,
            ),
            partitions: initial_partition_infos(DEFAULT_ORDERED_LOG_PARTITION_COUNT),
        };
        Self::save_partition_family_state_tx(
            trx,
            prefix,
            PartitionFamilyKind::OrderedLog,
            &family_component,
            &family,
        )?;
        Ok(family)
    }

    pub(crate) async fn ensure_ordered_log_family_state_cached_tx(
        trx: &Transaction,
        prefix: Option<&Vec<u8>>,
        stream_name: &StreamName,
        cache: &mut OrderedLogFamilyCache,
    ) -> StorageResult<ResolvedPartitionFamily> {
        let family_component = ordered_log_family_component(stream_name);
        if let Some(family) = cache.get(&family_component) {
            return Ok(family.clone());
        }

        let family = Self::ensure_ordered_log_family_state_tx(trx, prefix, stream_name).await?;
        cache.insert(family_component, family.clone());
        Ok(family)
    }
}
