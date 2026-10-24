use std::{future::Future, num::NonZeroUsize, sync::Arc};

use storage_common::{TtlConfigRecord, TtlSweepPageCommand, TtlSweepPageResult, TtlSweepRequest};
use storage_types::{
    BoundTableInfo, StorageEnum, StorageError, StorageResult, TableName, TimestampMillis,
};

use super::{
    DirectExpirationCandidateRead, DirectExpirationPageWrite, DirectExpirationScanRead,
    ExpectedExpirationPolicy, ExpirationCandidateBatch, ExpirationCandidateCursor,
    ExpirationCandidateObservation, ExpirationIndexLocator, ExpirationPagePlan, ExpirationPolicy,
    ExpirationScanRead,
};
use crate::{CatalogAttempt, DescribeTimeToLiveAttempt};

/// Physical reads and writes lent by one native attempt. Policy and page
/// decisions stay here, so SQL and KV only lower the same typed operations.
pub trait DirectExpirationTransaction {
    fn read_direct_policy(
        &mut self,
        table_name: &TableName,
    ) -> impl Future<Output = StorageResult<Option<(TtlConfigRecord, Arc<BoundTableInfo>)>>>;

    fn read_direct_index(
        &mut self,
        read: &DirectExpirationScanRead,
    ) -> impl Future<Output = StorageResult<(Vec<ExpirationIndexLocator>, bool)>>;

    fn read_direct_candidates(
        &mut self,
        read: &DirectExpirationCandidateRead,
    ) -> impl Future<Output = StorageResult<ExpirationCandidateObservation>>;

    fn write_direct_page(
        &mut self,
        write: DirectExpirationPageWrite,
    ) -> impl Future<Output = StorageResult<()>>;

    fn execute_direct_expiration(
        &mut self,
        command: &TtlSweepPageCommand,
    ) -> impl Future<Output = StorageResult<TtlSweepPageResult>>
    where
        Self: Sized,
    {
        async move {
            let (config, table_info) = self
                .read_direct_policy(&command.table_name)
                .await?
                .ok_or_else(|| StorageError::from(StorageEnum::ConditionalCheckFailed))?;
            let scan = DirectExpirationScanRead::try_new(command, config, (*table_info).clone())?;
            self.execute_direct_scan(scan).await
        }
    }

    fn execute_direct_request(
        &mut self,
        request: &TtlSweepRequest,
    ) -> impl Future<Output = StorageResult<TtlSweepPageResult>>
    where
        Self: Sized,
    {
        async move {
            let (config, table_info) = self
                .read_direct_policy(&request.table_name)
                .await?
                .ok_or_else(|| {
                    StorageError::validation("ttl sweep requires an enabled ttl policy")
                })?;
            let command = TtlSweepPageCommand::try_new(
                request.table_name.clone(),
                request.input.clone(),
                config.policy_generation,
                TimestampMillis::now(),
            )?;
            let scan = DirectExpirationScanRead::try_new(&command, config, (*table_info).clone())?;
            self.execute_direct_scan(scan).await
        }
    }

    fn execute_direct_scan(
        &mut self,
        scan: DirectExpirationScanRead,
    ) -> impl Future<Output = StorageResult<TtlSweepPageResult>>
    where
        Self: Sized,
    {
        async move {
            let write = prepare_direct_scan(self, scan).await?;
            let result = TtlSweepPageResult {
                deleted_items: write.result.deleted_items,
                next_cursor: write.result.next_cursor.clone(),
            };
            self.write_direct_page(write).await?;
            Ok(result)
        }
    }
}

pub(super) async fn prepare_direct_scan<T: DirectExpirationTransaction>(
    transaction: &mut T,
    scan: DirectExpirationScanRead,
) -> StorageResult<DirectExpirationPageWrite> {
    let (rows, more) = transaction.read_direct_index(&scan).await?;
    let candidates = DirectExpirationCandidateRead::try_new(scan, rows, more)?;
    let observation = transaction.read_direct_candidates(&candidates).await?;
    DirectExpirationPageWrite::try_new(candidates, observation)
}

pub async fn read_direct_expiration_policy_in_attempt<A>(
    attempt: &mut A,
    table_name: &TableName,
) -> StorageResult<Option<(TtlConfigRecord, Arc<BoundTableInfo>)>>
where
    A: CatalogAttempt + DescribeTimeToLiveAttempt,
{
    let Some(stamp) = attempt.read_catalog_stamp(table_name).await? else {
        return Ok(None);
    };
    let table = attempt.read_table_at_stamp(table_name, stamp).await?;
    let config = attempt.read_ttl_config(&table).await?;
    Ok(config.map(|config| (config, table)))
}

impl DirectExpirationScanRead {
    pub fn index_read(&self) -> ExpirationScanRead {
        ExpirationScanRead {
            table_name: self.table_name.clone(),
            table_incarnation: storage_types::maintenance::TargetIncarnation::from(
                *self.table_info.history_id.as_ref(),
            ),
            ttl_attribute: self.config.attribute_name.clone(),
            policy_generation: self.config.policy_generation,
            cutoff_seconds: self.cutoff_seconds,
            cursor: self.cursor.as_ref().map(|cursor| {
                storage_common::TtlMaintenanceCursor::from(ExpirationCandidateCursor {
                    expiration_seconds: cursor.expiration_seconds,
                    ttl_item_key: cursor.ttl_item_key.clone(),
                })
            }),
            limit: self.limit,
        }
    }

    pub fn try_new(
        command: &TtlSweepPageCommand,
        config: TtlConfigRecord,
        table_info: BoundTableInfo,
    ) -> StorageResult<Self> {
        if table_info.table_name != command.table_name {
            return Err(StorageError::internal(
                "expiration table metadata does not match its direct command",
            ));
        }
        let current = ExpirationPolicy {
            ttl_attribute: config.attribute_name.clone(),
            status: config.status.clone(),
            policy_generation: config.policy_generation,
            cursor: config.checkpoint.cursor.clone(),
        };
        ExpectedExpirationPolicy {
            ttl_attribute: command.input.ttl_attribute.clone(),
            policy_generation: command.expected_policy_generation,
            cursor: command.input.cursor.clone(),
        }
        .validate_current(&current)?;
        let cursor = command
            .input
            .cursor
            .as_ref()
            .map(ExpirationCandidateCursor::try_from)
            .transpose()?;
        let limit = NonZeroUsize::new(command.input.limit)
            .ok_or_else(|| StorageError::internal("validated expiration limit became zero"))?;
        Ok(Self {
            config,
            table_info,
            table_name: command.table_name.clone(),
            cutoff_seconds: command.input.scan_time.timestamp().max(0),
            cursor,
            limit,
            captured_at: command.captured_at,
        })
    }
}

impl DirectExpirationCandidateRead {
    pub fn try_new(
        scan: DirectExpirationScanRead,
        rows: Vec<ExpirationIndexLocator>,
        more: bool,
    ) -> StorageResult<Self> {
        let DirectExpirationScanRead {
            config,
            table_info,
            table_name,
            cutoff_seconds,
            cursor: _,
            limit,
            captured_at,
        } = scan;
        let plan =
            ExpirationPagePlan::try_new(rows, more, limit, cutoff_seconds, &table_info.info)?;
        Ok(Self {
            batch: ExpirationCandidateBatch {
                ttl_attribute: config.attribute_name.clone(),
                cutoff_seconds,
                candidates: plan.candidates,
            },
            config,
            table_info,
            table_name,
            next_cursor: plan.next_cursor,
            captured_at,
        })
    }
}

impl DirectExpirationPageWrite {
    pub fn try_new(
        read: DirectExpirationCandidateRead,
        observation: ExpirationCandidateObservation,
    ) -> StorageResult<Self> {
        let DirectExpirationCandidateRead {
            mut config,
            table_info,
            table_name,
            batch,
            next_cursor,
            captured_at,
        } = read;
        let deletions = batch.plan_deletions(observation)?;
        let deleted_items = u64::try_from(deletions.items.len())
            .map_err(|_| StorageError::internal("expiration deletion count exceeds u64"))?;
        config.checkpoint.cursor = next_cursor.clone();
        Ok(Self {
            config,
            table_info,
            table_name,
            deletions,
            captured_at,
            result: TtlSweepPageResult {
                deleted_items,
                next_cursor,
            },
        })
    }
}

impl<P: super::ExpirationPort, S: super::ExpirationEffectPolicy<P>> DirectExpirationTransaction
    for super::ExpirationAttempt<'_, P, S>
{
    async fn read_direct_policy(
        &mut self,
        table_name: &TableName,
    ) -> StorageResult<Option<(TtlConfigRecord, Arc<BoundTableInfo>)>> {
        let policy = read_direct_expiration_policy_in_attempt(self.port, table_name).await?;
        let Some((config, table)) = policy else {
            return Ok(None);
        };
        if !self.admission.admits_target(self.port, &table).await? {
            return Ok(None);
        }
        Ok(Some((config, table)))
    }

    async fn read_direct_index(
        &mut self,
        read: &DirectExpirationScanRead,
    ) -> StorageResult<(Vec<ExpirationIndexLocator>, bool)> {
        self.port
            .scan_expiration_index(&read.table_info, &read.index_read())
            .await
    }

    async fn read_direct_candidates(
        &mut self,
        read: &DirectExpirationCandidateRead,
    ) -> StorageResult<ExpirationCandidateObservation> {
        super::read_expiration_candidate_items_in_attempt(self.port, &read.table_info, &read.batch)
            .await
    }

    async fn write_direct_page(&mut self, write: DirectExpirationPageWrite) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self.port);
        let port = settlement.as_mut();
        let result = async {
            let table = Arc::new(write.table_info);
            write
                .deletions
                .prepare_deletions(port, &self.admission, Arc::clone(&table), write.captured_at)
                .await?;
            port.write_ttl_config(&table, write.config).await
        }
        .await;
        settlement.complete(result)
    }
}
