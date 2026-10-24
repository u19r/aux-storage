//! Discover configured TTL targets through the same protected catalog contract
//! on SQL and KV.

use std::sync::Arc;

use storage_types::{
    BoundTableInfo, StorageError, StorageResult, TableName, maintenance::TargetIncarnation,
};

use super::{ExpirationTarget, ExpirationTargetAdmission, ExpirationTargetRead};
use crate::{CatalogAttempt, DescribeTimeToLiveAttempt, ListTablesAttempt};

const TARGET_PAGE_SIZE: u32 = 64;

pub async fn read_expiration_target_in_attempt<A, S>(
    attempt: &mut A,
    read: &ExpirationTargetRead,
    admission: &S,
) -> StorageResult<Option<(ExpirationTarget, Arc<BoundTableInfo>)>>
where
    A: CatalogAttempt + DescribeTimeToLiveAttempt + ListTablesAttempt,
    S: ExpirationTargetAdmission<A>,
{
    match read {
        ExpirationTargetRead::Exact {
            table_name,
            table_incarnation,
        } => {
            let target = configured_target(attempt, table_name, admission).await?;
            Ok(target.filter(|(target, _)| &target.table_incarnation == table_incarnation))
        }
        ExpirationTargetRead::First => scan_targets(attempt, None, admission).await,
        ExpirationTargetRead::After { table_name } => {
            scan_targets(attempt, Some(table_name), admission).await
        }
    }
}

async fn scan_targets<A, S>(
    attempt: &mut A,
    after: Option<&TableName>,
    admission: &S,
) -> StorageResult<Option<(ExpirationTarget, Arc<BoundTableInfo>)>>
where
    A: CatalogAttempt + DescribeTimeToLiveAttempt + ListTablesAttempt,
    S: ExpirationTargetAdmission<A>,
{
    let mut cursor = after.cloned();
    loop {
        let page = attempt
            .list_name_page(cursor.as_ref(), TARGET_PAGE_SIZE)
            .await?;
        if page.names.len() > TARGET_PAGE_SIZE as usize {
            return Err(StorageError::internal(
                "expiration target page exceeded its limit",
            ));
        }
        if page.names.is_empty() {
            if page.more {
                return Err(StorageError::internal(
                    "expiration target page did not advance",
                ));
            }
            return Ok(None);
        }
        let full = page.names.len() == TARGET_PAGE_SIZE as usize;
        for name in page.names {
            if cursor
                .as_ref()
                .is_some_and(|previous| name.as_ref() <= previous.as_ref())
            {
                return Err(StorageError::internal("expiration targets are not ordered"));
            }
            cursor = Some(name.clone());
            if let Some(target) = configured_target(attempt, &name, admission).await? {
                return Ok(Some(target));
            }
        }
        if !page.more && !full {
            return Ok(None);
        }
    }
}

async fn configured_target<A, S>(
    attempt: &mut A,
    name: &TableName,
    admission: &S,
) -> StorageResult<Option<(ExpirationTarget, Arc<BoundTableInfo>)>>
where
    A: CatalogAttempt + DescribeTimeToLiveAttempt,
    S: ExpirationTargetAdmission<A>,
{
    let Some(stamp) = attempt.read_catalog_stamp(name).await? else {
        return Ok(None);
    };
    let table = attempt.read_table_at_stamp(name, stamp).await?;
    if !admission.admits_target(attempt, &table).await? {
        return Ok(None);
    }
    let Some(config) = attempt.read_ttl_config(&table).await? else {
        return Ok(None);
    };
    let target = ExpirationTarget {
        table_name: name.clone(),
        table_incarnation: TargetIncarnation::from(*table.history_id.as_ref()),
        ttl_attribute: config.attribute_name,
        status: config.status,
        policy_generation: config.policy_generation,
    };
    Ok(Some((target, table)))
}
