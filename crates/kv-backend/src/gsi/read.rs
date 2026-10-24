//! Protected GSI lifecycle and readiness observations.

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildStatus, GsiBuildTarget, GsiProjectionLifecycle,
};
use storage_provider::GsiReadinessSnapshot;
use storage_types::{IndexName, StorageError, StorageResult, storage_serde};

use super::key_layout::{gsi_build_key, index_incarnation};
use crate::{
    KvAttempt, ReadProtection,
    catalog::{KvIndexKind, KvTableIdentity},
};

pub(crate) async fn read_gsi_readiness<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    protection: ReadProtection,
    requested: Option<&IndexName>,
) -> StorageResult<GsiReadinessSnapshot> {
    let indexes = identity
        .indexes
        .iter()
        .filter(|index| {
            index.index_kind == KvIndexKind::UserGsi
                && requested.is_none_or(|name| index.index_name == *name)
        })
        .collect::<Vec<_>>();
    if requested.is_some() && indexes.len() != 1 {
        return Err(StorageError::internal(
            "KV GSI identity is absent from catalog",
        ));
    }
    let keys = indexes
        .iter()
        .map(|index| gsi_build_key(identity.table_id, index.index_id))
        .collect();
    let records = attempt.get_batch(keys, protection).await?;
    Ok(GsiReadinessSnapshot::try_from(decode_gsi_builds_for::<
        GsiBuildStatus,
    >(
        identity, &indexes, records,
    )?)?)
}

pub(crate) async fn read_activation_state<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    target: &GsiBuildTarget,
) -> StorageResult<GsiActivationState> {
    let index_id =
        target_index(identity, target).map_err(|_| GsiActivationState::conflict_error())?;
    let bytes = attempt
        .get(
            &gsi_build_key(identity.table_id, index_id),
            ReadProtection::Conflict,
        )
        .await?
        .ok_or_else(GsiActivationState::conflict_error)?;
    let build: GsiBuild<Vec<u8>> = storage_serde::from_bytes(&bytes)?;
    GsiActivationState::try_from_build(build, target)
}

pub(super) fn target_index(
    identity: &KvTableIdentity,
    target: &GsiBuildTarget,
) -> StorageResult<u16> {
    let index = identity
        .indexes
        .iter()
        .find(|index| {
            index.index_kind == KvIndexKind::UserGsi && index.index_name == target.index_name
        })
        .ok_or_else(|| StorageError::internal("KV GSI target index is absent"))?;
    if identity.deleted
        || identity.table_name != target.table_name
        || target.incarnation.as_ref()
            != index_incarnation(identity.table_id, index.index_id).as_slice()
    {
        return Err(StorageError::internal("KV GSI target has changed"));
    }
    Ok(index.index_id)
}

pub(crate) fn gsi_build_keys(identity: &KvTableIdentity) -> StorageResult<Vec<Vec<u8>>> {
    let indexes = identity
        .indexes
        .iter()
        .filter(|index| index.index_kind == KvIndexKind::UserGsi);
    let mut keys = Vec::new();
    keys.try_reserve_exact(indexes.clone().count())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for index in indexes {
        keys.push(gsi_build_key(identity.table_id, index.index_id));
    }
    Ok(keys)
}

pub(crate) fn decode_gsi_lifecycle(
    identity: &KvTableIdentity,
    records: Vec<Option<Vec<u8>>>,
) -> StorageResult<GsiProjectionLifecycle> {
    GsiProjectionLifecycle::try_from(decode_gsi_statuses(identity, records)?).map_err(Into::into)
}

fn decode_gsi_statuses(
    identity: &KvTableIdentity,
    records: Vec<Option<Vec<u8>>>,
) -> StorageResult<Vec<GsiBuildStatus>> {
    let indexes = identity
        .indexes
        .iter()
        .filter(|index| index.index_kind == KvIndexKind::UserGsi)
        .collect::<Vec<_>>();
    decode_gsi_builds_for(identity, &indexes, records)
}

pub(super) fn decode_gsi_builds_for<Build: serde::de::DeserializeOwned + AsRef<GsiBuildTarget>>(
    identity: &KvTableIdentity,
    indexes: &[&crate::catalog::KvIndexIdentity],
    records: Vec<Option<Vec<u8>>>,
) -> StorageResult<Vec<Build>> {
    if records.len() != indexes.len() {
        return Err(StorageError::internal(
            "KV GSI build read returned the wrong count",
        ));
    }
    let mut builds = Vec::with_capacity(indexes.len());
    for (index, record) in indexes.iter().zip(records) {
        let bytes = record
            .ok_or_else(|| StorageError::internal("KV GSI build is missing for table index"))?;
        let build: Build = storage_serde::from_bytes(&bytes)?;
        let target = build.as_ref();
        let incarnation = index_incarnation(identity.table_id, index.index_id);
        if target.table_name != identity.table_name
            || target.index_name != index.index_name
            || target.incarnation.as_ref() != incarnation.as_slice()
        {
            return Err(StorageError::internal(
                "KV GSI build identity does not match catalog",
            ));
        }
        builds.push(build);
    }
    Ok(builds)
}
