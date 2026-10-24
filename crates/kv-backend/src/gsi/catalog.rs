//! Register KV GSI builds and their system-stream history holds.

use serde::Serialize;
use storage_backfill::GsiBuild;
use storage_types::{IndexName, StorageError, StorageResult, storage_serde};

use super::{
    key_layout::{gsi_build_key, gsi_runnable_key, hold_key, index_incarnation},
    read::decode_gsi_builds_for,
};
use crate::{
    KvAttempt, KvMutation, ReadProtection,
    catalog::{KvIndexKind, KvTableIdentity},
};

/// Register a decided build and its stream-history holds in the caller's
/// native attempt. The caller has captured a protected SystemStream frontier.
pub async fn register_gsi_builds<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    builds: Vec<GsiBuild<()>>,
) -> StorageResult<()> {
    let mut keys = Vec::with_capacity(builds.len());
    let mut changes = Vec::new();
    for build in builds {
        let index = identity
            .indexes
            .iter()
            .find(|index| {
                index.index_name == build.target.index_name
                    && index.index_kind == KvIndexKind::UserGsi
            })
            .ok_or_else(|| StorageError::internal("KV GSI build has no index identity"))?;
        let incarnation = index_incarnation(identity.table_id, index.index_id);
        if build.target.table_name != identity.table_name
            || build.target.incarnation.as_ref() != incarnation.as_slice()
        {
            return Err(StorageError::internal(
                "KV GSI build identity does not match catalog",
            ));
        }
        keys.push(gsi_build_key(identity.table_id, index.index_id));
        changes.extend(
            build
                .state
                .required_stream_history()
                .frontier()
                .iter()
                .map(|position| {
                    hold_key(
                        build.state.required_stream_history(),
                        position,
                        &incarnation,
                    )
                    .map(|key| KvMutation::Put {
                        key,
                        value: Vec::new(),
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?,
        );
        changes.extend(build_mutations(identity.table_id, index.index_id, &build)?);
    }
    let expected = keys.len();
    let existing = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if existing.len() != expected {
        return Err(StorageError::internal("KV GSI build read count is invalid"));
    }
    if existing.into_iter().any(|row| row.is_some()) {
        return Err(StorageError::internal("KV GSI build already exists"));
    }
    attempt.stage(changes)
}

/// Delete physical build and history-hold keys selected by the shared table
/// lifecycle. Name reservation is staged by the table-update effect.
pub(crate) async fn delete_gsi_builds<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    selected: Option<&IndexName>,
) -> StorageResult<()> {
    let indexes = identity
        .indexes
        .iter()
        .filter(|index| {
            index.index_kind == KvIndexKind::UserGsi
                && selected.is_none_or(|name| index.index_name == *name)
        })
        .collect::<Vec<_>>();
    if selected.is_some() && indexes.len() != 1 {
        return Err(StorageError::internal(
            "KV deleted GSI has no index identity",
        ));
    }
    if indexes.is_empty() {
        return Ok(());
    }
    let keys = indexes
        .iter()
        .map(|index| gsi_build_key(identity.table_id, index.index_id))
        .collect::<Vec<_>>();
    let records = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    let builds: Vec<GsiBuild<Vec<u8>>> = decode_gsi_builds_for(identity, &indexes, records)?;
    let mut changes = Vec::new();

    for (index, build) in indexes.into_iter().zip(builds) {
        let incarnation = index_incarnation(identity.table_id, index.index_id);
        changes.extend(
            build
                .state
                .required_stream_history()
                .frontier()
                .iter()
                .map(|position| {
                    hold_key(
                        build.state.required_stream_history(),
                        position,
                        &incarnation,
                    )
                    .map(|key| KvMutation::Delete { key })
                })
                .collect::<StorageResult<Vec<_>>>()?,
        );
        changes.extend(delete_build_mutations(identity.table_id, index.index_id));
    }
    attempt.stage(changes)
}

pub(super) fn build_mutations<Cursor: Serialize>(
    table_id: u32,
    index_id: u16,
    build: &GsiBuild<Cursor>,
) -> StorageResult<[KvMutation; 2]> {
    let value = storage_serde::to_bytes(build)?;
    let runnable_key = gsi_runnable_key(table_id, index_id);
    let runnable = if build.state.permits_projection() {
        KvMutation::Put {
            key: runnable_key,
            value: value.clone(),
        }
    } else {
        KvMutation::Delete { key: runnable_key }
    };
    Ok([
        KvMutation::Put {
            key: gsi_build_key(table_id, index_id),
            value,
        },
        runnable,
    ])
}

fn delete_build_mutations(table_id: u32, index_id: u16) -> [KvMutation; 2] {
    [
        KvMutation::Delete {
            key: gsi_build_key(table_id, index_id),
        },
        KvMutation::Delete {
            key: gsi_runnable_key(table_id, index_id),
        },
    ]
}
