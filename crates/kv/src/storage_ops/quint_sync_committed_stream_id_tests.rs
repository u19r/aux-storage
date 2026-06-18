#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{AttributeValue, ItemKey, ItemStreamVersion, StreamItemId, TableName};

use crate::{
    backends::common::KvMutation,
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    sorted_kv_store::DirectWriteOperation,
    storage_ops::provider_impl::kv_mutation_to_direct_with_literal_templates,
    stream::helpers::{StreamEntryContext, create_item_update_stream_entries_wire_encoded},
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct CommitCase {
    #[serde(rename = "targetVersion")]
    target_version: i64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct CommittedStreamIdState {
    #[serde(rename = "lastCase")]
    last_case: CommitCase,
    #[serde(rename = "appliedStreamId")]
    applied_stream_id: i64,
    #[serde(rename = "localMetadataAllocated")]
    local_metadata_allocated: bool,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<CommittedStreamIdDriver> for CommittedStreamIdState {
    fn from_driver(driver: &CommittedStreamIdDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            applied_stream_id: driver.applied_stream_id,
            local_metadata_allocated: driver.local_metadata_allocated,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct CommittedStreamIdDriver {
    last_case: CommitCase,
    applied_stream_id: i64,
    local_metadata_allocated: bool,
    last_decision: String,
}

impl Default for CommittedStreamIdDriver {
    fn default() -> Self {
        Self {
            last_case: CommitCase { target_version: 1 },
            applied_stream_id: 1,
            local_metadata_allocated: false,
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for CommittedStreamIdDriver {
    type State = CommittedStreamIdState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(targetVersion: i64) => {
                self.check(targetVersion)?;
            },
            step(targetVersion: i64?) => {
                if let Some(target_version) = targetVersion {
                    self.check(target_version)?;
                }
            },
        })
    }
}

impl CommittedStreamIdDriver {
    fn check(&mut self, target_version: i64) -> Result {
        let (applied_stream_id, local_metadata_allocated) = materialized_stream_id(target_version)?;
        self.last_case = CommitCase { target_version };
        self.applied_stream_id = applied_stream_id;
        self.local_metadata_allocated = local_metadata_allocated;
        self.last_decision =
            decision_name(target_version, applied_stream_id, local_metadata_allocated).to_string();
        Ok(())
    }
}

fn materialized_stream_id(target_version: i64) -> Result<(i64, bool)> {
    let target_version = u64::try_from(target_version)?;
    let target = StreamItemId::from(ItemStreamVersion::new(target_version));
    let target_bytes = target.as_bytes();
    let table_name = TableName::new("CommittedStreamId");
    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk#1".to_string()),
        None,
    );
    let table_identity = TableIdentity::new(TableStorageId::new(1), table_name.clone(), Vec::new());
    let entries = create_item_update_stream_entries_wire_encoded(
        StreamEntryContext {
            table_identity: &table_identity,
            table_name: &table_name,
            item_key: &item_key,
        },
        br#"{"pk":{"S":"pk#1"}}"#,
        None,
        target,
        false,
        None,
    )?;

    let mut saw_local_metadata = false;
    let mut all_entries_match_target = true;
    for (template, value) in entries {
        let operation = kv_mutation_to_direct_with_literal_templates(KvMutation::PutTemplate {
            template,
            value,
        });
        match operation {
            DirectWriteOperation::Put { key, .. } => {
                all_entries_match_target &= key.ends_with(target_bytes);
            }
            DirectWriteOperation::PutTemplate { .. } => {
                saw_local_metadata = true;
                all_entries_match_target = false;
            }
            DirectWriteOperation::Delete { .. }
            | DirectWriteOperation::DeleteRange { .. }
            | DirectWriteOperation::CheckValue { .. } => {
                all_entries_match_target = false;
            }
        }
    }

    let applied_stream_id = if all_entries_match_target {
        i64::try_from(target_version)?
    } else {
        -1
    };
    Ok((applied_stream_id, saw_local_metadata))
}

fn decision_name(
    target_version: i64,
    applied_stream_id: i64,
    local_metadata_allocated: bool,
) -> &'static str {
    if target_version <= 0 {
        "rejected_zero_target"
    } else if local_metadata_allocated {
        "rejected_local_metadata"
    } else if applied_stream_id != target_version {
        "rejected_target_mismatch"
    } else {
        "accepted"
    }
}

#[quint_run(
    spec = "../../quint/sync_committed_stream_id_mbt.qnt",
    max_samples = 48,
    max_steps = 8,
    seed = "0x51d1d"
)]
fn sync_committed_stream_id_mbt_matches_kv_materialization() -> impl Driver {
    CommittedStreamIdDriver::default()
}
