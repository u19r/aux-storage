//! Each table index orders exactly one qualified source lineage.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest as _, Sha256};

use crate::{
    StorageError, StorageResult, StreamName, SystemStreamLineageId, SystemStreamPosition,
    SystemStreamScope, SystemStreamShardId, TableHistoryId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableHistoryLane {
    history_id: TableHistoryId,
    source_scope: SystemStreamScope,
    shard_id: SystemStreamShardId,
    lineage_id: SystemStreamLineageId,
}

impl TryFrom<(TableHistoryId, &SystemStreamScope, &SystemStreamPosition)> for TableHistoryLane {
    type Error = StorageError;

    fn try_from(
        (history_id, scope, position): (TableHistoryId, &SystemStreamScope, &SystemStreamPosition),
    ) -> StorageResult<Self> {
        if history_id.as_ref() == &[0; 16] {
            return Err(StorageError::internal(
                "Table history lane has no table identity",
            ));
        }
        Ok(Self {
            history_id,
            source_scope: scope.clone(),
            shard_id: position.shard_id().clone(),
            lineage_id: position.lineage_id().clone(),
        })
    }
}

impl TableHistoryLane {
    pub fn history_id(&self) -> TableHistoryId {
        self.history_id
    }
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }
    pub fn shard_id(&self) -> &SystemStreamShardId {
        &self.shard_id
    }
    pub fn lineage_id(&self) -> &SystemStreamLineageId {
        &self.lineage_id
    }

    pub fn physical_stream_name(&self) -> StorageResult<StreamName> {
        let mut name = self.history_id.stream_prefix();
        name.extend_from_slice(b"table/");
        name.extend_from_slice(URL_SAFE_NO_PAD.encode(self.encode_source()?).as_bytes());
        Ok(StreamName::new(&name))
    }

    /// Resolve this opaque ID against the native catalog before accepting it.
    pub fn public_shard_id(&self) -> StorageResult<String> {
        let mut digest = Sha256::new();
        digest.update(self.history_id.as_ref());
        digest.update(self.encode_source()?);
        Ok(hex::encode(digest.finalize()))
    }

    fn encode_source(&self) -> StorageResult<Vec<u8>> {
        Ok(serde_json::to_vec(&(
            &self.source_scope,
            &self.shard_id,
            &self.lineage_id,
        ))?)
    }
}
