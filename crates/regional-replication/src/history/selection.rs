//! A delivery continuation binds one immutable receiver and logical projection.

use replication_types::{
    configuration::membership::MembershipRevision,
    event::{
        content::{CanonicalEvent, CanonicalPayload},
        identity::CanonicalItemKey,
    },
    identity::{origin::OriginId, resource::TableLineage},
};
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TableName};

use super::view::{CanonicalViewRequest, DeliveryPosition};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CanonicalViewSelection {
    All,
    ItemMutations,
    Item { key: CanonicalItemKey },
    TableStream,
    System,
}

impl CanonicalViewSelection {
    pub(crate) fn includes(&self, event: &CanonicalEvent) -> bool {
        match (self, &event.as_ref().payload) {
            (Self::All, _)
            | (Self::ItemMutations, CanonicalPayload::Item(_))
            | (Self::TableStream, CanonicalPayload::TableStream { .. })
            | (Self::System, CanonicalPayload::System(_)) => true,
            (Self::Item { key }, CanonicalPayload::Item(item)) => key == &item.canonical_key,
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalViewCursor {
    trim: super::trim_authority::TrimRevision,
    table: TableName,
    lineage: TableLineage,
    membership: MembershipRevision,
    receiver: OriginId,
    selection: CanonicalViewSelection,
    checkpoint: Option<super::checkpoint::CanonicalCheckpointId>,
    position: DeliveryPosition,
}

impl CanonicalViewRequest {
    pub(crate) fn continuation(
        &self,
        trim: super::trim_authority::TrimRevision,
        position: DeliveryPosition,
    ) -> CanonicalViewCursor {
        CanonicalViewCursor {
            trim,
            table: self.table.clone(),
            lineage: self.lineage,
            membership: self.membership,
            receiver: self.receiver.clone(),
            selection: self.selection.clone(),
            checkpoint: self.checkpoint,
            position,
        }
    }

    pub(crate) fn checkpoint_scope(
        &self,
        trim: super::trim_authority::TrimRevision,
    ) -> CanonicalViewCursor {
        let mut scope = self.continuation(trim, 0u64.into());
        scope.checkpoint = None;
        scope
    }
}

impl CanonicalViewCursor {
    pub(crate) fn require_continuation(
        &self,
        request: &CanonicalViewRequest,
    ) -> StorageResult<DeliveryPosition> {
        let Some(cursor) = &request.after else {
            return Ok(0u64.into());
        };
        if cursor.trim != self.trim {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        if cursor.clone().at_position(0u64.into()) != *self {
            return Err(StorageError::validation(
                "Canonical view cursor does not match its selection",
            ));
        }
        Ok(cursor.position)
    }

    pub(crate) fn require_checkpoint_selection(
        &self,
        request: &CanonicalViewRequest,
    ) -> StorageResult<()> {
        if *self != request.checkpoint_scope(self.trim) {
            return Err(StorageError::validation(
                "Canonical checkpoint operation does not match its selection",
            ));
        }
        Ok(())
    }

    pub(crate) fn at_position(mut self, position: DeliveryPosition) -> Self {
        self.position = position;
        self
    }

    pub(crate) fn checkpoint_continuation(
        self,
        position: DeliveryPosition,
        operation: super::checkpoint::CanonicalCheckpointId,
    ) -> Self {
        let mut cursor = self.at_position(position);
        cursor.checkpoint = Some(operation);
        cursor
    }

    pub(crate) fn require_rollover_target(
        &self,
        request: &CanonicalViewRequest,
    ) -> StorageResult<()> {
        if self.table != request.table || self.selection != request.selection {
            return Err(StorageError::validation(
                "Checkpoint rollover must preserve table and projection",
            ));
        }
        Ok(())
    }

    pub(crate) fn retained_partition(
        &self,
        through: DeliveryPosition,
    ) -> StorageResult<super::hold_discovery::ReceiverHoldRange> {
        if self.position != DeliveryPosition::from(0) || self.checkpoint.is_some() {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(super::hold_discovery::ReceiverHoldRange {
            trim: self.trim,
            lineage: self.lineage,
            receiver: self.receiver.clone(),
            through,
        })
    }
}
