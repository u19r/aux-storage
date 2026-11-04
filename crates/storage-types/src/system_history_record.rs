//! System history owns a position and an exact item-history reference.

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    ItemHistoryPointer, ItemHistoryPublication, StorageError, StorageResult, SystemStreamPosition,
    TableHistoryId, TimestampMillis,
};

/// Removed records preserve source ordering after table deletion without
/// retaining a table identity, key, or replayable payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SystemHistoryRecord {
    position: SystemStreamPosition,
    created_at: TimestampMillis,
    pointer: Option<ItemHistoryPointer>,
}

impl TryFrom<ItemHistoryPointer> for SystemHistoryRecord {
    type Error = StorageError;

    fn try_from(pointer: ItemHistoryPointer) -> StorageResult<Self> {
        let ItemHistoryPublication::Event {
            table_position,
            created_at,
            ..
        } = pointer.metadata().publication()
        else {
            return Err(invalid_record());
        };
        Ok(Self {
            position: table_position.clone(),
            created_at: *created_at,
            pointer: Some(pointer),
        })
    }
}

impl SystemHistoryRecord {
    pub fn require_scope(&self, scope: &crate::SystemStreamScope) -> StorageResult<()> {
        if let Some(pointer) = &self.pointer {
            let ItemHistoryPublication::Event { source_scope, .. } =
                pointer.metadata().publication()
            else {
                return Err(invalid_record());
            };
            if source_scope != scope {
                return Err(invalid_record());
            }
        }
        Ok(())
    }
    pub fn position(&self) -> &SystemStreamPosition {
        &self.position
    }

    pub fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    pub fn pointer(&self) -> Option<&ItemHistoryPointer> {
        self.pointer.as_ref()
    }

    pub fn table_history_id(&self) -> Option<TableHistoryId> {
        self.pointer
            .as_ref()
            .map(|pointer| pointer.metadata().address().history_id)
    }

    pub fn into_removed(self) -> Self {
        Self {
            pointer: None,
            ..self
        }
    }

    pub fn with_versionstamp(self, stamp: [u8; 12]) -> StorageResult<Self> {
        let position = self
            .position
            .with_versionstamp(stamp)
            .map_err(|_| invalid_record())?;
        let pointer = self
            .pointer
            .map(|pointer| pointer.with_versionstamp(stamp))
            .transpose()?;
        Ok(Self {
            position,
            pointer,
            ..self
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemHistoryRecordInput {
    position: SystemStreamPosition,
    created_at: TimestampMillis,
    pointer: Option<ItemHistoryPointer>,
}

impl<'de> Deserialize<'de> for SystemHistoryRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = SystemHistoryRecordInput::deserialize(deserializer)?;
        let Some(pointer) = input.pointer else {
            return Ok(Self {
                position: input.position,
                created_at: input.created_at,
                pointer: None,
            });
        };
        let record = Self::try_from(pointer).map_err(serde::de::Error::custom)?;
        if record.position != input.position || record.created_at != input.created_at {
            return Err(serde::de::Error::custom(invalid_record()));
        }
        Ok(record)
    }
}

fn invalid_record() -> StorageError {
    StorageError::internal("Invalid SystemStream history reference")
}
