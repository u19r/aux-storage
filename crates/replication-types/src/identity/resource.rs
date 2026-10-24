use serde::{Deserialize, Serialize};

use crate::error::ReplicationProtocolError;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct TableIdentity([u8; 16]);

impl TableIdentity {
    /// Fresh creation retains its operation identity without a lossy hash.
    pub fn from_creation(operation: OperationIdentity) -> Self {
        Self(operation.0)
    }
}

impl TryFrom<[u8; 16]> for TableIdentity {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "TableIdentity")?;
        Ok(Self(value))
    }
}

impl From<TableIdentity> for [u8; 16] {
    fn from(value: TableIdentity) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for TableIdentity {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for TableIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct TableLineage([u8; 16]);

impl TableLineage {
    /// Fresh creation retains its operation identity as its event namespace.
    pub fn from_creation(operation: OperationIdentity) -> Self {
        Self(operation.0)
    }
}

impl TryFrom<[u8; 16]> for TableLineage {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "TableLineage")?;
        Ok(Self(value))
    }
}

impl From<TableLineage> for [u8; 16] {
    fn from(value: TableLineage) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for TableLineage {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for TableLineage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct OperationIdentity([u8; 16]);

impl OperationIdentity {
    pub fn fresh() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }
}

impl TryFrom<[u8; 16]> for OperationIdentity {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "OperationIdentity")?;
        Ok(Self(value))
    }
}

impl From<OperationIdentity> for [u8; 16] {
    fn from(value: OperationIdentity) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for OperationIdentity {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for OperationIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct ControlOperationId([u8; 16]);

impl TryFrom<[u8; 16]> for ControlOperationId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "ControlOperationId")?;
        Ok(Self(value))
    }
}

impl From<ControlOperationId> for [u8; 16] {
    fn from(value: ControlOperationId) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for ControlOperationId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for ControlOperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct DeploymentId([u8; 16]);

impl TryFrom<[u8; 16]> for DeploymentId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "DeploymentId")?;
        Ok(Self(value))
    }
}

impl From<DeploymentId> for [u8; 16] {
    fn from(value: DeploymentId) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for DeploymentId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct HistoryIdentity([u8; 16]);

impl TryFrom<[u8; 16]> for HistoryIdentity {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "HistoryIdentity")?;
        Ok(Self(value))
    }
}

impl From<HistoryIdentity> for [u8; 16] {
    fn from(value: HistoryIdentity) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for HistoryIdentity {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for HistoryIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct OriginalStreamRecordId([u8; 16]);
impl TryFrom<[u8; 16]> for OriginalStreamRecordId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "OriginalStreamRecordId")?;
        Ok(Self(value))
    }
}

impl From<OriginalStreamRecordId> for [u8; 16] {
    fn from(value: OriginalStreamRecordId) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for OriginalStreamRecordId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for OriginalStreamRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct ProjectionGeneration([u8; 16]);
impl TryFrom<[u8; 16]> for ProjectionGeneration {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "ProjectionGeneration")?;
        Ok(Self(value))
    }
}
impl From<ProjectionGeneration> for [u8; 16] {
    fn from(value: ProjectionGeneration) -> Self {
        value.0
    }
}
impl AsRef<[u8]> for ProjectionGeneration {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Display for ProjectionGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct TenantId([u8; 16]);
impl TryFrom<[u8; 16]> for TenantId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "TenantId")?;
        Ok(Self(value))
    }
}
impl From<TenantId> for [u8; 16] {
    fn from(value: TenantId) -> Self {
        value.0
    }
}
impl AsRef<[u8]> for TenantId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

fn validate_nonzero_identity(
    value: &[u8; 16],
    kind: &'static str,
) -> Result<(), ReplicationProtocolError> {
    if *value == [0; 16] {
        return Err(ReplicationProtocolError::InvalidIdentity { context: kind });
    }
    Ok(())
}
