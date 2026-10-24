use serde::{Deserialize, Serialize};

use crate::error::ReplicationProtocolError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct DatabaseGroupId([u8; 16]);

impl TryFrom<[u8; 16]> for DatabaseGroupId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "DatabaseGroupId")?;
        Ok(Self(value))
    }
}

impl From<DatabaseGroupId> for [u8; 16] {
    fn from(value: DatabaseGroupId) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for DatabaseGroupId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for DatabaseGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct ReplicaId([u8; 16]);

impl TryFrom<[u8; 16]> for ReplicaId {
    type Error = ReplicationProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        validate_nonzero_identity(&value, "ReplicaId")?;
        Ok(Self(value))
    }
}

impl From<ReplicaId> for [u8; 16] {
    fn from(value: ReplicaId) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for ReplicaId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct RegionId(String);

impl Serialize for RegionId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl RegionId {
    pub const MAX_BYTES: usize = crate::constants::MAX_REGION_ID_BYTES;
}

impl TryFrom<String> for RegionId {
    type Error = ReplicationProtocolError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_region_id(&value)?;
        Ok(Self(value))
    }
}

impl std::str::FromStr for RegionId {
    type Err = ReplicationProtocolError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.to_owned().try_into()
    }
}

impl From<RegionId> for String {
    fn from(value: RegionId) -> Self {
        value.0
    }
}

impl AsRef<str> for RegionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for RegionId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEndpoint {
    pub deployment: super::resource::DeploymentId,
    pub region: RegionId,
    pub group: DatabaseGroupId,
    pub replica: ReplicaId,
    pub address: String,
}

impl ReplicaEndpoint {
    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        if self.address.is_empty() {
            return Err(ReplicationProtocolError::InvalidIdentity {
                context: "replica.validate",
            });
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

fn validate_region_id(value: &str) -> Result<(), ReplicationProtocolError> {
    if value.is_empty()
        || value.len() > crate::constants::MAX_REGION_ID_BYTES
        || value.starts_with('-')
        || value.ends_with('-')
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(ReplicationProtocolError::InvalidIdentity {
            context: "replica.validate_region_id",
        });
    }
    Ok(())
}
