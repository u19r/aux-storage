use std::num::NonZeroU64;

use scheduled_bg_jobs::{JobId, Namespace};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::errors::TargetIncarnationError;
use crate::{Revision, constants::MAX_TARGET_INCARNATION_BYTES};

/// Opaque identity for one concrete table, index, stream or partition family.
/// Reusing a display name requires a new value.
#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize)]
pub struct TargetIncarnation(Vec<u8>);

impl TryFrom<Vec<u8>> for TargetIncarnation {
    type Error = TargetIncarnationError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(TargetIncarnationError::Empty);
        }
        if bytes.len() > MAX_TARGET_INCARNATION_BYTES {
            return Err(TargetIncarnationError::TooLong);
        }
        Ok(Self(bytes))
    }
}

impl From<[u8; 16]> for TargetIncarnation {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<[u8; 6]> for TargetIncarnation {
    fn from(bytes: [u8; 6]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<[u8; 4]> for TargetIncarnation {
    fn from(bytes: [u8; 4]) -> Self {
        Self(bytes.to_vec())
    }
}

impl AsRef<[u8]> for TargetIncarnation {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for TargetIncarnation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::try_from(bytes).map_err(serde::de::Error::custom)
    }
}

/// Closed maintenance responsibility. Scheduler queue names remain catalog
/// data and are not persisted as a second kind value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MaintenanceKind {
    GsiBackfill,
    PartitionReconciliation,
    StreamTrim,
    TtlSweep,
    QueueCleanup,
    PubsubFanout,
    PubsubDelivery,
}

impl MaintenanceKind {
    /// Stable physical discriminator for one durable maintenance task.
    #[must_use]
    pub const fn storage_tag(self) -> u8 {
        match self {
            Self::GsiBackfill => 1,
            Self::PartitionReconciliation => 6,
            Self::StreamTrim => 4,
            Self::TtlSweep => 5,
            Self::QueueCleanup => 7,
            Self::PubsubFanout => 8,
            Self::PubsubDelivery => 9,
        }
    }
}

/// Durable scope within which a job fence is comparable.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskScope {
    #[serde(
        serialize_with = "serialize_namespace",
        deserialize_with = "deserialize_namespace"
    )]
    pub destination: Namespace,
    pub target: TargetIncarnation,
    pub work: MaintenanceKind,
}

fn serialize_namespace<S>(value: &Namespace, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    value.as_bytes().serialize(serializer)
}

fn deserialize_namespace<'de, D>(deserializer: D) -> Result<Namespace, D::Error>
where D: Deserializer<'de> {
    let bytes = Vec::<u8>::deserialize(deserializer)?;
    Namespace::try_new(bytes).map_err(serde::de::Error::custom)
}

/// Scheduler claim authority bound to one durable destination scope.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAuthority {
    pub scope: TaskScope,
    #[serde(
        serialize_with = "serialize_job_id",
        deserialize_with = "deserialize_job_id"
    )]
    pub job: JobId,
    pub fence: NonZeroU64,
}

#[derive(PartialEq, Eq)]
pub enum TaskAdmission {
    Initialize {
        authority: TaskAuthority,
    },
    Evaluate {
        authority: TaskAuthority,
        expected_revision: Revision,
    },
    Superseded,
    BindingMismatch,
}

impl TaskAuthority {
    pub fn admit<P>(
        &self,
        current_target: Option<&TargetIncarnation>,
        progress: Option<&TaskProgress<P>>,
    ) -> TaskAdmission {
        if current_target != Some(&self.scope.target) {
            return TaskAdmission::Superseded;
        }

        let Some(progress) = progress else {
            return TaskAdmission::Initialize {
                authority: self.clone(),
            };
        };

        if progress.authority.scope.target != self.scope.target {
            return TaskAdmission::Superseded;
        }
        if progress.authority.scope != self.scope || progress.authority.job != self.job {
            return TaskAdmission::BindingMismatch;
        }
        if self.fence < progress.authority.fence {
            return TaskAdmission::Superseded;
        }

        TaskAdmission::Evaluate {
            authority: self.clone(),
            expected_revision: progress.revision,
        }
    }
}

fn serialize_job_id<S>(value: &JobId, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    value.as_bytes().serialize(serializer)
}

fn deserialize_job_id<'de, D>(deserializer: D) -> Result<JobId, D::Error>
where D: Deserializer<'de> {
    let bytes = <[u8; JobId::BYTE_LENGTH]>::deserialize(deserializer)?;
    Ok(JobId::from(bytes))
}

/// Durable domain state and the only semantic progress checkpoint.
#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgress<P> {
    pub authority: TaskAuthority,
    pub revision: Revision,
    pub state: P,
}

/// Result of one bounded domain invocation.
#[derive(Debug, PartialEq, Eq)]
pub enum StepResult<P> {
    Advanced(P),
    Complete,
    Superseded,
}
