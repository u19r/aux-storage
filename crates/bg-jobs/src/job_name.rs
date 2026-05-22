use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "group", rename_all = "snake_case")]
pub enum BackgroundJobName {
    Database {
        /// Database jobs are only registered when the storage provider is not
        /// `remote`, because remote storage owns those maintenance loops.
        kind: DatabaseJobKind,
    },
    Periodic {
        kind: PeriodicJobKind,
    },
    Immediate {
        kind: ImmediateJobKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundJobGroup {
    Database,
    Periodic,
    Immediate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseJobKind {
    GsiBackfill,
    GsiUpdate,
    PartitionFamilyReconcile,
    QueuePayloadCleanup,
    StreamTtlCleanup,
    StreamTrim,
    TtlSweep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeriodicJobKind {
    Maintenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImmediateJobKind {
    Task,
}

impl BackgroundJobName {
    pub const ALL: [Self; 9] = [
        Self::Database {
            kind: DatabaseJobKind::GsiBackfill,
        },
        Self::Database {
            kind: DatabaseJobKind::GsiUpdate,
        },
        Self::Database {
            kind: DatabaseJobKind::PartitionFamilyReconcile,
        },
        Self::Database {
            kind: DatabaseJobKind::QueuePayloadCleanup,
        },
        Self::Database {
            kind: DatabaseJobKind::StreamTtlCleanup,
        },
        Self::Database {
            kind: DatabaseJobKind::StreamTrim,
        },
        Self::Periodic {
            kind: PeriodicJobKind::Maintenance,
        },
        Self::Immediate {
            kind: ImmediateJobKind::Task,
        },
        Self::Database {
            kind: DatabaseJobKind::TtlSweep,
        },
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Database { kind } => kind.as_str(),
            Self::Periodic { kind } => kind.as_str(),
            Self::Immediate { kind } => kind.as_str(),
        }
    }

    #[must_use]
    pub fn all() -> &'static [Self] {
        &Self::ALL
    }

    #[must_use]
    pub const fn group(self) -> BackgroundJobGroup {
        match self {
            Self::Database { .. } => BackgroundJobGroup::Database,
            Self::Periodic { .. } => BackgroundJobGroup::Periodic,
            Self::Immediate { .. } => BackgroundJobGroup::Immediate,
        }
    }

    #[must_use]
    pub const fn requires_database_lock(self) -> bool {
        !matches!(self, Self::Immediate { .. })
    }
}

impl DatabaseJobKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GsiBackfill => "gsi-backfill",
            Self::GsiUpdate => "gsi-update",
            Self::PartitionFamilyReconcile => "partition-family-reconcile",
            Self::QueuePayloadCleanup => "queue-payload-cleanup",
            Self::StreamTtlCleanup => "stream-ttl-cleanup",
            Self::StreamTrim => "stream-trim",
            Self::TtlSweep => "ttl-sweep",
        }
    }
}

impl PeriodicJobKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Maintenance => "maintenance",
        }
    }
}

impl ImmediateJobKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
        }
    }
}

impl fmt::Display for BackgroundJobName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("unsupported background job '{value}'")]
pub struct BackgroundJobNameParseError {
    value: String,
}

impl BackgroundJobNameParseError {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl FromStr for BackgroundJobName {
    type Err = BackgroundJobNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "gsi-backfill" => Ok(Self::Database {
                kind: DatabaseJobKind::GsiBackfill,
            }),
            "gsi-update" => Ok(Self::Database {
                kind: DatabaseJobKind::GsiUpdate,
            }),
            "partition-family-reconcile" => Ok(Self::Database {
                kind: DatabaseJobKind::PartitionFamilyReconcile,
            }),
            "queue-payload-cleanup" => Ok(Self::Database {
                kind: DatabaseJobKind::QueuePayloadCleanup,
            }),
            "stream-ttl-cleanup" => Ok(Self::Database {
                kind: DatabaseJobKind::StreamTtlCleanup,
            }),
            "stream-trim" => Ok(Self::Database {
                kind: DatabaseJobKind::StreamTrim,
            }),
            "maintenance" => Ok(Self::Periodic {
                kind: PeriodicJobKind::Maintenance,
            }),
            "task" => Ok(Self::Immediate {
                kind: ImmediateJobKind::Task,
            }),
            "ttl-sweep" => Ok(Self::Database {
                kind: DatabaseJobKind::TtlSweep,
            }),
            _ => Err(BackgroundJobNameParseError::new(value)),
        }
    }
}
