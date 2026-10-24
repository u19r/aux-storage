use std::num::NonZeroU64;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use storage_types::{SystemStreamContractError, SystemStreamPosition, SystemStreamScope};

use crate::system_stream::{SystemStreamLineage, SystemStreamLineageCatalog};

/// Stable identity for one logical split operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SystemStreamSplitOperationId(String);

impl SystemStreamSplitOperationId {
    /// Validate and create an operation identity.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SystemStreamContractError> {
        let value = value.into();
        if value.is_empty()
            || value.trim().is_empty()
            || value.len() > storage_types::SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One child lineage and its initialized, one-based sequence value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamSplitChild {
    lineage: SystemStreamLineage,
    initial_sequence: NonZeroU64,
}

impl SystemStreamSplitChild {
    /// Validate a new active child and its first sequence value.
    pub fn try_new(
        lineage: SystemStreamLineage,
        initial_sequence: u64,
    ) -> Result<Self, SystemStreamContractError> {
        let initial_sequence =
            NonZeroU64::new(initial_sequence).ok_or(SystemStreamContractError::BackendContract)?;
        if lineage.sealed()
            || lineage.parent_lineage_id().is_none()
            || initial_sequence.get() == u64::MAX
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(Self {
            lineage,
            initial_sequence,
        })
    }

    #[must_use]
    pub fn lineage(&self) -> &SystemStreamLineage {
        &self.lineage
    }

    #[must_use]
    pub const fn initial_sequence(&self) -> NonZeroU64 {
        self.initial_sequence
    }

    pub fn initial_frontier(&self) -> Result<SystemStreamPosition, SystemStreamContractError> {
        self.lineage
            .position(format!("{:020}", self.initial_sequence.get() - 1))
    }
}

/// The validated provider input for one logical parent split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamSplitRequest {
    source_scope: SystemStreamScope,
    parent: SystemStreamLineage,
    children: [SystemStreamSplitChild; 2],
    operation_id: SystemStreamSplitOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LineageWire {
    shard_id: String,
    lineage_id: String,
    parent_lineage_id: Option<String>,
    sealed: bool,
}

impl From<&SystemStreamLineage> for LineageWire {
    fn from(lineage: &SystemStreamLineage) -> Self {
        Self {
            shard_id: lineage.shard_id().as_str().to_string(),
            lineage_id: lineage.lineage_id().as_str().to_string(),
            parent_lineage_id: lineage
                .parent_lineage_id()
                .map(|parent| parent.as_str().to_string()),
            sealed: lineage.sealed(),
        }
    }
}

impl TryFrom<LineageWire> for SystemStreamLineage {
    type Error = SystemStreamContractError;

    fn try_from(wire: LineageWire) -> Result<Self, Self::Error> {
        Self::try_new(
            wire.shard_id,
            wire.lineage_id,
            wire.parent_lineage_id,
            wire.sealed,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitChildWire {
    lineage: LineageWire,
    initial_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitRequestWire {
    source_scope: SystemStreamScope,
    parent: LineageWire,
    children: [SplitChildWire; 2],
    operation_id: String,
}

impl SystemStreamSplitRequest {
    /// Validate the scope, parent relation, and exact two-child shape.
    pub fn try_new(
        source_scope: SystemStreamScope,
        parent: SystemStreamLineage,
        children: [SystemStreamSplitChild; 2],
        operation_id: SystemStreamSplitOperationId,
    ) -> Result<Self, SystemStreamContractError> {
        validate_split_shape(&parent, &children)?;
        Ok(Self {
            source_scope,
            parent,
            children,
            operation_id,
        })
    }

    /// Validate a fresh split against the provider's current catalog.
    pub fn validate_against(
        &self,
        catalog: &SystemStreamLineageCatalog,
    ) -> Result<(), SystemStreamContractError> {
        validate_scope(&self.source_scope, catalog.source_scope())?;
        let current_parent = catalog
            .lineage(self.parent.lineage_id())
            .ok_or(SystemStreamContractError::UnknownLineage)?;
        if current_parent != &self.parent || current_parent.sealed() {
            return Err(SystemStreamContractError::BackendContract);
        }
        for child in &self.children {
            let child_lineage = child.lineage();
            if catalog.lineage(child_lineage.lineage_id()).is_some()
                || catalog
                    .entries()
                    .iter()
                    .any(|entry| entry.shard_id() == child_lineage.shard_id())
                || catalog.entries().iter().any(|entry| {
                    super::pointer_slot(entry.lineage_id())
                        == super::pointer_slot(child_lineage.lineage_id())
                })
            {
                return Err(SystemStreamContractError::BackendContract);
            }
        }
        if super::pointer_slot(self.children[0].lineage().lineage_id())
            == super::pointer_slot(self.children[1].lineage().lineage_id())
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(())
    }

    #[must_use]
    pub fn operation_fingerprint(&self) -> Vec<u8> {
        let mut fingerprint = Vec::with_capacity(256);
        push_u8(&mut fingerprint, self.source_scope.version());
        push_text(&mut fingerprint, self.source_scope.scope_id().as_str());
        push_u64(&mut fingerprint, self.source_scope.generation());
        push_lineage(&mut fingerprint, &self.parent);
        let mut children = [&self.children[0], &self.children[1]];
        children.sort_unstable_by(|left, right| {
            left.lineage()
                .shard_id()
                .as_str()
                .cmp(right.lineage().shard_id().as_str())
                .then_with(|| {
                    left.lineage()
                        .lineage_id()
                        .as_str()
                        .cmp(right.lineage().lineage_id().as_str())
                })
        });
        for child in children {
            push_child(&mut fingerprint, child);
        }
        push_text(&mut fingerprint, self.operation_id.as_str());
        fingerprint
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn parent(&self) -> &SystemStreamLineage {
        &self.parent
    }

    #[must_use]
    pub fn children(&self) -> &[SystemStreamSplitChild; 2] {
        &self.children
    }

    #[must_use]
    pub fn operation_id(&self) -> &SystemStreamSplitOperationId {
        &self.operation_id
    }
}

fn push_child(fingerprint: &mut Vec<u8>, child: &SystemStreamSplitChild) {
    push_lineage(fingerprint, child.lineage());
    push_u64(fingerprint, child.initial_sequence().get());
}

fn push_lineage(fingerprint: &mut Vec<u8>, lineage: &SystemStreamLineage) {
    push_text(fingerprint, lineage.shard_id().as_str());
    push_text(fingerprint, lineage.lineage_id().as_str());
    match lineage.parent_lineage_id() {
        Some(parent) => {
            push_u8(fingerprint, 1);
            push_text(fingerprint, parent.as_str());
        }
        None => push_u8(fingerprint, 0),
    }
    push_u8(fingerprint, u8::from(lineage.sealed()));
}

fn push_text(fingerprint: &mut Vec<u8>, value: &str) {
    push_u64(fingerprint, value.len() as u64);
    fingerprint.extend_from_slice(value.as_bytes());
}

fn push_u8(fingerprint: &mut Vec<u8>, value: u8) {
    fingerprint.push(value);
}

fn push_u64(fingerprint: &mut Vec<u8>, value: u64) {
    fingerprint.extend_from_slice(&value.to_le_bytes());
}

impl Serialize for SystemStreamSplitRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let children = self.children.each_ref().map(|child| SplitChildWire {
            lineage: LineageWire::from(child.lineage()),
            initial_sequence: child.initial_sequence().get(),
        });
        SplitRequestWire {
            source_scope: self.source_scope.clone(),
            parent: LineageWire::from(&self.parent),
            children,
            operation_id: self.operation_id.as_str().to_string(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamSplitRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = SplitRequestWire::deserialize(deserializer)?;
        let parent = SystemStreamLineage::try_from(wire.parent).map_err(D::Error::custom)?;
        let [left, right] = wire.children;
        let children = [decode_child::<D>(left)?, decode_child::<D>(right)?];
        let operation_id =
            SystemStreamSplitOperationId::try_new(wire.operation_id).map_err(D::Error::custom)?;
        Self::try_new(wire.source_scope, parent, children, operation_id).map_err(D::Error::custom)
    }
}

fn decode_child<'de, D: Deserializer<'de>>(
    child: SplitChildWire,
) -> Result<SystemStreamSplitChild, D::Error> {
    let lineage = SystemStreamLineage::try_from(child.lineage).map_err(D::Error::custom)?;
    SystemStreamSplitChild::try_new(lineage, child.initial_sequence).map_err(D::Error::custom)
}

fn validate_scope(
    actual: &SystemStreamScope,
    expected: &SystemStreamScope,
) -> Result<(), SystemStreamContractError> {
    if actual.version() != expected.version() || actual.scope_id() != expected.scope_id() {
        return Err(SystemStreamContractError::WrongScope);
    }
    if actual.generation() != expected.generation() {
        return Err(SystemStreamContractError::WrongGeneration);
    }
    Ok(())
}

fn validate_split_shape(
    parent: &SystemStreamLineage,
    children: &[SystemStreamSplitChild; 2],
) -> Result<(), SystemStreamContractError> {
    if parent.sealed() {
        return Err(SystemStreamContractError::BackendContract);
    }
    let [left, right] = children;
    if left.lineage().lineage_id() == right.lineage().lineage_id()
        || left.lineage().shard_id() == right.lineage().shard_id()
        || children
            .iter()
            .any(|child| child.lineage().lineage_id() == parent.lineage_id())
    {
        return Err(SystemStreamContractError::BackendContract);
    }
    if children.iter().any(|child| {
        child.lineage().parent_lineage_id() != Some(parent.lineage_id())
            || child.lineage().shard_id() == parent.shard_id()
    }) {
        return Err(SystemStreamContractError::BackendContract);
    }
    Ok(())
}

/// Whether a committed split receipt was newly applied or exactly replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemStreamSplitDisposition {
    Applied,
    Replayed,
}

/// The logical receipt of an atomic provider-owned split.
///
/// A provider returns this only after one native transaction has sealed the
/// parent, created both child catalog entries and sequences, and published its
/// private routing/topology.  The receipt deliberately carries no physical
/// identifier or unvalidated opaque witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamSplitResult {
    source_scope: SystemStreamScope,
    operation_id: SystemStreamSplitOperationId,
    sealed_parent: SystemStreamLineage,
    children: [SystemStreamSplitChild; 2],
    disposition: SystemStreamSplitDisposition,
}

/// Durable logical evidence for one committed lineage split.
///
/// Providers persist this receipt in their native transaction and separately
/// verify that their physical catalog and sequences still match it on replay.
#[derive(Clone, Serialize, Deserialize)]
pub struct SystemStreamSplitReceipt {
    pub split: SystemStreamSplitResult,
    pub sealed_after_offset: u64,
}

impl SystemStreamSplitReceipt {
    pub fn try_new(
        request: &SystemStreamSplitRequest,
        sealed_after_offset: u64,
    ) -> Result<Self, SystemStreamContractError> {
        if sealed_after_offset == 0 {
            return Err(SystemStreamContractError::BackendContract);
        }
        let sealed_parent = SystemStreamLineage::try_new(
            request.parent().shard_id().as_str(),
            request.parent().lineage_id().as_str(),
            request
                .parent()
                .parent_lineage_id()
                .map(|parent| parent.as_str().to_string()),
            true,
        )?;
        Ok(Self {
            split: SystemStreamSplitResult::try_new(
                request,
                sealed_parent,
                SystemStreamSplitDisposition::Applied,
            )?,
            sealed_after_offset,
        })
    }

    pub fn replay(
        &self,
        request: &SystemStreamSplitRequest,
    ) -> Result<SystemStreamSplitResult, SystemStreamContractError> {
        self.split.validate_retry(request)?;
        SystemStreamSplitResult::try_new(
            request,
            self.split.sealed_parent().clone(),
            SystemStreamSplitDisposition::Replayed,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitResultWire {
    source_scope: SystemStreamScope,
    operation_id: String,
    sealed_parent: LineageWire,
    children: [SplitChildWire; 2],
    disposition: SystemStreamSplitDisposition,
}

impl SystemStreamSplitResult {
    /// Validate and assemble a committed split receipt for one request.
    ///
    /// This constructor proves the logical identity and shape of the receipt;
    /// the provider must call it only after its native transaction has durably
    /// recorded the operation receipt and all split effects.
    pub fn try_new(
        request: &SystemStreamSplitRequest,
        sealed_parent: SystemStreamLineage,
        disposition: SystemStreamSplitDisposition,
    ) -> Result<Self, SystemStreamContractError> {
        if !sealed_parent.sealed() {
            return Err(SystemStreamContractError::BackendContract);
        }
        if sealed_parent.shard_id() != request.parent.shard_id()
            || sealed_parent.lineage_id() != request.parent.lineage_id()
            || sealed_parent.parent_lineage_id() != request.parent.parent_lineage_id()
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(Self {
            source_scope: request.source_scope.clone(),
            operation_id: request.operation_id.clone(),
            sealed_parent,
            children: request.children.clone(),
            disposition,
        })
    }

    /// Check that a retry names exactly the operation and logical split that
    /// produced this receipt; mismatches must be rejected before any effects.
    pub fn validate_retry(
        &self,
        request: &SystemStreamSplitRequest,
    ) -> Result<(), SystemStreamContractError> {
        validate_scope(&request.source_scope, &self.source_scope)?;
        if request.operation_id != self.operation_id
            || request.parent.shard_id() != self.sealed_parent.shard_id()
            || request.parent.lineage_id() != self.sealed_parent.lineage_id()
            || request.parent.parent_lineage_id() != self.sealed_parent.parent_lineage_id()
            || request.children != self.children
        {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(())
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn operation_id(&self) -> &SystemStreamSplitOperationId {
        &self.operation_id
    }

    #[must_use]
    pub fn sealed_parent(&self) -> &SystemStreamLineage {
        &self.sealed_parent
    }

    #[must_use]
    pub fn children(&self) -> &[SystemStreamSplitChild; 2] {
        &self.children
    }

    #[must_use]
    pub const fn disposition(&self) -> SystemStreamSplitDisposition {
        self.disposition
    }
}

impl Serialize for SystemStreamSplitResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let children = self.children.each_ref().map(|child| SplitChildWire {
            lineage: LineageWire::from(child.lineage()),
            initial_sequence: child.initial_sequence().get(),
        });
        SplitResultWire {
            source_scope: self.source_scope.clone(),
            operation_id: self.operation_id.as_str().to_string(),
            sealed_parent: LineageWire::from(&self.sealed_parent),
            children,
            disposition: self.disposition,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamSplitResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = SplitResultWire::deserialize(deserializer)?;
        let sealed_parent =
            SystemStreamLineage::try_from(wire.sealed_parent).map_err(D::Error::custom)?;
        let parent = SystemStreamLineage::try_new(
            sealed_parent.shard_id().as_str(),
            sealed_parent.lineage_id().as_str(),
            sealed_parent
                .parent_lineage_id()
                .map(|parent| parent.as_str().to_string()),
            false,
        )
        .map_err(D::Error::custom)?;
        let [left, right] = wire.children;
        let children = [decode_child::<D>(left)?, decode_child::<D>(right)?];
        let operation_id =
            SystemStreamSplitOperationId::try_new(wire.operation_id).map_err(D::Error::custom)?;
        let request =
            SystemStreamSplitRequest::try_new(wire.source_scope, parent, children, operation_id)
                .map_err(D::Error::custom)?;
        Self::try_new(&request, sealed_parent, wire.disposition).map_err(D::Error::custom)
    }
}
