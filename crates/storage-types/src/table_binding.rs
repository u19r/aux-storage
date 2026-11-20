//! Catalog metadata and its durable history identity from the same row.

use std::{ops::Deref, sync::Arc};

use crate::{StoredTableInfo, TableHistoryId, TableName, TimestampMillis};

/// Version used to check whether cached table and stream metadata are current.
/// FoundationDB keeps its complete native commit versionstamp; RocksDB owns a
/// durable nonzero counter in its namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogVersion {
    FoundationDb([u8; 10]),
    RocksDb(std::num::NonZeroU64),
}

/// Immutable-ID authority is independent of the reusable public name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableHistoryState {
    Active(Arc<TableHistorySchema>),
    Deleting,
}

/// The ID visibility observation supplies response keys without copying a
/// table name or key schema into every immutable pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableHistorySchema {
    pub source_table_name: crate::SystemStreamSourceTableName,
    pub key_schema: Vec<crate::KeySchemaElement>,
}

impl TryFrom<(TableName, Vec<crate::KeySchemaElement>)> for TableHistorySchema {
    type Error = crate::StorageError;

    fn try_from(
        (name, key_schema): (TableName, Vec<crate::KeySchemaElement>),
    ) -> Result<Self, Self::Error> {
        let source_table_name = crate::SystemStreamSourceTableName::try_from(&name)
            .map_err(|_| crate::StorageError::internal("Invalid history source table name"))?;
        Ok(Self {
            source_table_name,
            key_schema,
        })
    }
}

/// The public metadata Arc can be borrowed by shared request preparation
/// without copying its schemas. The enclosing cache entry retains the native
/// identity. `binding` holds everything a writer needs; one catalog stamp
/// binds it (W6F section 4.1).
#[derive(Debug, Clone)]
pub struct BoundTableInfo {
    pub history_id: TableHistoryId,
    pub info: Arc<StoredTableInfo>,
    pub binding: TableBinding,
}

impl BoundTableInfo {
    /// Item and stream reads: a hidden bootstrap target is not found (REG-0
    /// decision 2).
    pub fn require_data_readable(&self) -> crate::StorageResult<()> {
        if self
            .binding
            .regional
            .as_deref()
            .is_some_and(|binding| binding.state == crate::PairState::Joining)
        {
            return Err(crate::StorageError::table_not_found(&self.info.table_name));
        }
        Ok(())
    }
}

/// The writer-visible catalog binding of one table incarnation. Every change
/// of a field changes the catalog stamp, so a writer that confirms its stamp
/// needs no other catalog, GSI lifecycle, or TTL read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableBinding {
    /// One entry for each GSI of `StoredTableInfo::global_secondary_indexes`
    /// that has a build, in index-ID order.
    pub gsis: Arc<[BoundGsi]>,
    /// The highest index ID assigned in this incarnation. IDs and key-column
    /// generations are never reused.
    pub gsi_high_water: u16,
    pub ttl: Option<TtlBinding>,
    pub stream: Option<StreamBinding>,
    pub regional: Option<Arc<RegionalBinding>>,
}

/// GSI definition reference, physical key columns, build identity, and phase.
/// Key schema and projection stay in the table's GSI definition of the same
/// name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BoundGsi {
    pub name: crate::IndexName,
    pub index_id: u16,
    /// Key columns in key-schema order.
    pub columns: Vec<KeyColumn>,
    pub phase: GsiPhase,
    /// The physical build identity. A rebuild gets a new value.
    pub build: crate::maintenance::TargetIncarnation,
}

/// Writer-visible GSI phase. `Filling`, `Building` and `Active` are the
/// phases of the W6F build protocol; writers maintain the index in each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GsiPhase {
    Filling,
    Building,
    Active,
}

/// One SQL key column. Its physical name is `"<attr>__<kind>_<gen>"`; a new
/// GSI or a changed attribute type gets a new generation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyColumn {
    pub attribute: String,
    pub kind: crate::KeyAttributeType,
    pub generation: u16,
}

impl KeyColumn {
    #[must_use]
    pub fn physical_name(&self) -> String {
        let kind = match self.kind {
            crate::KeyAttributeType::S => "s",
            crate::KeyAttributeType::N => "n",
            crate::KeyAttributeType::B => "b",
        };
        format!("{}__{kind}_{}", self.attribute, self.generation)
    }
}

/// The TTL attribute and status. The sweep checkpoint is job state, not
/// binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlBinding {
    pub attribute: String,
    pub status: crate::TimeToLiveStatus,
}

impl TtlBinding {
    /// Enabled and enabling policies track item expiration.
    #[must_use]
    pub fn tracks(&self) -> bool {
        matches!(
            self.status,
            crate::TimeToLiveStatus::Enabled | crate::TimeToLiveStatus::Enabling
        )
    }
}

/// Stream entry contents and writer position facts. `topology_revision` and
/// `xid_bias` are read with the stamp from the stream topology record (W6F
/// section 4.4); until positions stop using the per-write history read they
/// are zero and no writer reads them.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamBinding {
    pub view: crate::StreamViewType,
    pub topology_revision: u64,
    pub xid_bias: u64,
}

/// A deployment region; the number is its rank (equal-stamp ties, membership
/// races).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct RegionId(pub std::num::NonZeroU16);

/// This region's view of its pair (REG-0 state table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairState {
    /// Hidden target of a bootstrap: no customer or TTL writes; scan pages
    /// accepted.
    Joining,
    /// Source of a bootstrap of its peer: ordinary service.
    Seeding,
    Active,
    /// Graceful removal of this region's replica: customer writes rejected.
    Leaving,
}

/// The pair binding of one table incarnation (at most two regions). Changing it
/// changes the catalog stamp, so every writer that confirms its stamp sees the
/// current pair.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegionalBinding {
    pub pair: crate::PairId,
    pub local: RegionId,
    pub peer: RegionId,
    pub state: PairState,
    /// The peer's configured region name (DescribeTable `Replicas`).
    pub peer_name: String,
}

impl RegionalBinding {
    /// Local customer and TTL mutations are admitted.
    #[must_use]
    pub fn admits_local_writes(&self) -> bool {
        matches!(self.state, PairState::Seeding | PairState::Active)
    }
}

impl TableBinding {
    /// The binding of a table that has no GSI build, no TTL policy, and no
    /// regional binding.
    #[must_use]
    pub fn for_info(info: &StoredTableInfo) -> Self {
        Self {
            stream: StreamBinding::for_info(info),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn gsi(&self, name: &crate::IndexName) -> Option<&BoundGsi> {
        self.gsis.iter().find(|gsi| gsi.name == *name)
    }

    /// Add a GSI with a new index ID. A key column is shared only with a user
    /// GSI that writers maintain and that has the same attribute and type;
    /// otherwise its generation is the new index ID.
    pub fn with_added_gsi(
        &self,
        index: &crate::GlobalSecondaryIndex,
        attributes: &[crate::AttributeDefinition],
        build: crate::maintenance::TargetIncarnation,
        phase: GsiPhase,
    ) -> crate::StorageResult<Self> {
        let index_id = self
            .gsi_high_water
            .checked_add(1)
            .ok_or_else(|| crate::StorageError::internal("GSI index IDs are exhausted"))?;
        let columns = index
            .key_schema
            .iter()
            .map(|element| {
                // The hidden TTL index keys a numeric attribute that is not
                // an attribute definition.
                let kind = attributes
                    .iter()
                    .find(|definition| definition.attribute_name == element.attribute_name)
                    .map_or(crate::KeyAttributeType::N, |definition| {
                        definition.attribute_type
                    });
                let generation = self
                    .gsis
                    .iter()
                    .filter(|gsi| !gsi.name.is_ttl_index())
                    .flat_map(|gsi| &gsi.columns)
                    .find(|column| {
                        column.attribute == element.attribute_name && column.kind == kind
                    })
                    .map_or(index_id, |column| column.generation);
                KeyColumn {
                    attribute: element.attribute_name.clone(),
                    kind,
                    generation,
                }
            })
            .collect();
        let mut gsis = self
            .gsis
            .iter()
            .filter(|gsi| gsi.name != index.index_name)
            .cloned()
            .collect::<Vec<_>>();
        gsis.push(BoundGsi {
            name: index.index_name.clone(),
            index_id,
            columns,
            phase,
            build,
        });
        Ok(Self {
            gsis: gsis.into(),
            gsi_high_water: index_id,
            ..self.clone()
        })
    }

    #[must_use]
    pub fn without_gsi(&self, name: &crate::IndexName) -> Self {
        Self {
            gsis: self
                .gsis
                .iter()
                .filter(|gsi| gsi.name != *name)
                .cloned()
                .collect(),
            ..self.clone()
        }
    }

    /// `None` when the GSI already has `phase`.
    pub fn with_gsi_phase(
        &self,
        name: &crate::IndexName,
        phase: GsiPhase,
    ) -> crate::StorageResult<Option<Self>> {
        let current = self
            .gsi(name)
            .ok_or_else(|| crate::StorageError::internal("GSI is absent from the binding"))?;
        if current.phase == phase {
            return Ok(None);
        }
        Ok(Some(Self {
            gsis: self
                .gsis
                .iter()
                .map(|gsi| {
                    let mut gsi = gsi.clone();
                    if gsi.name == *name {
                        gsi.phase = phase;
                    }
                    gsi
                })
                .collect(),
            ..self.clone()
        }))
    }
}

impl StreamBinding {
    #[must_use]
    pub fn for_info(info: &StoredTableInfo) -> Option<Self> {
        info.stream_specification
            .as_ref()
            .filter(|specification| specification.stream_enabled)
            .map(|specification| Self {
                view: specification
                    .stream_view_type
                    .clone()
                    .unwrap_or(crate::StreamViewType::NewAndOldImages),
                topology_revision: 0,
                xid_bias: 0,
            })
    }
}

/// The persisted GSI part of the binding. Families store it as one value in
/// the catalog record.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredGsiBinding {
    pub high_water: u16,
    pub gsis: Vec<BoundGsi>,
}

impl From<&TableBinding> for StoredGsiBinding {
    fn from(binding: &TableBinding) -> Self {
        Self {
            high_water: binding.gsi_high_water,
            gsis: binding.gsis.to_vec(),
        }
    }
}

impl Deref for BoundTableInfo {
    type Target = StoredTableInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl From<(StoredTableInfo, uuid::Uuid)> for BoundTableInfo {
    fn from((info, id): (StoredTableInfo, uuid::Uuid)) -> Self {
        Self {
            history_id: id.into(),
            binding: TableBinding::for_info(&info),
            info: Arc::new(info),
        }
    }
}

impl std::borrow::Borrow<StoredTableInfo> for BoundTableInfo {
    fn borrow(&self) -> &StoredTableInfo {
        &self.info
    }
}

impl From<BoundTableInfo> for StoredTableInfo {
    fn from(bound: BoundTableInfo) -> Self {
        Arc::unwrap_or_clone(bound.info)
    }
}

impl std::ops::DerefMut for BoundTableInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.info)
    }
}

/// One stream enable of one table incarnation. The enable stamp names the
/// stream; the store issues each enable a stamp above every earlier one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStreamIdentity {
    pub history_id: TableHistoryId,
    pub enabled_at: TimestampMillis,
}

impl BoundTableInfo {
    /// `None` until the table's first stream enable.
    pub fn stream_identity(&self) -> Option<TableStreamIdentity> {
        self.info
            .stream_enabled_at
            .map(|enabled_at| TableStreamIdentity {
                history_id: self.history_id,
                enabled_at,
            })
    }
}

/// Pin a name-based request to one accepted immutable incarnation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "TableHistoryTargetInput")]
pub struct TableHistoryTarget {
    pub table_name: TableName,
    pub history_id: TableHistoryId,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TableHistoryTargetInput {
    table_name: TableName,
    history_id: TableHistoryId,
}

impl TryFrom<TableHistoryTargetInput> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from(input: TableHistoryTargetInput) -> Result<Self, Self::Error> {
        if input.table_name.as_ref().is_empty() || input.history_id.as_ref() == &[0; 16] {
            return Err(crate::StorageError::raw_validation(
                "Invalid table incarnation",
            ));
        }
        Ok(Self {
            table_name: input.table_name,
            history_id: input.history_id,
        })
    }
}

impl From<&BoundTableInfo> for TableHistoryTarget {
    fn from(table: &BoundTableInfo) -> Self {
        Self {
            table_name: table.info.table_name.clone(),
            history_id: table.history_id,
        }
    }
}

impl TryFrom<(TableName, &str)> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from((table_name, id): (TableName, &str)) -> Result<Self, Self::Error> {
        let invalid = || crate::StorageError::raw_validation("Invalid table incarnation");
        let id = uuid::Uuid::parse_str(id).map_err(|_| invalid())?;
        if table_name.as_ref().is_empty() || id.is_nil() {
            return Err(invalid());
        }
        Ok(Self {
            table_name,
            history_id: id.into(),
        })
    }
}

impl TryFrom<&crate::TableDescription> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from(description: &crate::TableDescription) -> Result<Self, Self::Error> {
        Self::try_from((
            description.table_name.clone(),
            description.table_id.as_str(),
        ))
    }
}
