use replication_provider::runtime_configuration::deployment::DeploymentConfiguration;
use replication_types::{
    configuration::{
        control::{ConfigurationChange, ControlOperation},
        installed::RegionalSourceBinding,
        joining::JoiningTableDefinition,
        table::RegionalMode,
    },
    identity::{
        replica::RegionId,
        resource::{ControlOperationId, TableIdentity, TableLineage},
    },
};
use storage_types::{StorageError, StorageResult, TableName, UpdateTableRequest};

use crate::{
    bootstrap::SourceSchemaObservation, enable::EnablePlan, native_record::corrupt_record,
};

/// Allocate logical operation identities once at the request boundary. Native
/// retries reuse them; physical target incarnations still come from peers.
#[derive(Clone)]
pub(crate) struct ReplicaCreateRequest {
    pub table: TableName,
    operation: ControlOperationId,
    lineage: TableLineage,
    source: TableIdentity,
    targets: Vec<(RegionId, TableIdentity)>,
}

impl ReplicaCreateRequest {
    pub(crate) fn try_from_update(
        request: UpdateTableRequest,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<Self> {
        if crate::serving::replica_request::validate_replica_request(&request)?
            != crate::serving::replica_request::ReplicaActionKind::Create
        {
            return Err(StorageError::validation(
                "replica enable requires create actions",
            ));
        }
        let updates = request.replica_updates.ok_or_else(corrupt_record)?;
        let mut targets = Vec::with_capacity(updates.len());
        for update in updates {
            let create = update.create.ok_or_else(|| {
                StorageError::validation("ReplicaUpdates requires exactly one action")
            })?;
            let region = create
                .region_name
                .parse::<RegionId>()
                .map_err(|_| StorageError::validation("invalid replica region"))?;
            if region == configuration.region {
                return Err(StorageError::validation("replica region is already local"));
            }
            Self::require_peer(configuration, &region)?;
            let table = TableIdentity::try_from(uuid::Uuid::new_v4().into_bytes())
                .map_err(|_| corrupt_record())?;
            targets.push((region, table));
        }
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        if targets.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(StorageError::validation("duplicate replica region"));
        }
        let identity = uuid::Uuid::new_v4().into_bytes();
        Ok(Self {
            table: request.table_name,
            operation: identity.try_into().map_err(|_| corrupt_record())?,
            lineage: identity.try_into().map_err(|_| corrupt_record())?,
            source: uuid::Uuid::new_v4()
                .into_bytes()
                .try_into()
                .map_err(|_| corrupt_record())?,
            targets,
        })
    }

    pub(crate) fn source_identity(&self) -> TableIdentity {
        self.source
    }

    pub(crate) fn bind_enable_plan(
        &self,
        observation: &SourceSchemaObservation,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<EnablePlan> {
        let before = observation.source();
        let mut source = before.clone();
        let config = before
            .configuration
            .config
            .checked_next()
            .map_err(|_| corrupt_record())?;
        let mut members = self
            .targets
            .iter()
            .map(|(region, _)| region.clone())
            .collect::<Vec<_>>();
        members.push(configuration.region.clone());
        members.sort();
        source.configuration.config = config;
        source.configuration.lineage = self.lineage;
        source.configuration.regional = RegionalMode::Global {
            primary: configuration.region.clone(),
            members: members.clone(),
        };
        source.membership = u64::from(before.membership)
            .checked_add(1)
            .ok_or_else(corrupt_record)?
            .into();
        source.sources = self.bind_sources(configuration, (before.configuration.table, config))?;
        let operation = ControlOperation {
            id: self.operation,
            table: before.configuration.table,
            lineage: self.lineage,
            expected: before.configuration.config,
            proposed: config,
            content: [0; 32].into(),
            change: ConfigurationChange::EnableGlobal {
                primary: configuration.region.clone(),
                members: members.clone(),
            },
            home_region: configuration.region.clone(),
            participants: members,
            before: Some(before.configuration.clone()),
        };
        let targets = self
            .targets
            .iter()
            .map(|(_, table)| {
                let mut definition = source.configuration.clone();
                definition.table = *table;
                JoiningTableDefinition {
                    name: source.name.clone(),
                    configuration: definition,
                    membership: source.membership,
                    sources: source.sources.clone(),
                    schemas: source.schemas.clone(),
                }
            })
            .collect();
        EnablePlan::try_new(operation, source, observation, targets)
    }

    pub(crate) fn bind_addition_plan(
        &self,
        observation: &crate::addition::AdditionSourceObservation,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<crate::addition::AddRegionPlan> {
        let before = observation.source();
        let RegionalMode::Global { primary, members } = &before.configuration.regional else {
            return Err(corrupt_record());
        };
        if primary != &configuration.region {
            return Err(StorageError::validation(
                "replica addition requires configuration primary",
            ));
        }
        if self.targets.len() != 1 {
            return Err(StorageError::validation(
                "global replica addition requires one create action",
            ));
        }
        let (region, table) = &self.targets[0];
        if members.contains(region) {
            return Err(StorageError::validation(
                "replica region is already a member",
            ));
        }
        let peer = Self::require_peer(configuration, region)?;
        let config = before
            .configuration
            .config
            .checked_next()
            .map_err(|_| corrupt_record())?;
        let mut members = members.clone();
        members.push(region.clone());
        members.sort();
        let mut target_configuration = before.configuration.clone();
        target_configuration.table = *table;
        target_configuration.config = config;
        target_configuration.regional = RegionalMode::Global {
            primary: primary.clone(),
            members: members.clone(),
        };
        let mut sources = before
            .sources
            .iter()
            .filter(|source| &source.region != region)
            .cloned()
            .collect::<Vec<_>>();
        for source in &mut sources {
            source.config = config;
        }
        sources.push(RegionalSourceBinding {
            region: region.clone(),
            database_group: peer.group,
            generation: peer.generation,
            partitions: configuration.journal_partitions,
            table: *table,
            config,
        });
        sources.sort_by(|left, right| left.region.cmp(&right.region));
        let target = JoiningTableDefinition {
            name: before.name.clone(),
            configuration: target_configuration,
            membership: u64::from(before.membership)
                .checked_add(1)
                .ok_or_else(corrupt_record)?
                .into(),
            sources,
            schemas: before.schemas.clone(),
        };
        let operation = ControlOperation {
            id: self.operation,
            table: before.configuration.table,
            lineage: before.configuration.lineage,
            expected: before.configuration.config,
            proposed: config,
            content: [0; 32].into(),
            change: ConfigurationChange::AddRegion {
                region: region.clone(),
            },
            home_region: primary.clone(),
            participants: members,
            before: Some(before.configuration.clone()),
        };
        crate::addition::AddRegionPlan::try_new(operation, observation, target)
    }

    fn bind_sources(
        &self,
        configuration: &DeploymentConfiguration,
        source: (
            TableIdentity,
            replication_types::configuration::table::ConfigRevision,
        ),
    ) -> StorageResult<Vec<RegionalSourceBinding>> {
        let (table, config) = source;
        let mut sources = vec![RegionalSourceBinding {
            region: configuration.region.clone(),
            database_group: configuration.group,
            generation: configuration.journal_generation,
            partitions: configuration.journal_partitions,
            table,
            config,
        }];
        for (region, table) in &self.targets {
            let peer = Self::require_peer(configuration, region)?;
            sources.push(RegionalSourceBinding {
                region: region.clone(),
                database_group: peer.group,
                generation: peer.generation,
                partitions: configuration.journal_partitions,
                table: *table,
                config,
            });
        }
        sources.sort_by(|left, right| left.region.cmp(&right.region));
        Ok(sources)
    }

    fn require_peer<'a>(
        configuration: &'a DeploymentConfiguration,
        region: &RegionId,
    ) -> StorageResult<&'a replication_provider::runtime_configuration::deployment::PeerConfiguration>
    {
        let mut peers = configuration
            .peers
            .iter()
            .filter(|peer| &peer.region == region);
        let selected = peers
            .next()
            .ok_or_else(|| StorageError::validation("replica region is not configured"))?;
        if peers.any(|peer| peer.group != selected.group || peer.generation != selected.generation)
        {
            return Err(StorageError::validation(
                "replica region has ambiguous database generations",
            ));
        }
        Ok(selected)
    }
}
