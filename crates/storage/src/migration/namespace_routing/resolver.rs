use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use lru_ttl_cache::{CacheConfig, FetchingLruTtlCache, arc_fetch_fn};
use storage_types::{
    AttributeValue, IndexName, QueryTableRequest, StorageError, StorageResult, TableNamespace,
    TimestampMillis, from_hashmap,
};
use tokio::sync::RwLock;

use crate::{
    namespace_routing::{
        CutoverEvent, CutoverEventStatus, CutoverOverride, LocationBackendKindSerde,
        LocationDescriptorSerde, NamespaceRoute, NamespaceRouteMigrationModeSerde,
        NamespaceRouteRecord, NamespaceRouteRecordSerde, NamespaceStorageMigrationMode,
        NamespaceStorageMode, RouteTarget,
    },
    newtypes::DatabaseTrait,
    tables::Tables,
};

const TENANT_ROUTE_CACHE_CAPACITY: usize = 200_000;
const TENANT_ROUTE_CACHE_TTL: Duration = Duration::from_secs(60 * 30);
const TENANT_ROUTE_CACHE_REFRESH_TTL: Duration = Duration::from_secs(60 * 5);
const LOCATION_DESCRIPTOR_CACHE_CAPACITY: usize = 16_384;
const LOCATION_DESCRIPTOR_CACHE_TTL: Duration = Duration::from_secs(60 * 5);
const LOCATION_DESCRIPTOR_CACHE_REFRESH_TTL: Duration = Duration::from_secs(60);
const ROUTING_DICTIONARY_PK: &str = "SYS#ROUTING";
const ROUTING_DICTIONARY_SK_PREFIX: &str = "LOC#";
const NAMESPACE_ROUTE_PK_PREFIX: &str = "NS#";
const SHARED_TABLE_LOOKUP_MARKER: &str = "ST#1";
const SHARED_TABLE_LOOKUP_QUERY: &str = "gsi2pk = :pk";
const DEFAULT_CUTOVER_PAUSE_MS: i64 = 250;

pub struct NamespaceRouteResolver {
    default_connection_id: String,
    control_plane: Arc<dyn DatabaseTrait>,
    namespace_routes: FetchingLruTtlCache<TableNamespace, NamespaceRouteRecord, StorageError>,
    location_descriptors: FetchingLruTtlCache<u16, LocationDescriptorSerde, StorageError>,
    cutover_overrides: RwLock<HashMap<TableNamespace, CutoverOverride>>,
    write_pause_ms: i64,
}

impl NamespaceRouteResolver {
    #[must_use]
    pub fn new(
        default_connection_id: String,
        control_plane: Arc<dyn DatabaseTrait>,
        enable_background_refresh: bool,
    ) -> Self {
        let route_provider = Arc::clone(&control_plane);
        let route_fetch = arc_fetch_fn(move |namespace: TableNamespace| {
            let route_provider = Arc::clone(&route_provider);
            async move { fetch_namespace_route_record(route_provider, namespace).await }
        });

        let descriptor_provider = Arc::clone(&control_plane);
        let descriptor_fetch = arc_fetch_fn(move |loc: u16| {
            let descriptor_provider = Arc::clone(&descriptor_provider);
            async move { fetch_location_descriptor(descriptor_provider, loc).await }
        });

        let mut namespace_routes = CacheConfig::new()
            .with_capacity(TENANT_ROUTE_CACHE_CAPACITY)
            .with_ttl(TENANT_ROUTE_CACHE_TTL)
            .with_fetch(route_fetch);
        if enable_background_refresh {
            namespace_routes = namespace_routes.with_refresh_ttl(TENANT_ROUTE_CACHE_REFRESH_TTL);
        }

        let mut location_descriptors = CacheConfig::new()
            .with_capacity(LOCATION_DESCRIPTOR_CACHE_CAPACITY)
            .with_ttl(LOCATION_DESCRIPTOR_CACHE_TTL)
            .with_fetch(descriptor_fetch);
        if enable_background_refresh {
            location_descriptors =
                location_descriptors.with_refresh_ttl(LOCATION_DESCRIPTOR_CACHE_REFRESH_TTL);
        }

        Self {
            default_connection_id,
            control_plane,
            namespace_routes: FetchingLruTtlCache::new(namespace_routes),
            location_descriptors: FetchingLruTtlCache::new(location_descriptors),
            cutover_overrides: RwLock::new(HashMap::new()),
            write_pause_ms: DEFAULT_CUTOVER_PAUSE_MS,
        }
    }

    pub async fn preload_shared_table_namespaces(&self) -> StorageResult<()> {
        let mut next: Option<String> = None;
        loop {
            let request = QueryTableRequest {
                table_name: Tables::sys_namespaces(),
                index_name: Some(IndexName::new("gsi2")),
                key_condition_expression: SHARED_TABLE_LOOKUP_QUERY.to_string(),
                expression_attribute_names: None,
                expression_attribute_values: Some(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S(SHARED_TABLE_LOOKUP_MARKER.to_string()),
                )])),
                limit: Some(1_000),
                exclusive_start_key: next.clone(),
                scan_index_forward: Some(true),
                consistent_read: false,
            };
            let (items, token) = self.control_plane.query_table(&request).await?;
            for item in items {
                let map = item.into_attribute_map()?;
                if let Some((namespace, route_record)) = parse_namespace_route_record(map)? {
                    self.namespace_routes.insert(namespace, route_record);
                }
            }
            if token.is_none() {
                break;
            }
            next = token;
        }
        Ok(())
    }

    pub async fn resolve_route(&self, namespace: &TableNamespace) -> StorageResult<NamespaceRoute> {
        let route_record = self
            .namespace_routes
            .get_or_fetch(namespace)
            .await?
            .ok_or_else(|| {
                StorageError::table_not_found(&Tables::namespace(namespace).to_string())
            })?;
        self.route_for_record(namespace, &route_record).await
    }

    pub(crate) async fn route_for_record(
        &self,
        namespace: &TableNamespace,
        route_record: &NamespaceRouteRecord,
    ) -> StorageResult<NamespaceRoute> {
        let now_ms = TimestampMillis::now().timestamp_millis();
        let override_loc = self.effective_cutover_override(namespace, now_ms).await;

        let (mut read_loc, mut write_locs, writes_paused) =
            match route_record.migration_mode.clone() {
                NamespaceStorageMigrationMode::Single => {
                    (route_record.loc, vec![route_record.loc], false)
                }
                NamespaceStorageMigrationMode::DualWrite {
                    old_loc,
                    new_loc,
                    cutover_at_ms,
                } => {
                    let cutover_ms = cutover_at_ms.timestamp_millis();
                    let pause_start_ms = cutover_ms.saturating_sub(self.write_pause_ms);
                    let writes_paused = now_ms >= pause_start_ms && now_ms < cutover_ms;
                    let read_loc = if now_ms >= cutover_ms {
                        new_loc
                    } else {
                        old_loc
                    };
                    (
                        read_loc,
                        dedupe_locs([old_loc, new_loc].into_iter()),
                        writes_paused,
                    )
                }
            };

        if let Some(cutover_loc) = override_loc {
            read_loc = cutover_loc;
            if !write_locs.contains(&cutover_loc) {
                write_locs.push(cutover_loc);
            }
        }

        let read_target = self
            .target_for_loc(namespace, route_record.storage_mode, read_loc)
            .await?;

        let mut write_targets = Vec::with_capacity(write_locs.len());
        let mut seen_write_targets = HashSet::with_capacity(write_locs.len());
        for loc in write_locs {
            let target = self
                .target_for_loc(namespace, route_record.storage_mode, loc)
                .await?;
            let dedupe_key = (target.connection_id.clone(), target.table_name.clone());
            if seen_write_targets.insert(dedupe_key) {
                write_targets.push(target);
            }
        }

        Ok(NamespaceRoute {
            namespace: namespace.clone(),
            storage_mode: route_record.storage_mode,
            read_target,
            write_targets,
            writes_paused,
        })
    }

    pub fn invalidate_namespace(&self, namespace: &TableNamespace) {
        self.namespace_routes.remove(namespace);
    }

    pub fn seed_single_route(
        &self,
        namespace: TableNamespace,
        storage_mode: NamespaceStorageMode,
        loc: u16,
    ) {
        self.namespace_routes.insert(
            namespace,
            NamespaceRouteRecord {
                storage_mode,
                loc,
                migration_mode: NamespaceStorageMigrationMode::Single,
            },
        );
    }

    pub fn invalidate_location(&self, loc: u16) {
        self.location_descriptors.remove(&loc);
    }

    pub async fn apply_cutover_event(&self, event: &CutoverEvent) -> StorageResult<()> {
        match event.status {
            CutoverEventStatus::Canceled | CutoverEventStatus::Failed => {
                self.cutover_overrides
                    .write()
                    .await
                    .remove(&event.namespace);
            }
            CutoverEventStatus::Scheduled | CutoverEventStatus::Applied => {
                self.cutover_overrides.write().await.insert(
                    event.namespace.clone(),
                    CutoverOverride {
                        new_loc: event.new_loc,
                        effective_at_ms: event.effective_at_ms,
                    },
                );
            }
        }
        self.invalidate_namespace(&event.namespace);
        Ok(())
    }

    async fn effective_cutover_override(
        &self,
        namespace: &TableNamespace,
        now_ms: i64,
    ) -> Option<u16> {
        self.cutover_overrides
            .read()
            .await
            .get(namespace)
            .and_then(|value| {
                if now_ms >= value.effective_at_ms.timestamp_millis() {
                    Some(value.new_loc)
                } else {
                    None
                }
            })
    }

    async fn target_for_loc(
        &self,
        namespace: &TableNamespace,
        mode: NamespaceStorageMode,
        loc: u16,
    ) -> StorageResult<RouteTarget> {
        let table_name = mode.source_table_name(namespace, loc);
        let connection_id = self.connection_id_for_loc(loc).await?;
        Ok(RouteTarget {
            connection_id,
            table_name,
            loc,
        })
    }

    pub async fn resolve_target_for_loc(
        &self,
        namespace: &TableNamespace,
        mode: NamespaceStorageMode,
        loc: u16,
    ) -> StorageResult<RouteTarget> {
        self.target_for_loc(namespace, mode, loc).await
    }

    async fn connection_id_for_loc(&self, loc: u16) -> StorageResult<String> {
        if loc == 0 {
            return Ok(self.default_connection_id.clone());
        }
        let descriptor = self.location_descriptors.get_or_fetch(&loc).await?;
        if let Some(descriptor) = descriptor {
            return Ok(descriptor.connection_id.clone());
        }

        self.location_descriptors.remove(&loc);
        let descriptor = self.location_descriptors.get_or_fetch(&loc).await?;
        if let Some(descriptor) = descriptor {
            return Ok(descriptor.connection_id.clone());
        }

        Err(StorageError::validation(format!(
            "shared table routing failed closed: unknown location code {loc}"
        )))
    }
}

fn dedupe_locs<I>(locs: I) -> Vec<u16>
where I: Iterator<Item = u16> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for loc in locs {
        if seen.insert(loc) {
            output.push(loc);
        }
    }
    output
}

async fn fetch_namespace_route_record(
    control_plane: Arc<dyn DatabaseTrait>,
    namespace: TableNamespace,
) -> StorageResult<Option<NamespaceRouteRecord>> {
    let key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("{NAMESPACE_ROUTE_PK_PREFIX}{}", namespace.as_str())),
        ),
        ("sk".to_string(), AttributeValue::S("META".to_string())),
    ]);
    let item = control_plane
        .get_item(Tables::sys_namespaces(), key.into(), true)
        .await?;
    let Some(item) = item else {
        return Ok(None);
    };
    let map = item.into_attribute_map()?;
    let Some((_, route_record)) = parse_namespace_route_record(map)? else {
        return Ok(None);
    };
    Ok(Some(route_record))
}

pub(crate) fn parse_namespace_route_record(
    map: HashMap<String, AttributeValue>,
) -> StorageResult<Option<(TableNamespace, NamespaceRouteRecord)>> {
    if !map.contains_key("st") && !map.contains_key("loc") {
        return Ok(None);
    }

    let decoded: NamespaceRouteRecordSerde =
        from_hashmap(map).map_err(|error| StorageError::internal(&error.to_string()))?;

    let namespace = if let Some(raw) = decoded.id {
        parse_namespace_route_id(&raw)?
    } else if let Some(pk) = decoded.pk {
        pk.strip_prefix(NAMESPACE_ROUTE_PK_PREFIX)
            .map(parse_namespace_route_id)
            .transpose()?
            .flatten()
    } else {
        None
    };

    let Some(namespace) = namespace else {
        return Ok(None);
    };

    let migration_mode = match decoded.migration_mode {
        NamespaceRouteMigrationModeSerde::Single => NamespaceStorageMigrationMode::Single,
        NamespaceRouteMigrationModeSerde::DualWrite {
            old_loc,
            new_loc,
            cutover_at_ms,
        } => NamespaceStorageMigrationMode::DualWrite {
            old_loc,
            new_loc,
            cutover_at_ms,
        },
    };

    Ok(Some((
        namespace,
        NamespaceRouteRecord {
            storage_mode: NamespaceStorageMode::from_code(decoded.st),
            loc: decoded.loc,
            migration_mode,
        },
    )))
}

fn parse_namespace_route_id(raw: &str) -> StorageResult<Option<TableNamespace>> {
    if raw != "system" && !raw.starts_with(TableNamespace::PREFIX) {
        return Ok(None);
    }

    TableNamespace::parse_str(raw)
        .map(Some)
        .map_err(|_| StorageError::validation(format!("invalid table namespace: {raw}")))
}

async fn fetch_location_descriptor(
    control_plane: Arc<dyn DatabaseTrait>,
    loc: u16,
) -> StorageResult<Option<LocationDescriptorSerde>> {
    let sort_key = format!("{ROUTING_DICTIONARY_SK_PREFIX}{loc:05}");
    let key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(ROUTING_DICTIONARY_PK.to_string()),
        ),
        ("sk".to_string(), AttributeValue::S(sort_key)),
    ]);
    let item = control_plane
        .get_item(Tables::sys_namespaces(), key.into(), true)
        .await?;
    let Some(item) = item else {
        return Ok(None);
    };
    let map = item.into_attribute_map()?;
    let descriptor: LocationDescriptorSerde =
        from_hashmap(map).map_err(|error| StorageError::internal(&error.to_string()))?;
    let _ = match descriptor.backend_kind {
        LocationBackendKindSerde::RemoteAws
        | LocationBackendKindSerde::Sqlite
        | LocationBackendKindSerde::Rocksdb
        | LocationBackendKindSerde::Foundationdb
        | LocationBackendKindSerde::Postgres => true,
    };
    let _ = descriptor.metadata.len();
    Ok(Some(descriptor))
}
