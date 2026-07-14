use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

use storage_types::{
    AttributeValue, BatchGetItemRequest, IndexName, KeysAndAttributes, QueryTableRequest,
    StorageError, StorageResult, TableNamespace, TimestampMillis,
    context::{ErrorContext as _, WrappedError as _},
};

use crate::{DatabaseManager, migration_index_keys::MigrationIndexKeyCodec, tables::Tables};

const NAMESPACE_ROUTE_PK_PREFIX: &str = "NS#";
const NAMESPACE_ROUTE_SK: &str = "META";
const CUTOVER_PK: &str = "SYS#CUTOVER";
const CUTOVER_INDEX_PK: &str = "CUTOVER";
const CUTOVER_STATUS_SCHEDULED: &str = "scheduled";
const CUTOVER_STATUS_APPLIED: &str = "applied";
const MIGRATION_MODE_SINGLE: &str = "single";
const MIGRATION_MODE_DUAL_WRITE: &str = "dual_write";
const UPDATED_AT_ATTR: &str = "updated_at";
const BACKFILL_RECENCY_CONDITION: &str =
    "attribute_not_exists(#updated_at) OR #updated_at <= :updated_at";

#[derive(Debug, Clone)]
pub struct MigrationBackfillInput {
    pub namespace: TableNamespace,
    pub index_name: IndexName,
    pub source_loc: u16,
    pub destination_loc: u16,
    pub entity_types: Vec<String>,
    pub page_size: u32,
}

impl MigrationBackfillInput {
    #[must_use]
    pub fn new(
        namespace: TableNamespace,
        index_name: IndexName,
        source_loc: u16,
        destination_loc: u16,
        entity_types: Vec<String>,
    ) -> Self {
        Self {
            namespace,
            index_name,
            source_loc,
            destination_loc,
            entity_types,
            page_size: 250,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBackfillEntitySummary {
    pub entity_type: String,
    pub copied_items: usize,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBackfillSummary {
    pub namespace: TableNamespace,
    pub source_loc: u16,
    pub destination_loc: u16,
    pub copied_items: usize,
    pub entities: Vec<MigrationBackfillEntitySummary>,
}

#[derive(Debug, Clone)]
pub struct BeginDualWriteInput {
    pub namespace: TableNamespace,
    pub migration_id: String,
    pub old_loc: u16,
    pub new_loc: u16,
    pub effective_at_ms: TimestampMillis,
}

#[derive(Debug, Clone)]
pub struct CompleteCutoverInput {
    pub namespace: TableNamespace,
    pub migration_id: String,
    pub effective_at_ms: TimestampMillis,
    pub new_loc: u16,
}

pub struct DualWriteCoordinator {
    db: Arc<DatabaseManager>,
}

impl DualWriteCoordinator {
    #[must_use]
    pub fn new(db: Arc<DatabaseManager>) -> Self {
        Self { db }
    }

    pub async fn backfill_via_migration_index(
        &self,
        input: MigrationBackfillInput,
    ) -> StorageResult<MigrationBackfillSummary> {
        if input.entity_types.is_empty() {
            return Err(StorageError::validation(
                "migration index backfill requires at least one entity type",
            ));
        }
        if input.page_size == 0 {
            return Err(StorageError::validation(
                "migration index backfill page_size must be > 0",
            ));
        }

        let resolver = self.db.route_resolver().ok_or_else(|| {
            StorageError::validation(
                "migration index backfill requires namespace routing to be enabled (connection \
                 registry)",
            )
        })?;
        let route = resolver.resolve_route(&input.namespace).await?;
        let source_target = resolver
            .resolve_target_for_loc(&input.namespace, route.storage_mode, input.source_loc)
            .await?;
        let destination_target = resolver
            .resolve_target_for_loc(&input.namespace, route.storage_mode, input.destination_loc)
            .await?;

        let source_provider = self
            .db
            .provider_for_connection_for_migration(&source_target.connection_id)?;
        let destination_provider = self
            .db
            .provider_for_connection_for_migration(&destination_target.connection_id)?;

        let mut copied_total = 0usize;
        let mut entity_summaries = Vec::with_capacity(input.entity_types.len());
        let index_key_codec = MigrationIndexKeyCodec::new(input.index_name.clone());

        for entity_type in input.entity_types {
            let mut copied_items = 0usize;
            let mut checksum = 0u64;
            let mut next: Option<String> = None;

            loop {
                let request = QueryTableRequest {
                    table_name: source_target.table_name.clone(),
                    index_name: Some(index_key_codec.index_name().clone()),
                    key_condition_expression: index_key_codec.key_condition_expression(),
                    expression_attribute_names: None,
                    expression_attribute_values: Some(HashMap::from([(
                        ":pk".to_string(),
                        AttributeValue::S(
                            index_key_codec.partition_key(&input.namespace, entity_type.as_str()),
                        ),
                    )])),
                    projection_expression: None,
                    limit: Some(input.page_size),
                    exclusive_start_key: next.clone(),
                    scan_index_forward: Some(true),
                    consistent_read: false,
                };

                let (projected_items, token) = source_provider.query_table(&request).await?;
                let mut keys: Vec<storage_types::KeyAttributes> =
                    Vec::with_capacity(projected_items.len());

                for item in projected_items {
                    let map = item.into_attribute_map()?;
                    let (pk, sk) = extract_pk_sk_from_projection(&map, &index_key_codec)?;
                    keys.push(storage_types::KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S(pk)),
                        ("sk".to_string(), AttributeValue::S(sk)),
                    ]));
                }

                if !keys.is_empty() {
                    let response = source_provider
                        .batch_get_item(BatchGetItemRequest {
                            request_items: HashMap::from([(
                                source_target.table_name.clone(),
                                KeysAndAttributes {
                                    keys: keys.into(),
                                    attributes_to_get: None,
                                    projection_expression: None,
                                    expression_attribute_names: None,
                                    consistent_read: Some(true),
                                },
                            )]),
                            return_consumed_capacity: None,
                        })
                        .await?;

                    if let Some(unprocessed) = response.unprocessed_keys.as_ref()
                        && !unprocessed.is_empty()
                    {
                        return Err(StorageError::validation(
                            "migration index backfill failed closed: source batch_get returned \
                             unprocessed keys",
                        ));
                    }

                    let fetched_items = response
                        .responses
                        .and_then(|mut value| value.remove(&source_target.table_name))
                        .unwrap_or_default();

                    for item in fetched_items {
                        let map = item.into_attribute_map()?;
                        let (pk, sk) = required_pk_sk(&map)?;
                        let updated_at_ms = required_updated_at_millis(&map)?;
                        let pk = pk.to_string();
                        let sk = sk.to_string();
                        match destination_provider
                            .put_item(
                                destination_target.table_name.clone(),
                                map,
                                Some(BACKFILL_RECENCY_CONDITION.to_string()),
                                Some(HashMap::from([(
                                    "#updated_at".to_string(),
                                    UPDATED_AT_ATTR.to_string(),
                                )])),
                                Some(HashMap::from([(
                                    ":updated_at".to_string(),
                                    AttributeValue::N(updated_at_ms.to_string()),
                                )])),
                                None,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(error)
                                if matches!(
                                    error.to_enum(),
                                    storage_types::StorageEnum::ConditionalCheckFailed
                                ) =>
                            {
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                        checksum ^= hash_pk_sk(&pk, &sk);
                        copied_items = copied_items.saturating_add(1);
                    }
                }

                if token.is_none() {
                    break;
                }
                next = token;
            }

            copied_total = copied_total.saturating_add(copied_items);
            entity_summaries.push(MigrationBackfillEntitySummary {
                entity_type,
                copied_items,
                checksum,
            });
        }

        Ok(MigrationBackfillSummary {
            namespace: input.namespace,
            source_loc: input.source_loc,
            destination_loc: input.destination_loc,
            copied_items: copied_total,
            entities: entity_summaries,
        })
    }

    pub async fn begin_dual_write(&self, input: BeginDualWriteInput) -> StorageResult<()> {
        let mut namespace_item = self.load_namespace_metadata(&input.namespace).await?;
        let current_loc = namespace_loc(&namespace_item)?;
        if current_loc != input.old_loc {
            return Err(StorageError::validation(format!(
                "namespace {} current loc {current_loc} does not match expected old_loc {}",
                input.namespace, input.old_loc
            )));
        }
        namespace_item.insert(
            "migration_mode".to_string(),
            dual_write_migration_mode(input.old_loc, input.new_loc, input.effective_at_ms),
        );
        namespace_item.insert(
            "updated_at".to_string(),
            AttributeValue::N(TimestampMillis::now().timestamp_millis().to_string()),
        );
        self.write_namespace_metadata(namespace_item).await?;

        let cutover_item = cutover_event_item(
            &input.namespace,
            &input.migration_id,
            input.old_loc,
            input.new_loc,
            input.effective_at_ms,
            CUTOVER_STATUS_SCHEDULED,
        );
        self.control_plane()
            .put_item(
                Tables::sys_namespaces(),
                cutover_item,
                None,
                None,
                None,
                None,
            )
            .await?;

        if let Some(resolver) = self.db.route_resolver() {
            resolver.invalidate_namespace(&input.namespace);
        }
        Ok(())
    }

    pub async fn complete_cutover(&self, input: CompleteCutoverInput) -> StorageResult<()> {
        let mut namespace_item = self.load_namespace_metadata(&input.namespace).await?;
        namespace_item.insert(
            "loc".to_string(),
            AttributeValue::N(input.new_loc.to_string()),
        );
        namespace_item.insert("migration_mode".to_string(), single_migration_mode());
        namespace_item.insert(
            "updated_at".to_string(),
            AttributeValue::N(TimestampMillis::now().timestamp_millis().to_string()),
        );
        self.write_namespace_metadata(namespace_item).await?;

        let mut cutover_item = self
            .load_cutover_event(&input.namespace, &input.migration_id, input.effective_at_ms)
            .await?
            .unwrap_or_else(|| {
                cutover_event_item(
                    &input.namespace,
                    &input.migration_id,
                    0,
                    input.new_loc,
                    input.effective_at_ms,
                    CUTOVER_STATUS_APPLIED,
                )
            });
        cutover_item.insert(
            "status".to_string(),
            AttributeValue::S(CUTOVER_STATUS_APPLIED.to_string()),
        );
        cutover_item.insert(
            "updated_at".to_string(),
            AttributeValue::N(TimestampMillis::now().timestamp_millis().to_string()),
        );
        self.control_plane()
            .put_item(
                Tables::sys_namespaces(),
                cutover_item,
                None,
                None,
                None,
                None,
            )
            .await?;

        if let Some(resolver) = self.db.route_resolver() {
            resolver.invalidate_namespace(&input.namespace);
        }
        Ok(())
    }

    async fn load_namespace_metadata(
        &self,
        namespace: &TableNamespace,
    ) -> StorageResult<HashMap<String, AttributeValue>> {
        let key = HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::S(format!("{NAMESPACE_ROUTE_PK_PREFIX}{}", namespace.as_str())),
            ),
            (
                "sk".to_string(),
                AttributeValue::S(NAMESPACE_ROUTE_SK.to_string()),
            ),
        ]);
        self.control_plane()
            .get_item(Tables::sys_namespaces(), key.into(), true)
            .await?
            .ok_or_else(|| {
                StorageError::table_not_found(&format!(
                    "namespace metadata missing for namespace {}",
                    namespace.as_str()
                ))
            })?
            .into_attribute_map()
    }

    async fn write_namespace_metadata(
        &self,
        item: HashMap<String, AttributeValue>,
    ) -> StorageResult<()> {
        self.control_plane()
            .put_item(Tables::sys_namespaces(), item, None, None, None, None)
            .await
            .map(|_| ())
    }

    async fn load_cutover_event(
        &self,
        namespace: &TableNamespace,
        migration_id: &str,
        effective_at_ms: TimestampMillis,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let key = cutover_item_key(namespace, migration_id, effective_at_ms);
        self.control_plane()
            .get_item(Tables::sys_namespaces(), key.into(), true)
            .await?
            .map(|item| item.into_attribute_map())
            .transpose()
    }

    fn control_plane(&self) -> Arc<dyn crate::newtypes::DatabaseTrait> {
        self.db.control_plane_for_migration()
    }
}

fn required_pk_sk(map: &HashMap<String, AttributeValue>) -> StorageResult<(&str, &str)> {
    let pk = map.get("pk").and_then(as_string).ok_or_else(|| {
        StorageError::validation(
            "migration index backfill failed closed: fetched item missing string pk",
        )
    })?;
    let sk = map.get("sk").and_then(as_string).ok_or_else(|| {
        StorageError::validation(
            "migration index backfill failed closed: fetched item missing string sk",
        )
    })?;
    Ok((pk, sk))
}

fn required_updated_at_millis(map: &HashMap<String, AttributeValue>) -> StorageResult<i64> {
    match map.get(UPDATED_AT_ATTR) {
        Some(AttributeValue::N(value)) => value.parse::<i64>().map_err(|_| {
            StorageError::validation(
                "migration index backfill failed closed: updated_at must be a millisecond \
                 timestamp",
            )
        }),
        Some(_) => Err(StorageError::validation(
            "migration index backfill failed closed: updated_at must be numeric",
        )),
        None => Err(StorageError::validation(
            "migration index backfill failed closed: item missing updated_at",
        )),
    }
}

fn extract_pk_sk_from_projection(
    map: &HashMap<String, AttributeValue>,
    index_key_codec: &MigrationIndexKeyCodec,
) -> StorageResult<(String, String)> {
    if let Some((pk, sk)) = map
        .get("pk")
        .and_then(as_string)
        .zip(map.get("sk").and_then(as_string))
    {
        return Ok((pk.to_string(), sk.to_string()));
    }

    if let Some(encoded) = map
        .get(index_key_codec.sort_key_attribute())
        .and_then(as_string)
    {
        return index_key_codec.parse_sort_key(encoded).with_context(|| {
            format!("invalid projected {}", index_key_codec.sort_key_attribute())
        });
    }

    Err(StorageError::validation(format!(
        "migration index backfill failed closed: projection missing pk/sk and {}",
        index_key_codec.sort_key_attribute()
    )))
}

fn as_string(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::S(value) => Some(value.as_str()),
        _ => None,
    }
}

fn namespace_loc(map: &HashMap<String, AttributeValue>) -> StorageResult<u16> {
    match map.get("loc") {
        Some(AttributeValue::N(raw)) => raw.parse::<u16>().map_err(|_| {
            StorageError::validation("namespace loc must be an unsigned 16-bit integer")
        }),
        Some(_) => Err(StorageError::validation("namespace loc must be numeric")),
        None => Ok(0),
    }
}

fn single_migration_mode() -> AttributeValue {
    AttributeValue::M(HashMap::from([(
        "mode".to_string(),
        AttributeValue::S(MIGRATION_MODE_SINGLE.to_string()),
    )]))
}

fn dual_write_migration_mode(
    old_loc: u16,
    new_loc: u16,
    cutover_at_ms: TimestampMillis,
) -> AttributeValue {
    AttributeValue::M(HashMap::from([
        (
            "mode".to_string(),
            AttributeValue::S(MIGRATION_MODE_DUAL_WRITE.to_string()),
        ),
        (
            "old_loc".to_string(),
            AttributeValue::N(old_loc.to_string()),
        ),
        (
            "new_loc".to_string(),
            AttributeValue::N(new_loc.to_string()),
        ),
        (
            "cutover_at_ms".to_string(),
            AttributeValue::N(cutover_at_ms.timestamp_millis().to_string()),
        ),
    ]))
}

fn cutover_item_key(
    namespace: &TableNamespace,
    migration_id: &str,
    effective_at_ms: TimestampMillis,
) -> HashMap<String, AttributeValue> {
    let encoded_ms = format!("{:020}", effective_at_ms.timestamp_millis());
    let table_sk = format!(
        "CUTOVER#{encoded_ms}#{}#{migration_id}",
        namespace.storage_key()
    );
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(CUTOVER_PK.to_string())),
        ("sk".to_string(), AttributeValue::S(table_sk)),
    ])
}

fn cutover_event_item(
    namespace: &TableNamespace,
    migration_id: &str,
    old_loc: u16,
    new_loc: u16,
    effective_at_ms: TimestampMillis,
    status: &str,
) -> HashMap<String, AttributeValue> {
    let encoded_ms = format!("{:020}", effective_at_ms.timestamp_millis());
    let gsi3sk = format!("{encoded_ms}#{}#{migration_id}", namespace.storage_key());
    let table_sk = format!(
        "CUTOVER#{encoded_ms}#{}#{migration_id}",
        namespace.storage_key()
    );
    let now_ms = TimestampMillis::now().timestamp_millis();
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(CUTOVER_PK.to_string())),
        ("sk".to_string(), AttributeValue::S(table_sk)),
        (
            "namespace".to_string(),
            AttributeValue::S(namespace.as_str().to_string()),
        ),
        (
            "migration_id".to_string(),
            AttributeValue::S(migration_id.to_string()),
        ),
        (
            "old_loc".to_string(),
            AttributeValue::N(old_loc.to_string()),
        ),
        (
            "new_loc".to_string(),
            AttributeValue::N(new_loc.to_string()),
        ),
        (
            "effective_at_ms".to_string(),
            AttributeValue::N(effective_at_ms.timestamp_millis().to_string()),
        ),
        ("status".to_string(), AttributeValue::S(status.to_string())),
        (
            "gsi3pk".to_string(),
            AttributeValue::S(CUTOVER_INDEX_PK.to_string()),
        ),
        ("gsi3sk".to_string(), AttributeValue::S(gsi3sk)),
        (
            "created_at".to_string(),
            AttributeValue::N(now_ms.to_string()),
        ),
        (
            "updated_at".to_string(),
            AttributeValue::N(now_ms.to_string()),
        ),
    ])
}

fn hash_pk_sk(pk: &str, sk: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    pk.hash(&mut hasher);
    sk.hash(&mut hasher);
    hasher.finish()
}
