use std::sync::Arc;

use storage_types::{
    AttributeValue, IndexName, ItemKey, KeyAttributeType, KeySchemaElement, KeyType,
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName,
    condition::{
        CompiledCondition, Condition, KEY_CONDITION_TYPE_MISMATCH, evaluate_condition,
        validate_key_condition,
    },
    read_key_validation_error, validate_key_attribute_value_for_schema,
};

use super::RangeReadBudget;

/// The physical range of one Query or Scan page: typed fields only. Request
/// validation (projection, filter, GSI projection) happens before the plan is
/// built.
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct RangeReadPlan {
    table_name: TableName,
    index_name: Option<IndexName>,
    limit: Option<u32>,
    /// The request's start key as a page token; `BoundRangeReadPlan::page`
    /// decodes it against the bound table.
    start_token: Option<String>,
    forward: bool,
    /// Query only: the key condition compiled once per request. A parse error
    /// is kept here and returned by `bind`.
    key_condition: Option<Result<Arc<CompiledCondition>, StorageError>>,
}

/// The range a Query or Scan page reads.
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct RangeReadTarget {
    pub table_name: TableName,
    pub index_name: Option<IndexName>,
    pub limit: Option<u32>,
    pub start_token: Option<String>,
    pub consistent_read: bool,
}

/// Evidence that the native attempt resolved the requested range origin.
/// Providers obtain `GsiPublished` only after checking their backend's build
/// state on the same attempt that will execute the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeReadiness {
    BaseTable,
    /// The bound metadata selected a GSI, but the native readiness record has
    /// not been checked yet. Providers use this state to let the shared plan
    /// reject a missing index before consulting backend-specific GSI state.
    GsiPending,
    GsiPublished,
}

impl RangeReadPlan {
    /// A Query range. The caller validated the request and compiled its key
    /// condition once.
    pub(crate) fn query(
        target: RangeReadTarget,
        forward: bool,
        key_condition: Result<Arc<CompiledCondition>, StorageError>,
    ) -> StorageResult<Self> {
        Self::from_target(target, forward, Some(key_condition))
    }

    pub fn try_from_scan(request: ScanTableRequest) -> StorageResult<Self> {
        Self::from_target(
            RangeReadTarget {
                table_name: request.table_name,
                index_name: request.index_name,
                limit: request.limit,
                start_token: request.exclusive_start_key,
                consistent_read: request.consistent_read,
            },
            true,
            None,
        )
    }

    fn from_target(
        target: RangeReadTarget,
        forward: bool,
        key_condition: Option<Result<Arc<CompiledCondition>, StorageError>>,
    ) -> StorageResult<Self> {
        if target.consistent_read && target.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        Ok(Self {
            table_name: target.table_name,
            index_name: target.index_name,
            limit: target.limit,
            start_token: target.start_token,
            forward,
            key_condition,
        })
    }

    /// Bind the request to one table snapshot and index identity.
    /// `GsiPending` lets the native adapter check backend-specific readiness
    /// after this shared plan has rejected a missing index. A bound plan must
    /// be promoted with [`BoundRangeReadPlan::mark_gsi_published`] before its
    /// page can be executed.
    pub fn bind(
        &self,
        table: Arc<StoredTableInfo>,
        readiness: RangeReadiness,
    ) -> StorageResult<BoundRangeReadPlan<'_>> {
        if table.table_name != self.table_name {
            return Err(StorageError::internal(
                "range metadata identity must match the requested table",
            ));
        }
        let index = self
            .index_name
            .as_ref()
            .map(|name| {
                table
                    .global_secondary_indexes
                    .as_ref()
                    .and_then(|indexes| indexes.iter().find(|index| index.index_name == *name))
                    .ok_or_else(|| {
                        StorageError::validation(format!(
                            "The table '{}' does not have the specified index: {name}",
                            table.table_name
                        ))
                    })
            })
            .transpose()?;
        if index.is_some() && readiness == RangeReadiness::BaseTable {
            return Err(StorageError::validation("Requested index is not ready"));
        }
        let index = index.cloned();
        let condition = match &self.key_condition {
            Some(Ok(condition)) => {
                let schema = index
                    .as_ref()
                    .map(|index| index.key_schema.as_slice())
                    .unwrap_or(table.key_schema.as_slice());
                validate_key_condition(condition.condition(), schema)
                    .map_err(StorageError::validation)?;
                validate_query_key_values(condition.condition(), schema, &table)?;
                Some(Arc::clone(condition))
            }
            Some(Err(error)) => return Err(error.clone()),
            None => None,
        };
        Ok(BoundRangeReadPlan {
            plan: self,
            table,
            index_name: self.index_name.clone(),
            index,
            condition,
            readiness,
        })
    }
}

fn validate_query_key_values(
    condition: &Condition,
    schema: &[KeySchemaElement],
    table: &StoredTableInfo,
) -> StorageResult<()> {
    match condition {
        Condition::And { conditions } => {
            for condition in conditions {
                validate_query_key_values(condition, schema, table)?;
            }
        }
        Condition::Equal { field, value }
        | Condition::BeginsWith {
            field,
            prefix: value,
        } => {
            let element = schema
                .iter()
                .find(|key| key.attribute_name == *field)
                .ok_or_else(|| StorageError::validation("Query key condition not supported"))?;
            validate_key_attribute_value_for_schema(element, value)
                .map_err(read_key_validation_error)?;
            let kind = table
                .attribute_definitions
                .iter()
                .find(|definition| definition.attribute_name == *field)
                .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
            let matching = matches!(
                (kind, value),
                (KeyAttributeType::S, AttributeValue::S(_))
                    | (KeyAttributeType::N, AttributeValue::N(_))
                    | (KeyAttributeType::B, AttributeValue::B(_))
            );
            if !matching
                || (matches!(condition, Condition::BeginsWith { .. })
                    && kind == KeyAttributeType::N)
            {
                return Err(StorageError::validation(KEY_CONDITION_TYPE_MISMATCH));
            }
        }
        _ => {}
    }
    Ok(())
}

/// A bound plan keeps the table snapshot alive for a native range attempt.
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct BoundRangeReadPlan<'a> {
    plan: &'a RangeReadPlan,
    pub table: Arc<StoredTableInfo>,
    pub index_name: Option<IndexName>,
    /// The selected index is retained with the bound metadata snapshot so
    /// physical readers do not repeat identity or existence resolution.
    pub index: Option<storage_types::GlobalSecondaryIndex>,
    pub condition: Option<Arc<CompiledCondition>>,
    readiness: RangeReadiness,
}

/// The provider-neutral page controls resolved from a bound request. Physical
/// readers still own their SQL/KV predicates and row decoding, but they all
/// receive the same validated cursor, evaluation budget, and direction.
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
pub struct BoundRangeReadPage<'a> {
    pub condition: Option<&'a Condition>,
    pub start: Option<ItemKey>,
    pub budget: RangeReadBudget,
    pub forward: bool,
}

impl BoundRangeReadPlan<'_> {
    /// Record the native GSI readiness check for this bound plan.
    pub fn mark_gsi_published(mut self) -> StorageResult<Self> {
        if self.index.is_some() {
            if self.readiness == RangeReadiness::BaseTable {
                return Err(StorageError::validation("Requested index is not ready"));
            }
            self.readiness = RangeReadiness::GsiPublished;
        }
        Ok(self)
    }

    /// Resolve one page's cursor and evaluation budget after metadata and index
    /// readiness have been bound for this native attempt. A request without
    /// `Limit` reads up to `max_limit` items or the 1 MB budget.
    pub fn page(&self, max_limit: u32) -> StorageResult<BoundRangeReadPage<'_>> {
        if self.index.is_some() && self.readiness != RangeReadiness::GsiPublished {
            return Err(StorageError::validation("Requested index is not ready"));
        }
        let limit = if self.plan.key_condition.is_some() {
            storage_common::normalize_query_limit(self.plan.limit, max_limit)?
        } else {
            storage_common::normalize_limit(self.plan.limit, max_limit)?
        };
        let start = decode_cursor(&self.plan.start_token, &self.table, &self.index_name)?;
        let condition = self.condition.as_deref().map(CompiledCondition::condition);
        if let (Some(start), Some(condition), None) = (&start, condition, &self.index_name) {
            validate_start_key_in_range(start, condition, &self.table)?;
        }
        let forward = self.plan.forward;
        Ok(BoundRangeReadPage {
            condition,
            start,
            budget: RangeReadBudget::try_new(limit, crate::MAX_RANGE_READ_EVALUATED_BYTES)?,
            forward,
        })
    }
}

/// A base-table Query resumes only from a key its sort-key condition selects.
fn validate_start_key_in_range(
    start: &ItemKey,
    condition: &Condition,
    table: &StoredTableInfo,
) -> StorageResult<()> {
    let Some(hash_key) = table
        .key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .map(|key| key.attribute_name.as_str())
    else {
        return Ok(());
    };
    let key = start.key_attributes(table)?;
    let clauses = match condition {
        Condition::And { conditions } => conditions.as_slice(),
        single => std::slice::from_ref(single),
    };
    for clause in clauses {
        let mut on_hash_key = false;
        clause.visit_attribute_paths(&mut |path| on_hash_key |= path.root() == hash_key);
        if !on_hash_key && !evaluate_condition(&key, clause) {
            return Err(StorageError::validation(
                "The provided starting key does not match the range key predicate",
            ));
        }
    }
    Ok(())
}

fn decode_cursor(
    token: &Option<String>,
    table: &StoredTableInfo,
    index_name: &Option<IndexName>,
) -> StorageResult<Option<ItemKey>> {
    match token {
        Some(token) => ItemKey::item_key_from_next_page_token(token, table, index_name)
            .map_err(|error| storage_types::StorageEnum::KeyValidation(error).into()),
        None => Ok(None),
    }
}
