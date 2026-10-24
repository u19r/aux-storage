use std::sync::Arc;

use storage_condition::{Condition, parse_condition_expression_typed, validate_key_condition};
use storage_types::{
    AttributeValue, IndexName, ItemKey, KeyAttributeType, KeySchemaElement, QueryTableRequest,
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName,
    read_key_validation_error, validate_key_attribute_value_for_schema,
};

use super::RangeReadBudget;

/// The two retained internal range request shapes.
#[derive(Debug)]
pub enum RangeReadRequest {
    Query(QueryTableRequest),
    Scan(ScanTableRequest),
}

impl RangeReadRequest {
    #[must_use]
    pub fn table_name(&self) -> &TableName {
        match self {
            Self::Query(request) => &request.table_name,
            Self::Scan(request) => &request.table_name,
        }
    }

    #[must_use]
    pub fn index_name(&self) -> Option<&IndexName> {
        match self {
            Self::Query(request) => request.index_name.as_ref(),
            Self::Scan(request) => request.index_name.as_ref(),
        }
    }

    #[must_use]
    pub fn limit(&self) -> Option<u32> {
        match self {
            Self::Query(request) => request.limit,
            Self::Scan(request) => request.limit,
        }
    }
}

/// Provider-neutral range request with one owned request rule set.
#[derive(Debug)]
pub struct RangeReadPlan {
    request: RangeReadRequest,
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
    pub fn try_from_query(request: QueryTableRequest) -> StorageResult<Self> {
        request.validate_for_dynamodb()?;
        Self::from_validated_query(request)
    }

    /// The shared range executor has validated the complete query, including
    /// fields removed when projecting its key-only physical request.
    pub(crate) fn from_validated_query(request: QueryTableRequest) -> StorageResult<Self> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        Ok(Self {
            request: RangeReadRequest::Query(request),
        })
    }

    pub fn try_from_scan(request: ScanTableRequest) -> StorageResult<Self> {
        if request.consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        Ok(Self {
            request: RangeReadRequest::Scan(request),
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
        if table.table_name != *self.request.table_name() {
            return Err(StorageError::internal(
                "range metadata identity must match the requested table",
            ));
        }
        let index = self
            .request
            .index_name()
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
        if let RangeReadRequest::Query(request) = &self.request {
            storage_types::validate_gsi_projection(
                &table,
                self.request.index_name(),
                request.projection_expression.as_deref(),
                None,
                request.expression_attribute_names.as_ref(),
            )?;
        }
        let condition = if let RangeReadRequest::Query(request) = &self.request {
            let schema = index
                .as_ref()
                .map(|index| index.key_schema.as_slice())
                .unwrap_or(table.key_schema.as_slice());
            let condition = parse_condition_expression_typed(
                &request.key_condition_expression,
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_ref(),
            )
            .map_err(|error| {
                StorageError::validation(format!("Invalid key condition expression: {error}"))
            })?;
            validate_key_condition(&condition, schema).map_err(StorageError::validation)?;
            validate_query_key_values(&condition, schema, &table)?;
            Some(condition)
        } else {
            None
        };
        Ok(BoundRangeReadPlan {
            request: &self.request,
            table,
            index_name: self.request.index_name().cloned(),
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
            if matches!(condition, Condition::BeginsWith { .. })
                && (!matching || kind == KeyAttributeType::N)
            {
                return Err(StorageError::validation(
                    "begins_with is only valid for matching string or binary key attributes",
                ));
            }
            if !matching {
                return Err(StorageError::validation(format!(
                    "key condition value for '{field}' does not match its key type"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

/// A bound plan keeps the table snapshot alive for a native range attempt.
#[derive(Debug)]
pub struct BoundRangeReadPlan<'a> {
    pub request: &'a RangeReadRequest,
    pub table: Arc<StoredTableInfo>,
    pub index_name: Option<IndexName>,
    /// The selected index is retained with the bound metadata snapshot so
    /// physical readers do not repeat identity or existence resolution.
    pub index: Option<storage_types::GlobalSecondaryIndex>,
    pub condition: Option<storage_condition::Condition>,
    readiness: RangeReadiness,
}

/// The provider-neutral page controls resolved from a bound request. Physical
/// readers still own their SQL/KV predicates and row decoding, but they all
/// receive the same validated cursor, evaluation budget, and direction.
#[derive(Debug)]
pub struct BoundRangeReadPage<'a> {
    pub condition: Option<&'a storage_condition::Condition>,
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
    /// readiness have been bound for this native attempt.
    pub fn page(
        &self,
        default_limit: u32,
        max_limit: u32,
    ) -> StorageResult<BoundRangeReadPage<'_>> {
        if self.index.is_some() && self.readiness != RangeReadiness::GsiPublished {
            return Err(StorageError::validation("Requested index is not ready"));
        }
        let limit =
            storage_common::normalize_limit(self.request.limit(), default_limit, max_limit)?;
        let start = match self.request {
            RangeReadRequest::Query(request) => {
                decode_cursor(&request.exclusive_start_key, &self.table, &self.index_name)?
            }
            RangeReadRequest::Scan(request) => {
                decode_cursor(&request.exclusive_start_key, &self.table, &self.index_name)?
            }
        };
        let forward = match self.request {
            RangeReadRequest::Query(request) => request.scan_index_forward.unwrap_or(true),
            RangeReadRequest::Scan(_) => true,
        };
        Ok(BoundRangeReadPage {
            condition: self.condition.as_ref(),
            start,
            budget: RangeReadBudget::try_new(limit, crate::MAX_RANGE_READ_EVALUATED_BYTES)?,
            forward,
        })
    }
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
