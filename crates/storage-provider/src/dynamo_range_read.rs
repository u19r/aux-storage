//! Shared Query and Scan decisions around one native range-read attempt.

use std::{future::Future, sync::Arc};

use storage_common::{DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT};
use storage_condition::{Condition, evaluate_condition, parse_condition_expression_typed};
use storage_types::{
    AttributeProjection, BoundTableInfo, ConsumedCapacity, ConsumedCapacityMetrics,
    DynamoRequestValidate as _, ExclusiveStartKey, IndexName, ItemKey, KeyAttributes, QueryRequest,
    QueryResponse, QueryTableRequest, ReturnConsumedCapacity, ScanRequest, ScanResponse,
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName, WireItem,
    subset_expression_attribute_names_for_expression,
    subset_expression_attribute_values_for_expression, validate_gsi_projection,
    validate_gsi_required_attributes,
};

use crate::{
    BoundRangeReadPage, BoundRangeReadPlan, CachePublication, CatalogAttempt, GsiReadinessSnapshot,
    RangeReadItem, RangeReadPage, RangeReadPlan, RangeReadiness, TableMetadataCache,
    resolve_table_in_attempt,
};

/// The family lowerer checks index publication and reads a bounded physical
/// page. It does not parse Dynamo expressions or shape the API response.
pub trait DynamoRangeReadAttempt: CatalogAttempt {
    fn read_gsi_readiness(
        &mut self,
        table: &BoundTableInfo,
        index: Option<&IndexName>,
    ) -> impl Future<Output = StorageResult<GsiReadinessSnapshot>> + Send;

    fn require_gsi_queryable(
        &mut self,
        table: &BoundTableInfo,
        index: &IndexName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        async move {
            self.read_gsi_readiness(table, Some(index))
                .await?
                .require_queryable(index)
        }
    }

    fn read_range_page(
        &mut self,
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        page: &BoundRangeReadPage<'_>,
    ) -> impl Future<Output = StorageResult<RangeReadPage<RangeReadItem>>> + Send;
}

/// Query and Scan have one range lifecycle but distinct response types.
pub trait DynamoRangeReadShape: Send + 'static {
    type Response: Send + 'static;

    fn into_request(self) -> DynamoRangeRequest;
    fn finish(output: DynamoRangeOutput) -> Self::Response;
}

pub enum DynamoRangeRequest {
    Query(QueryRequest),
    Scan(ScanRequest),
}

/// Immutable evidence that the range request passed its owning boundary.
pub struct ValidatedDynamoRangeRequest(DynamoRangeRequest);

impl ValidatedDynamoRangeRequest {
    /// The validated index selection is needed by regional publication
    /// admission.
    pub fn selected_index(&self) -> Option<&IndexName> {
        self.0.index_name()
    }
}

impl AsRef<TableName> for ValidatedDynamoRangeRequest {
    fn as_ref(&self) -> &TableName {
        self.0.table_name()
    }
}

impl TryFrom<DynamoRangeRequest> for ValidatedDynamoRangeRequest {
    type Error = StorageError;

    fn try_from(request: DynamoRangeRequest) -> StorageResult<Self> {
        match &request {
            DynamoRangeRequest::Query(request) => request.validate_for_dynamodb(),
            DynamoRangeRequest::Scan(request) => request.validate_for_dynamodb(),
        }
        .map_err(StorageError::raw_validation)?;
        Ok(Self(request))
    }
}

pub struct DynamoRangeOutput {
    pub items: Option<Vec<RangeReadItem>>,
    pub count: u32,
    pub progress: crate::RangeReadProgress,
    pub continuation: Option<String>,
    pub last_evaluated_key: Option<KeyAttributes>,
    pub consumed_capacity: Option<ConsumedCapacity>,
    pub item_cursors: Option<Vec<KeyAttributes>>,
}

/// Query's private page sidecar lets ReadSequence trim a projected response
/// without fetching the same page again.
pub struct DynamoQueryPage {
    pub response: QueryResponse,
    pub item_cursors: Option<Vec<KeyAttributes>>,
}

impl DynamoRangeReadShape for QueryRequest {
    type Response = QueryResponse;

    fn into_request(self) -> DynamoRangeRequest {
        DynamoRangeRequest::Query(self)
    }

    fn finish(output: DynamoRangeOutput) -> Self::Response {
        QueryResponse {
            items: output
                .items
                .map(|items| items.into_iter().map(|source| source.item).collect()),
            count: output.count,
            scanned_count: output.progress.evaluated_count,
            last_evaluated_key: output.last_evaluated_key,
            consumed_capacity: output.consumed_capacity,
        }
    }
}

impl DynamoRangeReadShape for QueryTableRequest {
    type Response = RangeReadPage<WireItem>;

    fn into_request(self) -> DynamoRangeRequest {
        DynamoRangeRequest::Query(self.into())
    }

    fn finish(output: DynamoRangeOutput) -> Self::Response {
        RangeReadPage {
            items: output
                .items
                .unwrap_or_default()
                .into_iter()
                .map(|source| source.item)
                .collect(),
            progress: output.progress,
            continuation: output.continuation,
        }
    }
}

pub struct SourceQueryRequest(pub QueryTableRequest);

impl DynamoRangeReadShape for SourceQueryRequest {
    type Response = RangeReadPage<RangeReadItem>;

    fn into_request(self) -> DynamoRangeRequest {
        DynamoRangeRequest::Query(self.0.into())
    }

    fn finish(output: DynamoRangeOutput) -> Self::Response {
        RangeReadPage {
            items: output.items.unwrap_or_default(),
            progress: output.progress,
            continuation: output.continuation,
        }
    }
}

impl DynamoRangeReadShape for ScanRequest {
    type Response = ScanResponse;

    fn into_request(self) -> DynamoRangeRequest {
        DynamoRangeRequest::Scan(self)
    }

    fn finish(output: DynamoRangeOutput) -> Self::Response {
        ScanResponse {
            items: output
                .items
                .map(|items| items.into_iter().map(|source| source.item).collect()),
            count: output.count,
            scanned_count: output.progress.evaluated_count,
            last_evaluated_key: output.last_evaluated_key,
            consumed_capacity: output.consumed_capacity,
        }
    }
}

pub(crate) async fn query_for_read_sequence_in_attempt<A: DynamoRangeReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: QueryRequest,
) -> StorageResult<(DynamoQueryPage, Option<CachePublication>)> {
    // The graph owns static request validation. Input binding changes values,
    // which need operand checks here and table-schema checks in the attempt.
    request
        .validate_bound_values()
        .map_err(StorageError::raw_validation)?;
    let (mut output, publication) = range_read_output_in_attempt(
        attempt,
        cache,
        ValidatedDynamoRangeRequest(DynamoRangeRequest::Query(request)),
        true,
    )
    .await?;
    let item_cursors = output.item_cursors.take();
    Ok((
        DynamoQueryPage {
            response: QueryRequest::finish(output),
            item_cursors,
        },
        publication,
    ))
}

pub(crate) async fn range_read_output_in_attempt<A: DynamoRangeReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: ValidatedDynamoRangeRequest,
    include_item_cursors: bool,
) -> StorageResult<(DynamoRangeOutput, Option<CachePublication>)> {
    let (table, publication) = resolve_table_in_attempt(attempt, cache, request.as_ref()).await?;
    let output = range_read_output_on_table(attempt, table, request, include_item_cursors).await?;
    Ok((output, publication))
}

/// Shared Query/Scan execution after the request owner resolves metadata in
/// the same native snapshot. No second catalog lookup or read attempt.
pub async fn range_read_output_on_table<A: DynamoRangeReadAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    request: ValidatedDynamoRangeRequest,
    include_item_cursors: bool,
) -> StorageResult<DynamoRangeOutput> {
    let request = request.0;
    let info = &table.info;
    let (plan, filter) = request.prepare_range(info)?;
    let readiness = if request.index_name().is_some() {
        RangeReadiness::GsiPending
    } else {
        RangeReadiness::BaseTable
    };
    let bound = plan.bind(Arc::clone(info), readiness)?;
    if let Some(index) = request.index_name() {
        attempt.require_gsi_queryable(&table, index).await?;
    }
    let bound = bound.mark_gsi_published()?;
    let (default_limit, max_limit) = request.limits();
    let page = bound.page(default_limit, max_limit)?;
    let source = attempt
        .read_range_page(&table, &bound, &page)
        .await?
        .finish()?;
    // Scan has historically decoded the source before reporting a malformed
    // filter, so corrupt stored data retains precedence over expression error.
    let filter = if matches!(request, DynamoRangeRequest::Scan(_)) {
        let filter = request.filter_condition()?;
        request.validate_filter_projection(info, filter.as_ref())?;
        filter
    } else {
        filter
    };
    request.finish_page(info, filter.as_ref(), source, include_item_cursors)
}

impl DynamoRangeRequest {
    fn prepare_range(
        &self,
        table: &StoredTableInfo,
    ) -> StorageResult<(RangeReadPlan, Option<Condition>)> {
        if self.index_name().is_some_and(IndexName::is_ttl_index) {
            return Err(StorageError::validation(
                "The table does not have the specified index",
            ));
        }
        validate_gsi_projection(
            table,
            self.index_name(),
            self.projection_expression(),
            self.attributes_to_get(),
            self.expression_attribute_names(),
        )?;
        let filter = if matches!(self, DynamoRangeRequest::Query(_)) {
            self.filter_condition()?
        } else {
            None
        };
        self.validate_filter_projection(table, filter.as_ref())?;
        let plan = self.plan(table)?;
        Ok((plan, filter))
    }

    fn validate_filter_projection(
        &self,
        table: &StoredTableInfo,
        filter: Option<&Condition>,
    ) -> StorageResult<()> {
        let Some(filter) = filter.filter(|_| self.index_name().is_some()) else {
            return Ok(());
        };
        let mut required = Vec::new();
        filter.visit_attribute_paths(&mut |path| {
            required.push(path.split(['.', '[']).next().unwrap_or(path));
        });
        validate_gsi_required_attributes(table, self.index_name(), required)
    }

    fn table_name(&self) -> &TableName {
        match self {
            Self::Query(request) => &request.table_name,
            Self::Scan(request) => &request.table_name,
        }
    }

    fn index_name(&self) -> Option<&IndexName> {
        match self {
            Self::Query(request) => request.index_name.as_ref(),
            Self::Scan(request) => request.index_name.as_ref(),
        }
    }

    fn expression_attribute_names(&self) -> Option<&storage_types::VecMap<String, String>> {
        match self {
            Self::Query(request) => request.expression_attribute_names.as_ref(),
            Self::Scan(request) => request.expression_attribute_names.as_ref(),
        }
    }

    fn expression_attribute_values(&self) -> Option<&storage_types::AttributeMap> {
        match self {
            Self::Query(request) => request.expression_attribute_values.as_ref(),
            Self::Scan(request) => request.expression_attribute_values.as_ref(),
        }
    }

    fn projection_expression(&self) -> Option<&str> {
        match self {
            Self::Query(request) => request.projection_expression.as_deref(),
            Self::Scan(request) => request.projection_expression.as_deref(),
        }
    }

    fn attributes_to_get(&self) -> Option<&[String]> {
        match self {
            Self::Query(request) => request.attributes_to_get.as_deref(),
            Self::Scan(request) => request.attributes_to_get.as_deref(),
        }
    }

    fn filter_expression(&self) -> Option<&str> {
        match self {
            Self::Query(request) => request.filter_expression.as_deref(),
            Self::Scan(request) => request.filter_expression.as_deref(),
        }
    }

    fn limits(&self) -> (u32, u32) {
        match self {
            Self::Query(_) => (DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT),
            Self::Scan(_) => (DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT),
        }
    }

    fn filter_condition(&self) -> StorageResult<Option<Condition>> {
        self.filter_expression()
            .map(|expression| {
                parse_condition_expression_typed(
                    expression,
                    self.expression_attribute_names(),
                    self.expression_attribute_values(),
                )
                .map_err(|message| {
                    StorageError::validation(
                        message.replace("ConditionExpression", "FilterExpression"),
                    )
                })
            })
            .transpose()
    }

    fn plan(&self, table: &StoredTableInfo) -> StorageResult<RangeReadPlan> {
        match self {
            Self::Query(request) => {
                let key = &request.key_condition_expression;
                let query = QueryTableRequest {
                    table_name: request.table_name.clone(),
                    index_name: request.index_name.clone(),
                    key_condition_expression: key.clone(),
                    expression_attribute_names: subset_expression_attribute_names_for_expression(
                        key,
                        request.expression_attribute_names.as_ref(),
                    ),
                    expression_attribute_values: subset_expression_attribute_values_for_expression(
                        key,
                        request.expression_attribute_values.as_ref(),
                    ),
                    projection_expression: None,
                    limit: request.limit,
                    exclusive_start_key: page_start(
                        request.exclusive_start_key.as_ref(),
                        table,
                        request.index_name.as_ref(),
                    )
                    .map_err(|_| {
                        StorageError::validation("The provided starting key is invalid")
                    })?,
                    scan_index_forward: request.scan_index_forward,
                    consistent_read: request.consistent_read.unwrap_or(false),
                };
                RangeReadPlan::from_validated_query(query)
            }
            Self::Scan(request) => RangeReadPlan::try_from_scan(ScanTableRequest {
                table_name: request.table_name.clone(),
                index_name: request.index_name.clone(),
                limit: request.limit,
                exclusive_start_key: page_start(
                    request.exclusive_start_key.as_ref(),
                    table,
                    request.index_name.as_ref(),
                )
                .map_err(|error| {
                    StorageError::validation(format!(
                        "The provided starting key is invalid: {error}"
                    ))
                })?,
                consistent_read: request.consistent_read.unwrap_or(false),
            }),
        }
    }

    fn finish_page(
        &self,
        table: &StoredTableInfo,
        filter: Option<&Condition>,
        source: RangeReadPage<RangeReadItem>,
        include_item_cursors: bool,
    ) -> StorageResult<DynamoRangeOutput> {
        let projection = AttributeProjection::new(
            self.projection_expression(),
            self.attributes_to_get(),
            self.expression_attribute_names(),
        );
        let mut items = Vec::with_capacity(source.items.len());
        let mut item_cursors = (include_item_cursors && self.select() != Some("COUNT"))
            .then(|| Vec::with_capacity(source.items.len()));
        let mut count = 0_u32;
        for source_item in source.items {
            let RangeReadItem { key, item } = source_item;
            let item = if filter.is_some() || projection.is_some() || item_cursors.is_some() {
                let attributes = item.to_attribute_map()?;
                if let Some(condition) = filter
                    && !evaluate_condition(&attributes, condition)
                {
                    continue;
                }
                if let Some(cursors) = item_cursors.as_mut() {
                    cursors.push(key.key_attributes(table)?);
                }
                match projection.as_ref() {
                    Some(rule) => WireItem::try_from(rule.project(&attributes))?,
                    None => item,
                }
            } else {
                item
            };
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::internal("range result count overflow"))?;
            if self.select() != Some("COUNT") {
                items.push(RangeReadItem { key, item });
            }
        }
        let evaluated_bytes = source.progress.evaluated_bytes;
        let last_evaluated_key = source
            .continuation
            .as_deref()
            .map(|token| key_from_token(token, table, self.index_name()))
            .transpose()?
            .flatten();
        Ok(DynamoRangeOutput {
            items: (self.select() != Some("COUNT")).then_some(items),
            count,
            progress: source.progress,
            continuation: source.continuation,
            last_evaluated_key,
            consumed_capacity: self.consumed_capacity(evaluated_bytes),
            item_cursors,
        })
    }

    fn select(&self) -> Option<&str> {
        match self {
            Self::Query(request) => request.select.as_deref(),
            Self::Scan(request) => request.select.as_deref(),
        }
    }

    fn consumed_capacity(&self, evaluated_bytes: u64) -> Option<ConsumedCapacity> {
        let consistent = match self {
            Self::Query(request) => request.consistent_read.unwrap_or(false),
            Self::Scan(request) => request.consistent_read.unwrap_or(false),
        };
        let units = evaluated_bytes
            .div_ceil(crate::constants::READ_CAPACITY_UNIT_BYTES)
            .max(1) as f64
            * if consistent { 1.0 } else { 0.5 };
        match self.capacity()? {
            ReturnConsumedCapacity::None => None,
            ReturnConsumedCapacity::Total => Some(ConsumedCapacity {
                table_name: self.table_name().clone(),
                capacity_units: units,
                global_secondary_indexes: None,
            }),
            ReturnConsumedCapacity::Indexes => Some(ConsumedCapacity {
                table_name: self.table_name().clone(),
                capacity_units: units,
                global_secondary_indexes: self.index_name().map(|name| {
                    [(
                        name.to_string(),
                        ConsumedCapacityMetrics {
                            capacity_units: units,
                        },
                    )]
                    .into_iter()
                    .collect()
                }),
            }),
        }
    }

    fn capacity(&self) -> Option<ReturnConsumedCapacity> {
        match self {
            Self::Query(request) => request.return_consumed_capacity,
            Self::Scan(request) => request.return_consumed_capacity,
        }
    }
}

fn page_start(
    start: Option<&ExclusiveStartKey>,
    table: &StoredTableInfo,
    index: Option<&IndexName>,
) -> StorageResult<Option<String>> {
    start.map(|key| key.to_page_token(table, index)).transpose()
}

fn key_from_token(
    token: &str,
    table: &StoredTableInfo,
    index: Option<&IndexName>,
) -> StorageResult<Option<KeyAttributes>> {
    let Some(key) = ItemKey::item_key_from_next_page_token(token, table, &index.cloned())? else {
        return Ok(None);
    };
    key.key_attributes(table).map(Some)
}
