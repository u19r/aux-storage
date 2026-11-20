//! Shared Query and Scan decisions around one native range-read attempt.

use std::sync::Arc;

use storage_common::{MAX_QUERY_LIMIT, MAX_SCAN_LIMIT};
use storage_types::{
    AttributeProjection, BoundTableInfo, ConsumedCapacity, DynamoRequestValidate as _,
    ExclusiveStartKey, IndexName, ItemKey, KeyAttributes, QueryRequest, QueryResponse,
    QueryTableRequest, ReturnConsumedCapacity, ScanRequest, ScanResponse, ScanTableRequest,
    StorageError, StorageResult, StoredTableInfo, TableName, WireItem,
    condition::{
        CompiledCondition, Condition, evaluate_condition, validate_key_condition_operand_types,
    },
    subset_expression_attribute_names_for_expression,
    subset_expression_attribute_values_for_expression, validate_gsi_filter_attributes,
    validate_gsi_projection, validate_gsi_select,
};

use crate::{
    BoundRangeReadPage, BoundRangeReadPlan, CachePublication, ItemReadAttempt, RangeReadItem,
    RangeReadPage, RangeReadPlan, RangeReadTarget, RangeReadiness, TableMetadataCache,
};

/// Ordinary engines retain their existing authority observation and physical
/// page.
pub async fn read_range_observation_default<A: ItemReadAttempt + ?Sized>(
    attempt: &mut A,
    candidate: crate::TableReadCandidate<'_>,
    range: Option<(&BoundRangeReadPlan<'_>, &BoundRangeReadPage<'_>)>,
) -> StorageResult<crate::ReadObservationResult<Option<RangeReadPage<RangeReadItem>>>> {
    // A GSI Query keeps the version-first order: a stale GSI binding never
    // scans (B6).
    if matches!(range, Some((bound, _)) if bound.index_name.is_none())
        && crate::dynamo_pipelined_authority::begin_pipelined_authority(
            attempt,
            std::iter::once(candidate.catalog_version),
        )?
    {
        let page = match range {
            Some((bound, page)) => attempt
                .read_range_page(candidate.table, bound, page)
                .await
                .map(Some),
            None => Ok(None),
        };
        let authority = crate::dynamo_pipelined_authority::finish_pipelined_authority(
            attempt,
            std::iter::once((&candidate.table.info.table_name, Some(candidate))),
            page.is_ok(),
        )
        .await?;
        return Ok(match authority {
            crate::dynamo_pipelined_authority::PipelinedAuthority::Observed {
                version,
                stamps,
                confirmed,
            } => crate::ReadObservationResult {
                catalog_version: version,
                stamps: Some(stamps),
                // As the old path: an unconfirmed binding returns no page and
                // the stamp conflict answers first.
                result: if confirmed { page } else { Ok(None) },
            },
            crate::dynamo_pipelined_authority::PipelinedAuthority::ReadAgain {
                version,
                stamps,
            } => crate::ReadObservationResult {
                catalog_version: version,
                stamps: Some(stamps),
                result: match range {
                    Some((bound, page)) => attempt
                        .read_range_page(candidate.table, bound, page)
                        .await
                        .map(Some),
                    None => Ok(None),
                },
            },
            crate::dynamo_pipelined_authority::PipelinedAuthority::Failed => {
                crate::ReadObservationResult {
                    catalog_version: None,
                    stamps: None,
                    result: page,
                }
            }
        });
    }
    let version = attempt.read_catalog_version().await?;
    let stamps = crate::dynamo_catalog::observe_catalog_stamps(
        attempt,
        [(&candidate.table.info.table_name, Some(candidate))].into_iter(),
        version,
    )
    .await?;
    let result = if stamps == [Some(candidate.stamp)] {
        match range {
            Some((bound, page)) => attempt
                .read_range_page(candidate.table, bound, page)
                .await
                .map(Some),
            None => Ok(None),
        }
    } else {
        Ok(None)
    };
    Ok(crate::ReadObservationResult {
        catalog_version: version,
        stamps: Some(stamps),
        result,
    })
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

/// Query and Scan conditions compiled once per request, before the first
/// attempt. A parse error is kept and returned at the step where the
/// per-attempt parse returned it, so the error order is unchanged.
pub struct CompiledRangeConditions {
    pub(crate) filter: Result<Option<CompiledCondition>, StorageError>,
    pub(crate) key_condition: Option<Result<Arc<CompiledCondition>, StorageError>>,
}

impl CompiledRangeConditions {
    fn filter(&self) -> StorageResult<Option<&CompiledCondition>> {
        self.filter
            .as_ref()
            .map(Option::as_ref)
            .map_err(Clone::clone)
    }
}

/// Immutable evidence that the range request passed its owning boundary.
pub struct ValidatedDynamoRangeRequest(
    pub(crate) DynamoRangeRequest,
    pub(crate) CompiledRangeConditions,
);

impl ValidatedDynamoRangeRequest {
    /// Compile the conditions of a request its owner has validated.
    pub(crate) fn compile(request: DynamoRangeRequest) -> Self {
        let conditions = request.compile_conditions();
        Self(request, conditions)
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
        Ok(Self::compile(request))
    }
}

#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
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

impl crate::dynamo_read_execution::DynamoReadShape for ValidatedDynamoRangeRequest {
    type Response = DynamoRangeOutput;

    async fn execute<A: crate::ItemReadAttempt>(
        &self,
        attempt: &mut A,
        cache: &TableMetadataCache,
        bindings: &crate::RequestTableBindings,
    ) -> StorageResult<(Self::Response, Vec<CachePublication>)> {
        use crate::dynamo_read_execution::confirm_read_observation;
        let (candidate, generation) = cache.candidate(self.as_ref());
        let Some(candidate) = candidate else {
            let (response, publication) =
                range_read_output_in_attempt(attempt, cache, bindings, self, false).await?;
            return Ok((response, publication.into_iter().collect()));
        };
        let candidates = [(candidate, generation)];
        let candidate = &candidates[0].0;
        let table = &candidate.table;
        let request = &self.0;
        let conditions = &self.1;
        let prepared = request.prepare_range(&table.info, conditions);
        let bound = prepared
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|plan| request.bind_range(table, plan));
        let page = bound
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|bound| bound.page(request.max_limit()));
        let range = bound.as_ref().ok().zip(page.as_ref().ok());
        let observed = attempt
            .read_range_observation(candidate.read_candidate(), range)
            .await?;
        let (source, publications) =
            confirm_read_observation(cache, bindings, &candidates, observed)?;
        prepared.as_ref().map_err(Clone::clone)?;
        bound.as_ref().map_err(Clone::clone)?;
        page.as_ref().map_err(Clone::clone)?;
        let source = source
            .ok_or_else(|| StorageError::internal("confirmed range has no data page"))?
            .finish()?;
        let response = request.finish_source(&table.info, conditions, source, false)?;
        Ok((response, publications))
    }

    fn read_table_names(&self) -> impl Iterator<Item = &storage_types::TableName> {
        std::iter::once(self.as_ref())
    }
}

pub(crate) async fn query_for_read_sequence_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
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
        bindings,
        &ValidatedDynamoRangeRequest::compile(DynamoRangeRequest::Query(request)),
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

pub(crate) async fn range_read_output_in_attempt<A: ItemReadAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    bindings: &crate::RequestTableBindings,
    request: &ValidatedDynamoRangeRequest,
    include_item_cursors: bool,
) -> StorageResult<(DynamoRangeOutput, Option<CachePublication>)> {
    let (table, publication) = bindings
        .resolve_table(attempt, cache, request.as_ref())
        .await?;
    table.require_data_readable()?;
    let output = range_read_output_on_table(attempt, table, request, include_item_cursors).await?;
    Ok((output, publication))
}

/// Shared Query/Scan execution after the request owner resolves metadata in
/// the same native snapshot. No second catalog lookup or read attempt.
pub async fn range_read_output_on_table<A: ItemReadAttempt>(
    attempt: &mut A,
    table: Arc<BoundTableInfo>,
    request: &ValidatedDynamoRangeRequest,
    include_item_cursors: bool,
) -> StorageResult<DynamoRangeOutput> {
    let (request, conditions) = (&request.0, &request.1);
    let info = &table.info;
    let plan = request.prepare_range(info, conditions)?;
    let bound = request.bind_range(&table, &plan)?;
    let page = bound.page(request.max_limit())?;
    let source = attempt
        .read_range_page(&table, &bound, &page)
        .await?
        .finish()?;
    request.finish_source(info, conditions, source, include_item_cursors)
}

impl DynamoRangeRequest {
    fn bind_range<'a>(
        &self,
        table: &BoundTableInfo,
        plan: &'a RangeReadPlan,
    ) -> StorageResult<BoundRangeReadPlan<'a>> {
        let readiness = if self.index_name().is_some() {
            RangeReadiness::GsiPending
        } else {
            RangeReadiness::BaseTable
        };
        let bound = plan.bind(Arc::clone(&table.info), readiness)?;
        if let Some(index) = self.index_name() {
            crate::gsi_readiness_from_catalog(table, Some(index))?.require_queryable(index)?;
        }
        bound.mark_gsi_published()
    }

    pub(crate) fn finish_source(
        &self,
        info: &StoredTableInfo,
        conditions: &CompiledRangeConditions,
        source: RangeReadPage<RangeReadItem>,
        include_item_cursors: bool,
    ) -> StorageResult<DynamoRangeOutput> {
        // Stored Scan corruption retains precedence over a malformed filter.
        // A Scan filter on an unprojected index attribute sees it missing.
        let filter = conditions.filter()?;
        self.finish_page(
            info,
            filter.map(CompiledCondition::condition),
            source,
            include_item_cursors,
        )
    }

    pub(crate) fn prepare_range(
        &self,
        table: &StoredTableInfo,
        conditions: &CompiledRangeConditions,
    ) -> StorageResult<RangeReadPlan> {
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
        validate_gsi_select(table, self.index_name(), self.select())?;
        if let Self::Query(request) = self
            && let Some(Ok(_)) = &conditions.key_condition
        {
            validate_key_condition_operand_types(
                request
                    .key_condition_expression
                    .as_deref()
                    .unwrap_or_default(),
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_ref(),
                |attribute| {
                    table
                        .attribute_definitions
                        .iter()
                        .find(|definition| definition.attribute_name == attribute)
                        .map(|definition| definition.attribute_type)
                },
            )?;
        }
        if matches!(self, DynamoRangeRequest::Query(_)) {
            let filter = conditions.filter()?;
            self.validate_query_filter_projection(table, filter.map(CompiledCondition::condition))?;
            validate_query_filter_excludes_primary_key(
                table,
                self.index_name(),
                filter.map(CompiledCondition::condition),
            )?;
        }
        self.plan(table, conditions)
    }

    /// AWS rejects a Query filter on an attribute the index does not project;
    /// a Scan evaluates it against the projected item instead.
    fn validate_query_filter_projection(
        &self,
        table: &StoredTableInfo,
        filter: Option<&Condition>,
    ) -> StorageResult<()> {
        let Some(filter) = filter.filter(|_| self.index_name().is_some()) else {
            return Ok(());
        };
        let mut required = Vec::new();
        filter.visit_attribute_paths(&mut |path| {
            required.push(path.root());
        });
        validate_gsi_filter_attributes(table, self.index_name(), required)
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

    fn max_limit(&self) -> u32 {
        match self {
            Self::Query(_) => MAX_QUERY_LIMIT,
            Self::Scan(_) => MAX_SCAN_LIMIT,
        }
    }

    pub(crate) fn compile_conditions(&self) -> CompiledRangeConditions {
        let filter = self
            .filter_expression()
            .map(|expression| {
                CompiledCondition::compile_filter(
                    expression,
                    self.expression_attribute_names(),
                    self.expression_attribute_values(),
                )
            })
            .transpose();
        let key_condition = match self {
            Self::Query(request) => {
                // Validation rejects an absent expression before compilation;
                // an empty fallback would fail the compile with the
                // empty-expression text.
                let key = request
                    .key_condition_expression
                    .as_deref()
                    .unwrap_or_default();
                let names = subset_expression_attribute_names_for_expression(
                    key,
                    request.expression_attribute_names.as_ref(),
                );
                let values = subset_expression_attribute_values_for_expression(
                    key,
                    request.expression_attribute_values.as_ref(),
                );
                Some(
                    CompiledCondition::compile_key_condition(key, names.as_ref(), values.as_ref())
                        .map(Arc::new),
                )
            }
            Self::Scan(_) => None,
        };
        CompiledRangeConditions {
            filter,
            key_condition,
        }
    }

    fn plan(
        &self,
        table: &StoredTableInfo,
        conditions: &CompiledRangeConditions,
    ) -> StorageResult<RangeReadPlan> {
        match self {
            Self::Query(request) => {
                let target = RangeReadTarget {
                    table_name: request.table_name.clone(),
                    index_name: request.index_name.clone(),
                    limit: request.limit,
                    start_token: page_start(
                        request.exclusive_start_key.as_ref(),
                        table,
                        request.index_name.as_ref(),
                    )
                    .map_err(ExclusiveStartKey::query_error)?,
                    consistent_read: request.consistent_read.unwrap_or(false),
                };
                let key_condition = conditions.key_condition.clone().unwrap_or_else(|| {
                    Err(StorageError::internal(
                        "query has no compiled key condition",
                    ))
                });
                RangeReadPlan::query(
                    target,
                    request.scan_index_forward.unwrap_or(true),
                    key_condition,
                )
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
                    ExclusiveStartKey::scan_error(error, table, request.index_name.as_ref())
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
        let roots = (filter.is_some() || projection.is_some()).then(|| {
            let mut roots: Vec<&str> = Vec::new();
            let mut push = |root| {
                if !roots.contains(&root) {
                    roots.push(root);
                }
            };
            if let Some(condition) = filter {
                condition.visit_root_names(&mut push);
            }
            if let Some(rule) = projection.as_ref() {
                rule.visit_root_names(&mut push);
            }
            roots
        });
        let mut items = Vec::with_capacity(source.items.len());
        let mut item_cursors = (include_item_cursors && self.select() != Some("COUNT"))
            .then(|| Vec::with_capacity(source.items.len()));
        let mut count = 0_u32;
        for source_item in source.items {
            let RangeReadItem { key, item } = source_item;
            let item = match roots.as_deref() {
                Some(roots) => {
                    let attributes = item.project_attributes(roots)?;
                    if let Some(condition) = filter
                        && !evaluate_condition(&attributes, condition)
                    {
                        continue;
                    }
                    match projection.as_ref() {
                        Some(rule) => WireItem::try_from(rule.project(&attributes))?,
                        None => item,
                    }
                }
                None => item,
            };
            if let Some(cursors) = item_cursors.as_mut() {
                cursors.push(key.key_attributes(table)?);
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::internal("range result count overflow"))?;
            if self.select() != Some("COUNT") {
                items.push(RangeReadItem { key, item });
            }
        }
        let last_evaluated_key = match source.continuation.as_deref() {
            Some(token) => key_from_token(token, table, self.index_name())?,
            // A Query that evaluated exactly its `Limit` keeps a
            // LastEvaluatedKey although nothing follows (a Scan does not).
            None if self.query_limit_reached(source.progress.evaluated_count) => source
                .progress
                .last_evaluated_key
                .as_ref()
                .map(|key| key.key_attributes(table))
                .transpose()?,
            None => None,
        };
        Ok(DynamoRangeOutput {
            items: (self.select() != Some("COUNT")).then_some(items),
            count,
            progress: source.progress,
            continuation: source.continuation,
            last_evaluated_key,
            consumed_capacity: self.consumed_capacity(),
            item_cursors,
        })
    }

    fn query_limit_reached(&self, evaluated_count: u32) -> bool {
        match self {
            Self::Query(request) => request
                .limit
                .is_some_and(|limit| limit != 0 && evaluated_count >= limit.min(MAX_QUERY_LIMIT)),
            Self::Scan(_) => false,
        }
    }

    fn select(&self) -> Option<&str> {
        match self {
            Self::Query(request) => request.select.as_deref(),
            Self::Scan(request) => request.select.as_deref(),
        }
    }

    fn consumed_capacity(&self) -> Option<ConsumedCapacity> {
        ConsumedCapacity::zero(self.capacity(), self.table_name(), self.index_name())
    }

    fn capacity(&self) -> Option<ReturnConsumedCapacity> {
        match self {
            Self::Query(request) => request.return_consumed_capacity,
            Self::Scan(request) => request.return_consumed_capacity,
        }
    }
}

/// A base-table Query filter cannot name the table's own key attributes.
fn validate_query_filter_excludes_primary_key(
    table: &StoredTableInfo,
    index: Option<&IndexName>,
    filter: Option<&Condition>,
) -> StorageResult<()> {
    let Some(filter) = filter.filter(|_| index.is_none()) else {
        return Ok(());
    };
    let mut key_attribute = None;
    filter.visit_attribute_paths(&mut |path| {
        if key_attribute.is_none()
            && table
                .key_schema
                .iter()
                .any(|key| key.attribute_name == path.root())
        {
            key_attribute = Some(path.root().to_owned());
        }
    });
    match key_attribute {
        Some(name) => Err(StorageError::validation(format!(
            "Filter Expression can only contain non-primary key attributes: Primary key \
             attribute: {name}"
        ))),
        None => Ok(()),
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
