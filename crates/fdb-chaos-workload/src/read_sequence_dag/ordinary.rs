use std::collections::BTreeMap;

use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{NormalizedResult, ReadSequenceDagWorkload},
};

type OrdinaryNodeResult = Vec<Vec<HashMap<String, AttributeValue>>>;
type OrdinaryResults = BTreeMap<ReadSequenceNodeId, OrdinaryNodeResult>;

pub(crate) const ORDINARY_BATCH_GET_MAX_ATTEMPTS: usize = 4;
const ORDINARY_BATCH_GET_RETRY_BASE_DELAY_MS: u64 = 10;

#[derive(Debug)]
pub(super) enum OrdinaryFailure {
    Storage(String),
    Fixture(String),
}

impl OrdinaryFailure {
    fn storage(error: impl Into<String>) -> Self {
        Self::Storage(error.into())
    }

    fn fixture(error: impl Into<String>) -> Self {
        Self::Fixture(error.into())
    }
}

impl From<String> for OrdinaryFailure {
    fn from(error: String) -> Self {
        Self::Fixture(error)
    }
}

struct OrdinaryContext<'a> {
    simulation: &'a WorkloadContext,
    provider: &'a Arc<dyn storage_provider::DynamoOperations>,
    request: &'a ReadSequenceRequest,
    plan: &'a storage_types::ReadSequencePlan,
    results: &'a OrdinaryResults,
}

impl ReadSequenceDagWorkload {
    /// Execute the fixture through the ordinary provider path. The planner
    /// owns wave order; this module only supplies the fixed fixture reads.
    pub(super) async fn run_ordinary_fixture(
        &self,
        provider: &Arc<dyn storage_provider::DynamoOperations>,
        request: &ReadSequenceRequest,
        plan: &storage_types::ReadSequencePlan,
    ) -> Result<NormalizedResult, OrdinaryFailure> {
        let mut results = OrdinaryResults::new();
        for wave in &plan.graph.waves {
            for node_id in wave {
                let context = OrdinaryContext {
                    simulation: &self.context,
                    provider,
                    request,
                    plan,
                    results: &results,
                };
                let result = load_node(&context, *node_id).await?;
                results.insert(*node_id, result);
            }
        }
        Ok(normalize_results(results, plan))
    }
}

async fn load_node(
    context: &OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    let node = context
        .request
        .nodes
        .get(node_id.index())
        .ok_or_else(|| format!("ordinary node {} is outside request", node_id.index()))?;
    match &node.operation {
        ReadSequenceNodeOperation::Query(query) => load_query(context.provider, query).await,
        ReadSequenceNodeOperation::Get(get) => load_get(context, node_id, node, get).await,
        ReadSequenceNodeOperation::BatchGet(_) => Err(OrdinaryFailure::fixture(
            "ordinary fixture does not include BatchGet",
        )),
    }
}

async fn load_query(
    provider: &Arc<dyn storage_provider::DynamoOperations>,
    query: &QueryRequest,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    let items = load_query_items(provider, query).await?;
    let attributes = items
        .into_iter()
        .map(|item| {
            <HashMap<String, AttributeValue> as storage_types::TryFromWireItem>::try_from_wire_item(
                &item,
            )
            .map_err(|error| OrdinaryFailure::fixture(storage_error_detail(&error)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(vec![attributes])
}

async fn load_query_items(
    provider: &Arc<dyn storage_provider::DynamoOperations>,
    query: &QueryRequest,
) -> Result<Vec<WireItem>, OrdinaryFailure> {
    let mut items = Vec::new();
    let mut exclusive_start_key = None;
    loop {
        let result = provider
            .query_page(QueryTableRequest {
                table_name: query.table_name.clone(),
                index_name: query.index_name.clone(),
                key_condition_expression: query.key_condition_expression.clone(),
                expression_attribute_names: query.expression_attribute_names.clone(),
                expression_attribute_values: query.expression_attribute_values.clone(),
                projection_expression: query.projection_expression.clone(),
                limit: query.limit,
                exclusive_start_key: exclusive_start_key.take(),
                scan_index_forward: query.scan_index_forward,
                consistent_read: query.consistent_read.unwrap_or(false),
            })
            .await
            .map_err(|error| OrdinaryFailure::storage(storage_error_detail(&error)))?;
        let continuation = result.continuation;
        items.extend(result.items);
        if continuation.is_none() {
            break;
        }
        exclusive_start_key = continuation;
    }
    Ok(items)
}

async fn load_get(
    context: &OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
    get: &GetItemRequest,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    if context.plan.graph.dependencies[node_id.index()].is_empty() {
        return load_root_get(context.provider, get).await;
    }
    load_child_get(context, node_id, node, get).await
}

async fn load_root_get(
    provider: &Arc<dyn storage_provider::DynamoOperations>,
    get: &GetItemRequest,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    let item = provider
        .get_item(get.clone())
        .await
        .map_err(|error| OrdinaryFailure::storage(storage_error_detail(&error)))?
        .item
        .map(|item| {
            <HashMap<String, AttributeValue> as storage_types::TryFromWireItem>::try_from_wire_item(
                &item,
            )
            .map_err(|error| OrdinaryFailure::fixture(storage_error_detail(&error)))
        })
        .transpose()?;
    Ok(vec![item.into_iter().collect()])
}

async fn load_child_get(
    context: &OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
    get: &GetItemRequest,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    let parent_items = parent_items(context, node_id)?;
    let input = node
        .inputs()
        .values()
        .next()
        .ok_or_else(|| format!("ordinary child {} has no input", node.name))?;
    let source_field = input
        .from
        .select
        .0
        .strip_prefix("$.Query.Items[*].")
        .ok_or_else(|| "ordinary fixture selector is not physical".to_string())?;
    let (key_name, _) = get
        .key
        .iter()
        .next()
        .ok_or_else(|| format!("ordinary child {} has an empty key", node.name))?;
    let keys = child_keys(parent_items, source_field, key_name);
    let response = load_child_batch(context.simulation, context.provider, get, keys).await?;
    child_items_in_parent_order(response, get, parent_items, source_field, key_name)
}

fn parent_items<'a>(
    context: &'a OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
) -> Result<&'a Vec<HashMap<String, AttributeValue>>, OrdinaryFailure> {
    let [parent_id] = context.plan.graph.dependencies[node_id.index()].as_slice() else {
        return Err(OrdinaryFailure::fixture(
            "ordinary fixture child has more than one dependency",
        ));
    };
    context
        .results
        .get(parent_id)
        .and_then(|invocations| invocations.first())
        .ok_or_else(|| {
            format!(
                "ordinary parent {} returned no invocation",
                parent_id.index()
            )
        })
        .map_err(OrdinaryFailure::fixture)
}

fn child_keys(
    parent_items: &[HashMap<String, AttributeValue>],
    source_field: &str,
    key_name: &str,
) -> Vec<KeyAttributes> {
    parent_items
        .iter()
        .filter_map(|item| {
            item.get(source_field)
                .cloned()
                .map(|value| KeyAttributes::from(HashMap::from([(key_name.to_string(), value)])))
        })
        .collect()
}

async fn load_child_batch(
    simulation: &WorkloadContext,
    provider: &Arc<dyn storage_provider::DynamoOperations>,
    get: &GetItemRequest,
    keys: Vec<KeyAttributes>,
) -> Result<storage_types::BatchGetItemResponse, OrdinaryFailure> {
    let mut request_items: HashMap<TableName, KeysAndAttributes> = [(
        get.table_name.clone(),
        KeysAndAttributes {
            keys: keys.into_iter().collect(),
            attributes_to_get: None,
            projection_expression: None,
            expression_attribute_names: None,
            consistent_read: get.consistent_read,
        },
    )]
    .into_iter()
    .collect();
    let mut responses = HashMap::new();

    for attempt in 0..ORDINARY_BATCH_GET_MAX_ATTEMPTS {
        let mut response = provider
            .batch_get_item(BatchGetItemRequest {
                request_items: request_items.into_iter().collect(),
                return_consumed_capacity: None,
            })
            .await
            .map_err(|error| OrdinaryFailure::storage(storage_error_detail(&error)))?;
        let unprocessed_keys = merge_batch_get_page(&mut responses, &mut response);
        let Some(next_request_items) = next_batch_get_request(attempt, unprocessed_keys)? else {
            return Ok(storage_types::BatchGetItemResponse {
                responses: (!responses.is_empty()).then(|| responses.into_iter().collect()),
                unprocessed_keys: None,
                consumed_capacity: None,
            });
        };
        simulation
            .delay(std::time::Duration::from_millis(
                ORDINARY_BATCH_GET_RETRY_BASE_DELAY_MS << attempt,
            ))
            .await
            .map_err(|error| {
                OrdinaryFailure::storage(format!("ordinary BatchGet retry delay failed: {error}"))
            })?;
        request_items = next_request_items;
    }

    unreachable!("ordinary BatchGet retry loop has a fixed attempt bound")
}

pub(crate) fn merge_batch_get_page(
    responses: &mut HashMap<TableName, Vec<WireItem>>,
    response: &mut storage_types::BatchGetItemResponse,
) -> HashMap<TableName, KeysAndAttributes> {
    for (table_name, mut items) in response.responses.take().unwrap_or_default() {
        responses.entry(table_name).or_default().append(&mut items);
    }
    response
        .unprocessed_keys
        .take()
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, keys)| !keys.keys.is_empty())
        .collect()
}

pub(crate) fn next_batch_get_request(
    attempt: usize,
    unprocessed_keys: HashMap<TableName, KeysAndAttributes>,
) -> Result<Option<HashMap<TableName, KeysAndAttributes>>, OrdinaryFailure> {
    if unprocessed_keys.is_empty() {
        return Ok(None);
    }
    if attempt + 1 == ORDINARY_BATCH_GET_MAX_ATTEMPTS {
        return Err(OrdinaryFailure::storage(format!(
            "ordinary child batch exhausted retries with {} unprocessed key(s)",
            unprocessed_keys
                .values()
                .map(|keys| keys.keys.len())
                .sum::<usize>()
        )));
    }
    Ok(Some(unprocessed_keys))
}

fn child_items_in_parent_order(
    response: storage_types::BatchGetItemResponse,
    get: &GetItemRequest,
    parent_items: &[HashMap<String, AttributeValue>],
    source_field: &str,
    key_name: &str,
) -> Result<OrdinaryNodeResult, OrdinaryFailure> {
    let child_items = decode_child_items(response, get, key_name)?;
    Ok(parent_items
        .iter()
        .filter_map(|item| {
            let Some(AttributeValue::S(key)) = item.get(source_field) else {
                return None;
            };
            child_items.get(key).cloned().map(|item| vec![item])
        })
        .collect())
}

fn decode_child_items(
    response: storage_types::BatchGetItemResponse,
    get: &GetItemRequest,
    key_name: &str,
) -> Result<HashMap<String, HashMap<String, AttributeValue>>, OrdinaryFailure> {
    let mut child_items = HashMap::new();
    let mut responses = response.responses.unwrap_or_default();
    for item in responses.remove(&get.table_name).unwrap_or_default() {
        let attributes = <HashMap<String, AttributeValue> as storage_types::TryFromWireItem>::try_from_wire_item(&item)
            .map_err(|error| OrdinaryFailure::fixture(storage_error_detail(&error)))?;
        let Some(AttributeValue::S(key_value)) = attributes.get(key_name) else {
            return Err(OrdinaryFailure::fixture(format!(
                "ordinary child response is missing key {key_name}"
            )));
        };
        child_items.insert(key_value.clone(), attributes);
    }
    Ok(child_items)
}

fn normalize_results(
    results: OrdinaryResults,
    plan: &storage_types::ReadSequencePlan,
) -> NormalizedResult {
    results
        .into_iter()
        .map(|(node_id, invocations)| (plan.graph.node_names[node_id.index()].clone(), invocations))
        .collect()
}
