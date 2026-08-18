use std::collections::BTreeMap;

use crate::{
    common::*,
    imports::*,
    read_sequence_dag::{FdbChaosProvider, NormalizedResult, ReadSequenceDagWorkload},
};

type OrdinaryNodeResult = Vec<Vec<HashMap<String, AttributeValue>>>;
type OrdinaryResults = BTreeMap<ReadSequenceNodeId, OrdinaryNodeResult>;

struct OrdinaryContext<'a> {
    provider: &'a Arc<FdbChaosProvider>,
    request: &'a ReadSequenceRequest,
    plan: &'a storage_types::ReadSequencePlan,
    results: &'a OrdinaryResults,
}

impl ReadSequenceDagWorkload {
    /// Execute the fixture through the ordinary provider path. The planner
    /// owns wave order; this module only supplies the fixed fixture reads.
    pub(super) async fn run_ordinary_fixture(
        &self,
        provider: &Arc<FdbChaosProvider>,
        request: &ReadSequenceRequest,
        plan: &storage_types::ReadSequencePlan,
    ) -> Result<NormalizedResult, String> {
        let mut results = OrdinaryResults::new();
        for wave in &plan.graph.waves {
            for node_id in wave {
                let context = OrdinaryContext {
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
) -> Result<OrdinaryNodeResult, String> {
    let node = context
        .request
        .nodes
        .get(node_id.index())
        .ok_or_else(|| format!("ordinary node {} is outside request", node_id.index()))?;
    match &node.operation {
        ReadSequenceNodeOperation::Query(query) => load_query(context.provider, query).await,
        ReadSequenceNodeOperation::Get(get) => load_get(context, node_id, node, get).await,
        ReadSequenceNodeOperation::BatchGet(_) => {
            Err("ordinary fixture does not include BatchGet".to_string())
        }
    }
}

async fn load_query(
    provider: &Arc<FdbChaosProvider>,
    query: &QueryRequest,
) -> Result<OrdinaryNodeResult, String> {
    let items = load_query_items(provider, query).await?;
    let attributes = items
        .into_iter()
        .map(|item| {
            item.to_attribute_map()
                .map_err(|error| storage_error_detail(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(vec![attributes])
}

async fn load_query_items(
    provider: &Arc<FdbChaosProvider>,
    query: &QueryRequest,
) -> Result<Vec<WireItem>, String> {
    let mut items = Vec::new();
    let mut exclusive_start_key = None;
    loop {
        let (page, continuation) = provider
            .query_table(&QueryTableRequest {
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
            .map_err(|error| storage_error_detail(&error))?;
        items.extend(page);
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
) -> Result<OrdinaryNodeResult, String> {
    if context.plan.graph.dependencies[node_id.index()].is_empty() {
        return load_root_get(context.provider, get).await;
    }
    load_child_get(context, node_id, node, get).await
}

async fn load_root_get(
    provider: &Arc<FdbChaosProvider>,
    get: &GetItemRequest,
) -> Result<OrdinaryNodeResult, String> {
    let item = provider
        .get_item(
            get.table_name.clone(),
            get.key.clone(),
            get.consistent_read.unwrap_or(false),
        )
        .await
        .map_err(|error| storage_error_detail(&error))?
        .map(|item| {
            item.to_attribute_map()
                .map_err(|error| storage_error_detail(&error))
        })
        .transpose()?;
    Ok(vec![item.into_iter().collect()])
}

async fn load_child_get(
    context: &OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
    get: &GetItemRequest,
) -> Result<OrdinaryNodeResult, String> {
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
    let response = load_child_batch(context.provider, get, keys).await?;
    child_items_in_parent_order(response, get, parent_items, source_field, key_name)
}

fn parent_items<'a>(
    context: &'a OrdinaryContext<'_>,
    node_id: ReadSequenceNodeId,
) -> Result<&'a Vec<HashMap<String, AttributeValue>>, String> {
    let [parent_id] = context.plan.graph.dependencies[node_id.index()].as_slice() else {
        return Err("ordinary fixture child has more than one dependency".to_string());
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
    provider: &Arc<FdbChaosProvider>,
    get: &GetItemRequest,
    keys: Vec<KeyAttributes>,
) -> Result<storage_types::BatchGetWireItemResponse, String> {
    let response = provider
        .batch_get_item(BatchGetItemRequest {
            request_items: [(
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
            .collect(),
            return_consumed_capacity: None,
        })
        .await
        .map_err(|error| storage_error_detail(&error))?;
    if response
        .unprocessed_keys
        .as_ref()
        .is_some_and(|keys| !keys.is_empty())
    {
        return Err("ordinary fixture child batch has unprocessed keys".to_string());
    }
    Ok(response)
}

fn child_items_in_parent_order(
    response: storage_types::BatchGetWireItemResponse,
    get: &GetItemRequest,
    parent_items: &[HashMap<String, AttributeValue>],
    source_field: &str,
    key_name: &str,
) -> Result<OrdinaryNodeResult, String> {
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
    response: storage_types::BatchGetWireItemResponse,
    get: &GetItemRequest,
    key_name: &str,
) -> Result<HashMap<String, HashMap<String, AttributeValue>>, String> {
    let mut child_items = HashMap::new();
    let mut responses = response.responses.unwrap_or_default();
    for item in responses.remove(&get.table_name).unwrap_or_default() {
        let attributes = item
            .to_attribute_map()
            .map_err(|error| storage_error_detail(&error))?;
        let Some(AttributeValue::S(key_value)) = attributes.get(key_name) else {
            return Err(format!("ordinary child response is missing key {key_name}"));
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
