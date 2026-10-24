//! Coalesce one Get node's bound keys while retaining invocation order.

use storage_types::{
    AttributeMap, AttributeProjection, BatchGetItemRequest, GetItemRequest, GetItemResponse,
    KeysAndAttributes, ReadSequenceInvocationPayload, ReadSequenceInvocationResult,
    ReadSequenceNodeOperation, ReadSequenceNodeResult, StorageError, StorageResult, VecMap,
    WireItem, bind_operation,
};

use super::node::{NodeExecution, NodeOutcome};
use crate::{CachePublication, ReadSequenceStepReader};

impl NodeExecution<'_> {
    pub(super) async fn execute_get_fanout<R: ReadSequenceStepReader>(
        self,
        reader: &mut R,
    ) -> StorageResult<NodeOutcome> {
        let requests = self
            .bindings
            .iter()
            .map(|binding| {
                match bind_operation(&self.node.operation, binding.resolved_inputs())?
                    .with_consistency(self.consistency)
                {
                    ReadSequenceNodeOperation::Get(request) => Ok(*request),
                    _ => Err(StorageError::internal("Get fanout requires a Get node")),
                }
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let (items, publications) = read_keys(reader, &requests).await?;
        let mut invocations = Vec::with_capacity(requests.len());
        let mut item_count = 0;
        for (ordinal, (binding, request)) in self.bindings.into_iter().zip(requests).enumerate() {
            let item = projected_item(&items, &request)?;
            item_count += u32::from(item.is_some());
            invocations.push(ReadSequenceInvocationResult {
                ordinal: ordinal as u32,
                input_refs: binding.into_input_refs(),
                result: ReadSequenceInvocationPayload::Get(GetItemResponse { item }),
            });
        }
        Ok(NodeOutcome {
            item_cursors: vec![None; invocations.len()],
            result: ReadSequenceNodeResult {
                name: self.node.name.clone(),
                invocations,
            },
            continuation: None,
            publications,
            item_count,
        })
    }
}

async fn read_keys<R: ReadSequenceStepReader>(
    reader: &mut R,
    requests: &[GetItemRequest],
) -> StorageResult<(Vec<AttributeMap>, Vec<CachePublication>)> {
    let Some(first) = requests.first() else {
        return Ok((Vec::new(), Vec::new()));
    };
    // Binding expansion owns Get-key deduplication.
    let keys = requests
        .iter()
        .map(|request| request.key.clone())
        .collect::<Vec<_>>();
    let mut items = Vec::new();
    let mut publications = Vec::new();
    // DynamoDB BatchGet accepts at most 100 unique keys. Keep full images until
    // each invocation is matched; a customer projection may omit its key.
    for keys in keys.chunks(100) {
        let request = BatchGetItemRequest {
            request_items: VecMap::from_iter([(
                first.table_name.clone(),
                KeysAndAttributes {
                    keys: keys.to_vec(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: first.consistent_read,
                },
            )]),
            return_consumed_capacity: first.return_consumed_capacity,
        };
        let (response, loaded) =
            crate::dynamo_read_sequence_step::complete_batch_get(reader, request).await?;
        publications.extend(loaded);
        for (_, returned) in response.responses.unwrap_or_default() {
            items.extend(
                returned
                    .into_iter()
                    .map(AttributeMap::try_from)
                    .collect::<StorageResult<Vec<_>>>()?,
            );
        }
    }
    Ok((items, publications))
}

fn projected_item(
    items: &[AttributeMap],
    request: &GetItemRequest,
) -> StorageResult<Option<WireItem>> {
    items
        .iter()
        .find(|item| {
            request
                .key
                .iter()
                .all(|(name, value)| item.get(name) == Some(value))
        })
        .map(|item| {
            let projection = AttributeProjection::new(
                request.projection_expression.as_deref(),
                request.attributes_to_get.as_deref(),
                request.expression_attribute_names.as_ref(),
            );
            WireItem::try_from(
                projection.map_or_else(|| item.clone(), |projection| projection.project(item)),
            )
        })
        .transpose()
}
