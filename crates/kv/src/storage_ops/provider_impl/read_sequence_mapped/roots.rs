use std::collections::HashMap;

use futures::future::try_join_all;
use storage_provider::{
    ReadSequenceFlatResult, ReadSequenceFlatRow, ReadSequenceUnsupportedReason,
};
use storage_types::{
    GetItemRequest, ReadSequenceNode, ReadSequenceNodeId, ReadSequenceNodeOperation, StorageResult,
};

use crate::storage_ops::provider_impl::SortedKvDbStorageProvider;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn execute_independent_roots(
        &self,
        roots: &[(ReadSequenceNodeId, &ReadSequenceNode)],
        rows_by_node: &mut [Vec<ReadSequenceFlatRow>],
    ) -> StorageResult<Option<ReadSequenceUnsupportedReason>> {
        let rows = try_join_all(
            roots
                .iter()
                .map(|(node_id, node)| self.execute_independent_root(*node_id, node)),
        )
        .await?;
        for ((node_id, _), row) in roots.iter().zip(rows) {
            let Some(row) = row else {
                return Ok(Some(ReadSequenceUnsupportedReason::OperationShape));
            };
            rows_by_node[node_id.index()] = vec![row];
        }
        Ok(None)
    }

    async fn execute_independent_root(
        &self,
        node_id: ReadSequenceNodeId,
        node: &ReadSequenceNode,
    ) -> StorageResult<Option<ReadSequenceFlatRow>> {
        match &node.operation {
            ReadSequenceNodeOperation::Get(request) => self
                .execute_independent_get(node_id, request)
                .await
                .map(Some),
            ReadSequenceNodeOperation::BatchGet(request) => {
                self.execute_independent_batch_get(node_id, request).await
            }
            ReadSequenceNodeOperation::Query(_) => Ok(None),
        }
    }

    async fn execute_independent_get(
        &self,
        node_id: ReadSequenceNodeId,
        request: &GetItemRequest,
    ) -> StorageResult<ReadSequenceFlatRow> {
        let item = self
            .get_item_impl(
                request.table_name.clone(),
                request.key.clone(),
                request.consistent_read.unwrap_or(false),
            )
            .await?
            .map(|item| item.to_attribute_map())
            .transpose()?
            .map(Into::into);
        Ok(ReadSequenceFlatRow {
            node: node_id,
            invocation_ordinal: 0,
            input_refs: Default::default(),
            result: ReadSequenceFlatResult::Get { item },
        })
    }

    async fn execute_independent_batch_get(
        &self,
        node_id: ReadSequenceNodeId,
        request: &storage_types::BatchGetItemRequest,
    ) -> StorageResult<Option<ReadSequenceFlatRow>> {
        let response = self.batch_get_item_impl(request.clone()).await?;
        if response
            .unprocessed_keys
            .as_ref()
            .is_some_and(|keys| !keys.is_empty())
        {
            return Ok(None);
        }
        let responses = response
            .responses
            .unwrap_or_default()
            .into_iter()
            .map(|(table_name, items)| {
                items
                    .into_iter()
                    .map(|item| item.to_attribute_map().map(Into::into))
                    .collect::<StorageResult<Vec<_>>>()
                    .map(|items| (table_name, items))
            })
            .collect::<StorageResult<HashMap<_, _>>>()?;
        Ok(Some(ReadSequenceFlatRow {
            node: node_id,
            invocation_ordinal: 0,
            input_refs: Default::default(),
            result: ReadSequenceFlatResult::BatchGet { responses },
        }))
    }
}
