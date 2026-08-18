//! Whole-plan FoundationDB mapped-range lowering.
//!
//! The lowering handles one base/GSI Query and one key-derived Get, or one
//! point Get and one input-derived primary-table partition Query. A distinct
//! child table is read directly with a Tuple mapper; a same-item child is
//! materialized from the source row and never follows a GSI to its base item.

use std::{collections::HashMap, sync::Arc};

use storage_provider::{
    ReadSequenceExecution, ReadSequenceMappedOptions, ReadSequenceMappedRangePage,
    ReadSequenceMappedRangeRequest, ReadSequenceMappedSelection, ReadSequenceUnsupportedReason,
    select_read_sequence_mapped_edges,
};
use storage_types::{
    GetItemRequest, QueryRequest, ReadSequenceConsistency, ReadSequenceNode, ReadSequenceNodeId,
    ReadSequenceNodeOperation, ReadSequencePlan, StorageError, StorageResult,
};

use crate::storage_ops::provider_impl::{
    SortedKvDbStorageProvider,
    read_sequence_mapped_bounds::{mapped_get_bounds, mapped_query_bounds},
    read_sequence_mapped_descriptors::{mapped_descriptors, mapped_get_query_descriptors},
    read_sequence_mapped_layout::{mapped_get_query_physical_layout, mapped_physical_layout},
    read_sequence_mapped_metrics::{mapped_selection_reason, record_mapped_selection},
    read_sequence_mapped_rows::{flatten_rows, mapped_edge_rows, mapped_get_query_rows},
};

pub(super) mod bindings;
mod roots;

use bindings::{MappedInput, MappedKeyBinding, mapped_child_binding, mapped_child_query_binding};

#[derive(Clone, Copy)]
pub(super) enum MappedParentOperation {
    Query,
    Get,
}

pub(super) struct MappedSequenceShape<'a> {
    pub(super) parent_id: ReadSequenceNodeId,
    pub(super) child_id: ReadSequenceNodeId,
    pub(super) parent_name: &'a str,
    pub(super) inputs: Vec<MappedInput<'a>>,
    pub(super) keys: Vec<MappedKeyBinding<'a>>,
    pub(super) iterates: bool,
    pub(super) index_name: Option<&'a storage_types::IndexName>,
    pub(super) parent_query: &'a QueryRequest,
    pub(super) child_get: &'a GetItemRequest,
    pub(super) independent_roots: Vec<(ReadSequenceNodeId, &'a ReadSequenceNode)>,
}

pub(super) struct MappedGetQueryShape<'a> {
    pub(super) parent_id: ReadSequenceNodeId,
    pub(super) child_id: ReadSequenceNodeId,
    pub(super) parent_name: &'a str,
    pub(super) inputs: Vec<MappedInput<'a>>,
    pub(super) keys: Vec<MappedKeyBinding<'a>>,
    pub(super) parent_get: &'a GetItemRequest,
    pub(super) child_query: &'a QueryRequest,
    pub(super) independent_roots: Vec<(ReadSequenceNodeId, &'a ReadSequenceNode)>,
}

struct MappedRange {
    begin: Vec<u8>,
    end: Vec<u8>,
    mapper: Option<Vec<u8>>,
    exclusive_start: Option<Vec<u8>>,
    reverse: bool,
    same_item: bool,
    parent: Arc<crate::keyspace::table_identity::StoredTableMetadata>,
    child: Arc<crate::keyspace::table_identity::StoredTableMetadata>,
}

impl<'a> MappedSequenceShape<'a> {
    pub(super) fn from_plan(
        plan: &'a ReadSequencePlan,
    ) -> Result<Self, ReadSequenceUnsupportedReason> {
        if plan.nodes.len() < 2 {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let (parent_id, child_id) =
            mapped_edge(plan).ok_or(ReadSequenceUnsupportedReason::OperationShape)?;
        let parent = &plan.nodes[parent_id.index()];
        let child = &plan.nodes[child_id.index()];
        let parent_query = query_operation(parent)?;
        let child_get = get_operation(child)?;
        let binding = mapped_child_binding(parent_query, parent.name.as_str(), child, child_get)?;
        if !mapped_inputs_visible(
            parent_query.projection_expression.as_deref(),
            parent_query.attributes_to_get.as_deref(),
            parent_query.expression_attribute_names.as_ref(),
            &binding.inputs,
        ) {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let independent_roots = independent_roots(plan, parent_id, child_id)?;
        Ok(Self {
            parent_id,
            child_id,
            parent_name: &parent.name,
            inputs: binding.inputs,
            keys: binding.keys,
            iterates: binding.iterates,
            index_name: parent_query.index_name.as_ref(),
            parent_query,
            child_get,
            independent_roots,
        })
    }
}

impl<'a> MappedGetQueryShape<'a> {
    pub(super) fn from_plan(
        plan: &'a ReadSequencePlan,
    ) -> Result<Self, ReadSequenceUnsupportedReason> {
        if plan.nodes.len() < 2 {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let (parent_id, child_id) =
            mapped_edge(plan).ok_or(ReadSequenceUnsupportedReason::OperationShape)?;
        let parent = &plan.nodes[parent_id.index()];
        let child = &plan.nodes[child_id.index()];
        let parent_get = get_operation(parent)?;
        let child_query = query_operation(child)?;
        if parent_get.key.iter().any(|(_, value)| {
            storage_types::read_sequence_input_marker_name(value).is_some()
                || storage_types::read_sequence_string_template_name(value).is_some()
                || storage_types::read_sequence_input_literal_name(value).is_some()
        }) {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let binding =
            mapped_child_query_binding(parent_get, parent.name.as_str(), child, child_query)?;
        if !mapped_inputs_visible(
            parent_get.projection_expression.as_deref(),
            parent_get.attributes_to_get.as_deref(),
            parent_get.expression_attribute_names.as_ref(),
            &binding.inputs,
        ) {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let independent_roots = independent_roots(plan, parent_id, child_id)?;
        Ok(Self {
            parent_id,
            child_id,
            parent_name: &parent.name,
            inputs: binding.inputs,
            keys: binding.keys,
            parent_get,
            child_query,
            independent_roots,
        })
    }
}

pub(super) fn mapped_edge(
    plan: &ReadSequencePlan,
) -> Option<(ReadSequenceNodeId, ReadSequenceNodeId)> {
    (0..plan.nodes.len()).find_map(|candidate_child| {
        let [candidate_parent] = plan.graph.dependencies.get(candidate_child)?.as_slice() else {
            return None;
        };
        plan.graph
            .dependencies
            .get(candidate_parent.index())
            .is_some_and(Vec::is_empty)
            .then_some((
                *candidate_parent,
                ReadSequenceNodeId::from_index(candidate_child),
            ))
    })
}

fn query_operation(
    node: &ReadSequenceNode,
) -> Result<&QueryRequest, ReadSequenceUnsupportedReason> {
    match &node.operation {
        ReadSequenceNodeOperation::Query(query) => Ok(query),
        _ => Err(ReadSequenceUnsupportedReason::OperationShape),
    }
}

fn get_operation(
    node: &ReadSequenceNode,
) -> Result<&GetItemRequest, ReadSequenceUnsupportedReason> {
    match &node.operation {
        ReadSequenceNodeOperation::Get(get) => Ok(get),
        _ => Err(ReadSequenceUnsupportedReason::OperationShape),
    }
}

fn mapped_inputs_visible(
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    inputs: &[MappedInput<'_>],
) -> bool {
    let Some(projection) = storage_types::AttributeProjection::new(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    ) else {
        return true;
    };
    inputs.iter().all(|input| {
        let item = HashMap::from([(
            input.attribute_name.to_string(),
            storage_types::AttributeValue::NULL(false),
        )]);
        projection
            .project(&item)
            .get(input.attribute_name)
            .is_some()
    })
}

fn independent_roots(
    plan: &ReadSequencePlan,
    parent_id: ReadSequenceNodeId,
    child_id: ReadSequenceNodeId,
) -> Result<Vec<(ReadSequenceNodeId, &ReadSequenceNode)>, ReadSequenceUnsupportedReason> {
    let roots = plan
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(index, node)| {
            let node_id = ReadSequenceNodeId::from_index(index);
            (node_id != parent_id && node_id != child_id).then_some((node_id, node))
        })
        .collect::<Vec<_>>();
    for (node_id, node) in &roots {
        validate_independent_root(plan, *node_id, node)?;
    }
    Ok(roots)
}

fn validate_independent_root(
    plan: &ReadSequencePlan,
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
) -> Result<(), ReadSequenceUnsupportedReason> {
    validate_root_dependencies(plan, node_id, node)?;
    match &node.operation {
        ReadSequenceNodeOperation::Get(request) if is_plain_get(request) => Ok(()),
        ReadSequenceNodeOperation::BatchGet(request) => validate_batch_root(request),
        _ => Err(ReadSequenceUnsupportedReason::OperationShape),
    }
}

fn validate_root_dependencies(
    plan: &ReadSequencePlan,
    node_id: ReadSequenceNodeId,
    node: &ReadSequenceNode,
) -> Result<(), ReadSequenceUnsupportedReason> {
    if !plan.graph.dependencies[node_id.index()].is_empty()
        || !node.inputs().is_empty()
        || node.iterate.is_some()
        || !node.after().is_empty()
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    Ok(())
}

fn is_plain_get(request: &GetItemRequest) -> bool {
    request.attributes_to_get.is_none()
        && request.projection_expression.is_none()
        && request.expression_attribute_names.is_none()
        && request.return_consumed_capacity.is_none()
        && request.consistent_read != Some(true)
}

fn validate_batch_root(
    request: &storage_types::BatchGetItemRequest,
) -> Result<(), ReadSequenceUnsupportedReason> {
    let Some((_, keys)) = request.request_items.iter().next() else {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    };
    (request.request_items.len() == 1
        && !keys.keys.is_empty()
        && request.return_consumed_capacity.is_none()
        && keys.attributes_to_get.is_none()
        && keys.projection_expression.is_none()
        && keys.expression_attribute_names.is_none()
        && keys.consistent_read != Some(true))
    .then_some(())
    .ok_or(ReadSequenceUnsupportedReason::OperationShape)
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn execute_read_sequence_plan_mapped_impl(
        &self,
        plan: &ReadSequencePlan,
        consistency: ReadSequenceConsistency,
        continuation: Option<&str>,
    ) -> StorageResult<ReadSequenceExecution> {
        if let Some(reason) = mapped_execution_rejection(consistency, continuation) {
            return Ok(ReadSequenceExecution::Unsupported(reason));
        }
        let Some((parent_id, child_id)) = mapped_edge(plan) else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::OperationShape,
            ));
        };
        if matches!(
            (
                &plan.nodes[parent_id.index()].operation,
                &plan.nodes[child_id.index()].operation
            ),
            (
                ReadSequenceNodeOperation::Get(_),
                ReadSequenceNodeOperation::Query(_)
            )
        ) {
            return self.execute_get_query_mapped_impl(plan, consistency).await;
        }
        self.execute_query_get_mapped_impl(plan, consistency).await
    }

    async fn execute_query_get_mapped_impl(
        &self,
        plan: &ReadSequencePlan,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<ReadSequenceExecution> {
        let shape = match MappedSequenceShape::from_plan(plan) {
            Ok(shape) => shape,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };

        let range = match self.prepare_mapped_range(plan, &shape, consistency).await? {
            Ok(range) => range,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        let (page, parent, child, same_item) = match self.read_mapped_page(range).await? {
            Ok(page) => page,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        let Some(mut rows_by_node) =
            mapped_edge_rows(&shape, page, plan.nodes.len(), same_item, &parent, &child)?
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::PhysicalLayout,
            ));
        };
        if let Some(reason) = self
            .execute_independent_roots(&shape.independent_roots, &mut rows_by_node)
            .await?
        {
            return Ok(ReadSequenceExecution::Unsupported(reason));
        }
        let rows = flatten_rows(rows_by_node);
        ::metrics::counter!("storage.read_sequence.mapped.success.total").increment(1);
        Ok(ReadSequenceExecution::Executed(
            storage_provider::ReadSequenceExecuted {
                rows,
                next_continuation: None,
            },
        ))
    }

    async fn execute_get_query_mapped_impl(
        &self,
        plan: &ReadSequencePlan,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<ReadSequenceExecution> {
        let shape = match MappedGetQueryShape::from_plan(plan) {
            Ok(shape) => shape,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        let range = match self
            .prepare_get_query_mapped_range(plan, &shape, consistency)
            .await?
        {
            Ok(range) => range,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        let (page, parent, child, same_item) = match self.read_mapped_page(range).await? {
            Ok(page) => page,
            Err(reason) => return Ok(ReadSequenceExecution::Unsupported(reason)),
        };
        if same_item {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::PhysicalLayout,
            ));
        }
        let Some(mut rows_by_node) =
            mapped_get_query_rows(&shape, page, plan.nodes.len(), &parent, &child)?
        else {
            return Ok(ReadSequenceExecution::Unsupported(
                ReadSequenceUnsupportedReason::PhysicalLayout,
            ));
        };
        if let Some(reason) = self
            .execute_independent_roots(&shape.independent_roots, &mut rows_by_node)
            .await?
        {
            return Ok(ReadSequenceExecution::Unsupported(reason));
        }
        let rows = flatten_rows(rows_by_node);
        ::metrics::counter!("storage.read_sequence.mapped.success.total").increment(1);
        Ok(ReadSequenceExecution::Executed(
            storage_provider::ReadSequenceExecuted {
                rows,
                next_continuation: None,
            },
        ))
    }

    async fn prepare_mapped_range(
        &self,
        plan: &ReadSequencePlan,
        shape: &MappedSequenceShape<'_>,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Result<MappedRange, ReadSequenceUnsupportedReason>> {
        let parent = self.mapped_metadata(&shape.parent_query.table_name).await?;
        let child = self.mapped_metadata(&shape.child_get.table_name).await?;
        let layout = match mapped_physical_layout(shape, &parent, &child)? {
            Ok(layout) => layout,
            Err(reason) => return Ok(Err(reason)),
        };
        let bounds = mapped_query_bounds(&parent, shape.parent_query)?;
        let descriptors = mapped_descriptors(
            shape,
            self.kv_store.supports_read_sequence_mapped_range() && bounds.is_some(),
        );
        let selection = self.mapped_selection(plan, &descriptors, consistency);
        record_mapped_selection(&selection);
        if !mapped_edge_selected(shape, &selection) {
            return Ok(Err(mapped_selection_reason(&selection)));
        }
        let Some(bounds) = bounds else {
            return Ok(Err(ReadSequenceUnsupportedReason::OperationShape));
        };
        Ok(Ok(MappedRange {
            begin: bounds.begin,
            end: bounds.end,
            mapper: layout.mapper,
            exclusive_start: bounds.exclusive_start,
            reverse: bounds.reverse,
            same_item: layout.same_item,
            parent,
            child,
        }))
    }

    async fn prepare_get_query_mapped_range(
        &self,
        plan: &ReadSequencePlan,
        shape: &MappedGetQueryShape<'_>,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Result<MappedRange, ReadSequenceUnsupportedReason>> {
        let parent = self.mapped_metadata(&shape.parent_get.table_name).await?;
        let child = self.mapped_metadata(&shape.child_query.table_name).await?;
        let layout = match mapped_get_query_physical_layout(shape, &parent, &child)? {
            Ok(layout) => layout,
            Err(reason) => return Ok(Err(reason)),
        };
        let bounds = mapped_get_bounds(&parent, shape.parent_get)?;
        let descriptors = mapped_get_query_descriptors(
            shape,
            self.kv_store.supports_read_sequence_mapped_range() && bounds.is_some(),
        );
        let selection = self.mapped_selection(plan, &descriptors, consistency);
        record_mapped_selection(&selection);
        if !mapped_get_query_edge_selected(shape, &selection) {
            return Ok(Err(mapped_selection_reason(&selection)));
        }
        let Some(bounds) = bounds else {
            return Ok(Err(ReadSequenceUnsupportedReason::OperationShape));
        };
        Ok(Ok(MappedRange {
            begin: bounds.begin,
            end: bounds.end,
            mapper: layout.mapper,
            exclusive_start: bounds.exclusive_start,
            reverse: bounds.reverse,
            same_item: layout.same_item,
            parent,
            child,
        }))
    }

    async fn mapped_metadata(
        &self,
        table_name: &storage_types::TableName,
    ) -> StorageResult<Arc<crate::keyspace::table_identity::StoredTableMetadata>> {
        self.get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))
    }

    fn mapped_selection(
        &self,
        plan: &ReadSequencePlan,
        descriptors: &[(
            ReadSequenceNodeId,
            storage_provider::ReadSequencePhysicalDescriptor,
        )],
        consistency: ReadSequenceConsistency,
    ) -> ReadSequenceMappedSelection {
        let api_version = self.kv_store.read_sequence_mapped_range_api_version();
        select_read_sequence_mapped_edges(
            plan,
            descriptors,
            ReadSequenceMappedOptions {
                foundationdb: api_version > 0,
                api_version,
                enabled: self.kv_store.supports_read_sequence_mapped_range(),
                consistency,
            },
        )
    }

    async fn read_mapped_page(
        &self,
        range: MappedRange,
    ) -> StorageResult<
        Result<
            (
                ReadSequenceMappedRangePage,
                Arc<crate::keyspace::table_identity::StoredTableMetadata>,
                Arc<crate::keyspace::table_identity::StoredTableMetadata>,
                bool,
            ),
            ReadSequenceUnsupportedReason,
        >,
    > {
        let reverse = range.reverse;
        let same_item = range.same_item;
        let parent = range.parent;
        let child = range.child;
        let Some(page) = self
            .kv_store
            .read_sequence_mapped_range(ReadSequenceMappedRangeRequest {
                begin: range.begin,
                end: range.end,
                mapper: range.mapper,
                exclusive_start: range.exclusive_start,
                reverse: range.reverse,
                // Mapped execution is selected only for an unbounded plan.  Let
                // FoundationDB finish the complete mapped range; a page marked
                // `more` still falls back below rather than publishing partial
                // secondary results.
                target_bytes: 4 * 1024 * 1024,
            })
            .await?
        else {
            return Ok(Err(ReadSequenceUnsupportedReason::PhysicalLayout));
        };
        if page.more {
            ::metrics::counter!("storage.read_sequence.mapped.fallback.total", "reason" => "continuation")
                .increment(1);
            return Ok(Err(ReadSequenceUnsupportedReason::Continuation));
        }
        page.validate_complete(reverse)?;
        Ok(Ok((page, parent, child, same_item)))
    }
}

fn mapped_execution_rejection(
    consistency: ReadSequenceConsistency,
    continuation: Option<&str>,
) -> Option<ReadSequenceUnsupportedReason> {
    if continuation.is_some() {
        Some(ReadSequenceUnsupportedReason::Continuation)
    } else if consistency != ReadSequenceConsistency::Eventual {
        Some(ReadSequenceUnsupportedReason::OperationShape)
    } else {
        None
    }
}

fn mapped_edge_selected(
    shape: &MappedSequenceShape<'_>,
    selection: &ReadSequenceMappedSelection,
) -> bool {
    matches!(selection.selected.as_slice(), [edge]
        if edge.parent == shape.parent_id
            && edge.child == shape.child_id
            && shape.inputs.iter().any(|input| input.name == edge.input_name))
}

fn mapped_get_query_edge_selected(
    shape: &MappedGetQueryShape<'_>,
    selection: &ReadSequenceMappedSelection,
) -> bool {
    matches!(selection.selected.as_slice(), [edge]
        if edge.parent == shape.parent_id
            && edge.child == shape.child_id
            && shape.inputs.iter().any(|input| input.name == edge.input_name))
}
