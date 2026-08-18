use storage_provider::{ReadSequencePhysicalDescriptor, ReadSequencePhysicalOperation};
use storage_types::ReadSequenceNodeId;

use crate::storage_ops::provider_impl::read_sequence_mapped::{
    MappedGetQueryShape, MappedSequenceShape,
};

pub(super) fn mapped_descriptors(
    shape: &MappedSequenceShape<'_>,
    tuple_prefix_safe: bool,
) -> [(ReadSequenceNodeId, ReadSequencePhysicalDescriptor); 2] {
    [
        (
            shape.parent_id,
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::PrefixRange,
                tuple_schema: true,
                tuple_prefix_safe,
                selector_physical: true,
                unsupported_projection: shape.parent_query.select.as_deref() == Some("COUNT"),
                secondary_limit_safe: shape.parent_query.limit.is_none(),
                read_your_writes: shape.parent_query.consistent_read == Some(true),
                ..Default::default()
            },
        ),
        (
            shape.child_id,
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::Point,
                tuple_schema: true,
                tuple_prefix_safe,
                selector_physical: true,
                read_your_writes: shape.child_get.consistent_read == Some(true),
                ..Default::default()
            },
        ),
    ]
}

pub(super) fn mapped_get_query_descriptors(
    shape: &MappedGetQueryShape<'_>,
    tuple_prefix_safe: bool,
) -> [(ReadSequenceNodeId, ReadSequencePhysicalDescriptor); 2] {
    [
        (
            shape.parent_id,
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::Point,
                tuple_schema: true,
                tuple_prefix_safe,
                selector_physical: true,
                read_your_writes: shape.parent_get.consistent_read == Some(true),
                ..Default::default()
            },
        ),
        (
            shape.child_id,
            ReadSequencePhysicalDescriptor {
                operation: ReadSequencePhysicalOperation::PrefixRange,
                tuple_schema: true,
                tuple_prefix_safe,
                selector_physical: true,
                unsupported_projection: shape.child_query.select.as_deref() == Some("COUNT"),
                secondary_limit_safe: shape.child_query.limit.is_none(),
                continuation_safe: shape.child_query.exclusive_start_key.is_none()
                    && shape.child_query.scan_index_forward != Some(false),
                read_your_writes: shape.child_query.consistent_read == Some(true),
                ..Default::default()
            },
        ),
    ]
}
