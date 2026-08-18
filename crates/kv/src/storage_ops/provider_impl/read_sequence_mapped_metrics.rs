use storage_provider::{
    ReadSequenceMappedRejectionReason, ReadSequenceMappedSelection, ReadSequenceUnsupportedReason,
};

pub(super) fn record_mapped_selection(selection: &ReadSequenceMappedSelection) {
    ::metrics::counter!("storage.read_sequence.mapped.edge.total", "state" => "candidate")
        .increment(selection.assessments.len() as u64);
    ::metrics::counter!("storage.read_sequence.mapped.edge.total", "state" => "selected")
        .increment(selection.selected.len() as u64);
    for assessment in &selection.assessments {
        let state = assessment
            .reason
            .map(mapped_rejection_label)
            .unwrap_or("eligible");
        ::metrics::counter!("storage.read_sequence.mapped.edge.assessment.total", "reason" => state)
            .increment(1);
    }
}

pub(super) fn mapped_selection_reason(
    selection: &ReadSequenceMappedSelection,
) -> ReadSequenceUnsupportedReason {
    let reason = selection
        .assessments
        .iter()
        .find_map(|assessment| assessment.reason);
    match reason {
        Some(
            ReadSequenceMappedRejectionReason::NonTupleSource
            | ReadSequenceMappedRejectionReason::NonTupleTarget
            | ReadSequenceMappedRejectionReason::TupleTypeMismatch
            | ReadSequenceMappedRejectionReason::SelectorNotPhysical
            | ReadSequenceMappedRejectionReason::SourceNotRange
            | ReadSequenceMappedRejectionReason::ApiVersion
            | ReadSequenceMappedRejectionReason::Disabled
            | ReadSequenceMappedRejectionReason::NotFoundationdb,
        ) => ReadSequenceUnsupportedReason::PhysicalLayout,
        Some(
            ReadSequenceMappedRejectionReason::ProjectionSemantics
            | ReadSequenceMappedRejectionReason::SecondaryLimit
            | ReadSequenceMappedRejectionReason::Continuation
            | ReadSequenceMappedRejectionReason::Consistency
            | ReadSequenceMappedRejectionReason::ReadYourWrites
            | ReadSequenceMappedRejectionReason::ChildOperation
            | ReadSequenceMappedRejectionReason::MultipleDataParents,
        )
        | None => ReadSequenceUnsupportedReason::OperationShape,
        Some(
            ReadSequenceMappedRejectionReason::EstimatedMissCost
            | ReadSequenceMappedRejectionReason::NoLatencyBenefit,
        ) => ReadSequenceUnsupportedReason::BackendCapability,
    }
}

const fn mapped_rejection_label(reason: ReadSequenceMappedRejectionReason) -> &'static str {
    match reason {
        ReadSequenceMappedRejectionReason::NotFoundationdb => "not_foundationdb",
        ReadSequenceMappedRejectionReason::ApiVersion => "api_version",
        ReadSequenceMappedRejectionReason::Disabled => "disabled",
        ReadSequenceMappedRejectionReason::SourceNotRange => "source_not_range",
        ReadSequenceMappedRejectionReason::ChildOperation => "child_operation",
        ReadSequenceMappedRejectionReason::MultipleDataParents => "multiple_data_parents",
        ReadSequenceMappedRejectionReason::SelectorNotPhysical => "selector_not_physical",
        ReadSequenceMappedRejectionReason::NonTupleSource => "non_tuple_source",
        ReadSequenceMappedRejectionReason::NonTupleTarget => "non_tuple_target",
        ReadSequenceMappedRejectionReason::TupleTypeMismatch => "tuple_type_mismatch",
        ReadSequenceMappedRejectionReason::ProjectionSemantics => "projection_semantics",
        ReadSequenceMappedRejectionReason::SecondaryLimit => "secondary_limit",
        ReadSequenceMappedRejectionReason::Continuation => "continuation",
        ReadSequenceMappedRejectionReason::Consistency => "consistency",
        ReadSequenceMappedRejectionReason::ReadYourWrites => "read_your_writes",
        ReadSequenceMappedRejectionReason::EstimatedMissCost => "estimated_miss_cost",
        ReadSequenceMappedRejectionReason::NoLatencyBenefit => "no_latency_benefit",
    }
}
