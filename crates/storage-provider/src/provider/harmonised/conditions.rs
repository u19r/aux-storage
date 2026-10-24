use storage_condition::{Condition, evaluate_wire_condition, parse_condition_expression_opt};
use storage_types::{StorageResult, WireItem};

/// Parsed but unevaluated condition. Providers fetch the old item and hand
/// it over; they never parse or evaluate. Constructed during `prepare_*`,
/// resolved inside the transaction via [`UncheckedCondition::with_old_item`].
#[derive(Debug, Clone)]
pub struct UncheckedCondition {
    parsed: Condition,
}

impl UncheckedCondition {
    pub fn parse(
        condition_expression: Option<String>,
        expression_attribute_names: Option<storage_types::VecMap<String, String>>,
        expression_attribute_values: Option<storage_types::AttributeMap>,
    ) -> StorageResult<Option<Self>> {
        parse_condition_expression_opt(
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        )
        .map(|parsed| parsed.map(|parsed| Self { parsed }))
    }

    #[must_use]
    pub fn with_old_item(self, old: Option<WireItem>) -> WithOldItem {
        WithOldItem {
            parsed: self.parsed,
            old: old.map(Box::new),
        }
    }

    /// Evaluate against an image that is already owned by the finish stage.
    ///
    /// Conditions are often followed by stream, TTL, or return-value work
    /// that still needs the same image.  Borrowing here keeps the successful
    /// path from cloning and re-boxing that image.  Callers clone only after a
    /// failed condition when the public error may need the old item.
    pub fn check_borrowed(&self, old: Option<&WireItem>) -> StorageResult<bool> {
        evaluate_wire_condition(old, &self.parsed)
    }

    /// Transfer the parsed predicate to a native lowering that retains the
    /// shared request preparation. The native backend still evaluates the
    /// predicate against its attempt-local observation; this method only
    /// avoids parsing the same expression a second time.
    #[must_use]
    pub fn into_condition(self) -> Condition {
        self.parsed
    }
}

/// Condition paired with the old item fetched inside the transaction.
/// [`WithOldItem::check`] is the sole evaluation site in the codebase.
/// Callers pass key-materialized images: stored rows predate key
/// materialization, so `finish_with_old` merges request keys in first.
pub struct WithOldItem {
    parsed: Condition,
    old: Option<Box<WireItem>>,
}

impl WithOldItem {
    pub fn check(self) -> StorageResult<CheckedCondition> {
        let passed = evaluate_wire_condition(self.old.as_deref(), &self.parsed)?;
        if passed {
            Ok(CheckedCondition::Pass(ProceedToken {}))
        } else {
            Ok(CheckedCondition::Fail(ConditionFailReceipt {
                old_item: self.old,
            }))
        }
    }
}

/// Outcome of condition evaluation.
pub enum CheckedCondition {
    Pass(ProceedToken),
    Fail(ConditionFailReceipt),
}

/// Proof a condition passed. Threaded through `finish_with_old` so a write
/// cannot proceed without evaluation.
pub struct ProceedToken {}

/// Evidence a condition failed, carrying the old image for
/// `ReturnValuesOnConditionCheckFailure` mapping.
#[derive(Debug)]
pub struct ConditionFailReceipt {
    pub old_item: Option<Box<WireItem>>,
}
