//! Idempotent creation retains the actual protected native incarnation.

use storage_types::{ReadProtection, StorageResult};

use crate::{
    Queue, QueueAttempt, QueueCatalogRecord, QueueError, QueueResult,
    queue_redrive::{RedrivePolicy, find_redrive_target},
};

pub enum QueueCreation {
    Created(QueueCatalogRecord),
    Existing(QueueCatalogRecord),
}

impl From<QueueCreation> for Queue {
    fn from(value: QueueCreation) -> Self {
        match value {
            QueueCreation::Created(record) | QueueCreation::Existing(record) => record.queue,
        }
    }
}

/// Idempotency is decided first: an existing queue with the same effective
/// attributes is returned even when the dead-letter target of its policy was
/// deleted later. Only a new queue checks that `dead_letter_target` exists
/// (design D10).
pub async fn create_queue_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    proposed: QueueCatalogRecord,
    dead_letter_target: Option<&RedrivePolicy>,
) -> StorageResult<QueueResult<QueueCreation>> {
    if let Some(existing) = attempt
        .read_queue_by_name(&proposed.queue.queue_name, ReadProtection::Conflict)
        .await?
    {
        return Ok(match first_difference(&existing.queue, &proposed.queue) {
            None => Ok(QueueCreation::Existing(existing)),
            Some(difference) => Err(queue_exists(&difference)),
        });
    }
    if let Some(target) = dead_letter_target
        && let Err(error) = find_redrive_target(attempt, target, &proposed.queue.queue_name).await?
    {
        return Ok(Err(error));
    }
    attempt.write_queue_record(proposed.clone(), None).await?;
    attempt.stage_queue_wake(crate::QueueWakeSlot::of_queue(&proposed.queue.queue_name))?;
    Ok(Ok(QueueCreation::Created(proposed)))
}

/// What the request names and the queue does not hold. Only requested
/// attributes and tags count: an omitted one never conflicts, and a requested
/// default equals a stored default. A `Policy` is compared without `Sid`
/// (suite scripts). Attributes are compared by name order.
fn first_difference(existing: &Queue, requested: &Queue) -> Option<String> {
    let mut names: Vec<&String> = requested.attributes.keys().collect();
    names.sort();
    for name in names {
        let wanted = requested.attributes[name].as_str();
        let held = existing
            .attributes
            .get(name)
            .map(String::as_str)
            .or_else(|| {
                crate::constants::QUEUE_ATTRIBUTE_DEFAULTS
                    .iter()
                    .find_map(|(default, value)| (default == name).then_some(*value))
            });
        let same = if name == "Policy" {
            held.is_some_and(|held| policies_match(held, wanted))
        } else {
            held == Some(wanted)
        };
        if !same {
            return Some(format!("attribute {name}"));
        }
    }
    // The AWS text ends in a space after `tags`.
    (!requested.tags.is_empty() && requested.tags != existing.tags).then(|| "tags ".to_owned())
}

fn remove_sid(statement: &mut serde_json::Value) {
    if let Some(object) = statement.as_object_mut() {
        object.remove("Sid");
    }
}

fn policies_match(held: &str, wanted: &str) -> bool {
    let strip = |text: &str| {
        let mut value = serde_json::from_str::<serde_json::Value>(text).ok()?;
        match value.get_mut("Statement") {
            Some(serde_json::Value::Array(items)) => items.iter_mut().for_each(remove_sid),
            Some(statement) => remove_sid(statement),
            None => {}
        }
        Some(value)
    };
    match (strip(held), strip(wanted)) {
        (Some(held), Some(wanted)) => held == wanted,
        _ => held == wanted,
    }
}

fn queue_exists(difference: &str) -> QueueError {
    QueueError::validation_with_detail(
        crate::QueueValidationKind::QueueAlreadyExists,
        format!("A queue already exists with the same name and a different value for {difference}"),
    )
}
