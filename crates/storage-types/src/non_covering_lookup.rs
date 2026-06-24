use std::{collections::BTreeMap, fmt};

use crate::{AttributeMap, KeyAttributes};

#[derive(Debug, Clone, PartialEq)]
pub struct NonCoveringLookupCandidate {
    pub parent_index: usize,
    pub key: KeyAttributes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NonCoveringLookupPlan {
    pub fetches: Vec<NonCoveringLookupFetch>,
    pub parent_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NonCoveringLookupFetch {
    pub key: KeyAttributes,
    pub parent_indexes: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonCoveringLookupJoinMode {
    LeftOne,
    RequiredOne,
    Array,
    InnerOne,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NonCoveringLookupAttachment {
    Missing,
    Item(AttributeMap),
    Items(Vec<AttributeMap>),
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NonCoveringLookupError {
    CandidateLimitExceeded { actual: usize, limit: u32 },
    InvalidKey { message: String },
    FetchedItemCountMismatch { expected: usize, actual: usize },
    RequiredItemMissing { parent_index: usize },
    ParentIndexOverflow { parent_index: usize },
}

impl fmt::Display for NonCoveringLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateLimitExceeded { actual, limit } => write!(
                formatter,
                "non-covering lookup has {actual} candidate keys, exceeding the limit of {limit}"
            ),
            Self::InvalidKey { message } => {
                write!(formatter, "non-covering lookup key is invalid: {message}")
            }
            Self::FetchedItemCountMismatch { expected, actual } => write!(
                formatter,
                "non-covering lookup received {actual} fetched items for {expected} planned \
                 fetches"
            ),
            Self::RequiredItemMissing { parent_index } => write!(
                formatter,
                "non-covering lookup required item is missing for parent index {parent_index}"
            ),
            Self::ParentIndexOverflow { parent_index } => write!(
                formatter,
                "non-covering lookup parent index {parent_index} cannot be represented"
            ),
        }
    }
}

impl std::error::Error for NonCoveringLookupError {}

pub fn plan_non_covering_lookup(
    candidates: impl IntoIterator<Item = NonCoveringLookupCandidate>,
    candidate_limit: u32,
) -> Result<NonCoveringLookupPlan, NonCoveringLookupError> {
    let mut fetch_indexes_by_key = BTreeMap::<String, usize>::new();
    let mut fetches = Vec::<NonCoveringLookupFetch>::new();
    let mut parent_count = 0usize;
    let candidate_limit = candidate_limit as usize;
    let mut candidate_count = 0usize;

    for candidate in candidates {
        candidate_count += 1;
        if candidate_count > candidate_limit {
            return Err(NonCoveringLookupError::CandidateLimitExceeded {
                actual: candidate_count,
                limit: candidate_limit as u32,
            });
        }
        parent_count = parent_count.max(candidate.parent_index.checked_add(1).ok_or(
            NonCoveringLookupError::ParentIndexOverflow {
                parent_index: candidate.parent_index,
            },
        )?);

        let key_id = candidate.key.canonical_dynamo_json().map_err(|error| {
            NonCoveringLookupError::InvalidKey {
                message: error.to_string(),
            }
        })?;
        if let Some(fetch_index) = fetch_indexes_by_key.get(&key_id).copied() {
            fetches[fetch_index]
                .parent_indexes
                .push(candidate.parent_index);
            continue;
        }

        let fetch_index = fetches.len();
        fetch_indexes_by_key.insert(key_id, fetch_index);
        fetches.push(NonCoveringLookupFetch {
            key: candidate.key,
            parent_indexes: vec![candidate.parent_index],
        });
    }

    Ok(NonCoveringLookupPlan {
        fetches,
        parent_count,
    })
}

pub fn merge_non_covering_lookup_items(
    plan: &NonCoveringLookupPlan,
    fetched_items: Vec<Option<AttributeMap>>,
    join_mode: NonCoveringLookupJoinMode,
) -> Result<Vec<NonCoveringLookupAttachment>, NonCoveringLookupError> {
    if fetched_items.len() != plan.fetches.len() {
        return Err(NonCoveringLookupError::FetchedItemCountMismatch {
            expected: plan.fetches.len(),
            actual: fetched_items.len(),
        });
    }

    let mut attachments = initial_attachments(plan.parent_count, join_mode);
    for (fetch, item) in plan.fetches.iter().zip(fetched_items) {
        for parent_index in &fetch.parent_indexes {
            apply_fetched_item(
                &mut attachments[*parent_index],
                *parent_index,
                item.as_ref(),
                join_mode,
            )?;
        }
    }

    Ok(attachments)
}

fn initial_attachments(
    parent_count: usize,
    join_mode: NonCoveringLookupJoinMode,
) -> Vec<NonCoveringLookupAttachment> {
    (0..parent_count)
        .map(|_| match join_mode {
            NonCoveringLookupJoinMode::Array => NonCoveringLookupAttachment::Items(Vec::new()),
            NonCoveringLookupJoinMode::InnerOne => NonCoveringLookupAttachment::Dropped,
            NonCoveringLookupJoinMode::LeftOne | NonCoveringLookupJoinMode::RequiredOne => {
                NonCoveringLookupAttachment::Missing
            }
        })
        .collect()
}

fn apply_fetched_item(
    attachment: &mut NonCoveringLookupAttachment,
    parent_index: usize,
    item: Option<&AttributeMap>,
    join_mode: NonCoveringLookupJoinMode,
) -> Result<(), NonCoveringLookupError> {
    match (join_mode, item) {
        (NonCoveringLookupJoinMode::RequiredOne, None) => {
            Err(NonCoveringLookupError::RequiredItemMissing { parent_index })
        }
        (
            NonCoveringLookupJoinMode::LeftOne | NonCoveringLookupJoinMode::RequiredOne,
            Some(item),
        )
        | (NonCoveringLookupJoinMode::InnerOne, Some(item)) => {
            *attachment = NonCoveringLookupAttachment::Item(item.clone());
            Ok(())
        }
        (NonCoveringLookupJoinMode::Array, Some(item)) => {
            if let NonCoveringLookupAttachment::Items(items) = attachment {
                items.push(item.clone());
            }
            Ok(())
        }
        (
            NonCoveringLookupJoinMode::LeftOne
            | NonCoveringLookupJoinMode::Array
            | NonCoveringLookupJoinMode::InnerOne,
            None,
        ) => Ok(()),
    }
}
