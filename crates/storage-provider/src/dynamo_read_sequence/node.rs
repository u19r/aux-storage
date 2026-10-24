//! Bind and execute one graph node on the caller's native read attempt.

use storage_types::{
    ExclusiveStartKey, InvocationBindings, KeyAttributes, ReadSequenceConsistency,
    ReadSequenceInvocationPayload, ReadSequenceInvocationResult, ReadSequenceNode,
    ReadSequenceNodeOperation, ReadSequenceNodeResult, ReadSequenceValidationError, StorageError,
    StorageResult, bind_operation,
};

use crate::{
    CachePublication, ReadSequenceStepReader, constants::READ_SEQUENCE_DEFAULT_QUERY_ITEMS,
};

pub(super) struct NodeOutcome {
    pub result: ReadSequenceNodeResult,
    pub item_cursors: Vec<Option<Vec<KeyAttributes>>>,
    pub continuation: Option<(u32, Option<ExclusiveStartKey>)>,
    pub publications: Vec<CachePublication>,
    pub item_count: u32,
}

pub(super) struct NodeExecution<'a> {
    pub node: &'a ReadSequenceNode,
    pub bindings: Vec<InvocationBindings>,
    pub consistency: ReadSequenceConsistency,
    pub resume_ordinal: Option<u32>,
    pub cursor: Option<ExclusiveStartKey>,
    pub item_limit: u32,
    pub fanout_limit: u32,
}

impl NodeExecution<'_> {
    pub fn reservation(&self) -> StorageResult<u32> {
        let count = self.bindings.len();
        if count > self.fanout_limit as usize {
            return Err(ReadSequenceValidationError::FanoutLimitExceeded {
                actual: u32::try_from(count).unwrap_or(u32::MAX),
                limit: self.fanout_limit,
            }
            .into());
        }
        let start = self.resume_ordinal()?;
        if start == count {
            return Ok(0);
        }
        let per_invocation = match &self.node.operation {
            ReadSequenceNodeOperation::Get(_) => 1,
            ReadSequenceNodeOperation::BatchGet(request) => request
                .request_items
                .values()
                .map(|keys| u32::try_from(keys.keys.len()).unwrap_or(u32::MAX))
                .try_fold(0u32, u32::checked_add)
                .unwrap_or(u32::MAX),
            ReadSequenceNodeOperation::Query(request) => request
                .limit
                .unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS)
                .min(self.item_limit),
        };
        // Query can stop at the node budget and resume at the next invocation;
        // Get and BatchGet must reserve their complete fixed key set.
        let reservation = if matches!(self.node.operation, ReadSequenceNodeOperation::Query(_)) {
            per_invocation
        } else {
            u32::try_from(count.saturating_sub(start))
                .ok()
                .and_then(|count| count.checked_mul(per_invocation))
                .unwrap_or(u32::MAX)
        };
        if reservation > self.item_limit {
            return Err(ReadSequenceValidationError::FanoutLimitExceeded {
                actual: reservation,
                limit: self.item_limit,
            }
            .into());
        }
        Ok(reservation)
    }

    fn resume_ordinal(&self) -> StorageResult<usize> {
        let Some(ordinal) = self.resume_ordinal else {
            return Ok(0);
        };
        if !matches!(self.node.operation, ReadSequenceNodeOperation::Query(_))
            || ordinal as usize >= self.bindings.len()
        {
            return Err(ReadSequenceValidationError::StaleToken.into());
        }
        Ok(ordinal as usize)
    }

    pub async fn execute<R: ReadSequenceStepReader>(
        self,
        reader: &mut R,
    ) -> StorageResult<NodeOutcome> {
        let start = self.resume_ordinal()?;
        if self.bindings.len() > 1
            && matches!(self.node.operation, ReadSequenceNodeOperation::Get(_))
        {
            return self.execute_get_fanout(reader).await;
        }
        let mut invocations = Vec::with_capacity(self.bindings.len().saturating_sub(start));
        let mut item_cursors = Vec::with_capacity(invocations.capacity());
        let mut publications = Vec::new();
        let mut item_count = 0u32;
        let mut continuation = None;
        let invocation_count = self.bindings.len();
        for (ordinal, binding) in self.bindings.into_iter().enumerate().skip(start) {
            let mut operation = bind_operation(&self.node.operation, binding.resolved_inputs())?;
            if let ReadSequenceNodeOperation::Query(request) = &mut operation {
                let remaining = self.item_limit.saturating_sub(item_count);
                if remaining == 0 {
                    continuation = Some((ordinal as u32, None));
                    break;
                }
                request.limit = Some(
                    request
                        .limit
                        .unwrap_or(READ_SEQUENCE_DEFAULT_QUERY_ITEMS)
                        .min(remaining),
                );
                if self.resume_ordinal == Some(ordinal as u32)
                    && let Some(cursor) = &self.cursor
                {
                    request.exclusive_start_key = Some(cursor.clone());
                }
            }
            let step = reader.read_step(operation, self.consistency).await?;
            item_count = item_count
                .checked_add(step.payload.item_count())
                .ok_or_else(|| {
                    StorageError::from(ReadSequenceValidationError::TotalReadLimitExceeded {
                        actual: u32::MAX,
                        limit: self.item_limit,
                    })
                })?;
            if let ReadSequenceInvocationPayload::Query(response) = &step.payload
                && let Some(cursor) = &response.last_evaluated_key
            {
                continuation = Some((ordinal as u32, Some(ExclusiveStartKey::Key(cursor.clone()))));
            }
            item_cursors.push(step.item_cursors);
            publications.extend(step.publications);
            invocations.push(ReadSequenceInvocationResult {
                ordinal: ordinal as u32,
                input_refs: binding.into_input_refs(),
                result: step.payload,
            });
            if continuation.is_some() {
                break;
            }
            if matches!(self.node.operation, ReadSequenceNodeOperation::Query(_))
                && item_count >= self.item_limit
                && ordinal + 1 < invocation_count
            {
                continuation = Some(((ordinal + 1) as u32, None));
                break;
            }
        }
        Ok(NodeOutcome {
            result: ReadSequenceNodeResult {
                name: self.node.name.clone(),
                invocations,
            },
            item_cursors,
            continuation,
            publications,
            item_count,
        })
    }
}
