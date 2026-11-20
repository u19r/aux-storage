use storage_types::{
    StorageError, StorageResult, TransactPutRequest, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsRequest,
};

/// Saves a result after a job is cancelled or permanently fails.
/// The caller chooses the database writes. Storage checks the job token.
#[derive(Clone)]
pub struct JobTerminalProjection {
    request: TransactWriteItemsRequest,
}

impl TryFrom<TransactWriteItemsRequest> for JobTerminalProjection {
    type Error = StorageError;

    fn try_from(mut request: TransactWriteItemsRequest) -> StorageResult<Self> {
        let [member] = request.transact_items.as_mut_slice() else {
            return Err(StorageError::validation(
                "terminal projection requires one update",
            ));
        };
        if member.put.is_some() || member.delete.is_some() || member.condition_check.is_some() {
            return Err(StorageError::validation(
                "terminal projection requires only an update",
            ));
        }
        let Some(update) = &mut member.update else {
            return Err(StorageError::validation(
                "terminal projection requires an update",
            ));
        };
        if request
            .client_request_token
            .as_deref()
            .is_none_or(|value| value.is_empty())
        {
            return Err(StorageError::validation(
                "terminal projection requires a replay identity",
            ));
        }
        require_existing_target(update)?;
        Ok(Self { request })
    }
}

impl JobTerminalProjection {
    /// Saves a result, an index entry and a counter update in one transaction.
    /// Existing transaction checks validate keys, expressions, duplicate
    /// targets and size limits.
    pub fn try_with_index_and_counter(
        result_update: TransactUpdateRequest,
        index_put: TransactPutRequest,
        counter_update: TransactUpdateRequest,
        request_token: String,
    ) -> StorageResult<Self> {
        if result_update.table_name != index_put.table_name
            || result_update.table_name != counter_update.table_name
        {
            return Err(StorageError::validation(
                "job result, index, and counter writes must use the same table",
            ));
        }
        let mut job_writes = Self::try_from(TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                update: Some(result_update),
                ..Default::default()
            }],
            client_request_token: Some(request_token),
            ..Default::default()
        })?;
        job_writes.request.transact_items.extend([
            TransactWriteItem {
                put: Some(index_put),
                ..Default::default()
            },
            TransactWriteItem {
                update: Some(counter_update),
                ..Default::default()
            },
        ]);
        Ok(job_writes)
    }

    pub(crate) fn into_request(self) -> TransactWriteItemsRequest {
        self.request
    }
}

fn require_existing_target(update: &mut storage_types::TransactUpdateRequest) -> StorageResult<()> {
    let (key, _) = update.key.iter().next().ok_or_else(|| {
        StorageError::validation("terminal projection requires a destination key")
    })?;
    let names = update
        .expression_attribute_names
        .get_or_insert_with(Default::default);
    let mut alias = "#job_projection_key".to_string();
    while names.get(alias.as_str()).is_some() {
        alias.push('_');
    }
    names.insert(alias.clone(), key.to_string());
    let condition = update
        .condition_expression
        .take()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            StorageError::validation("terminal projection requires a destination condition")
        })?;
    update.condition_expression = Some(format!("({condition}) AND attribute_exists({alias})"));
    Ok(())
}
