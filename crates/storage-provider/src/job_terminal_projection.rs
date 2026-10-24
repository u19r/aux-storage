use storage_types::{StorageError, StorageResult, TransactWriteItemsRequest};

/// A terminal lifecycle witness may publish one conditional destination update.
/// It cannot create or delete rows, or authorize a batch of execution effects.
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
