use sha2::{Digest, Sha256};
use storage_types::{
    AttributeMap, StorageError, StorageResult, TransactConditionCheckRequest, TransactEncodeItem,
    TransactWriteItemsEncodeRequest, VecMap, WireItem,
};

use super::DatabaseManager;

struct TransactionConditions<'a> {
    expression: &'a mut Option<String>,
    names: &'a mut Option<VecMap<String, String>>,
    values: &'a mut Option<AttributeMap>,
}

impl DatabaseManager {
    /// Commit an effect only while both its native lease and domain guard hold.
    pub async fn transact_write_authorized_effect(
        &self,
        mut request: TransactWriteItemsEncodeRequest,
        authorization: &super::DomainEffectAuthorization,
    ) -> StorageResult<()> {
        authorization.authorize_items(&mut request.transact_items)?;
        let Some(authority) = authorization.job_authority() else {
            self.transact_write_items_encode(request).await?;
            return Ok(());
        };
        let mut request: storage_types::TransactWriteItemsRequest = request.try_into()?;
        let identity = serde_json::to_vec(&request.transact_items)
            .map_err(|_| StorageError::internal("job effect identity encoding failed"))?;
        request.client_request_token = Some(hex::encode(&Sha256::digest(identity)[..16]));
        self.transact_write_job_effect(request, authority.clone())
            .await?;
        Ok(())
    }

    /// Merge guards into writes to the guarded key; transactions cannot contain
    /// a separate condition check and write for that same key.
    pub fn guard_job_effect_items(
        items: &mut Vec<TransactEncodeItem>,
        guard: &TransactConditionCheckRequest,
    ) -> StorageResult<()> {
        for item in items.iter_mut() {
            if let Some(conditions) = find_guarded_write(item, guard)? {
                merge_guard_conditions(conditions, guard)?;
                return Ok(());
            }
            if let Some(check) = &mut item.condition_check
                && check.table_name == guard.table_name
                && check.key == guard.key
            {
                let mut expression = Some(check.condition_expression.clone());
                merge_guard_conditions(
                    TransactionConditions {
                        expression: &mut expression,
                        names: &mut check.expression_attribute_names,
                        values: &mut check.expression_attribute_values,
                    },
                    guard,
                )?;
                check.condition_expression = expression
                    .ok_or_else(|| StorageError::internal("merged guard expression is missing"))?;
                return Ok(());
            }
        }
        items.push(TransactEncodeItem {
            condition_check: Some(guard.clone()),
            ..Default::default()
        });
        Ok(())
    }
}

fn find_guarded_write<'a>(
    item: &'a mut TransactEncodeItem,
    guard: &TransactConditionCheckRequest,
) -> StorageResult<Option<TransactionConditions<'a>>> {
    if let Some(put) = &mut item.put
        && put.table_name == guard.table_name
        && matches_guard_key(put.item.item(), guard)?
    {
        return Ok(Some(TransactionConditions {
            expression: &mut put.condition_expression,
            names: &mut put.expression_attribute_names,
            values: &mut put.expression_attribute_values,
        }));
    }
    if let Some(update) = &mut item.update
        && update.table_name == guard.table_name
        && update.key == guard.key
    {
        return Ok(Some(TransactionConditions {
            expression: &mut update.condition_expression,
            names: &mut update.expression_attribute_names,
            values: &mut update.expression_attribute_values,
        }));
    }
    if let Some(delete) = &mut item.delete
        && delete.table_name == guard.table_name
        && delete.key == guard.key
    {
        return Ok(Some(TransactionConditions {
            expression: &mut delete.condition_expression,
            names: &mut delete.expression_attribute_names,
            values: &mut delete.expression_attribute_values,
        }));
    }
    Ok(None)
}

fn matches_guard_key(
    item: &WireItem,
    guard: &TransactConditionCheckRequest,
) -> StorageResult<bool> {
    for (name, value) in guard.key.iter() {
        if item.attribute_value(name)?.as_ref() != Some(value) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn merge_guard_conditions(
    conditions: TransactionConditions<'_>,
    guard: &TransactConditionCheckRequest,
) -> StorageResult<()> {
    let names = conditions.names.get_or_insert_with(Default::default);
    for (name, value) in guard.expression_attribute_names.clone().unwrap_or_default() {
        if names.get(&name).is_some_and(|existing| existing != &value) {
            return Err(StorageError::internal("job effect guard name collision"));
        }
        names.insert(name, value);
    }
    if names.is_empty() {
        *conditions.names = None;
    }
    let values = conditions.values.get_or_insert_with(Default::default);
    for (name, value) in guard
        .expression_attribute_values
        .clone()
        .unwrap_or_default()
        .iter()
    {
        if values.get(name).is_some_and(|existing| existing != value) {
            return Err(StorageError::internal("job effect guard value collision"));
        }
        values.insert(name, value.clone());
    }
    if values.is_empty() {
        *conditions.values = None;
    }
    *conditions.expression = Some(match conditions.expression.take() {
        Some(existing) => format!("({existing}) AND ({})", guard.condition_expression),
        None => guard.condition_expression.clone(),
    });
    Ok(())
}
