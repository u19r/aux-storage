use crate::{common::*, constants::*, imports::*, table_atomicity::*};

impl TableAtomicityWorkload {
    pub(crate) fn owned_key(&self, index: u64) -> String {
        format!("client-{}/key-{}", self.client_id, index % self.key_count)
    }

    pub(crate) fn shared_key(&self, index: u64) -> String {
        format!("shared/key-{}", index % self.shared_key_count.max(1))
    }

    pub(crate) fn operation_key(&mut self, key_roll: u64, shared_roll: u64) -> String {
        if self.shared_key_count > 0 && shared_roll % 100 < self.shared_operation_percent {
            self.shared_operation_count += 1;
            self.shared_key(key_roll)
        } else {
            self.owned_key(key_roll)
        }
    }

    pub(crate) fn transact_side_effect_key(
        &self,
        key: &str,
        client_id: i32,
        sequence: u64,
    ) -> String {
        format!("{key}/txn-side-effect/client-{client_id}/{sequence}")
    }

    pub(crate) fn value(&self, sequence: u64) -> String {
        format!("value-client-{}-{sequence}", self.client_id)
    }

    pub(crate) fn item(&self, key: &str, value: &str) -> storage_types::AttributeMap {
        let mut item = storage_types::AttributeMap::from_iter([
            ("pk".to_string(), AttributeValue::S(key.to_string())),
            ("payload".to_string(), AttributeValue::S(value.to_string())),
        ]);
        if let Some((category, score)) = self.gsi_projection(key, value) {
            item.insert(GSI_CATEGORY_ATTR.to_string(), AttributeValue::S(category));
            item.insert(GSI_SCORE_ATTR.to_string(), AttributeValue::N(score));
        }
        item
    }

    pub(crate) fn owned_key_index(&self, key: &str) -> Option<u64> {
        key.strip_prefix(&format!("client-{}/key-", self.client_id))?
            .parse()
            .ok()
    }

    pub(crate) fn gsi_projection(&self, key: &str, value: &str) -> Option<(String, String)> {
        let key_index = self.owned_key_index(key)?;
        Some((
            format!("client-{}/category-{}", self.client_id, key_index % 4),
            gsi_score(key, value),
        ))
    }

    pub(crate) fn gsi_entry_from_item(
        &self,
        item: &impl storage_types::AttributeValueLookup,
    ) -> Result<GsiEntry, String> {
        let key = string_attr(item, "pk")?;
        let value = string_attr(item, "payload")?;
        let sort = number_attr(item, GSI_SCORE_ATTR)?;
        Ok(GsiEntry::new(key, sort, value))
    }

    pub(crate) fn key_attributes(&self, key: &str) -> KeyAttributes {
        KeyAttributes::from([("pk".to_string(), AttributeValue::S(key.to_string()))])
    }

    pub(crate) fn payload_from_item(
        &self,
        key: &str,
        item: Option<storage_types::AttributeMap>,
    ) -> Result<Option<String>, String> {
        let Some(item) = item else {
            return Ok(None);
        };
        match item.get("payload") {
            Some(AttributeValue::S(value)) => Ok(Some(value.clone())),
            Some(other) => Err(format!("key {key} payload has non-string value: {other:?}")),
            None => Err(format!("key {key} is present without payload attribute")),
        }
    }
}
