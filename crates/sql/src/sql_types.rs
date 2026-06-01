use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlParam {
    pub value: SqlValue,
}

impl SqlParam {
    #[must_use]
    pub fn null() -> Self {
        Self {
            value: SqlValue::Null,
        }
    }

    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            value: SqlValue::Text(value.into()),
        }
    }

    #[must_use]
    pub fn integer(value: i64) -> Self {
        Self {
            value: SqlValue::Integer(value),
        }
    }

    #[must_use]
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self {
            value: SqlValue::Bytes(value.into()),
        }
    }

    #[must_use]
    pub fn boolean(value: bool) -> Self {
        Self {
            value: SqlValue::Bool(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlRow {
    pub columns: HashMap<String, SqlValue>,
}

impl SqlRow {
    #[must_use]
    pub fn new(columns: HashMap<String, SqlValue>) -> Self {
        Self { columns }
    }

    #[must_use]
    pub fn get(&self, column: &str) -> Option<&SqlValue> {
        self.columns.get(column)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    #[must_use]
    pub fn new(raw: String) -> Self {
        // PostgreSQL identifiers are max 63 bytes, so truncate deterministically.
        const MAX_BYTES: usize = 63;
        if raw.len() <= MAX_BYTES {
            return Self(raw);
        }

        let mut truncated = String::with_capacity(MAX_BYTES);
        let mut bytes = 0usize;
        for ch in raw.chars() {
            let ch_len = ch.len_utf8();
            if bytes + ch_len > MAX_BYTES {
                break;
            }
            truncated.push(ch);
            bytes += ch_len;
        }
        Self(truncated)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
