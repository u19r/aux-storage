use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize, ToSchema)]
pub struct IndexName(String);

impl IndexName {
    pub fn new(name: &(impl ToString + ?Sized)) -> Self {
        IndexName(name.to_string())
    }
    #[must_use]
    pub fn sanitized_name(&self) -> String {
        self.0.replace(['\'', '"', ';'], "")
    }
}
impl std::fmt::Display for IndexName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl AsRef<str> for IndexName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&IndexName> for String {
    fn from(index_name: &IndexName) -> Self {
        index_name.0.clone()
    }
}
