use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StreamUserIdentity {
    #[serde(rename = "type")]
    pub user_type: String,
    pub principal_id: String,
}

impl StreamUserIdentity {
    #[must_use]
    pub fn service_ttl() -> Self {
        Self {
            user_type: "Service".to_string(),
            principal_id: "dynamodb.amazonaws.com".to_string(),
        }
    }
}
