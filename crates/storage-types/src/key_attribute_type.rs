use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum KeyAttributeType {
    S, // String
    N, // Number
    B, // Binary
}
