use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Argument error: {message}")]
    Argument { message: String },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Interpolation error at {path}: {message}")]
    Interpolation { path: String, message: String },
    #[error("Override parse error: {message}")]
    OverrideParse { message: String },
    #[error("Schema error: {message}")]
    Schema { message: String },
    #[error("Validation error: {message}")]
    Validation { message: String },
}

impl ConfigError {
    pub fn argument(message: impl Into<String>) -> Self {
        Self::Argument {
            message: message.into(),
        }
    }

    pub fn override_parse(message: impl Into<String>) -> Self {
        Self::OverrideParse {
            message: message.into(),
        }
    }

    pub fn interpolation(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Interpolation {
            path: path.into(),
            message: message.into(),
        }
    }

    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema {
            message: message.into(),
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }
}
