use std::sync::Arc;

use storage_types::VecMap;

use crate::admission::{
    admission_types::{AdmissionConfig, AdmissionConfigError},
    constants::{FIXED_INGRESS_MAXIMUM, FIXED_INGRESS_MINIMUM},
    controller::AdmissionController,
};

/// Immutable registry of one controller per configured physical connection.
#[derive(Debug, Clone)]
pub struct AdmissionRegistry {
    controllers: Arc<VecMap<String, AdmissionController>>,
    default_controller: AdmissionController,
}

impl AdmissionRegistry {
    pub fn new(
        default_connection_id: impl Into<Arc<str>>,
        connection_ids: impl IntoIterator<Item = String>,
        config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        let connection_ids = connection_ids
            .into_iter()
            .map(|connection_id| (connection_id, config));
        Self::new_with_connection_configs(default_connection_id, connection_ids, config)
    }

    /// Build one controller per connection; use the fallback only when the
    /// default connection has no explicit configuration.
    pub fn new_with_connection_configs(
        default_connection_id: impl Into<Arc<str>>,
        connection_configs: impl IntoIterator<Item = (String, AdmissionConfig)>,
        fallback_config: AdmissionConfig,
    ) -> Result<Self, AdmissionConfigError> {
        let default_connection_id = default_connection_id.into();
        let mut controllers = VecMap::new();
        for (connection_id, config) in connection_configs {
            controllers.insert(
                connection_id.clone(),
                AdmissionController::new(connection_id, config)?,
            );
        }
        let default_controller = match controllers.get(default_connection_id.as_ref()) {
            Some(controller) => {
                AdmissionController::validate_config(fallback_config)?;
                controller.clone()
            }
            None => {
                let controller =
                    AdmissionController::new(Arc::clone(&default_connection_id), fallback_config)?;
                controllers.insert(default_connection_id.to_string(), controller.clone());
                controller
            }
        };
        Ok(Self {
            controllers: Arc::new(controllers),
            default_controller,
        })
    }

    pub fn for_connection(&self, connection_id: &str) -> Option<&AdmissionController> {
        self.controllers.get(connection_id)
    }

    #[must_use]
    pub fn default_controller(&self) -> &AdmissionController {
        &self.default_controller
    }

    pub fn connection_ids(&self) -> impl Iterator<Item = &str> {
        self.controllers.keys().map(String::as_str)
    }

    #[must_use]
    pub fn fixed_ingress_limit(&self) -> usize {
        let configured = self
            .controllers
            .values()
            .map(|controller| {
                let config = controller.config();
                config
                    .maximum_concurrency
                    .saturating_add(config.queue_capacity)
            })
            .fold(0usize, usize::saturating_add);
        configured.clamp(FIXED_INGRESS_MINIMUM, FIXED_INGRESS_MAXIMUM)
    }
}
