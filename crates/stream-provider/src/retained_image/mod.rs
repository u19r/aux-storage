//! Exact immutable image preparation and retained-byte reconstruction.
//! Native address selection and capacity ownership belong to the caller.

mod address;
mod batch;
mod original_batch;
mod prepared_bodies;
pub use original_batch::RetainedOriginalBatch;
mod capacity;
mod causal_body;
mod charge_account;
pub use charge_account::ImageChargeAccount;
mod charge_custody;
mod custody_preparation;
mod custody_read;
mod custody_record;
mod custody_write;
mod owner_observation;
mod owner_record;
mod ownership_capacity;
mod ownership_graph;
mod ownership_observation;
mod stream_ownership;
pub use ownership_capacity::ownership_graph_length_bound;
mod ownership_batch;
mod ownership_changes;
mod ownership_inventory;
mod ownership_preparation;
mod ownership_release;
pub use ownership_changes::{RetainedOwnershipAttempt, RetainedOwnershipChanges};
mod owner_transition;
pub use owner_transition::RetainedOwnerTransition;
mod owner_retirement;
mod primary_owner;
pub use charge_custody::ImageChargeCustody;
pub use owner_record::{RetainedOwnerKind, RetainedOwnerRecord};
pub use owner_retirement::RetainedOwnerRetirement;
pub use primary_owner::PrimaryImageOwner;

pub use crate::constants::GSI_IMAGE_LOCATOR_NAMESPACE;
mod causal_manifest;
mod decoder;
mod error;
mod hydration;
mod hydration_wave;
mod manifest;
mod manifest_split;
pub use causal_manifest::RetainedCausalBody;
mod causal_hydration;
mod image_read_work;
mod json_output;
mod json_projection;
#[cfg(test)]
mod json_projection_tests;
mod manifest_read;
mod manifest_write;
mod numeric_token;
mod page_budget;
mod read_work;
pub use page_budget::RetainedImagePageBudget;
mod descriptor_codec;
mod item;
mod references;
mod stored_source;
pub use stored_source::{load_stored_image, require_materialized_image};
mod projection;
mod projection_codec;
mod projection_preparation;
pub use projection::{AcceptedProjectionPlan, RetainedProjectionProof, RetainedProjectionRecord};
pub(crate) mod pointer;
mod stream_hydration;
pub(crate) mod stream_image;
pub mod stream_row;
mod system_history_hydration;
pub use item::{AcceptedImagePlan, RetainedItemRecord};
mod observation;
mod payload;
mod payload_read;
#[cfg(test)]
mod payload_tests;
mod payload_write;
mod preparation;
mod read;

pub use hydration::ImageHydrationBatch;
pub use manifest::RetainedImageManifest;
pub use payload::RetainedPayload;
pub use preparation::{PreparedImageCustody, RetainedImageCustody};

pub mod system_stream;

pub mod verified_system_stream;

#[cfg(test)]
mod verified_system_stream_tests;
