mod dispatch;
mod queue_manager;
mod receive_wait;
mod wake_handle;

pub(crate) use queue_manager::queue_not_found;
pub use queue_manager::{QueueEndpoint, QueueManager};
#[cfg(test)]
pub(crate) use queue_manager::{batch_error_entry, selected_queue_attributes};
pub use wake_handle::QueueWakeHandle;

#[cfg(test)]
mod batch_error_tests;

#[cfg(test)]
mod attribute_projection_tests;

#[cfg(test)]
mod receive_tests;

#[cfg(all(test, feature = "sqlite"))]
mod receive_wait_tests;

#[cfg(test)]
mod receive_wait_state_tests;

#[cfg(all(test, feature = "sqlite"))]
mod clock_skew_tests;
