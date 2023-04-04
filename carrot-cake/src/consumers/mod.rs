//! Facilities to consume messages from a RabbitMq queue. Check out [`ConsumerGroup`] as a
//! starting point.
// Re-export for convenience
pub use truelayer_sli::SliErrorType;

pub use builders::{
    ConsumerGroup, ConsumerGroupBuilder, ConsumerGroupConfigurationBuilder, MessageHandler,
    MessageHandlerBuilder,
};
pub use error::{ErrorType, HandlerError};
pub use extensions::{get_message_local_item, set_message_local_item};
pub use handler::{AsyncClosure, ClosureHandler, Handler};
pub use incoming_message::Incoming;
pub use pre_start_hook::ConsumerPreStartHook;
pub use processing_middleware::{Next, ProcessingMiddleware};
pub use telemetry_middleware::{
    MessageProcessing, ProcessingError, ProcessingOutcome, TelemetryMiddleware,
};
pub use transient_error_hook::{ConsumerTransientErrorHook, ShouldRequeue};

mod builders;
mod consumer;
mod error;
mod extensions;
mod handler;
pub mod hooks;
mod incoming_message;
mod pre_start_hook;
mod processing_middleware;
mod telemetry_middleware;
mod transient_error_hook;
