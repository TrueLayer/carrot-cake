//! Facilities to publish messages to a RabbitMq exchange. Check out [`Publisher`] as a starting point.
mod message_envelope;
mod publisher;
mod publisher_middleware;

pub use message_envelope::MessageEnvelope;
pub use publisher::{Publisher, PublisherBuilder, PublisherError};
pub use publisher_middleware::{Next, PublisherMiddleware};
