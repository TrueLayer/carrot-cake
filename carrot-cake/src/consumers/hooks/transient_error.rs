//! A collection of transient error hooks.

use crate::consumers::{ConsumerTransientErrorHook, ShouldRequeue};
use lapin::message::Delivery;

/// Never requeue a message that failed with a transient error.
pub struct NeverRequeue;

#[async_trait::async_trait]
impl ConsumerTransientErrorHook for NeverRequeue {
    async fn on_transient_error(&self, _delivery: &Delivery) -> ShouldRequeue {
        ShouldRequeue::DeadLetterOrDiscard
    }
}

/// Always requeue a message that failed with a transient error.
pub struct AlwaysRequeue;

#[async_trait::async_trait]
impl ConsumerTransientErrorHook for AlwaysRequeue {
    async fn on_transient_error(&self, _delivery: &Delivery) -> ShouldRequeue {
        ShouldRequeue::Requeue
    }
}
