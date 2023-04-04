use lapin::message::Delivery;

#[async_trait::async_trait]
/// A hook to determine if failed messages due to a transient error should be requeued.
///
/// # Use case
///
/// The framework nacks all messages if processing returns an error.
/// By default, those messages are never requeued.  
///
/// If the error encountered during processing is marked as [`ErrorType::Transient`]
/// you can inject your own logic to determine if the failed messages should be requeued using a
/// transient error hook.
///
/// # On fatal errors
///
/// A transient error hook, as the name implies, is only invoked on transient errors.
/// Errors marked as [`ErrorType::Fatal`] are nacked and never requeued.
///
/// # Plug and play implementations
///
/// You can find two ready-to-go hooks in the [`hooks::transient_error`] module -
/// [`AlwaysRequeue`] and [`NeverRequeue`].
///
/// A more interesting implementation is provided in [`rust-amqp-retry`](https://github.com/TrueLayer/rusty-bunny/tree/main/src/amqp-retry)
/// to retry messages with a delay.
///
/// [`ErrorType::Transient`]: crate::consumers::ErrorType::Transient
/// [`ErrorType::Fatal`]: crate::consumers::ErrorType::Fatal
/// [`hooks::transient_error`]: crate::consumers::hooks::transient_error
/// [`AlwaysRequeue`]: crate::consumers::hooks::transient_error::AlwaysRequeue
/// [`NeverRequeue`]: crate::consumers::hooks::transient_error::NeverRequeue
pub trait ConsumerTransientErrorHook: Send + Sync + 'static {
    /// `ConsumerRetryHook::on_transient_error` determines if the `requeue` flag should be
    /// set to true by returning a [`ShouldRequeue`] instance.
    ///
    /// If [`ShouldRequeue::Requeue`] is returned, the message will be requeued and become
    /// _immediately_ available again for consumption.
    /// If [`ShouldRequeue::Discard`] is returned, the message will not be requeued
    /// and it will not end up in the dead letter exchange if configured.
    /// If [`ShouldRequeue::DeadLetterOrDiscard`] is returned, the message will be not be requeued
    /// and it will end up in the dead letter exchange if configured.
    async fn on_transient_error(&self, delivery: &Delivery) -> ShouldRequeue;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Determines if a failed message should be re-queued.
///
/// Check out [`ConsumerTransientErrorHook`]'s documentation for more details.
pub enum ShouldRequeue {
    Requeue,
    Discard,
    DeadLetterOrDiscard,
}
