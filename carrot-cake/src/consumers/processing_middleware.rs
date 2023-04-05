//! Middleware types are heavily inspired by `tide`'s approach to middleware.
use crate::consumers::{Handler, HandlerError, Incoming};
use std::sync::Arc;

/// Middlewares to execute logic before and after the message handler function.
///
/// # Use case
///
/// The main purpose of processing middlewares is to extract and centralise common non-business logic
/// that might impact the outcome of the processing.  
///
/// Before the handler is executed, processing middlewares can:
///
/// - Extract information from the incoming message and [record it in the message extensions];
/// - Skip the execution of the handler entirely (e.g. an authorization middleware if auth fails);
///
/// After the handler has been executed, middlewares can:
///
/// - [Extract information recorded in the message extensions] to perform a task;
/// - Perform actions based on the handler's outcome (e.g. log errors);
/// - Modify the handler's outcome (e.g. change error severity).
///
/// The sky is the limit, but beware that abusing middlewares to perform application logic is often a
/// one-way ticket to mysterious bugs that are difficult to troubleshoot.
///
/// # What middleware should I use?
///
/// Does the processing outcome (success/failure) change based on the logic executed in the middleware?
///
/// If yes, use a `ProcessingMiddleware`.
/// If no, use a [`TelemetryMiddleware`].
///
/// # Plug-ang-play middlewares
///
/// There is a rich ecosystem of ready-to-go middlewares for message consumers:
///
/// - [`truelayer-pubsub-observability`](https://github.com/TrueLayer/rust-pubsub-observability/)
///   provides telemetry middlewares for logging, collecting metrics and distributed tracing;
/// - [`amqp-auth`](https://github.com/TrueLayer/rust-amqp-auth) provides a processing middleware
///   to verify the signature on an incoming message.
///
/// [record it in the message extensions]: crate::consumers::get_message_local_item
/// [`TelemetryMiddleware`]: crate::consumers::TelemetryMiddleware
/// [Extract information recorded in the message extensions]: crate::consumers::set_message_local_item
#[async_trait::async_trait]
pub trait ProcessingMiddleware<Context, Error>: 'static + Send + Sync {
    /// Asynchronously handle the request, and return a response.
    async fn handle<'a>(
        &'a self,
        incoming: Incoming<'a, Context>,
        next: Next<'a, Context, Error>,
    ) -> Result<(), HandlerError<Error>>;
}

/// The remainder of the processing middleware chain, including the final message handler.
#[allow(missing_debug_implementations)]
pub struct Next<'a, Context, Error> {
    pub(super) handler: &'a dyn Handler<Context, Error>,
    /// The remainder of the processing middleware chain.
    pub(super) next_middleware: &'a [Arc<dyn ProcessingMiddleware<Context, Error>>],
}

impl<'a, Context: 'static, Error: 'static> Next<'a, Context, Error> {
    /// Asynchronously execute the remaining processing middleware chain.
    pub async fn run(mut self, incoming: Incoming<'_, Context>) -> Result<(), HandlerError<Error>> {
        // If there is at least one processing middleware in the chain, get a reference to it and store
        // the remaining ones in `next_middleware`.
        // Then call the middleware passing `self` in the handler, recursively.
        if let Some((current, next)) = self.next_middleware.split_first() {
            self.next_middleware = next;
            current.handle(incoming, self).await
        } else {
            // We have executed all processing middlewares (or simply there were none) and it's now
            // the turn of the message handler itself.
            self.handler.handle(incoming).await
        }
    }
}
