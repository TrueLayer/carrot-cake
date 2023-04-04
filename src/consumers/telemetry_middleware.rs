//! Middleware types are heavily inspired by `tide`'s approach to middleware.
use crate::consumers::processing_middleware::Next;
use crate::consumers::{
    ConsumerTransientErrorHook, ErrorType, HandlerError, Incoming, ShouldRequeue,
};
use lapin::options::{BasicAckOptions, BasicNackOptions, BasicRejectOptions};
use std::sync::Arc;

use super::incoming_message::Delivery;

/// Middlewares to collect and emit telemetry data based on the outcome of message processing.
///
/// # Use case
///
/// `TelemetryMiddleware`s get **read-only** access to the input and outputs of message processing.
///
/// `TelemetryMiddleware`s execute after **all** the message processing has taken place,
/// including ack/nacking with the broker. They are therefore best places to emit logs,
/// collect metrics, etc.. - telemetry!
///
/// Before the handler is executed, telemetry middlewares can:
///
/// - Extract information from the incoming message and [record it in the message extensions];
///
/// After the handler has been executed, middlewares can:
///
/// - [Extract information recorded in the message extensions] to perform a task;
/// - Perform actions based on the handler's outcome (e.g. log errors).
///
/// # What middleware should I use?
///
/// Does the processing outcome (success/failure) change based on the logic executed in the middleware?
///
/// If yes, use a [`ProcessingMiddleware`].
/// If no, use a `TelemetryMiddleware`.
///
/// [record it in the message extensions]: crate::consumers::get_message_local_item
/// [Extract information recorded in the message extensions]: crate::consumers::set_message_local_item
/// [`ProcessingMiddleware`]: crate::consumers::ProcessingMiddleware
#[async_trait::async_trait]
pub trait TelemetryMiddleware<Context, Error>: 'static + Send + Sync {
    /// Asynchronously handle the request, and return a response.
    async fn handle<'a>(
        &'a self,
        incoming: &'a Incoming<Context>,
        next: MessageProcessing<'a, Context, Error>,
    ) -> ProcessingOutcome<Error>;
}

/// The remainder of the middleware chain (telemetry + processing), including the final message handler.
#[allow(missing_debug_implementations)]
pub struct MessageProcessing<'a, Context, Error> {
    /// Logic to handle transient failures returned by the processing chain.
    pub(super) transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
    /// The chain of processing middlewares, including the final message handler.
    pub(super) processing_chain: Next<'a, Context, Error>,
    /// The remainder of the telemetry middleware chain.
    pub(super) next_telemetry_middleware: &'a [Arc<dyn TelemetryMiddleware<Context, Error>>],
}

/// The outcome of message processing:
/// - processing middleware chain;
/// - message handler;
/// - ack/nack against the AMQP broker.
///
/// [`ProcessingOutcome`] is what [`TelemetryMiddleware`]s work with on the way out in
/// the middleware execution pipeline.
///
/// You can convert into a `Result` using [`ProcessingOutcome::result`].
///
/// # Why a struct?
///
/// [`TelemetryMiddleware`] should pass the message processing outcome unaltered along
/// the telemetry middleware chain.
/// To ensure no tampering (mostly by mistake), we encapsulate `Result<(), ProcessingError>` into
/// a struct, [`ProcessingOutcome`].
/// [`ProcessingOutcome`] does not expose any constructor: it is impossible to build a new
/// [`ProcessingOutcome`] in [`TelemetryMiddleware::handle`]. The telemetry middleware is forced
/// to propagate the outcome returned by [`MessageProcessing`]
#[derive(Debug)]
pub struct ProcessingOutcome<Error> {
    outcome: Result<(), ProcessingError<Error>>,
    broker_action: BrokerAction,
}

impl<Error> ProcessingOutcome<Error> {
    pub fn result(&self) -> &Result<(), ProcessingError<Error>> {
        &self.outcome
    }

    /// Returns `true` if we instructed the broker to requeue the message after a transient failure
    /// in processing. Returns `false` otherwise.
    ///
    /// It returns `true` even if we experienced an issue when dispatching the nack instruction to
    /// the AMQP broker (e.g. network timeout).
    pub fn was_requeued(&self) -> bool {
        match &self.broker_action {
            BrokerAction::Ack => false,
            BrokerAction::Nack => true,
            BrokerAction::Reject => false,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessingError<Error> {
    /// An error was encountered while processing the message.
    #[error("An error was encountered while processing of the message.")]
    HandlerError(HandlerError<Error>),
    /// Failed to ack message.
    #[error("Failed to ack message.")]
    AckError(#[source] anyhow::Error),
    /// Failed to nack message.
    #[error("Failed to nack message.")]
    NackError {
        #[source]
        error: anyhow::Error,

        /// The processing error that led us to try to tell the AMQP broker to nack the message.
        handler_error: Option<Error>,

        error_type: ErrorType,
    },
}

impl<'a, Context: 'static, Error: 'static> MessageProcessing<'a, Context, Error> {
    /// Asynchronously execute the remaining middleware chain.
    pub async fn run(mut self, incoming: &Incoming<Context>) -> ProcessingOutcome<Error> {
        // If there is at least one middleware in the chain, get a reference to it and store
        // the remaining ones in `next_middleware`.
        // Then call the middleware passing `self` in the handler, recursively.
        if let Some((current, next)) = self.next_telemetry_middleware.split_first() {
            self.next_telemetry_middleware = next;
            current.handle(incoming, self).await
        } else {
            // We have now executed all telemetry middlewares (or simply there were none).
            // Time to kick-off the processing chain: processing middlewares + handler.
            let Self {
                transient_error_hook,
                processing_chain,
                ..
            } = self;

            let message = &incoming.message;
            let outcome = processing_chain.run(incoming).await;

            match ack_or_nack(transient_error_hook, message, &outcome).await {
                Ok(broker_action) => ProcessingOutcome {
                    outcome: outcome.map_err(ProcessingError::HandlerError).map(|_| ()),
                    broker_action,
                },
                Err(inner_error) => match inner_error {
                    InnerBrokerError::AckError(e) => ProcessingOutcome {
                        outcome: Err(ProcessingError::AckError(e)),
                        broker_action: BrokerAction::Ack,
                    },
                    InnerBrokerError::NackError { error, requeue } => {
                        // We can get a Nack failure independently of the value returned by the handler
                        // because it can return both `Ok(BrokerAction::Nack)` and `Err(HandlerError { error_type: Transient })`.
                        // If we failed to Nack a message while processing a success (e.g., an `Ok(_)` return),
                        // the only sensible thing to do is map it to a transient error.
                        let (error_type, handler_error) = match outcome {
                            Ok(_) => (ErrorType::Transient, None),
                            Err(e) => (e.error_type, Some(e.inner_error)),
                        };
                        ProcessingOutcome {
                            outcome: Err(ProcessingError::NackError {
                                error,
                                handler_error,
                                error_type,
                            }),
                            broker_action: requeue.into(),
                        }
                    }
                },
            }
        }
    }
}

enum InnerBrokerError {
    AckError(anyhow::Error),
    NackError {
        error: anyhow::Error,
        requeue: ShouldRequeue,
    },
}

/// The action we asked the broker to take when finalising the processing of
/// the current message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerAction {
    /// Positive acknowledgement - the message can be removed from the queue.
    /// This happens if the message was processed successfully or
    /// as a consequence of a transient error the message will be retried later.
    Ack,
    /// Negative acknowledgement - the message was not processed successfully and should be requeued
    /// to retry processing.
    Nack,
    /// Rejection. The message was not processed successfully and should NOT be requeued.
    /// The message will be sent to the dead letter exchange if configured.
    Reject,
}

impl BrokerAction {
    async fn execute(&self, acker: &lapin::acker::Acker) -> Result<(), InnerBrokerError> {
        match self {
            // Acknowledge the message
            Self::Ack => {
                let ack_options = BasicAckOptions { multiple: false };

                acker
                    .ack(ack_options)
                    .await
                    .map_err(anyhow::Error::from)
                    .map_err(InnerBrokerError::AckError)
            }

            // Put the message back to the queue
            Self::Nack => {
                let nack_options = BasicNackOptions {
                    multiple: false,
                    requeue: true,
                };

                acker
                    .nack(nack_options)
                    .await
                    .map_err(anyhow::Error::from)
                    .map_err(|e| InnerBrokerError::NackError {
                        error: e,
                        requeue: ShouldRequeue::Requeue,
                    })
            }

            // Remove the message from the queue and send it to the DLQ (if configured)
            Self::Reject => {
                let reject_options = BasicRejectOptions { requeue: false };

                acker
                    .reject(reject_options)
                    .await
                    .map_err(anyhow::Error::from)
                    .map_err(|e| InnerBrokerError::NackError {
                        error: e,
                        requeue: ShouldRequeue::DeadLetterOrDiscard,
                    })
            }
        }
    }
}

/// Based on the outcome of processing communicate with the AMQP broker to ack/nack/reject the message.
/// If processing failed, it takes care to determine (via the transient error hook) if the message
/// should be requeued (nack), requeued with ack (ack) or not (reject).
async fn ack_or_nack<Error>(
    transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
    message: &Delivery,
    outcome: &Result<BrokerAction, HandlerError<Error>>,
) -> Result<BrokerAction, InnerBrokerError> {
    match outcome {
        Ok(action) => {
            action.execute(&message.acker).await?;
            Ok(*action)
        }
        Err(e) => match e.error_type {
            ErrorType::Fatal => handle_fatal_error(message).await,
            ErrorType::Transient => handle_transient_error(message, transient_error_hook).await,
        },
    }
}

/// Removes the message from the queue, rejecting it.
/// If a dead letter exchange has been configured, the rejected message will be delivered to it.
async fn handle_fatal_error(message: &Delivery) -> Result<BrokerAction, InnerBrokerError> {
    BrokerAction::Reject.execute(&message.acker).await?;
    Ok(BrokerAction::Reject)
}

/// Determines how to handle a transient error on the basis of transient error hook:
/// - `ShouldRequeue::Requeue`: the message is nacked and re-delivered to the queue.
/// - `ShouldRequeue::DoNotRequeue`: the message is rejected
/// and sent to the dead letter exchange if configured.
/// - `ShouldRequeue::DoNotRequeueAck`: the message is acked.
async fn handle_transient_error(
    message: &Delivery,
    transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
) -> Result<BrokerAction, InnerBrokerError> {
    let action: BrokerAction = transient_error_hook
        .on_transient_error(message)
        .await
        .into();

    action.execute(&message.acker).await?;
    Ok(action)
}
