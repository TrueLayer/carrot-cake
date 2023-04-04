//! The `Handler` trait is heavily inspired by `tide`'s approach to endpoint handlers.
use crate::consumers::{error::HandlerError, Incoming};
use std::future::Future;

/// Implementers of the `Handler` trait can be used in our RabbitMq [`MessageHandler`]s
/// to process messages retrieved from a queue.
///
/// # Scope
///
/// `handle` does not get access to the underlying RabbitMq channel.
/// The framework takes care of acking/nacking the message with the broker according to the outcome
/// of processing (check out [`HandlerError`] for more details).
/// This decouples the low-level interactions with the message broker and the retry logic from
/// the actual business logic associated with the processing of a message.
///
/// # Implementors
///
/// While you can implement `Handler` for a struct or enum, 99% of the time you will be relying
/// on our implementation of `Handle` for async functions that have a matching signature -
/// `Fn(Delivery, Arc<Context>) -> Fut`. See below for more details.
///
/// [`MessageHandler`]: crate::consumers::MessageHandler
#[async_trait::async_trait]
pub trait Handler<Context>: Send + Sync + 'static {
    async fn handle(&self, incoming: Incoming<'_, Context>) -> Result<(), HandlerError>;
}

/// Implement the [`Handler`] trait for all Boxed handlers.
///
/// E.g. Box<dyn Handler<Context>>.
#[async_trait::async_trait]
impl<Context, H> Handler<Context> for Box<H>
where
    Context: Send + Sync + 'static,
    H: Handler<Context> + ?Sized,
{
    async fn handle(&self, incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        H::handle(self, incoming).await
    }
}

/// `AsyncClosure` is implemented for all functions of the form:
/// ```ignore
/// async fn(incoming: Incoming<'_, Context>) -> impl Into<HandlerError>;
/// ```
///
/// When combined with the [`ClosureHandler`] type, you get a [`Handler`] that can be used
/// by consumer groups. `MessageHandlerBuilder::handler` will automatically perform this wrapping for you.
pub trait AsyncClosure<'a, Context>: Send + Sync + 'static {
    type Output: Future<Output = Result<(), Self::Err>> + Send + 'a;
    type Err: Into<HandlerError> + 'static;
    fn call(&'a self, incoming: Incoming<'a, Context>) -> Self::Output;
}

/// Implement `HandlerClosure` for all functions that match the required signature.
impl<'a, F, Fut, Err, Context> AsyncClosure<'a, Context> for F
where
    Context: 'static,
    F: Send + Sync + 'static,
    F: Fn(Incoming<'a, Context>) -> Fut,
    Fut: Future<Output = Result<(), Err>> + Send + 'a,
    Err: Into<HandlerError> + 'static,
{
    type Err = Err;
    type Output = Fut;

    fn call(&'a self, incoming: Incoming<'a, Context>) -> Self::Output {
        // `self`, in this case, is a function, which we are calling on its argument using
        // parenthesis notation - self(_)
        (self)(incoming)
    }
}

/// Wrapper type to turn [`ClosureHandler`] into a [`Handler`]
pub struct ClosureHandler<H>(pub H);

/// Implement the [`Handler`] trait for all [`ClosureHandler`]s that match the expected signature.
///
/// We do not require handlers to return a [`HandlerError`] directly - it is enough for them to
/// return an error type that can be converted to [`HandlerError`]
#[async_trait::async_trait]
impl<Context, F> Handler<Context> for ClosureHandler<F>
where
    Context: Send + Sync + 'static,
    F: for<'a> AsyncClosure<'a, Context>,
{
    async fn handle(&self, incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        self.0.call(incoming).await.map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use carrot_cake_amqp::amqp::AMQPProperties;
    use lapin::message::Delivery;
    use std::sync::Arc;

    async fn handler(_incoming: Incoming<'_, ()>) -> Result<(), HandlerError> {
        Ok(())
    }

    // This asserts that the implementation of Handler for Box<dyn Handler>
    // calls down the chain and does not recurse.
    #[tokio::test]
    async fn test_boxed_handler() {
        let handler: Box<dyn Handler<()>> = Box::new(ClosureHandler(handler));
        check(handler).await;
    }

    async fn check(h: impl Handler<()>) {
        let message = Incoming {
            context: Arc::new(()),
            message: &Delivery {
                delivery_tag: 0,
                exchange: "".into(),
                routing_key: "".into(),
                redelivered: false,
                properties: AMQPProperties::default(),
                data: vec![],
                acker: Default::default(),
            },
            queue_name: "".into(),
        };
        assert!(h.handle(message).await.is_ok());
    }
}
