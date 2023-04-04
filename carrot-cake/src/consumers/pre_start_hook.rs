use crate::rabbit_mq::{Channel, WITHOUT_PUBLISHER_CONFIRMATION};
use amq_protocol_types::FieldTable;
use std::future::Future;

#[async_trait::async_trait]
/// A hook to execute logic before a [`MessageHandler`] starts processing messages.
///
/// # Use case
///
/// [`MessageHandler`]s process messages retrieved from a queue. Who is in charge of creating the queue
/// resource? What about the exchanges the queue needs to be bound to?
///
/// Depending on your setup, this might be done via Terraform or manually.
/// Often, though, it is preferable to have consumers in charge of creating the objects they
/// rely on.
///
/// A pre-start hook gives you access to the a channel, allowing you to perform actions against
/// the message broker before the consumer actually starts pulling messages.
///
/// # Plug and play implementations
///
/// You can find two ready-to-go pre-start hooks in the [`hooks::pre_start`] module -
/// [`DurableExchangeBinder`] and [`DurableQueueCreator`].
///
/// [`MessageHandler`]: crate::consumers::MessageHandler
/// [`DurableExchangeBinder`]: crate::consumers::hooks::pre_start::DurableExchangeBinder
/// [`DurableQueueCreator`]: crate::consumers::hooks::pre_start::DurableQueueCreator
/// [`hooks::pre_start`]: crate::consumers::hooks::pre_start
pub trait ConsumerPreStartHook: Send + Sync + 'static {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error>;
}

// Implement the ConsumerPreStartHook trait for all async functions that match our expected signature.
#[async_trait::async_trait]
impl<F: Send + Sync + 'static, Fut> ConsumerPreStartHook for F
where
    // The expected function signature
    F: Fn(&Channel<WITHOUT_PUBLISHER_CONFIRMATION>, &str) -> Fut,
    Fut: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
{
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        _args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        // `self`, in this case, is a function, which we are calling on its argument using
        // parenthesis notation - self(_, _)
        // We are then awaiting its result.
        (self)(channel, queue_name).await
    }
}
