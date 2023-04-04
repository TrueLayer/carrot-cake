use amq_protocol_types::FieldTable;
use lapin::Channel;

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
/// [`MessageHandler`]: crate::consumers::MessageHandler
/// [`hooks::pre_start`]: crate::consumers::hooks::pre_start
pub trait ConsumerPreStartHook: Send + Sync + 'static {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error>;
}

/// assert it's object safe
type _CHECK = Box<dyn ConsumerPreStartHook>;
