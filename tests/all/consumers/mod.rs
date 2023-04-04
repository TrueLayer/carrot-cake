mod handlers;
mod hooks;
mod middlewares;
mod processing_middlewares;

use carrot_cake::amqp::types::FieldTable;
use carrot_cake::amqp::Channel;
use carrot_cake::consumers::ConsumerPreStartHook;

#[derive(Clone, Copy)]
pub struct TempQueueCreator;

#[async_trait::async_trait]
impl ConsumerPreStartHook for TempQueueCreator {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        _args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        let options = carrot_cake::amqp::options::QueueDeclareOptions {
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: true,
            nowait: false,
        };
        channel
            .queue_declare(queue_name, options, <_>::default())
            .await?;
        Ok(())
    }
}
