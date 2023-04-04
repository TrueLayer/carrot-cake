mod handlers;
mod hooks;
mod middlewares;
mod processing_middlewares;

use amq_protocol_types::FieldTable;
use carrot_cake::{
    consumers::ConsumerPreStartHook,
    rabbit_mq::{Channel, WITHOUT_PUBLISHER_CONFIRMATION},
};

#[derive(Clone, Copy)]
pub struct TempQueueCreator;

#[async_trait::async_trait]
impl ConsumerPreStartHook for TempQueueCreator {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        _args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        let options = lapin::options::QueueDeclareOptions {
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: true,
            nowait: false,
        };
        channel
            .raw()
            .queue_declare(queue_name, options, <_>::default())
            .await?;
        Ok(())
    }
}
