use amq_protocol_types::{AMQPValue, FieldTable};
use carrot_cake_amqp::rabbit_mq::{Channel, WITHOUT_PUBLISHER_CONFIRMATION};

use crate::consumers::ConsumerPreStartHook;

/// Enable [priority queue](https://www.rabbitmq.com/priority.html) and set maximum priority this queue should support.
///
/// Warning: This value is immutable, so enabling priority queue or changing the value
/// will require rolling a new queue.
pub struct PriorityQueueBinder<E: ConsumerPreStartHook> {
    /// Specify the hook to enable priority queues.
    pub hook: E,
    /// Valid values are between 1-255.
    /// There is some in-memory and on-disk cost per priority level per queue. There is also an additional CPU cost,
    /// especially when consuming, so you may not wish to create huge numbers of levels.
    ///
    /// Read more [here](https://www.rabbitmq.com/priority.html#behaviour)
    pub priority: u8,
}

#[async_trait::async_trait]
impl<H: ConsumerPreStartHook> ConsumerPreStartHook for PriorityQueueBinder<H> {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        mut args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        args.insert(
            "x-max-priority".into(),
            AMQPValue::ShortShortUInt(self.priority),
        );

        self.hook.run(channel, queue_name, args).await
    }
}
