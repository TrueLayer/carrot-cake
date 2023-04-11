//! A collection of pre-start hooks.
use crate::consumers::ConsumerPreStartHook;
use crate::rabbit_mq::{Channel, WITHOUT_PUBLISHER_CONFIRMATION};
use amq_protocol_types::{AMQPValue, FieldTable};
use lapin::options::ExchangeDeclareOptions;
use lapin::ExchangeKind;

use super::ExchangeBuilder;

/// Pre-start hook wraps another pre-start hook with a [dead letter exchange](https://www.rabbitmq.com/dlx.html),
///
/// Specifically, it creates the queue `<queue_name>.deadletter` that is bound to the fanout exchange `<queue_name>.deadletter`
/// and marks `<queue_name>.deadletter` as the dead letter exchange for queue.
///
/// Given that AMQP queue's settings are immutable,
/// switching from `PreStartHook` to `WithDeadLetterQueue<PreStartHook>`
/// requires rolling a new queue.
pub struct WithDeadLetterQueue<H: ConsumerPreStartHook> {
    pub inner: H,
}

#[async_trait::async_trait]
impl<E: ConsumerPreStartHook> ConsumerPreStartHook for WithDeadLetterQueue<E> {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        mut queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        let dead_letter = format!("{queue_name}.deadletter");
        channel
            .create_exchange(
                &dead_letter,
                ExchangeKind::Fanout,
                ExchangeDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
            )
            .await?;
        channel.create_durable_queue(&dead_letter).await?;
        channel.bind_queue(&dead_letter, &dead_letter, "").await?;

        queue_args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dead_letter.into()),
        );
        self.inner.run(channel, queue_name, queue_args).await
    }
}

impl<E: ExchangeBuilder> ExchangeBuilder for WithDeadLetterQueue<E> {
    fn exchange_name(&self) -> &str {
        self.inner.exchange_name()
    }

    type RoutingKeys<'a> = E::RoutingKeys<'a> where E: 'a;

    fn routing_keys(&self) -> Self::RoutingKeys<'_> {
        self.inner.routing_keys()
    }
}

/// An iterator based on a slice, that has a mechanism to turn the entries into string-slices
type StringSliceIter<'a, T> = std::iter::Map<std::slice::Iter<'a, T>, fn(&'a T) -> &'a str>;

pub struct ExchangeToDurableQueueBinder {
    pub exchange_name: String,
    pub exchange_kind: ExchangeKind,
    pub exchange_options: ExchangeDeclareOptions,
    pub routing_keys: Vec<String>,
}

impl ExchangeBuilder for ExchangeToDurableQueueBinder {
    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    type RoutingKeys<'a> = StringSliceIter<'a, String>;

    fn routing_keys(&self) -> Self::RoutingKeys<'_> {
        self.routing_keys.iter().map(String::as_str)
    }
}

#[async_trait::async_trait]
impl ConsumerPreStartHook for ExchangeToDurableQueueBinder {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        exchange_to_durable_queue_binder(
            self,
            channel,
            queue_name,
            queue_args,
            self.exchange_kind.clone(),
            self.exchange_options,
        )
        .await
    }
}

/// It creates a durable exchange, a durable queue and binds it to the exchange
/// name specified using the passed-in routing key.
#[derive(Clone)]
pub struct DurableExchangeBinder {
    pub exchange_name: String,
    pub routing_key: String,
}

impl ExchangeBuilder for DurableExchangeBinder {
    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    type RoutingKeys<'a> = std::iter::Once<&'a str>;

    fn routing_keys(&self) -> Self::RoutingKeys<'_> {
        std::iter::once(&self.routing_key)
    }
}

#[async_trait::async_trait]
impl ConsumerPreStartHook for DurableExchangeBinder {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        let options = ExchangeDeclareOptions {
            passive: false,
            // The exchange will survive RabbitMq server restarts
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
        };
        exchange_to_durable_queue_binder(
            self,
            channel,
            queue_name,
            queue_args,
            ExchangeKind::Direct,
            options,
        )
        .await
    }
}

async fn exchange_to_durable_queue_binder<E: ExchangeBuilder>(
    e: &E,
    channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
    queue_name: &str,
    queue_args: FieldTable,
    exchange_kind: ExchangeKind,
    exchange_options: ExchangeDeclareOptions,
) -> Result<(), anyhow::Error> {
    channel
        .create_exchange(e.exchange_name(), exchange_kind, exchange_options)
        .await?;

    channel
        .create_durable_queue_with_args(queue_name, queue_args)
        .await?;

    for routing_key in e.routing_keys() {
        channel
            .bind_queue(queue_name, e.exchange_name(), routing_key)
            .await?;
    }

    Ok(())
}

/// Hook that ensure a durable queue is created & bound to an existing exchange
/// with potentially multiple routing keys.
pub struct DurableQueueToExistingExchange {
    pub exchange_name: String,
    pub routing_keys: Vec<String>,
}

impl ExchangeBuilder for DurableQueueToExistingExchange {
    fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    type RoutingKeys<'a> = StringSliceIter<'a, String>;

    fn routing_keys(&self) -> Self::RoutingKeys<'_> {
        self.routing_keys.iter().map(String::as_str)
    }
}

#[async_trait::async_trait]
impl ConsumerPreStartHook for DurableQueueToExistingExchange {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> anyhow::Result<()> {
        channel
            .create_durable_queue_with_args(queue_name, queue_args)
            .await?;
        for routing_key in &self.routing_keys {
            channel
                .bind_queue(queue_name, &self.exchange_name, routing_key)
                .await?;
        }
        Ok(())
    }
}

/// Declare a durable queue to consume responses.
/// The queue is not bound to any exchange.
#[derive(Clone)]
pub struct DurableQueueCreator;

#[async_trait::async_trait]
impl ConsumerPreStartHook for DurableQueueCreator {
    async fn run(
        &self,
        channel: &Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        channel
            .create_durable_queue_with_args(queue_name, queue_args)
            .await?;
        Ok(())
    }
}
