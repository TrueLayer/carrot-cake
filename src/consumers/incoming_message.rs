use amq_protocol_types::{DeliveryTag, ShortString};
use lapin::{acker::Acker, BasicProperties};
use std::sync::Arc;

/// A dequeued message enriched with some auxiliary data, ready for processing.
///
/// `Incoming` is the input type of our message handler functions (check out
/// [`Handler`](crate::consumers::Handler)'s docs for more details).
pub struct Incoming<C> {
    /// `context` is a set of resources that are required to process the message and are outside
    /// the lifecycle of the message itself - e.g. an HTTP client for a third-party API, a db connection
    /// pool, etc.
    ///
    /// # Ownership
    ///
    /// The context is behind an `Arc` pointer: this allows multiple messages to access the same
    /// context concurrently without having to create an ad-hoc instance of `context` for each
    /// message - it might be expensive!
    //
    // # Implementation notes
    //
    // We use `Arc` instead of a simple `&` because message handling is asynchronous and our
    // runtime (`tokio`) is multi-thread: `handle` could be executed as a task on an arbitrary thread,
    // hence we need to be able to share that reference across thread boundaries safely.
    pub context: Arc<C>,
    /// `message` is what we received from RabbitMq: it includes headers, payload, delivery tag, etc.
    pub message: Delivery,
    /// The name of the queue.
    pub queue_name: String,
}

/// A received AMQP message.
#[derive(Debug, PartialEq)]
pub struct Delivery {
    /// The delivery tag of the message.
    pub delivery_tag: DeliveryTag,

    /// The exchange of the message. May be an empty string
    /// if the default exchange is used.
    pub exchange: ShortString,

    /// The routing key of the message. May be an empty string
    /// if no routing key is specified.
    pub routing_key: ShortString,

    /// Whether this message was redelivered
    pub redelivered: bool,

    /// Contains the properties and the headers of the
    /// message.
    pub properties: BasicProperties,

    /// The payload of the message in binary format.
    pub data: Vec<u8>,

    /// The acker used to ack/nack the message
    // Hidden from public interface, to stop a message being acked / rejected inside a message handler.
    // AMQP protocol specifics that a message must not be acked /rejected multiple times:
    // https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.ack.delivery-tag
    pub(crate) acker: Acker,
}

impl From<lapin::message::Delivery> for Delivery {
    fn from(value: lapin::message::Delivery) -> Self {
        Self {
            delivery_tag: value.delivery_tag,
            exchange: value.exchange,
            routing_key: value.routing_key,
            redelivered: value.redelivered,
            properties: value.properties,
            data: value.data,
            acker: value.acker,
        }
    }
}
