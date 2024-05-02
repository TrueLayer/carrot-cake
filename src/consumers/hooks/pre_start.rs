//! A collection of pre-start hooks.
//!
//! ## Examples of queue/exchange setups
//!
//! ```
//! use carrot_cake::consumers::hooks::pre_start::{
//!     Bind,
//!     DeclareDurableExchange,
//!     DeclareDurableQueue,
//!     ExistingExchange,
//!     Headers,
//!     RoutingKey,
//!     WithDeadLetterQueue,
//!     ExchangeKind,
//! };
//!
//! // we want to bind a queue to an exchange
//! let pre_start_hook = Bind {
//!     // we will create a new durable exchange
//!     exchange: DeclareDurableExchange {
//!         // called `some.exchange`
//!         name: "some.exchange".to_owned(),
//!         // and it is a topic exchange
//!         kind: ExchangeKind::Topic,
//!     },
//!     // we will also create a durable queue, and a dead letter exchange/queue to handle
//!     // rejected messages
//!     queue: WithDeadLetterQueue(DeclareDurableQueue),
//!     // we want to bind this queue and exchange together using a few routing keys
//!     binding: vec![
//!         RoutingKey::from("message1"),
//!         RoutingKey::from("message2"),
//!         RoutingKey::from("message3"),
//!     ],
//! };
//! # drop(pre_start_hook);
//!
//! // we want to declare a durable queue that publishers will
//! // publish to directly, no bindings or exchanges required.
//! let pre_start_hook = DeclareDurableQueue;
//! # drop(pre_start_hook);
//!
//! // we want to bind a queue to an exchange using a single set of headers
//! let pre_start_hook = Bind {
//!     // Using an existing exchange
//!     exchange: ExistingExchange {
//!         // called `some.exchange`
//!         name: "some.exchange".to_owned(),
//!     },
//!     // we will also create a durable queue
//!     queue: DeclareDurableQueue,
//!     // we want to bind this queue and exchange together when all the headers match
//!     binding: Headers::All(vec![
//!         ("currency".to_owned(), "EUR".to_owned()),
//!         ("country".to_owned(), "FR".to_owned()),
//!     ]),
//! };
//! # drop(pre_start_hook);
//! ```

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::consumers::ConsumerPreStartHook;
use amq_protocol_types::{AMQPValue, FieldTable, LongString, ShortString};
use async_trait::async_trait;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::Channel;
pub use lapin::ExchangeKind;

#[async_trait::async_trait]
/// Defines how to setup an exchange
pub trait ExchangeSetup: Send + Sync + 'static {
    /// The name of the exchange that will be set up.
    fn exchange_name(&self) -> &str;

    /// Ensures the exchange is set up.
    async fn setup_exchange(&self, channel: &Channel) -> Result<(), anyhow::Error>;
}

/// Declares a new durable exchange
pub struct DeclareDurableExchange {
    pub name: String,
    pub kind: ExchangeKind,
}

/// Declares a new transient exchange
pub struct DeclareExchange {
    pub name: String,
    pub kind: ExchangeKind,
}

/// Uses a pre-existing exchange
pub struct ExistingExchange {
    pub name: String,
}

#[async_trait]
impl ExchangeSetup for DeclareDurableExchange {
    fn exchange_name(&self) -> &str {
        &self.name
    }

    async fn setup_exchange(&self, channel: &Channel) -> Result<(), anyhow::Error> {
        channel
            .exchange_declare(
                &self.name,
                self.kind.clone(),
                ExchangeDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ExchangeSetup for DeclareExchange {
    fn exchange_name(&self) -> &str {
        &self.name
    }

    async fn setup_exchange(&self, channel: &Channel) -> Result<(), anyhow::Error> {
        channel
            .exchange_declare(
                &self.name,
                self.kind.clone(),
                ExchangeDeclareOptions {
                    passive: false,
                    durable: false,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ExchangeSetup for ExistingExchange {
    fn exchange_name(&self) -> &str {
        &self.name
    }

    async fn setup_exchange(&self, _channel: &Channel) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Pre-start hook wraps another pre-start hook with a [dead letter exchange](https://www.rabbitmq.com/dlx.html),
///
/// Specifically, it creates the queue `<queue_name>.deadletter` that is bound to the fanout exchange `<queue_name>.deadletter`
/// and marks `<queue_name>.deadletter` as the dead letter exchange for queue.
///
/// Given that AMQP queue's settings are immutable,
/// switching from `PreStartHook` to `WithDeadLetterQueue<PreStartHook>`
/// requires rolling a new queue.
pub struct WithDeadLetterQueue<H: ConsumerPreStartHook>(pub H);

#[async_trait::async_trait]
#[allow(unused_doc_comments)]
impl<E: ConsumerPreStartHook> ConsumerPreStartHook for WithDeadLetterQueue<E> {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        mut queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        let dead_letter = format!("{queue_name}.deadletter");

        let dlx = Bind {
            /// declare a durable dead letter exchange
            exchange: DeclareDurableExchange {
                name: dead_letter.clone(),
                kind: ExchangeKind::Fanout,
            },
            /// and a durable queue
            queue: DeclareDurableQueue,
            /// bind them without a routing key
            binding: RoutingKey(String::new()),
        };
        dlx.run(channel, &dead_letter, FieldTable::default())
            .await?;

        // insert the DLX arg for the queue
        queue_args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dead_letter.into()),
        );
        self.0.run(channel, queue_name, queue_args).await
    }
}

/// Bind the exchange defined by [`ExchangeSetup`] to the queue that will be defined in the nested `Queue` [`ConsumerPreStartHook`]
/// using the [`Binding`] method specified.
pub struct Bind<Exchange: ExchangeSetup, Queue: ConsumerPreStartHook, Binds: Binding> {
    pub exchange: Exchange,
    pub queue: Queue,
    pub binding: Binds,
}

#[async_trait::async_trait]
impl<E: ExchangeSetup, Q: ConsumerPreStartHook, B: Binding> ConsumerPreStartHook for Bind<E, Q, B> {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        self.exchange.setup_exchange(channel).await?;
        self.queue.run(channel, queue_name, queue_args).await?;
        self.binding
            .bind(channel, self.exchange.exchange_name(), queue_name)
            .await?;

        Ok(())
    }
}

/// Declare a durable queue to consume responses.
#[derive(Clone)]
pub struct DeclareDurableQueue;

#[async_trait::async_trait]
impl ConsumerPreStartHook for DeclareDurableQueue {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        queue_args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },
                queue_args,
            )
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
/// Defines how to bind an exchange to a queue
///
/// Example implementations:
/// * [`Headers`] - binds the queue to a [header exchange](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-headers)
/// * [`RoutingKey`] - binds the queue to an exchange using routing keys.
/// * [`Vec<_>`] - binds the queue to an exchange using many bindings, eg `Vec<RoutingKey>` will bind using many routing keys.
pub trait Binding: Send + Sync + 'static {
    /// Ensure the exchange is created
    async fn bind(
        &self,
        channel: &Channel,
        exchange_name: &str,
        queue_name: &str,
    ) -> Result<(), anyhow::Error>;
}

#[derive(Clone)]
/// Binds a header exchange to a queue.
///
/// The header values are registered as `(Key, Value)` pairs.
pub enum Headers {
    /// All the headers must match for the message to be routed
    All(Vec<(String, String)>),
    /// Any the headers must match for the message to be routed
    Any(Vec<(String, String)>),
}

#[async_trait::async_trait]
impl Binding for Headers {
    async fn bind(
        &self,
        channel: &Channel,
        exchange_name: &str,
        queue_name: &str,
    ) -> Result<(), anyhow::Error> {
        let (headers, match_type) = match self {
            Self::All(headers) => (headers, LongString::from("all")),
            Self::Any(headers) => (headers, LongString::from("any")),
        };
        let mut args: BTreeMap<ShortString, AMQPValue> = headers
            .iter()
            .cloned()
            .map(|(key, value)| (key.into(), LongString::from(value).into()))
            .collect();
        args.insert("x-match".into(), match_type.into());

        channel
            .queue_bind(
                queue_name,
                exchange_name,
                "",
                QueueBindOptions { nowait: false },
                args.into(),
            )
            .await?;

        Ok(())
    }
}

/// Binds an exchange to a queue using a routing key.
pub struct RoutingKey(pub String);

impl From<&str> for RoutingKey {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for RoutingKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[async_trait::async_trait]
impl Binding for RoutingKey {
    async fn bind(
        &self,
        channel: &Channel,
        exchange_name: &str,
        queue_name: &str,
    ) -> Result<(), anyhow::Error> {
        channel
            .queue_bind(
                queue_name,
                exchange_name,
                &self.0,
                QueueBindOptions { nowait: false },
                FieldTable::default(),
            )
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<B: Binding> Binding for Vec<B> {
    async fn bind(
        &self,
        channel: &Channel,
        exchange_name: &str,
        queue_name: &str,
    ) -> Result<(), anyhow::Error> {
        for b in self {
            b.bind(channel, exchange_name, queue_name).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Binding for Arc<dyn Binding> {
    async fn bind(
        &self,
        channel: &Channel,
        exchange_name: &str,
        queue_name: &str,
    ) -> Result<(), anyhow::Error> {
        self.deref().bind(channel, exchange_name, queue_name).await
    }
}

/// Enable [priority queue](https://www.rabbitmq.com/priority.html) and set maximum priority this queue should support.
///
/// Warning: This value is immutable, so enabling priority queue or changing the value
/// will require rolling a new queue.
pub struct WithPriority<E: ConsumerPreStartHook> {
    /// Specify the queue to specify priority on.
    pub queue: E,
    /// Valid values are between 1-255.
    /// There is some in-memory and on-disk cost per priority level per queue. There is also an additional CPU cost,
    /// especially when consuming, so you may not wish to create huge numbers of levels.
    ///
    /// Read more [here](https://www.rabbitmq.com/priority.html#behaviour)
    pub priority: u8,
}

#[async_trait::async_trait]
impl<H: ConsumerPreStartHook> ConsumerPreStartHook for WithPriority<H> {
    async fn run(
        &self,
        channel: &Channel,
        queue_name: &str,
        mut args: FieldTable,
    ) -> Result<(), anyhow::Error> {
        args.insert(
            "x-max-priority".into(),
            AMQPValue::ShortShortUInt(self.priority),
        );

        self.queue.run(channel, queue_name, args).await
    }
}
