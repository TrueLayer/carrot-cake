//! `carrot-cake` is a pub/sub framework, built on top of [`lapin`](https://crates.io/crates/lapin),
//! to make it easy and ergonomic to work with RabbitMQ, our message broker.
//!
//! `carrot-cake` provides facilities to both consume and publish messages,
//! with support for middlewares and best-practices embedded in.
//!
//! [`Publisher`](crate::publishers::Publisher) and [`ConsumerGroup`](crate::consumers::ConsumerGroup)
//! are the best starting points to learn more about what `carrot-cake` provides and how
//! to leverage it.
//!
//! ## Examples
//!
//! Check the [`examples` directory](https://github.com/TrueLayer/carrot-cake/tree/main/carrot-cake/examples)
//! on GitHub as well to see the library in action.

pub mod consumers;
pub mod publishers;

// Re-export of `carrot_cake_amqp::amqp`.
pub use carrot_cake_amqp::amqp;

// Re-export of `carrot_cake_amqp::rabbit_mq`.
pub use carrot_cake_amqp::{configuration, rabbit_mq, BasicPropertiesExt};

// Re-export of `lapin` exchange options.
pub use lapin::{options::ExchangeDeclareOptions, ExchangeKind};
