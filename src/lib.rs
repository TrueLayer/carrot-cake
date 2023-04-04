//! `carrot-cake` is a pub/sub framework, built on top of [`lapin`],
//! to make it easy and ergonomic to work with RabbitMQ.
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

pub mod amqp;
pub mod pool;
