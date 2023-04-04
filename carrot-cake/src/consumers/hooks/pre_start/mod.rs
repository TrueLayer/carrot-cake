mod hooks;
mod priority_queue;

pub use hooks::*;
pub use priority_queue::PriorityQueueBinder;

use crate::consumers::ConsumerPreStartHook;

/// `ExchangeBuilder` is a special [`ConsumerPreStartHook`] that,
/// as well as creating a queue, also created an exchange and binds the queue to it through
/// some routing keys.
pub trait ExchangeBuilder: ConsumerPreStartHook {
    fn exchange_name(&self) -> &str;

    type RoutingKeys<'a>: Iterator<Item = &'a str>
    where
        Self: 'a;
    fn routing_keys(&self) -> Self::RoutingKeys<'_>;
}
