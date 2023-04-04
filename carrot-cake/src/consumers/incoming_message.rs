use lapin::message::Delivery;
use std::sync::Arc;

/// A dequeued message enriched with some auxiliary data, ready for processing.
///
/// `Incoming` is the input type of our message handler functions (check out
/// [`Handler`](crate::consumers::Handler)'s docs for more details).
pub struct Incoming<'d, C> {
    /// `context` is a set of resources that are required to process the message and are outside
    /// the lifecycle of the message itself - e.g. an HTTP client for a third-party API, a db connection
    /// pool, etc.
    ///
    /// # Ownership
    ///
    /// The context is behind an `Arc` pointer: this allows multiple messages to access the same
    /// context concurrently without having to create an ad-hoc instance of `context` for each
    /// message - it might be expensive!
    ///
    /// # Implementation notes
    ///
    /// We use `Arc` instead of a simple `&` because message handling is asynchronous and our
    /// runtime (`tokio`) is multi-thread: `handle` could be executed as a task on an arbitrary thread,
    /// hence we need to be able to share that reference across thread boundaries safely.
    pub context: Arc<C>,
    /// `message` is what we received from RabbitMq: it includes headers, payload, delivery tag, etc.
    pub message: &'d Delivery,
    /// The name of the queue.
    pub queue_name: String,
}
