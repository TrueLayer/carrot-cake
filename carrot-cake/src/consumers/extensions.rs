//! An extension type-map is useful to share data between different middlewares on the incoming
//! path (`RabbitMq` -> `Middleware 1` -> ... -> `Middleware N` -> `Handler`) and the corresponding
//! outgoing path (`Handler` -> `Middleware N` -> ... -> `Middleware 1 -> RabbitMq Ack/Nack`).
//!
//! Our implementation relies on task-local storage, making it ergonomic to use from different
//! parts of the codebase without having to explicitly pass it around as an argument.
//! It keeps it "orthogonal" to the functional requirements.
//!
//! # Common use cases
//!
//! ## Measuring elapsed time
//!
//! Start a timer when message handling begins in a middleware, store the clock in the extensions,
//! retrieve the clock when message processing completes, compute the elapsed time.
//!
//! ## Enriching log context
//!
//! Capture additional information about a task that is not inferrable from the message payload
//! on its own (e.g. it requires calling another service).
//! The additional context can then be used to enrich the context available
//! on the closing log record for the message processing span.
//!
#![allow(clippy::declare_interior_mutable_const)] // silence tokio::task_local warning

use std::cell::RefCell;
use std::future::Future;
use task_local_extensions::Extensions;

tokio::task_local! {
    /// A type-map used by to share data across middlewares, request/response path, etc.
    /// It is behind a RefCell to allow insert by caller holding just a & reference instead of a
    /// &mut reference.
    static EXTENSIONS: RefCell<Extensions>;
}

/// Sets a task local to `Extensions` before `fut` is run,
/// and fetches the contents of the task local Extensions after completion
/// and returns it.
///
/// Used by the pub-sub framework itself to make the extensions type-map available within
/// the message processing context. This function is not exposed externally.
pub(crate) async fn with_extensions<T>(
    extensions: Extensions,
    fut: impl Future<Output = T>,
) -> (Extensions, T) {
    EXTENSIONS
        .scope(RefCell::new(extensions), async move {
            let response = fut.await;
            let extensions = RefCell::new(Extensions::new());

            EXTENSIONS.with(|ext| ext.swap(&extensions));

            (extensions.into_inner(), response)
        })
        .await
}

/// Retrieve an item from message-local storage based on its type.
///
/// It returns `None` if the message-local storage does not contain an item of that type.
pub fn get_message_local_item<T: Send + Sync + Clone + 'static>() -> Option<T> {
    EXTENSIONS
        .try_with(|e| e.borrow().get::<T>().cloned())
        .ok()
        .flatten()
}

/// Insert an item in message-local storage.
/// If another item with the same type was already stored in message-local storage it gets replaced.
pub fn set_message_local_item<T: Send + Sync + 'static>(item: T) {
    let _ = EXTENSIONS
        .try_with(|e| e.borrow_mut().insert(item))
        .map_err(|_| tracing::warn!("Failed to set local item in the message context extensions."));
}
