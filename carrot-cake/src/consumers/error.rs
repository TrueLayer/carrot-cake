use std::fmt;

/// The error type returned by message handlers.
///
/// It contains all the information we need for our observability needs, both in terms of
/// logging and metrics.
#[derive(Debug)]
pub struct HandlerError<E> {
    /// The underlying error returned by the message handler.
    pub inner_error: E,
    /// `error_type` distinguishes two classes of errors:
    /// - transient errors; message processing might succeed if retried after a short delay
    /// - fatal errors; no matter how many times you retry, processing will never succeed
    ///
    /// Check out [`ErrorType`]'s documentation for more details.
    pub error_type: ErrorType,
}

impl<E: std::error::Error + 'static> std::error::Error for HandlerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner_error)
    }
}

impl<E: fmt::Display> fmt::Display for HandlerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Handling of a message failed due to a {} issue - ",
            self.error_type
        )?;
        write!(f, ".\n{}", self.inner_error)
    }
}

/// Types of failure when handling a message.
/// Used by the pub sub framework to handle retries/naks/dead letter queues/etc.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ErrorType {
    /// Message processing might succeed if retried after a short delay.
    ///
    /// E.g. the message handler encountered a time out when trying to call an API to fulfill
    /// the message processing requirements.
    ///
    /// The pubsub framework will execute the [transient error hook](crate::consumers::ConsumerTransientErrorHook)
    /// before nacking the message.
    Transient,
    /// Message processing will never succeed, no matter how many times you retry or how long
    /// you wait.
    ///
    /// E.g. the message payload is malformed and cannot be deserialized.
    ///
    /// The message will be nacked.
    Fatal,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient => write!(f, "transient"),
            Self::Fatal => write!(f, "fatal"),
        }
    }
}
