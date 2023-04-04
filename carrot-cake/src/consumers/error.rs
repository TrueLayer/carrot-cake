use std::fmt;
use truelayer_sli::SliErrorType;

/// The error type returned by message handlers.
///
/// It contains all the information we need for our observability needs, both in terms of
/// logging and metrics.
#[derive(Debug)]
pub struct HandlerError {
    /// The underlying error returned by the processing, wrapped in an `anyhow::Error` opaque
    /// error type.
    pub inner_error: anyhow::Error,
    /// `error_type` distinguishes two classes of errors:
    /// - transient errors; message processing might succeed if retried after a short delay
    /// - fatal errors; no matter how many times you retry, processing will never succeed
    ///
    /// Check out [`ErrorType`]'s documentation for more details.
    pub error_type: ErrorType,
    /// The SLI error type assigns the labels defined in
    /// [ADR14](https://github.com/TrueLayer/truelayer-architecture/blob/main/adrs/truelayer-wide/0014-log-structure.md)
    /// to compute our service level indicators (SLIs) for message consumers.
    ///
    /// See [`SliErrorType`]'s documentation for more details.
    pub sli_error_type: SliErrorType,
}

impl std::error::Error for HandlerError {}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Handling of a message failed due to a {} issue - ",
            self.error_type
        )?;
        match &self.sli_error_type {
            SliErrorType::InvalidRequestError => f.write_str("InvalidRequestError"),
            SliErrorType::ServiceError => f.write_str("ServiceError"),
            SliErrorType::ExternalDependencyError { dependency_name } => write!(
                f,
                "ExternalDependencyError[{}]",
                dependency_name
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or_else(|| "unknown")
            ),
            SliErrorType::InternalDependencyError { dependency_name } => write!(
                f,
                "InternalDependencyError[{}]",
                dependency_name
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or_else(|| "unknown")
            ),
        }?;
        write!(f, ".\n{:?}", self.inner_error)
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
