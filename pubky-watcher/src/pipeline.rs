//! Contracts that plug into the generic event-processing pipeline.
//!
//! Downstream crates specialize these traits with a concrete event type and
//! error type. The processor/runner orchestration in this crate stays generic
//! and only depends on the interfaces defined here.

use async_trait::async_trait;

/// Outcome of parsing a single event line from a homeserver.
#[derive(Debug)]
pub enum LineParseOutcome<E> {
    Parsed(E),
    Skipped,
    Unrecognized { reason: String },
}

/// Parses homeserver event lines into a typed event.
pub trait ParseFromLine: Sized {
    type Error;
    fn parse_line(line: &str) -> Result<LineParseOutcome<Self>, Self::Error>;
}

/// Metadata exposed for tracing during event processing.
pub trait EventMetadata {
    fn uri(&self) -> &str;
    fn event_type_display(&self) -> &str;
    fn user_id(&self) -> String;
    fn resource_label(&self) -> String;
    fn resource_id(&self) -> String;
}

/// Classifies errors for retry dispatch in the generic pipeline.
pub trait RetryableError: std::fmt::Display + Send + Sync {
    fn should_not_retry_now(&self) -> bool;
    fn is_missing_dependency(&self) -> bool;
    fn should_enqueue_for_retry(&self) -> bool;
}

/// Handles a parsed event.
#[async_trait]
pub trait EventHandler<E, Err>: Send + Sync {
    async fn handle(&self, event: &E) -> Result<(), Err>;
}

/// Enqueues failed events for later retry.
///
/// Called from [`crate::TEventProcessor::handle_error`] after an error is classified
/// as retryable. The two methods let implementations choose different scheduling
/// policies for dependency failures vs other transient failures.
#[async_trait]
pub trait EventRetryScheduler<E, Err>: Send + Sync {
    /// Queue an event that failed due to a missing dependency.
    ///
    /// Used when [`RetryableError::is_missing_dependency`] is true.
    ///
    /// `origin_homeserver_id` identifies the homeserver the event originated from,
    /// so a later retry can correlate the event back to that source.
    async fn queue_missing_dep(&self, event: &E, origin_homeserver_id: &str) -> Result<(), Err>;

    /// Queue an event that failed with a transient error.
    ///
    /// Used for retryable errors that are not missing dependencies.
    ///
    /// `origin_homeserver_id` identifies the homeserver the event originated from,
    /// so a later retry can correlate the event back to that source.
    async fn queue_transient(&self, event: &E, origin_homeserver_id: &str) -> Result<(), Err>;
}
