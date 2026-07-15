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
#[async_trait]
pub trait EventRetryScheduler<E, Err>: Send + Sync {
    async fn queue_missing_dep(&self, event: &E, origin_homeserver_id: &str) -> Result<(), Err>;

    async fn queue_transient(&self, event: &E, origin_homeserver_id: &str) -> Result<(), Err>;
}
