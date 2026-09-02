use std::sync::Arc;
use std::time::Duration;

use tracing::Instrument;

use crate::traits::{
    EventHandler, EventRetryScheduler, LineParseOutcome, ParseFromLine, RetryableError,
};

/// Per-homeserver hard timeout (seconds).
pub const PROCESSING_TIMEOUT_SECS: u64 = 3_600;

/// Outcome of a single [`TEventProcessor::run`].
#[derive(Debug)]
pub enum RunError<E> {
    Internal(E),
    Panicked,
    TimedOut,
}

impl<E> RunError<E> {
    pub fn is_panic(&self) -> bool {
        matches!(self, RunError::Panicked)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, RunError::TimedOut)
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RunError<E> {}

impl<E: std::fmt::Display> std::fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Internal(err) => write!(f, "Internal error: {err}"),
            RunError::Panicked => write!(f, "Execution panicked"),
            RunError::TimedOut => write!(f, "Execution timed out"),
        }
    }
}

/// Classifies `error` with [`RetryableError`] and enqueues retryable failures.
///
/// Opt-in helper for retry-aware consumers. Call from an overridden
/// [`TEventProcessor::handle_error`]. The default `handle_error` does not use this —
/// it fails fast without requiring [`RetryableError`].
pub async fn dispatch_retryable_error<E, Err>(
    event: &E,
    error: Err,
    scheduler: &dyn EventRetryScheduler<E, Err>,
    origin_homeserver_id: &str,
) -> Result<(), Err>
where
    Err: RetryableError,
{
    if error.should_not_retry_now() {
        tracing::warn!("Got should-not-retry-now error, stopping batch: {error}");
        return Err(error);
    }

    if !error.should_enqueue_for_retry() {
        tracing::debug!("Error not worth retrying, skipping event: {error}");
        return Ok(());
    }

    if error.is_missing_dependency() {
        scheduler
            .queue_missing_dep(event, origin_homeserver_id)
            .await
    } else {
        tracing::warn!("Transient error, queuing event for retry: {error}");
        scheduler.queue_transient(event, origin_homeserver_id).await
    }
}

/// Asynchronous event processor interface for the Watcher service.
#[async_trait::async_trait]
pub trait TEventProcessor<E, Err>: Send + Sync + 'static
where
    E: Send + Sync + 'static,
    Err: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
{
    /// Value produced after a successful processor run.
    type Output: Send + 'static;

    fn event_handler(&self) -> &Arc<dyn EventHandler<E, Err> + Send + Sync>;

    fn instance_name(&self) -> String;

    async fn run(self: Arc<Self>) -> Result<Self::Output, RunError<Err>> {
        let timeout = self
            .custom_timeout()
            .unwrap_or(Duration::from_secs(PROCESSING_TIMEOUT_SECS));

        let instance_name = self.instance_name();
        let span = tracing::info_span!("event_processor.run", service = %instance_name);
        let handle = tokio::spawn(self.run_internal().instrument(span));

        let join_result = tokio::time::timeout(timeout, handle)
            .await
            .inspect_err(|_| tracing::error!("Event processor timed out for {instance_name}"))
            .map_err(|_| RunError::TimedOut)?;

        let run_internal_result = join_result
            .inspect_err(|je| {
                tracing::error!("JoinError by event processor for {instance_name}: {je:?}")
            })
            .map_err(|_| RunError::Panicked)?;

        run_internal_result
            .inspect_err(|e| tracing::error!("Event processor failed for {instance_name}: {e:?}"))
            .map_err(RunError::Internal)
    }

    async fn run_internal(self: Arc<Self>) -> Result<Self::Output, Err>;

    fn custom_timeout(&self) -> Option<Duration> {
        None
    }

    async fn process_event_line(&self, line: &str) -> Result<(), Err>
    where
        E: ParseFromLine<Error = Err>,
    {
        match E::parse_line(line) {
            Err(e) => tracing::warn!("{e}"),
            Ok(LineParseOutcome::Skipped) => {}
            Ok(LineParseOutcome::Unrecognized { reason }) => {
                tracing::warn!("Unrecognized event URI: {reason}");
            }
            Ok(LineParseOutcome::Parsed(event)) => {
                tracing::debug!("Processing event: {:?}", std::any::type_name::<E>());
                self.handle_event(&event).await?;
            }
        }

        Ok(())
    }

    /// Default: fail fast (propagate the error). No retry classification.
    ///
    /// Retry-aware consumers should override this and call
    /// [`dispatch_retryable_error`] with a [`RetryableError`] implementor.
    async fn handle_error(&self, _event: &E, error: Err) -> Result<(), Err> {
        Err(error)
    }

    async fn should_process_event(&self, _event: &E) -> Result<bool, Err> {
        Ok(true)
    }

    async fn handle_event(&self, event: &E) -> Result<(), Err> {
        match self.should_process_event(event).await {
            Ok(true) => {}
            Ok(false) => return Ok(()),
            Err(e) => return self.handle_error(event, e).await,
        }

        if let Err(e) = self.event_handler().handle(event).await {
            self.handle_error(event, e).await?;
        }

        Ok(())
    }
}
