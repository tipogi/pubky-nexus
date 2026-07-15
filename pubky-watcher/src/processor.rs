use std::sync::Arc;
use std::time::Duration;

use tracing::Instrument;

use crate::constants::PROCESSING_TIMEOUT_SECS;
use crate::error::RunError;
use crate::hooks::{
    EventHandler, EventMetadata, EventRetryScheduler, LineParseOutcome, ParseFromLine,
    RetryableError,
};

/// Asynchronous event processor interface for the Watcher service.
#[async_trait::async_trait]
pub trait TEventProcessor<E, Err>: Send + Sync + 'static
where
    E: Send + Sync + 'static + EventMetadata,
    Err: RetryableError + std::fmt::Debug + Send + Sync + 'static,
{
    fn event_handler(&self) -> &Arc<dyn EventHandler<E, Err> + Send + Sync>;

    fn instance_name(&self) -> String;

    fn retry_scheduler(&self) -> Option<&Arc<dyn EventRetryScheduler<E, Err> + Send + Sync>> {
        None
    }

    fn homeserver_id(&self) -> Option<&str> {
        None
    }

    async fn run(self: Arc<Self>) -> Result<(), RunError<Err>> {
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

    async fn run_internal(self: Arc<Self>) -> Result<(), Err>;

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

    async fn handle_error(&self, event: &E, error: Err) -> Result<(), Err> {
        if error.should_not_retry_now() {
            tracing::warn!("Got should-not-retry-now error, stopping batch: {error}");
            return Err(error);
        }

        if !error.should_enqueue_for_retry() {
            tracing::debug!(
                "Error not worth retrying, skipping event {}: {error}",
                event.uri()
            );
            return Ok(());
        }

        let Some(scheduler) = self.retry_scheduler() else {
            return Ok(());
        };

        let Some(homeserver_id) = self.homeserver_id() else {
            tracing::warn!(
                "Retryable error but no origin homeserver to persist; skipping retry for {}",
                event.uri()
            );
            return Ok(());
        };

        if error.is_missing_dependency() {
            scheduler.queue_missing_dep(event, homeserver_id).await
        } else {
            tracing::warn!("Transient error, queuing event for retry: {error}");
            scheduler.queue_transient(event, homeserver_id).await
        }
    }

    async fn should_process_event(&self, _event: &E) -> Result<bool, Err> {
        Ok(true)
    }

    #[tracing::instrument(
        name = "event.process",
        skip_all,
        fields(
            event.resource = %event.resource_label(),
            event.uri = %event.uri(),
            event.r#type = %event.event_type_display(),
            event.user_id = %event.user_id(),
            event.resource_id = %event.resource_id(),
            instance = %self.instance_name(),
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        )
    )]
    async fn handle_event(&self, event: &E) -> Result<(), Err> {
        let span = tracing::Span::current();

        match self.should_process_event(event).await {
            Ok(true) => {}
            Ok(false) => {
                span.record("otel.status_code", "UNSET");
                span.record("otel.status_message", "SKIPPED");
                return Ok(());
            }
            Err(e) => {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_message", tracing::field::display(&e));
                return self.handle_error(event, e).await;
            }
        }

        if let Err(e) = self.event_handler().handle(event).await {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_message", tracing::field::display(&e));

            self.handle_error(event, e).await?;
        } else {
            span.record("otel.status_code", "OK");
        }

        Ok(())
    }
}
