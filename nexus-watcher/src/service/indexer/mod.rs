mod homeserver;
mod key_based;

pub use homeserver::HsEventProcessor;
pub use key_based::{KeyBasedEventProcessor, KeyBasedEventSource, PubkyKeyBasedEventSource};

pub use crate::errors::{EventProcessorError, RunError};
pub use crate::events::Event;
pub use pubky_watcher::TEventProcessor;

pub type DynEventProcessor =
    dyn TEventProcessor<Event, EventProcessorError, Output = ()> + Send + Sync;

/// OpenTelemetry meter name shared by all watcher indexer metrics.
pub(super) const METER_NAME: &str = "nexus.watcher";

/// Runs the generic event-processing lifecycle with Nexus-specific tracing.
#[tracing::instrument(
    name = "event.process",
    skip_all,
    fields(
        event.resource = %event.parsed_uri.resource(),
        event.uri = %event.uri,
        event.r#type = %event.event_type,
        event.user_id = %event.parsed_uri.user_id(),
        event.resource_id = event.parsed_uri.resource().id().unwrap_or_default(),
        instance = %processor.instance_name(),
        otel.status_code = tracing::field::Empty,
        otel.status_message = tracing::field::Empty,
    )
)]
pub(super) async fn handle_event_with_tracing<P>(
    processor: &P,
    event: &Event,
) -> Result<(), EventProcessorError>
where
    P: TEventProcessor<Event, EventProcessorError> + ?Sized,
{
    let span = tracing::Span::current();

    match processor.should_process_event(event).await {
        Ok(true) => {}
        Ok(false) => {
            span.record("otel.status_code", "UNSET");
            span.record("otel.status_message", "SKIPPED");
            return Ok(());
        }
        Err(error) => {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_message", tracing::field::display(&error));
            return processor.handle_error(event, error).await;
        }
    }

    if let Err(error) = processor.event_handler().handle(event).await {
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_message", tracing::field::display(&error));
        processor.handle_error(event, error).await?;
    } else {
        span.record("otel.status_code", "OK");
    }

    Ok(())
}
