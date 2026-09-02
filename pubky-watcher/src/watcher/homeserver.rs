use std::sync::Arc;

use pubky::{EventCursor, Method, PublicKey};
use tokio::sync::watch::Receiver;
use tracing::{debug, warn};

use crate::events::{read_stream_capped, EventBatch, HomeserverEvent};
use crate::processor::TEventProcessor;
use crate::traits::{EventHandler, LineParseOutcome, ParseFromLine};
use crate::PubkyConnector;

use super::{
    invalid_data, run_processor, validate_limit, DynHandler, Missing, WatchOutcome, WatcherError,
    DEFAULT_EVENTS_LIMIT, MAX_EVENTS_BODY,
};

/// Builder for a single homeserver-wide `/events/` watcher.
pub struct HomeserverWatcherBuilder<H> {
    homeserver: PublicKey,
    handler: H,
    events_limit: u16,
}

impl HomeserverWatcherBuilder<Missing> {
    pub(super) fn new(homeserver: PublicKey) -> Self {
        Self {
            homeserver,
            handler: Missing,
            events_limit: DEFAULT_EVENTS_LIMIT,
        }
    }
}

impl<H> HomeserverWatcherBuilder<H> {
    /// Maximum number of events requested in this run.
    pub fn events_limit(mut self, limit: u16) -> Self {
        self.events_limit = limit;
        self
    }

    /// Handler invoked for each parsed `PUT` or `DEL` event.
    pub fn handler<NewH>(self, handler: NewH) -> HomeserverWatcherBuilder<NewH>
    where
        NewH: EventHandler<HomeserverEvent, WatcherError> + Send + Sync + 'static,
    {
        HomeserverWatcherBuilder {
            homeserver: self.homeserver,
            handler,
            events_limit: self.events_limit,
        }
    }
}

impl<H> HomeserverWatcherBuilder<H>
where
    H: EventHandler<HomeserverEvent, WatcherError> + Send + Sync + 'static,
{
    /// Build the watcher. The caller retains responsibility for cursor storage.
    pub fn build(self, shutdown_rx: Receiver<bool>) -> Result<HomeserverWatcher, WatcherError> {
        validate_limit(self.events_limit)?;
        Ok(HomeserverWatcher {
            config: Arc::new(HomeserverConfig {
                homeserver: self.homeserver,
                handler: Arc::new(self.handler),
                events_limit: self.events_limit,
                shutdown_rx,
            }),
        })
    }
}

struct HomeserverConfig {
    homeserver: PublicKey,
    handler: Arc<DynHandler<HomeserverEvent>>,
    events_limit: u16,
    shutdown_rx: Receiver<bool>,
}

/// Polls one homeserver-wide `/events/` feed.
pub struct HomeserverWatcher {
    config: Arc<HomeserverConfig>,
}

impl HomeserverWatcher {
    /// Process one batch from `cursor` and return the cursor safe for the next run.
    pub async fn run(&self, cursor: EventCursor) -> Result<WatchOutcome, WatcherError> {
        run_processor::<HomeserverEvent, WatchOutcome, _>(HomeserverEventProcessor {
            config: self.config.clone(),
            cursor,
        })
        .await
    }
}

struct HomeserverEventProcessor {
    config: Arc<HomeserverConfig>,
    cursor: EventCursor,
}

#[async_trait::async_trait]
impl TEventProcessor<HomeserverEvent, WatcherError> for HomeserverEventProcessor {
    type Output = WatchOutcome;

    fn event_handler(&self) -> &Arc<DynHandler<HomeserverEvent>> {
        &self.config.handler
    }

    fn instance_name(&self) -> String {
        format!("HomeserverWatcher({})", self.config.homeserver)
    }

    async fn run_internal(self: Arc<Self>) -> Result<Self::Output, WatcherError> {
        if *self.config.shutdown_rx.borrow() {
            debug!(
                homeserver = %self.config.homeserver,
                cursor = self.cursor.id(),
                "Homeserver watcher interrupted before polling"
            );
            return Ok(WatchOutcome::interrupted(self.cursor, 0));
        }

        let body = self.poll_events().await?;
        let batch = EventBatch::from_body(&body);
        let next_cursor = validate_batch_cursor(self.cursor, &batch)?;
        debug!(
            homeserver = %self.config.homeserver,
            event_count = batch.event_lines.len(),
            next_cursor = next_cursor.id(),
            "Fetched homeserver event batch"
        );
        let mut processed_events = 0;

        for line in &batch.event_lines {
            if *self.config.shutdown_rx.borrow() {
                debug!(
                    homeserver = %self.config.homeserver,
                    cursor = self.cursor.id(),
                    processed_events,
                    "Homeserver watcher interrupted"
                );
                return Ok(WatchOutcome::interrupted(self.cursor, processed_events));
            }

            match HomeserverEvent::parse_line(line)? {
                LineParseOutcome::Parsed(event) => {
                    self.handle_event(&event).await?;
                    processed_events += 1;
                }
                LineParseOutcome::Skipped => {}
                LineParseOutcome::Unrecognized { reason } => {
                    warn!(%reason, "Skipping unrecognized homeserver event");
                }
            }
        }

        debug!(
            homeserver = %self.config.homeserver,
            cursor = next_cursor.id(),
            processed_events,
            "Homeserver watcher completed"
        );
        Ok(WatchOutcome::completed(next_cursor, processed_events))
    }
}

impl HomeserverEventProcessor {
    async fn poll_events(&self) -> Result<String, WatcherError> {
        let url = format!(
            "https://{}/events/?cursor={}&limit={}",
            self.config.homeserver,
            self.cursor.id(),
            self.config.events_limit
        );
        debug!(
            homeserver = %self.config.homeserver,
            cursor = self.cursor.id(),
            limit = self.config.events_limit,
            "Polling homeserver events"
        );

        let response = PubkyConnector::get()?
            .client()
            .request(Method::GET, &url)
            .send()
            .await?
            .error_for_status()?;

        let (bytes, exceeded) =
            read_stream_capped(response.bytes_stream(), MAX_EVENTS_BODY).await?;
        if exceeded {
            return Err(invalid_data("homeserver event response exceeded 10 MiB"));
        }
        String::from_utf8(bytes).map_err(Into::into)
    }
}

fn validate_batch_cursor(
    requested: EventCursor,
    batch: &EventBatch<'_>,
) -> Result<EventCursor, WatcherError> {
    let raw = batch
        .cursor
        .ok_or_else(|| invalid_data("homeserver response did not contain a cursor"))?;
    let id = raw
        .parse::<u64>()
        .map_err(|_| invalid_data("homeserver returned a non-numeric cursor"))?;

    if batch.has_events() && id <= requested.id() {
        return Err(invalid_data(
            "homeserver batch cursor did not advance after events",
        ));
    }
    if !batch.has_events() && id != requested.id() {
        return Err(invalid_data("idle homeserver response changed the cursor"));
    }

    Ok(EventCursor::new(id))
}

#[cfg(test)]
mod tests {
    use super::validate_batch_cursor;
    use crate::EventBatch;
    use pubky::EventCursor;

    #[test]
    fn event_batch_cursor_must_advance() {
        let batch = EventBatch::from_body("PUT pubky://example/pub/x\ncursor: 41");
        assert!(validate_batch_cursor(EventCursor::new(41), &batch).is_err());
    }

    #[test]
    fn idle_batch_cursor_must_stay_at_requested_position() {
        let batch = EventBatch::from_body("cursor: 42");
        assert!(validate_batch_cursor(EventCursor::new(41), &batch).is_err());
        assert_eq!(
            validate_batch_cursor(EventCursor::new(42), &batch)
                .unwrap()
                .id(),
            42
        );
    }

    #[test]
    fn cursor_must_be_present_and_numeric() {
        assert!(validate_batch_cursor(
            EventCursor::new(41),
            &EventBatch::from_body("PUT pubky://example/pub/x")
        )
        .is_err());
        assert!(validate_batch_cursor(
            EventCursor::new(41),
            &EventBatch::from_body("PUT pubky://example/pub/x\ncursor: nope")
        )
        .is_err());
    }
}
