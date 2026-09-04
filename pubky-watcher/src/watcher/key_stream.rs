use std::sync::Arc;

use futures_util::StreamExt;
use pubky::{Event, EventCursor, PublicKey};
use tokio::sync::watch::Receiver;
use tracing::debug;

use crate::processor::TEventProcessor;
use crate::traits::EventHandler;
use crate::KeyEventSource;

use super::{
    invalid_data, invalid_input, run_processor, validate_limit, DynHandler, Missing, WatchOutcome,
    WatcherError, DEFAULT_EVENTS_LIMIT,
};

/// Builder for multiple user streams on one homeserver.
pub struct KeyStreamWatcherBuilder<H> {
    homeserver: PublicKey,
    users: Vec<(PublicKey, EventCursor)>,
    handler: H,
    events_limit: u16,
    path: String,
    event_source: Arc<dyn KeyEventSource>,
}

impl KeyStreamWatcherBuilder<Missing> {
    pub(super) fn new<C>(
        client: C,
        homeserver: PublicKey,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> Self
    where
        C: KeyEventSource,
    {
        Self {
            homeserver,
            users,
            handler: Missing,
            events_limit: DEFAULT_EVENTS_LIMIT,
            path: "/pub/".to_string(),
            event_source: Arc::new(client),
        }
    }
}

impl<H> KeyStreamWatcherBuilder<H> {
    /// Maximum number of events requested in this run.
    pub fn events_limit(mut self, limit: u16) -> Self {
        self.events_limit = limit;
        self
    }

    /// Restrict stream events to this resource-path prefix.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Handler invoked for each Pubky stream event.
    pub fn handler<NewH>(self, handler: NewH) -> KeyStreamWatcherBuilder<NewH>
    where
        NewH: EventHandler<Event, WatcherError> + Send + Sync + 'static,
    {
        KeyStreamWatcherBuilder {
            homeserver: self.homeserver,
            users: self.users,
            handler,
            events_limit: self.events_limit,
            path: self.path,
            event_source: self.event_source,
        }
    }
}

impl<H> KeyStreamWatcherBuilder<H>
where
    H: EventHandler<Event, WatcherError> + Send + Sync + 'static,
{
    /// Build the watcher. The caller retains responsibility for cursor storage.
    pub fn build(self, shutdown_rx: Receiver<bool>) -> Result<KeyStreamWatcher, WatcherError> {
        validate_limit(self.events_limit)?;
        validate_users(&self.users)?;
        validate_path(&self.path)?;

        Ok(KeyStreamWatcher {
            config: Arc::new(KeyStreamConfig {
                homeserver: self.homeserver,
                users: self.users,
                handler: Arc::new(self.handler),
                events_limit: self.events_limit,
                path: self.path,
                event_source: self.event_source,
                shutdown_rx,
            }),
        })
    }
}

struct KeyStreamConfig {
    homeserver: PublicKey,
    users: Vec<(PublicKey, EventCursor)>,
    handler: Arc<DynHandler<Event>>,
    events_limit: u16,
    path: String,
    event_source: Arc<dyn KeyEventSource>,
    shutdown_rx: Receiver<bool>,
}

/// Cursor result for a multi-user key-stream run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyStreamOutcome {
    /// Safe cursor for each configured user, in input order.
    pub cursors: Vec<(PublicKey, EventCursor)>,
    /// Number of events delivered successfully to the handler.
    pub processed_events: usize,
    /// Whether the fetched stream was fully processed.
    pub completed: bool,
}

impl KeyStreamOutcome {
    fn new(
        cursors: Vec<(PublicKey, EventCursor)>,
        processed_events: usize,
        completed: bool,
    ) -> Self {
        Self {
            cursors,
            processed_events,
            completed,
        }
    }
}

/// Polls multiple users through `/events-stream` on one homeserver.
pub struct KeyStreamWatcher {
    config: Arc<KeyStreamConfig>,
}

impl KeyStreamWatcher {
    /// Process one finite stream and return a safe cursor for each user.
    pub async fn run(&self) -> Result<KeyStreamOutcome, WatcherError> {
        run_processor::<Event, KeyStreamOutcome, _>(KeyStreamEventProcessor {
            config: self.config.clone(),
        })
        .await
    }
}

struct KeyStreamEventProcessor {
    config: Arc<KeyStreamConfig>,
}

#[async_trait::async_trait]
impl TEventProcessor<Event, WatcherError> for KeyStreamEventProcessor {
    type Output = KeyStreamOutcome;

    fn event_handler(&self) -> &Arc<DynHandler<Event>> {
        &self.config.handler
    }

    fn instance_name(&self) -> String {
        format!(
            "KeyStreamWatcher({}; {} users)",
            self.config.homeserver,
            self.config.users.len()
        )
    }

    async fn run_internal(self: Arc<Self>) -> Result<Self::Output, WatcherError> {
        let mut cursors = self.config.users.clone();
        let mut processed_events = 0;
        for user_index in 0..self.config.users.len() {
            if *self.config.shutdown_rx.borrow() {
                debug!(
                    homeserver = %self.config.homeserver,
                    processed_events,
                    "Key stream watcher interrupted"
                );
                return Ok(KeyStreamOutcome::new(cursors, processed_events, false));
            }

            let (user, cursor) = {
                let (user, cursor) = &self.config.users[user_index];
                (user.clone(), *cursor)
            };
            let outcome = self.process_user(&user, cursor).await?;
            cursors[user_index].1 = outcome.cursor;
            processed_events += outcome.processed_events;

            if !outcome.completed {
                return Ok(KeyStreamOutcome::new(cursors, processed_events, false));
            }
        }

        debug!(
            homeserver = %self.config.homeserver,
            user_count = self.config.users.len(),
            processed_events,
            "Key stream watcher completed"
        );
        Ok(KeyStreamOutcome::new(cursors, processed_events, true))
    }
}

impl KeyStreamEventProcessor {
    async fn process_user(
        &self,
        user: &PublicKey,
        cursor: EventCursor,
    ) -> Result<WatchOutcome, WatcherError> {
        debug!(
            homeserver = %self.config.homeserver,
            %user,
            cursor = cursor.id(),
            limit = self.config.events_limit,
            path = %self.config.path,
            "Polling key event stream"
        );
        let mut stream = self
            .config
            .event_source
            .key_event_stream(
                &self.config.homeserver,
                user,
                cursor,
                self.config.events_limit,
                &self.config.path,
            )
            .await?;

        let mut events = Vec::with_capacity(self.config.events_limit as usize);
        while let Some(result) = stream.next().await {
            if events.len() >= self.config.events_limit as usize {
                return Err(invalid_data(
                    "event stream returned more than the requested limit",
                ));
            }
            events.push(result?);
        }

        validate_stream_events(user, cursor, &events)?;
        debug!(
            homeserver = %self.config.homeserver,
            %user,
            event_count = events.len(),
            "Fetched key event stream"
        );

        let mut safe_cursor = cursor;
        let mut processed_events = 0;
        for event in events {
            if *self.config.shutdown_rx.borrow() {
                debug!(
                    homeserver = %self.config.homeserver,
                    %user,
                    cursor = safe_cursor.id(),
                    processed_events,
                    "Key stream watcher interrupted"
                );
                return Ok(WatchOutcome::interrupted(safe_cursor, processed_events));
            }

            self.handle_event(&event).await?;
            safe_cursor = event.cursor;
            processed_events += 1;
        }

        debug!(
            homeserver = %self.config.homeserver,
            %user,
            cursor = safe_cursor.id(),
            processed_events,
            "Key stream completed"
        );
        Ok(WatchOutcome::completed(safe_cursor, processed_events))
    }
}

fn validate_users(users: &[(PublicKey, EventCursor)]) -> Result<(), WatcherError> {
    if users.is_empty() {
        return Err(invalid_input("at least one user stream is required"));
    }
    for (index, (user, _)) in users.iter().enumerate() {
        if users[..index].iter().any(|(existing, _)| existing == user) {
            return Err(invalid_input(
                "a key event stream cannot contain duplicate users",
            ));
        }
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), WatcherError> {
    if !path.starts_with('/') {
        return Err(invalid_input("event-stream path must start with '/'"));
    }
    Ok(())
}

fn validate_stream_events(
    user: &PublicKey,
    requested: EventCursor,
    events: &[Event],
) -> Result<(), WatcherError> {
    let mut floor = requested.id();
    for event in events {
        if &event.resource.owner != user {
            return Err(invalid_data(
                "event stream returned an event for a different user",
            ));
        }
        if event.cursor.id() <= floor {
            return Err(invalid_data("event stream returned a non-advancing cursor"));
        }
        floor = event.cursor.id();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures_util::stream;
    use pubky::{Event, EventCursor, Keypair, PublicKey};

    use crate::{
        ClientResult, EventHandler, KeyEventSource, KeyEventStream, Watcher, WatcherError,
    };

    struct EmptySource;

    #[async_trait]
    impl KeyEventSource for EmptySource {
        async fn key_event_stream(
            &self,
            _homeserver: &PublicKey,
            _user: &PublicKey,
            _cursor: EventCursor,
            _limit: u16,
            _path: &str,
        ) -> ClientResult<KeyEventStream> {
            Ok(Box::pin(stream::empty()))
        }
    }

    struct IgnoreEvents;

    #[async_trait]
    impl EventHandler<Event, WatcherError> for IgnoreEvents {
        async fn handle(&self, _event: &Event) -> Result<(), WatcherError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn explicit_source_does_not_require_global_initialization() {
        let homeserver = Keypair::random().public_key();
        let user = Keypair::random().public_key();
        let users = vec![(user.clone(), EventCursor::new(9))];
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let watcher = Watcher::key_stream(EmptySource, homeserver, users)
            .handler(IgnoreEvents)
            .build(shutdown_rx)
            .unwrap();

        let outcome = watcher.run().await.unwrap();
        assert_eq!(outcome.cursors, vec![(user, EventCursor::new(9))]);
        assert!(outcome.completed);
    }
}
