//! Single-homeserver convenience watchers.
//!
//! [`Watcher::homeserver`] polls one homeserver-wide `/events/` feed.
//! [`Watcher::key_stream`] polls users through `/events-stream` on one
//! homeserver. Both return the cursor that is safe to persist after the run.

mod homeserver;
mod key_stream;

use std::io::{Error, ErrorKind};
use std::sync::Arc;

use pubky::{EventCursor, PublicKey};

use crate::processor::{RunError, TEventProcessor};
use crate::{HomeserverEventSource, KeyEventSource};

pub use homeserver::{HomeserverWatcher, HomeserverWatcherBuilder};
pub use key_stream::{KeyStreamOutcome, KeyStreamWatcher, KeyStreamWatcherBuilder};

pub(super) const DEFAULT_EVENTS_LIMIT: u16 = 100;
pub(super) const MAX_EVENTS_BODY: usize = 10 * 1024 * 1024;
pub(super) type DynHandler<E> = dyn crate::EventHandler<E, WatcherError> + Send + Sync;

/// Error returned by convenience watcher construction or execution.
pub type WatcherError = Box<dyn std::error::Error + Send + Sync>;

/// Marker for a required builder field that has not been supplied.
#[derive(Debug, Default)]
pub struct Missing;

/// Result of one watcher run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchOutcome {
    /// Cursor safe to supply to the next watcher run.
    pub cursor: EventCursor,
    /// Number of events delivered successfully to the handler.
    pub processed_events: usize,
    /// Whether the fetched batch or stream was fully processed.
    pub completed: bool,
}

impl WatchOutcome {
    pub(super) fn completed(cursor: EventCursor, processed_events: usize) -> Self {
        Self {
            cursor,
            processed_events,
            completed: true,
        }
    }

    pub(super) fn interrupted(cursor: EventCursor, processed_events: usize) -> Self {
        Self {
            cursor,
            processed_events,
            completed: false,
        }
    }
}

/// Entry point for selecting a single-homeserver indexing mode.
pub struct Watcher;

impl Watcher {
    /// Configure one homeserver-wide `GET /events/` poller.
    pub fn homeserver<C>(client: C, homeserver: PublicKey) -> HomeserverWatcherBuilder<Missing>
    where
        C: HomeserverEventSource,
    {
        HomeserverWatcherBuilder::new(client, homeserver)
    }

    /// Configure a multi-user `/events-stream` poller on one homeserver.
    pub fn key_stream<C>(
        client: C,
        homeserver: PublicKey,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> KeyStreamWatcherBuilder<Missing>
    where
        C: KeyEventSource,
    {
        KeyStreamWatcherBuilder::new(client, homeserver, users)
    }
}

pub(super) async fn run_processor<E, Output, P>(processor: P) -> Result<Output, WatcherError>
where
    E: Send + Sync + 'static,
    Output: Send + 'static,
    P: TEventProcessor<E, WatcherError, Output = Output>,
{
    Arc::new(processor)
        .run()
        .await
        .map_err(|error: RunError<WatcherError>| Box::new(error) as WatcherError)
}

pub(super) fn validate_limit(limit: u16) -> Result<(), WatcherError> {
    if limit == 0 {
        return Err(invalid_input("events limit must be greater than zero"));
    }
    Ok(())
}

pub(super) fn invalid_input(message: &'static str) -> WatcherError {
    Error::new(ErrorKind::InvalidInput, message).into()
}

pub(super) fn invalid_data(message: &'static str) -> WatcherError {
    Error::new(ErrorKind::InvalidData, message).into()
}
