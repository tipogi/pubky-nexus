//! Watchers of one homeserver, for an application that needs no more than a handler and a
//! place to keep cursors.
//!
//! [`Watcher::feed`] reads a homeserver's `/events/` feed, which covers every user it
//! hosts under one cursor. [`Watcher::stream`] reads a known set of users'
//! `/events-stream`s on one homeserver, with one cursor each. Each run returns the cursors
//! that are safe to persist; where they are kept is the application's choice.

#![allow(
    clippy::result_large_err,
    clippy::large_enum_variant,
    reason = "the errors carry the keys involved (a `PublicKey` is about 200 bytes); a run fails at most once"
)]

mod feed_consumer;
mod stream_consumer;

use std::fmt::{Debug, Display};
use std::future::Future;

use pubky::{EventCursor, PublicKey};
use thiserror::Error;

use crate::run::{wait_on_own_task, Panicked, SLOW_RUN_THRESHOLD};
use crate::{FeedReader, HsError, StreamReader};

pub use feed_consumer::{FeedConsumer, FeedConsumerBuilder};
pub use stream_consumer::{
    StreamConsumer, StreamConsumerBuilder, StreamOutcome, DEFAULT_STREAM_PATH,
};

/// The events a run requests when the builder sets no limit.
/// Set another with [`FeedConsumerBuilder::events_limit`] or
/// [`StreamConsumerBuilder::events_limit`].
pub const DEFAULT_EVENTS_LIMIT: u16 = 100;

/// Why a builder refused to build its watcher: see [`FeedConsumerBuilder::build`] and
/// [`StreamConsumerBuilder::build`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The events limit ([`FeedConsumerBuilder::events_limit`],
    /// [`StreamConsumerBuilder::events_limit`]) is zero, so a run could never read an event.
    #[error("the events limit must be greater than zero")]
    ZeroEventsLimit,
    /// [`Watcher::stream`] was given no users to read.
    #[error("a key stream needs at least one user")]
    NoUsers,
    /// [`Watcher::stream`] was given `user_pubkey` more than once, each with its own cursor.
    #[error("a key stream cannot list a user twice")]
    DuplicateUser {
        /// The user listed twice.
        user_pubkey: PublicKey,
    },
    /// The key stream's path ([`StreamConsumerBuilder::path`]) does not start with `/`.
    #[error("the event-stream path must start with '/'")]
    InvalidPath {
        /// The path given to [`StreamConsumerBuilder::path`].
        path: String,
    },
}

/// A homeserver answer that breaks the events protocol. The run fails before any event of
/// the answer is handled, so the cursor it started from is still safe to persist.
/// A failed run carries it as [`WatcherError::Protocol`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ProtocolViolation {
    /// A `/events/` batch had events but no `cursor:` line.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent events without a cursor line")]
    MissingCursor,
    /// A `/events/` batch's cursor is not a number.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent a cursor that is no number: {raw:?}")]
    MalformedCursor {
        /// The cursor as the homeserver sent it.
        /// Without its [`CURSOR_PREFIX`](crate::CURSOR_PREFIX).
        raw: String,
    },
    /// Events came with a cursor that does not move past the one they had to: the one the
    /// run asked from, or on a key stream, the previous event's.
    /// Checked by [`FeedConsumer::run`] and [`StreamConsumer::run`].
    #[error("{}", cursor_not_advanced(.user_pubkey.as_ref(), *.requested, *.got))]
    CursorNotAdvanced {
        /// The user whose stream it was, on a [`StreamConsumer`]; `None` on a
        /// [`FeedConsumer`].
        user_pubkey: Option<PublicKey>,
        /// The cursor the events had to move past.
        /// On a [`StreamConsumer`], the previous event's cursor after the first event.
        requested: u64,
        /// The cursor the homeserver sent.
        /// The batch's cursor on a [`FeedConsumer`], the event's on a [`StreamConsumer`].
        got: u64,
    },
    /// A `/events/` batch had no events but moved the cursor, which would skip whatever lies
    /// between.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent no events but moved the cursor from {requested} to {got}")]
    CursorMovedWithoutEvents {
        /// The cursor the run asked from.
        requested: u64,
        /// The cursor the homeserver sent.
        /// The batch's cursor line ([`FeedBatch::cursor`](crate::FeedBatch::cursor)).
        got: u64,
    },
    /// A user's stream held more events than the run asked for.
    /// Checked by [`StreamConsumer::run`].
    #[error(
        "the stream of {} returned {got} events, more than the {limit} requested",
        .user_pubkey.z32()
    )]
    TooManyEvents {
        /// The user whose stream it was.
        user_pubkey: PublicKey,
        /// The events the stream held.
        got: usize,
        /// The events the run asked for.
        /// Set by [`StreamConsumerBuilder::events_limit`].
        limit: u16,
    },
    /// A user's stream held an event of another user, which would let one user's stream
    /// inject another's events.
    /// Checked by [`StreamConsumer::run`].
    #[error(
        "the stream of {} returned an event of {}",
        .user_pubkey.z32(),
        .event_user_pubkey.z32()
    )]
    ForeignEvent {
        /// The user whose stream it was.
        user_pubkey: PublicKey,
        /// The owner of the event ([`StreamEvent::user_pubkey`](crate::StreamEvent::user_pubkey)).
        event_user_pubkey: PublicKey,
    },
}

fn cursor_not_advanced(user_pubkey: Option<&PublicKey>, requested: u64, got: u64) -> String {
    match user_pubkey {
        None => format!(
            "the homeserver sent events with cursor {got}, which does not move past {requested}"
        ),
        Some(user_pubkey) => format!(
            "the stream of {} returned cursor {got}, which does not move past {requested}",
            user_pubkey.z32()
        ),
    }
}

/// Why a run of a [`FeedConsumer`] or a [`StreamConsumer`] failed. `Err` is the
/// handler's own error, given back as it was returned.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WatcherError<Err> {
    /// A homeserver call failed, with an [`HsError`].
    #[error(transparent)]
    Homeserver(#[from] HsError),
    /// The homeserver's answer breaks the events protocol, as a [`ProtocolViolation`] says.
    #[error(transparent)]
    Protocol(#[from] ProtocolViolation),
    /// The handler failed on an event; the events before it were handled.
    /// `Err` is what the [`EventHandler`](crate::EventHandler) returned.
    ///
    /// The error is displayed but is not this variant's `source()`: `Err` need not implement
    /// [`std::error::Error`], so a `Box<dyn Error>` or a `String` can be one. A reporter that
    /// walks a `WatcherError`'s chain stops at the handler's message, so match this variant to
    /// reach the handler's error and its causes.
    #[error("{0}")]
    Handler(Err),
    /// The run's task panicked; the panic is logged, not returned. A processor reports the
    /// same with [`RunError::Panicked`](crate::RunError::Panicked).
    #[error("the watcher run panicked")]
    Panicked,
}

/// A builder whose handler is not set yet.
/// [`FeedConsumerBuilder::handler`] and [`StreamConsumerBuilder::handler`]
/// replace it.
#[derive(Debug, Default)]
pub struct Missing;

/// How a run of a [`FeedConsumer`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedOutcome {
    /// The cursor to start the next run from, safe to persist: every event before it was
    /// handled.
    /// Pass it to the next [`FeedConsumer::run`].
    pub checkpoint: EventCursor,
    /// The events handed to the handler.
    /// Each went to the handler's [`EventHandler::handle`](crate::EventHandler::handle).
    pub handled_events: usize,
    /// Whether the run handled everything it read. A run stopped by the shutdown signal given
    /// to [`FeedConsumerBuilder::build`] returns `false`, with the cursor it started from,
    /// so the next run reads the batch again.
    pub completed: bool,
}

impl FeedOutcome {
    fn completed(checkpoint: EventCursor, handled_events: usize) -> Self {
        Self {
            checkpoint,
            handled_events,
            completed: true,
        }
    }

    fn interrupted(checkpoint: EventCursor, handled_events: usize) -> Self {
        Self {
            checkpoint,
            handled_events,
            completed: false,
        }
    }
}

/// Picks what to watch on one homeserver: [`Watcher::feed`] reads its `/events/` feed,
/// [`Watcher::stream`] a known set of users' streams.
///
/// ```ignore
/// let consumer = Watcher::feed(WatcherClient::mainnet()?, hs_pubkey)
///     .handler(MyHandler)
///     .build(shutdown_rx)?;
/// let outcome = watcher.run(cursor).await?;
/// // persist outcome.checkpoint
/// ```
pub struct Watcher;

impl Watcher {
    /// Watches `hs_pubkey`'s `/events/` feed, which covers every user it hosts under one
    /// cursor. The handler gets each [`FeedEvent`](crate::FeedEvent).
    pub fn feed<C>(client: C, hs_pubkey: PublicKey) -> FeedConsumerBuilder<Missing>
    where
        C: FeedReader,
    {
        FeedConsumerBuilder::new(client, hs_pubkey)
    }

    /// Watches the `/events-stream` of each of `users` on `hs_pubkey`, from the cursor given
    /// with it. The handler gets each [`StreamEvent`](crate::StreamEvent).
    pub fn stream<C>(
        client: C,
        hs_pubkey: PublicKey,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> StreamConsumerBuilder<Missing>
    where
        C: StreamReader,
    {
        StreamConsumerBuilder::new(client, hs_pubkey, users)
    }
}

/// Runs `read`, a run of the watcher `instance_name` on `hs_pubkey`, on its own task, as
/// [`run_on_own_task`](crate::run_on_own_task) does a processor's, and returns its outcome. A
/// panic is [`WatcherError::Panicked`].
async fn run_consumer<T, Err>(
    instance_name: &'static str,
    hs_pubkey: &str,
    read: impl Future<Output = Result<T, WatcherError<Err>>> + Send + 'static,
) -> Result<T, WatcherError<Err>>
where
    T: Send + 'static,
    Err: Display + Debug + Send + 'static,
{
    match wait_on_own_task(instance_name, Some(hs_pubkey), SLOW_RUN_THRESHOLD, read).await {
        Ok(result) => result,
        Err(Panicked) => Err(WatcherError::Panicked),
    }
}

fn validate_limit(limit: u16) -> Result<(), ConfigError> {
    if limit == 0 {
        return Err(ConfigError::ZeroEventsLimit);
    }
    Ok(())
}
