//! The Consumers: ready-made ways to consume one Homeserver's Events, for an Indexer that
//! needs no more than a Handler and a place to keep Cursors.
//!
//! [`Watcher::feed`] builds a Feed consumer, which reads a Homeserver's Events feed: every
//! user it hosts, under one Cursor. [`Watcher::stream`] builds a Stream consumer, which reads
//! the Event streams of chosen User pubkeys on one Homeserver, with one Cursor each. Each is
//! given a Handler, and each call makes one Run and returns its Checkpoint, or one per User
//! pubkey; where it is stored is the Indexer's choice.

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

/// The Events a Run asks for when the builder sets no limit.
/// Set another with [`FeedConsumerBuilder::events_limit`] or
/// [`StreamConsumerBuilder::events_limit`].
pub const DEFAULT_EVENTS_LIMIT: u16 = 100;

/// Why a builder refused to build its Consumer: see [`FeedConsumerBuilder::build`] and
/// [`StreamConsumerBuilder::build`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The events limit ([`FeedConsumerBuilder::events_limit`],
    /// [`StreamConsumerBuilder::events_limit`]) is zero, so a Run could never read an Event.
    #[error("the events limit must be greater than zero")]
    ZeroEventsLimit,
    /// The Event stream's path ([`StreamConsumerBuilder::path`]) does not start with `/`.
    #[error("the event-stream path must start with '/'")]
    InvalidPath {
        /// The path given to [`StreamConsumerBuilder::path`].
        path: String,
    },
}

/// A Protocol violation: an answer from a Homeserver that breaks the rules of its Events
/// feed or Event stream. The Run fails before any Event in that answer is handled, so the
/// Cursor it started from is still its Checkpoint.
/// A failed Run carries it as [`WatcherError::Protocol`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ProtocolViolation {
    /// An Events feed's Batch had Events but no `cursor:` line.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent events without a cursor line")]
    MissingCursor,
    /// The Cursor closing an Events feed's Batch is not a number.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent a cursor that is no number: {raw:?}")]
    MalformedCursor {
        /// The Cursor as the Homeserver sent it.
        /// Without its [`CURSOR_PREFIX`](crate::CURSOR_PREFIX).
        raw: String,
    },
    /// Events came with a Cursor that does not move past the one they had to: the one the
    /// Run read from, or on an Event stream, the previous Event's.
    /// Checked by [`FeedConsumer::run`] and [`StreamConsumer::run`].
    #[error("{}", cursor_not_advanced(.user_pubkey.as_ref(), *.requested, *.got))]
    CursorNotAdvanced {
        /// The User pubkey whose Event stream it was, on a [`StreamConsumer`]; `None` on a
        /// [`FeedConsumer`].
        user_pubkey: Option<PublicKey>,
        /// The Cursor the Events had to move past.
        /// On a [`StreamConsumer`], the previous Event's Cursor after the first Event.
        requested: u64,
        /// The Cursor the Homeserver sent.
        /// The Batch's Cursor on a [`FeedConsumer`], the Event's on a [`StreamConsumer`].
        got: u64,
    },
    /// An Events feed's Batch had no Events but moved the Cursor, which would skip whatever
    /// lies between.
    /// Checked by [`FeedConsumer::run`].
    #[error("the homeserver sent no events but moved the cursor from {requested} to {got}")]
    CursorMovedWithoutEvents {
        /// The Cursor the Run read from.
        requested: u64,
        /// The Cursor the Homeserver sent.
        /// The Batch's cursor line ([`FeedBatch::cursor`](crate::FeedBatch::cursor)).
        got: u64,
    },
    /// An Event stream's Batch held more Events than the Run asked for.
    /// Checked by [`StreamConsumer::run`].
    #[error(
        "the stream of {} returned {got} events, more than the {limit} requested",
        .user_pubkey.z32()
    )]
    TooManyEvents {
        /// The User pubkey whose Event stream it was.
        user_pubkey: PublicKey,
        /// The Events the Batch held.
        got: usize,
        /// The Events the Run asked for.
        /// Set by [`StreamConsumerBuilder::events_limit`].
        limit: u16,
    },
    /// A User pubkey's Event stream held an Event of another User pubkey, which would let
    /// one user's Event stream inject another's Events.
    /// Checked by [`StreamConsumer::run`].
    #[error(
        "the stream of {} returned an event of {}",
        .user_pubkey.z32(),
        .event_user_pubkey.z32()
    )]
    ForeignEvent {
        /// The User pubkey whose Event stream it was.
        user_pubkey: PublicKey,
        /// The User pubkey of the Event
        /// ([`StreamEvent::user_pubkey`](crate::StreamEvent::user_pubkey)).
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

/// Why a Run of a [`FeedConsumer`] or a [`StreamConsumer`] failed. `Err` is the
/// Handler's own error, given back as it was returned.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WatcherError<Err> {
    /// A Read of the Homeserver failed, with an [`HsError`].
    #[error(transparent)]
    Homeserver(#[from] HsError),
    /// The Homeserver's answer is a Protocol violation, as a [`ProtocolViolation`] says.
    #[error(transparent)]
    Protocol(#[from] ProtocolViolation),
    /// The Handler failed on an Event; the Events before it were handled.
    /// `Err` is what the [`EventHandler`](crate::EventHandler) returned.
    ///
    /// The error is displayed but is not this variant's `source()`: `Err` need not implement
    /// [`std::error::Error`], so a `Box<dyn Error>` or a `String` can be one. A reporter that
    /// walks a `WatcherError`'s chain stops at the Handler's message, so match this variant to
    /// reach the Handler's error and its causes.
    #[error("{0}")]
    Handler(Err),
    /// A Run of a [`StreamConsumer`] was given `user_pubkey` more than once, each with its
    /// own Cursor. [`StreamConsumer::run`] checks its users before any Read, so nothing was
    /// read and nothing was handled.
    #[error("the run lists the user {} twice", .user_pubkey.z32())]
    DuplicateUser {
        /// The User pubkey listed twice.
        user_pubkey: PublicKey,
    },
    /// The Run's task panicked; the panic is logged, not returned. An Indexer's own
    /// [`Run`](crate::Run) reports the same with
    /// [`RunError::Panicked`](crate::RunError::Panicked).
    #[error("the watcher run panicked")]
    Panicked,
}

/// A builder whose Handler is not set yet.
/// [`FeedConsumerBuilder::handler`] and [`StreamConsumerBuilder::handler`]
/// replace it.
#[derive(Debug, Default)]
pub struct Missing;

/// How a Run of a [`FeedConsumer`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedOutcome {
    /// The Checkpoint: the Cursor the Run returns, with every Event before it handled. It is
    /// the one an Indexer may store.
    /// Pass it to the next [`FeedConsumer::run`].
    pub checkpoint: EventCursor,
    /// The Events handed to the Handler.
    /// Each went to the Handler's [`EventHandler::handle`](crate::EventHandler::handle).
    pub handled_events: usize,
    /// Whether the Run handled everything it read. A Run stopped by the shutdown signal given
    /// to [`FeedConsumerBuilder::build`] returns `false`, with the Cursor it started from as
    /// its Checkpoint, so the next Run reads the Batch again.
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

/// The crate's entry type: picks the Consumer of one Homeserver. [`Watcher::feed`] builds a
/// Feed consumer, which reads its Events feed, and [`Watcher::stream`] a Stream consumer,
/// which reads the Event streams of chosen User pubkeys.
///
/// ```ignore
/// let consumer = Watcher::feed(WatcherClient::mainnet()?, hs_pubkey)
///     .handler(MyHandler)
///     .build(shutdown_rx)?;
/// let outcome = consumer.run(cursor).await?;
/// // store outcome.checkpoint
/// ```
pub struct Watcher;

impl Watcher {
    /// A Feed consumer of the Homeserver `hs_pubkey`: it reads its Events feed, which covers
    /// every user it hosts under one Cursor. The Handler gets each
    /// [`FeedEvent`](crate::FeedEvent).
    pub fn feed<C>(client: C, hs_pubkey: PublicKey) -> FeedConsumerBuilder<Missing>
    where
        C: FeedReader,
    {
        FeedConsumerBuilder::new(client, hs_pubkey)
    }

    /// A Stream consumer of the Homeserver `hs_pubkey`: it reads the Event streams of chosen
    /// User pubkeys on it. The users are not given here: each [`StreamConsumer::run`] takes
    /// them, each with the Cursor to read it from, and returns one Checkpoint per User pubkey.
    /// The Handler gets each [`StreamEvent`](crate::StreamEvent).
    ///
    /// ```ignore
    /// let consumer = Watcher::stream(WatcherClient::mainnet()?, hs_pubkey)
    ///     .handler(MyHandler)
    ///     .build(shutdown_rx)?;
    /// let outcome = consumer.run(users).await?;
    /// // store outcome.checkpoints: they are the next Run's users as they are
    /// ```
    pub fn stream<C>(client: C, hs_pubkey: PublicKey) -> StreamConsumerBuilder<Missing>
    where
        C: StreamReader,
    {
        StreamConsumerBuilder::new(client, hs_pubkey)
    }
}

/// Runs `read`, a Run of the Consumer `instance_name` on `hs_pubkey`, on its own task, as
/// [`run_on_own_task`](crate::run_on_own_task) does an Indexer's own, and returns its
/// outcome. A panic is [`WatcherError::Panicked`].
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
