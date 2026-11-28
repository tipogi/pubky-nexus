//! # Pubky Watcher
//!
//! `pubky-watcher` consumes Homeservers' Events for an Indexer. It brings the Client that reads
//! them, the Consumers that hand each Event to the Indexer's Handler, and the Cycle that goes
//! round every Homeserver the Indexer follows. The Indexer brings the meaning: what an Event
//! does, where Cursors are kept, and which Homeservers to follow.
//!
//! The parts:
//!
//! - **The Client and its four Reads.** [`WatcherClient`] is the crate's read-only access to
//!   the Pubky network. Each Read is a trait of its own: [`FeedReader`] reads a Homeserver's
//!   Events feed (`/events/`), [`StreamReader`] a User pubkey's Event stream
//!   (`/events-stream`), [`ResourceReader`] the Resource an Event names, and [`PkdnsLookup`]
//!   the Homeserver record a User pubkey publishes. A failed Read of a Homeserver is an
//!   [`HsError`], a failed PKDNS lookup a [`PkdnsLookupError`].
//! - **The two Consumers.** [`Watcher::feed`] builds a [`FeedConsumer`], which reads a
//!   Homeserver's Events feed, and [`Watcher::stream`] a [`StreamConsumer`], which reads the
//!   Event streams of chosen User pubkeys. Each is given an [`EventHandler`]; each call makes
//!   one Run and returns its Checkpoint, or one per User pubkey: the Cursor an Indexer may
//!   store.
//! - **The Protocol checks.** The rules of an Events feed and an Event stream, which the
//!   Consumers apply, for an Indexer's own Run to apply too: [`FeedBatch::closing_cursor`],
//!   [`check_stream_cursors`], [`check_event_user_pubkey`] and [`check_stream_limit`]. Each
//!   reports the [`ProtocolViolation`] in a Homeserver's answer, and the caller chooses what
//!   follows.
//! - **[`Run`].** One pass over one Homeserver, for an Indexer that writes its own: its own
//!   Cursor store, its own Event type ([`FromEventLine`]), a Budget.
//! - **[`Cycle`].** One round over the Homeservers an Indexer follows, one Turn each, reported
//!   in [`CycleStats`].
//! - **The Retry hooks.** Opt-in, through [`RetryableError`], [`RetryQueue`] and
//!   [`dispatch_retryable_error`]: the crate sorts a Handler's failure into a Missing
//!   dependency or a Transient failure, and the Indexer owns the queue and its limits.
//!
//! The Client takes a Homeserver pubkey and a User pubkey as a [`pubky::PublicKey`]. The crate
//! brings no database, no metrics and no configuration with it.

#![warn(missing_docs)]

mod client;
mod consumers;
mod cycle;
mod feed;
#[cfg(test)]
mod fixtures;
mod hooks;
mod protocol;
mod run;

pub use client::{
    published_homeserver, ClientError, EventType, FeedReader, HsError, HsErrorKind, HsOp,
    PkdnsLookup, PkdnsLookupError, ResourceReader, StreamEvent, StreamReader, WatcherClient,
    WatcherClientBuilder, DEFAULT_MAX_FILE_SIZE, DEFAULT_REQUEST_TIMEOUT,
};
pub use consumers::{
    ConfigError, FeedConsumer, FeedConsumerBuilder, FeedOutcome, Missing, StopReason,
    StreamConsumer, StreamConsumerBuilder, StreamOutcome, Watcher, WatcherError,
    DEFAULT_EVENTS_LIMIT, DEFAULT_STREAM_PATH,
};
pub use cycle::{turn_status_from_run, Cycle, CycleReport, CycleStats, Turn, TurnStatus};
pub use feed::{FeedBatch, FeedEvent, CURSOR_PREFIX};
pub use hooks::{EventHandler, EventLineOutcome, FromEventLine, RetryQueue, RetryableError};
pub use protocol::{
    check_event_user_pubkey, check_stream_cursors, check_stream_limit, ProtocolViolation,
};
pub use run::{
    dispatch_retryable_error, run_on_own_task, Run, RunError, RunOutcome, SLOW_RUN_THRESHOLD,
};
