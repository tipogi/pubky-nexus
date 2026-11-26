//! # Pubky Watcher
//!
//! Reads events from Pubky homeservers, for applications that index them: search engines,
//! moderation tools, archives, application-specific indexes.
//!
//! The crate owns the generic mechanics: the homeserver client and its capability traits,
//! framing a homeserver's `/events/` body, parsing its lines, running a processor and waiting
//! for it, running processors across many homeservers, and the hooks through which an
//! application plugs in its own handling and retry policy. The application owns what an event
//! means, where it is stored, where cursors are kept, and its retry and backoff policy.
//!
//! - [`WatcherClient`] makes every homeserver call, split into narrow capability traits:
//!   [`FeedReader`], [`StreamReader`], [`ResourceReader`] and
//!   [`PkdnsLookup`]. A failure is an [`HsError`].
//! - [`Run`] is one run over one homeserver, generic over the application's
//!   event and error types; [`Cycle`] runs one per homeserver.
//! - Retry is opt-in through [`RetryableError`], [`RetryQueue`] and
//!   [`dispatch_retryable_error`].
//!
//! Homeservers and users are [`pubky::PublicKey`]s in the client. The crate brings no
//! database, metrics or application config with it.

#![warn(missing_docs)]

mod client;
mod cycle;
mod feed;
mod hooks;
mod run;
mod watcher;

pub use client::{
    published_homeserver, ClientError, EventType, FeedReader, HsError, HsErrorKind, HsOp,
    PkdnsLookup, PkdnsLookupError, ResourceReader, StreamEvent, StreamReader, WatcherClient,
    WatcherClientBuilder, DEFAULT_MAX_FILE_SIZE, DEFAULT_REQUEST_TIMEOUT,
};
pub use cycle::{turn_status_from_run, Cycle, CycleReport, CycleStats, Turn, TurnStatus};
pub use feed::{FeedBatch, FeedEvent, CURSOR_PREFIX};
pub use hooks::{EventHandler, EventLineOutcome, FromEventLine, RetryQueue, RetryableError};
pub use run::{
    dispatch_retryable_error, run_on_own_task, Run, RunError, RunOutcome, SLOW_RUN_THRESHOLD,
};
pub use watcher::{
    ConfigError, HomeserverWatcher, HomeserverWatcherBuilder, KeyStreamOutcome, KeyStreamWatcher,
    KeyStreamWatcherBuilder, Missing, ProtocolError, WatchOutcome, Watcher, WatcherError,
    DEFAULT_EVENTS_LIMIT, DEFAULT_KEY_STREAM_PATH,
};
