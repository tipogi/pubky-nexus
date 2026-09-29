//! # Pubky Watcher
//!
//! Poll events from pubky homeservers.
//!
//! Generic library for subscribing to Pubky homeserver event streams and
//! processing PUT/DEL events. Intended for external developers building
//! indexers, sync pipelines, or other event-driven applications on top of
//! Pubky homeservers.
//!
//! ## Quick path
//!
//! Implement [`EventHandler`] for [`HomeserverEvent`], then poll one homeserver:
//!
//! ```ignore
//! Watcher::homeserver(WatcherClient::mainnet()?, homeserver)
//!     .handler(MyHandler)
//!     .build(shutdown_rx)?
//!     .run(EventCursor::new(cursor))
//!     .await?
//! ```
//!
//! The returned [`WatchOutcome`] contains the cursor the application may
//! persist. Use [`Watcher::key_stream`] to poll multiple user streams on that
//! homeserver.
//!
//! ## Advanced path
//!
//! Inject a [`WatcherClient`] capability, split a `GET /events/` body with
//! [`EventBatch`], implement the processing traits, then run a
//! [`TEventProcessor`] (one tick) or a [`TEventProcessorRunner`] (many
//! homeservers). Nexus uses this path for custom cursors, backoff, and retries.
//!
//! Domain-specific indexing — such as Nexus graph and Redis rules — lives in
//! higher-level crates like `nexus-watcher`.

mod client;
mod events;
mod processor;
mod runner;
mod traits;
mod watcher;

pub use client::{
    ClientError, ClientResponse, ClientResult, HomeserverEventSource, HomeserverResolver,
    KeyEventSource, KeyEventStream, ResourceReader, ResponseBody, WatcherClient,
};
pub use events::{read_stream_capped, EventBatch, EventMethod, HomeserverEvent, CURSOR_PREFIX};
pub use processor::{dispatch_retryable_error, RunError, TEventProcessor, PROCESSING_TIMEOUT_SECS};
pub use runner::{
    status_from_run_result, ProcessedStats, ProcessorRunStats, ProcessorRunStatus,
    RunAllProcessorsStats, TEventProcessorRunner,
};
pub use traits::{
    EventHandler, EventRetryScheduler, LineParseOutcome, ParseFromLine, RetryableError,
};
pub use watcher::{
    HomeserverWatcher, HomeserverWatcherBuilder, KeyStreamOutcome, KeyStreamWatcher,
    KeyStreamWatcherBuilder, Missing, WatchOutcome, Watcher, WatcherError,
};
