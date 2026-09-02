//! # Pubky Watcher
//!
//! Poll events from pubky homeservers.
//!
//! Generic library for subscribing to Pubky homeserver event streams and
//! processing PUT/DEL events. Intended for external developers building
//! indexers, sync pipelines, or other event-driven applications on top of
//! Pubky homeservers.
//!
//! Connect with [`PubkyConnector`] (the [pubky](https://crates.io/crates/pubky) SDK
//! client), split a `GET /events/` body with [`EventBatch`], implement the traits,
//! then run a [`TEventProcessor`] (one tick) or a [`TEventProcessorRunner`]
//! (many homeservers).
//!
//! Domain-specific indexing — such as Nexus graph and Redis rules — lives in
//! higher-level crates like `nexus-watcher`.

mod client;
mod events;
mod processor;
mod runner;
mod traits;

pub use client::{ClientError, ClientResult, PubkyConnector};
pub use events::{EventBatch, CURSOR_PREFIX};
pub use processor::{dispatch_retryable_error, RunError, TEventProcessor, PROCESSING_TIMEOUT_SECS};
pub use runner::{
    status_from_run_result, ProcessedStats, ProcessorRunStats, ProcessorRunStatus,
    RunAllProcessorsStats, TEventProcessorRunner,
};
pub use traits::{
    EventHandler, EventRetryScheduler, LineParseOutcome, ParseFromLine, RetryableError,
};
