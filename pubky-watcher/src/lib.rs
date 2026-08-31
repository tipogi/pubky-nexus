//! Poll events from pubky homeservers.
//!
//! Connect with [`PubkyConnector`] (the [pubky](https://crates.io/crates/pubky) SDK
//! client), split a `GET /events/` body with [`EventBatch`], implement the traits,
//! then run a [`TEventProcessor`] (one tick) or a [`TEventProcessorRunner`]
//! (many homeservers).

mod client;
mod events;
mod processor;
mod runner;
mod traits;

pub use client::{ClientError, ClientResult, PubkyConnector};
pub use events::{EventBatch, CURSOR_PREFIX};
pub use processor::{RunError, TEventProcessor, PROCESSING_TIMEOUT_SECS};
pub use runner::{
    status_from_run_result, ProcessedStats, ProcessorRunStats, ProcessorRunStatus,
    RunAllProcessorsStats, TEventProcessorRunner,
};
pub use traits::{
    EventHandler, EventMetadata, EventRetryScheduler, LineParseOutcome, ParseFromLine,
    RetryableError,
};
