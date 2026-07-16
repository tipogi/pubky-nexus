//! Poll events from pubky homeservers.
//!
//! Generic pipeline orchestration: parse event lines, dispatch to handlers,
//! enqueue retries, and run processor loops.

pub mod client;
pub mod constants;
pub mod error;
pub mod pipeline;
pub mod processor;
pub mod runner;
pub mod stats;

pub use client::{
    ClientError, ClientResult, HomeserverResolver, PubkyConnector, PubkyConnectorResolver,
};
pub use constants::PROCESSING_TIMEOUT_SECS;
pub use error::RunError;
pub use pipeline::{
    EventHandler, EventMetadata, EventRetryScheduler, LineParseOutcome, ParseFromLine,
    RetryableError,
};
pub use processor::TEventProcessor;
pub use runner::{status_from_run_result, TEventProcessorRunner};
pub use stats::{
    ProcessedStats, ProcessorRunStats, ProcessorRunStatus, RunAllProcessorsStats,
};
