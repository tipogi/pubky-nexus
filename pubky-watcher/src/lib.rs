//! Poll events from pubky homeservers.
//!
//! Generic pipeline orchestration: parse event lines, dispatch to handlers,
//! enqueue retries, and run processor loops.

pub mod client;
pub mod constants;
pub mod error;
pub mod hooks;
pub mod processor;
pub mod resolver;
pub mod runner;
pub mod stats;

pub use client::{ClientError, ClientResult, PubkyConnector};
pub use constants::PROCESSING_TIMEOUT_SECS;
pub use error::RunError;
pub use hooks::{
    EventHandler, EventMetadata, EventRetryScheduler, LineParseOutcome, ParseFromLine,
    RetryableError,
};
pub use processor::TEventProcessor;
pub use resolver::{HomeserverResolver, PubkyConnectorResolver};
pub use runner::{status_from_run_result, TEventProcessorRunner};
pub use stats::{
    ProcessedStats, ProcessorRunStats, ProcessorRunStatus, RunAllProcessorsStats,
};
