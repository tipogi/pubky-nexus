use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch::Receiver;
use tracing::{debug, error, info};

use crate::processor::{RunError, TEventProcessor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorRunStatus {
    FailedToBuild,
    Ok,
    Error,
    Panic,
    Timeout,
    Skipped,
}

pub struct ProcessorRunStats {
    pub hs_id: String,
    pub duration: Duration,
    pub status: ProcessorRunStatus,
}

#[derive(Default)]
pub struct RunAllProcessorsStats {
    pub stats: Vec<ProcessorRunStats>,
}

impl RunAllProcessorsStats {
    pub fn add_run_result(
        &mut self,
        hs_id: String,
        duration: Duration,
        status: ProcessorRunStatus,
    ) {
        self.stats.push(ProcessorRunStats {
            hs_id,
            duration,
            status,
        });
    }

    fn count(&self, status: ProcessorRunStatus) -> usize {
        self.stats.iter().filter(|ps| ps.status == status).count()
    }

    pub fn count_ok(&self) -> usize {
        self.count(ProcessorRunStatus::Ok)
    }

    pub fn count_error(&self) -> usize {
        self.count(ProcessorRunStatus::Error)
    }

    pub fn count_panic(&self) -> usize {
        self.count(ProcessorRunStatus::Panic)
    }

    pub fn count_timeout(&self) -> usize {
        self.count(ProcessorRunStatus::Timeout)
    }

    pub fn count_failed_to_build(&self) -> usize {
        self.count(ProcessorRunStatus::FailedToBuild)
    }

    pub fn count_skipped(&self) -> usize {
        self.count(ProcessorRunStatus::Skipped)
    }
}

pub struct ProcessedStats(pub RunAllProcessorsStats);

pub fn status_from_run_result<Err>(result: Result<(), RunError<Err>>) -> ProcessorRunStatus {
    match result {
        Ok(_) => ProcessorRunStatus::Ok,
        Err(RunError::Internal(_)) => ProcessorRunStatus::Error,
        Err(RunError::Panicked) => ProcessorRunStatus::Panic,
        Err(RunError::TimedOut) => ProcessorRunStatus::Timeout,
    }
}

/// The orchestrator that builds and runs event processors in the Watcher service.
#[async_trait::async_trait]
pub trait TEventProcessorRunner<E, Err>: Send + Sync
where
    E: Send + Sync + 'static,
    Err: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
{
    fn shutdown_rx(&self) -> Receiver<bool>;

    async fn build(
        &self,
        hs_id: &str,
    ) -> Result<
        Arc<dyn TEventProcessor<E, Err> + Send + Sync>,
        Box<dyn std::error::Error + Send + Sync>,
    >;

    async fn pre_run(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn post_run(&self, stats: RunAllProcessorsStats) -> ProcessedStats {
        ProcessedStats(stats)
    }

    async fn run(&self) -> Result<ProcessedStats, Box<dyn std::error::Error + Send + Sync>> {
        let hs_ids = self.pre_run().await?;
        let mut run_stats = RunAllProcessorsStats::default();

        for hs_id in hs_ids {
            if *self.shutdown_rx().borrow() {
                info!(hs_id = %hs_id, "Shutdown detected; exiting run loop");
                break;
            }

            if self.backoff_hs_should_skip(&hs_id).await {
                debug!(%hs_id, "Skipping homeserver in backoff");
                run_stats.add_run_result(hs_id, Duration::ZERO, ProcessorRunStatus::Skipped);
                continue;
            }

            let t0 = Instant::now();
            let status = match self.build(&hs_id).await {
                Ok(event_processor) => status_from_run_result(event_processor.run().await),
                Err(e) => {
                    error!(hs_id = %hs_id, error = %e, "Failed to build event processor");
                    ProcessorRunStatus::FailedToBuild
                }
            };
            let duration = t0.elapsed();

            self.backoff_hs_record_result(&hs_id, &status).await;
            run_stats.add_run_result(hs_id, duration, status);
        }

        let processed_stats = self.post_run(run_stats).await;
        Ok(processed_stats)
    }

    async fn backoff_hs_should_skip(&self, _hs_id: &str) -> bool {
        false
    }

    async fn backoff_hs_record_result(&self, _hs_id: &str, _status: &ProcessorRunStatus) {}
}
