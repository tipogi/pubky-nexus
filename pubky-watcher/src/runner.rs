use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch::Receiver;
use tracing::{debug, error, info};

use crate::error::RunError;
use crate::hooks::{EventMetadata, RetryableError};
use crate::processor::TEventProcessor;
use crate::stats::{ProcessedStats, ProcessorRunStatus, RunAllProcessorsStats};

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
    E: Send + Sync + 'static + EventMetadata,
    Err: RetryableError + std::fmt::Debug + Send + Sync + 'static,
{
    fn shutdown_rx(&self) -> Receiver<bool>;

    async fn build(
        &self,
        hs_id: &str,
    ) -> Result<Arc<dyn TEventProcessor<E, Err> + Send + Sync>, Box<dyn std::error::Error + Send + Sync>>;

    async fn pre_run(
        &self,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn post_run(&self, stats: RunAllProcessorsStats) -> ProcessedStats {
        ProcessedStats(stats)
    }

    async fn run(
        &self,
    ) -> Result<ProcessedStats, Box<dyn std::error::Error + Send + Sync>> {
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
