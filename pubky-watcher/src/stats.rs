use std::time::Duration;

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
