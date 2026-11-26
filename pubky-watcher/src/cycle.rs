//! Running processors across many homeservers.
//!
//! Use a [`Cycle`] when one application watches many homeservers: each
//! cycle it lists them, skips those in backoff, builds and runs one processor per
//! homeserver, one after the other, and reports how each turn ended in
//! [`CycleStats`].

use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch::Receiver;
use tracing::{debug, error, info};

use super::run::{Run, RunError};

/// How one homeserver's turn in a runner cycle ended.
/// Recorded in [`Turn::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnStatus {
    /// [`Cycle::build_run`] failed, so no processor ran.
    FailedToBuild,
    /// The processor's run completed ([`turn_status_from_run`] of `Ok`).
    Ok,
    /// The processor's run failed ([`RunError::Internal`]).
    Error,
    /// The processor's run panicked ([`RunError::Panicked`]).
    Panic,
    /// The processor's run stopped at its budget ([`RunError::BudgetSpent`]).
    BudgetSpent,
    /// [`Cycle::should_skip`] skipped the homeserver.
    Skipped,
}

/// One homeserver's turn in a runner cycle.
/// Collected in [`CycleStats`].
pub struct Turn {
    /// The homeserver's ID, as [`Cycle::homeservers`] listed it.
    pub hs_pubkey: String,
    /// How long building and running its processor took; zero when it was skipped.
    /// Measured by [`Cycle::run`].
    pub duration: Duration,
    /// How the turn ended, as a [`TurnStatus`].
    pub status: TurnStatus,
}

/// Every homeserver's turn in a runner cycle, in order.
/// Handed to [`Cycle::after_cycle`].
#[derive(Default)]
pub struct CycleStats {
    /// One [`Turn`] per homeserver, in the order they ran.
    pub turns: Vec<Turn>,
}

impl CycleStats {
    /// Records the turn of `hs_pubkey`, which took `duration` and ended with `status`.
    /// Appends a [`Turn`].
    pub fn add_turn(&mut self, hs_pubkey: String, duration: Duration, status: TurnStatus) {
        let turn = Turn {
            hs_pubkey,
            duration,
            status,
        };
        self.turns.push(turn);
    }

    fn count(&self, status: TurnStatus) -> usize {
        self.turns
            .iter()
            .filter(|turn| turn.status == status)
            .count()
    }

    /// Number of homeservers whose run completed ([`TurnStatus::Ok`]).
    pub fn count_ok(&self) -> usize {
        self.count(TurnStatus::Ok)
    }

    /// Number of homeservers whose run failed with an error ([`TurnStatus::Error`]).
    pub fn count_error(&self) -> usize {
        self.count(TurnStatus::Error)
    }

    /// Number of homeservers whose run panicked ([`TurnStatus::Panic`]).
    pub fn count_panic(&self) -> usize {
        self.count(TurnStatus::Panic)
    }

    /// Number of homeservers whose run stopped at its budget
    /// ([`TurnStatus::BudgetSpent`]).
    pub fn count_budget_spent(&self) -> usize {
        self.count(TurnStatus::BudgetSpent)
    }

    /// Number of homeservers whose processor could not be built
    /// ([`TurnStatus::FailedToBuild`]).
    pub fn count_failed_to_build(&self) -> usize {
        self.count(TurnStatus::FailedToBuild)
    }

    /// Number of homeservers skipped because they were in backoff
    /// ([`TurnStatus::Skipped`]).
    pub fn count_skipped(&self) -> usize {
        self.count(TurnStatus::Skipped)
    }
}

/// A cycle's [`CycleStats`] once [`Cycle::after_cycle`] has processed
/// them. [`Cycle::run`] returns it.
pub struct CycleReport(pub CycleStats);

/// The [`TurnStatus`] a processor run that ended with `result` is recorded with.
pub fn turn_status_from_run<Err>(result: Result<(), RunError<Err>>) -> TurnStatus {
    match result {
        Ok(_) => TurnStatus::Ok,
        Err(RunError::Internal(_)) => TurnStatus::Error,
        Err(RunError::Panicked) => TurnStatus::Panic,
        Err(RunError::BudgetSpent) => TurnStatus::BudgetSpent,
    }
}

/// The orchestrator that builds and runs an event processor per homeserver, one after the
/// other, so that runs never overlap.
///
/// Generic over the processors' event type `E` and error type `Err`. Which homeservers run,
/// in what order, how many per cycle and which are skipped are hooks: [`Self::homeservers`],
/// [`Self::run_limit`] and [`Self::should_skip`].
///
/// # Implementation Notes
/// - The `build_run` method should create and return a fully configured event processor ready for immediate use
/// - Implementors should ensure that created processors are properly isolated and don't share mutable state unless explicitly intended
#[async_trait::async_trait]
pub trait Cycle<E, Err>: Send + Sync
where
    E: Send + Sync + 'static,
    Err: Display + Debug + Send + Sync + 'static,
{
    /// Why this runner could not list its homeservers ([`Self::homeservers`]) or build a
    /// processor ([`Self::build_run`]). These are the implementor's own lookups, so the
    /// implementor picks the type.
    type Error: Display + Debug + Send + Sync + 'static;

    /// The shutdown signal: once it is `true`, [`Self::run`] stops before the next homeserver.
    fn shutdown_rx(&self) -> Receiver<bool>;

    /// Creates and returns a new event processor instance for the specified homeserver.
    ///
    /// # Parameters
    /// * `hs_pubkey` - The homeserver ID. Represents the homeserver this event processor will
    ///   fetch and process events from.
    ///
    /// # Returns
    /// A reference to the event processor instance, ready to be executed with its
    /// [`Run::run`] method.
    ///
    /// # Errors
    /// Returns an error if the event processor couldn't be built.
    async fn build_run(&self, hs_pubkey: &str) -> Result<Arc<dyn Run<E, Err>>, Self::Error>;

    /// Pre-processing step before the main run loop.
    ///
    /// Determines the list of target HS IDs to process in this run cycle, ordered by priority.
    ///
    /// The list is not truncated here: [`Self::run_limit`] is applied in [`Self::run`], after
    /// the HS IDs in backoff are skipped.
    async fn homeservers(&self) -> Result<Vec<String>, Self::Error>;

    /// Maximum number of HS IDs built and run per cycle. HS IDs skipped by
    /// [`Self::should_skip`] don't count.
    ///
    /// No limit by default. Runners that cap the targets per run should overwrite this.
    fn run_limit(&self) -> usize {
        usize::MAX
    }

    /// Post-processing of a cycle's [`CycleStats`], at the end of [`Self::run`].
    ///
    /// No-op default implementation. Callers that perform post-processing should overwrite this.
    async fn after_cycle(&self, stats: CycleStats) -> CycleReport {
        CycleReport(stats)
    }

    /// Main run loop: builds and runs event processors for the relevant targets.
    ///
    /// # Returns
    /// Statistics about the event processor run results, summarized as [`CycleReport`].
    async fn run(&self) -> Result<CycleReport, Self::Error> {
        let hs_pubkeys = self.homeservers().await?;
        let run_limit = self.run_limit();
        let mut runs = 0;
        let mut cycle_stats = CycleStats::default();

        for hs_pubkey in hs_pubkeys {
            if runs >= run_limit {
                break;
            }

            if *self.shutdown_rx().borrow() {
                info!(homeserver = %hs_pubkey, "Shutdown detected; exiting run loop");
                break;
            }

            if self.should_skip(&hs_pubkey).await {
                debug!(homeserver = %hs_pubkey, "Skipping homeserver in backoff");
                cycle_stats.add_turn(hs_pubkey, Duration::ZERO, TurnStatus::Skipped);
                continue;
            }

            runs += 1;
            let t0 = Instant::now();
            let status = match self.build_run(&hs_pubkey).await {
                Ok(run) => turn_status_from_run(run.run().await),
                Err(e) => {
                    error!(homeserver = %hs_pubkey, error = %e, "Failed to build event processor");
                    TurnStatus::FailedToBuild
                }
            };
            let duration = t0.elapsed();

            self.record_turn(&hs_pubkey, &status).await;
            cycle_stats.add_turn(hs_pubkey, duration, status);
        }

        let report = self.after_cycle(cycle_stats).await;
        Ok(report)
    }

    /// Called before building a HS's processor, to check whether it should be skipped, e.g.
    /// because it is in backoff. A skipped HS does not count against [`Self::run_limit`].
    ///
    /// No-op default implementation. Runners that use backoff should overwrite as needed.
    async fn should_skip(&self, _hs_pubkey: &str) -> bool {
        false
    }

    /// Called by [`Self::run`] after a HS is processed (build + run), with the
    /// [`TurnStatus`] of its turn, to update the backoff that
    /// [`Self::should_skip`] checks.
    ///
    /// No-op default implementation. Runners that use backoff should overwrite as needed.
    async fn record_turn(&self, _hs_pubkey: &str, _status: &TurnStatus) {}
}
