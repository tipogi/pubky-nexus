//! The [`Cycle`]: one round over the Homeservers an Indexer follows.
//!
//! Use a [`Cycle`] when one Indexer follows many Homeservers: each Cycle lists them and gives
//! each its Turn, one after the other. A Homeserver in backoff is skipped; for any other, the
//! Cycle builds its [`Run`] and runs it. How each Turn ended is reported in [`CycleStats`].

use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch::Receiver;
use tracing::{debug, error, info};

use super::run::{Run, RunError};

/// How one Homeserver's Turn in a Cycle ended: how its Run went, or why it had none.
/// Recorded in [`Turn::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnStatus {
    /// [`Cycle::build_run`] failed, so the Homeserver had no Run.
    FailedToBuild,
    /// The Run completed ([`turn_status_from_run`] of `Ok`).
    Ok,
    /// The Run failed ([`RunError::Internal`]).
    Error,
    /// The Run panicked ([`RunError::Panicked`]).
    Panic,
    /// The Run reached its Budget ([`RunError::BudgetSpent`]).
    BudgetSpent,
    /// [`Cycle::should_skip`] skipped the Homeserver, so it had no Run.
    Skipped,
}

/// A Turn: one Homeserver's place in a Cycle, its Run or the reason it had none.
/// Collected in [`CycleStats`].
pub struct Turn {
    /// The Homeserver pubkey, as z-base-32 text, as [`Cycle::homeservers`] listed it.
    pub hs_pubkey: String,
    /// How long building and running its Run took; zero when it was skipped.
    /// Measured by [`Cycle::run`].
    pub duration: Duration,
    /// How the Turn ended, as a [`TurnStatus`].
    pub status: TurnStatus,
}

/// Every Homeserver's Turn in a Cycle, in order.
/// Handed to [`Cycle::after_cycle`].
#[derive(Default)]
pub struct CycleStats {
    /// One [`Turn`] per Homeserver, in the order they were taken.
    pub turns: Vec<Turn>,
}

impl CycleStats {
    /// Records the Turn of `hs_pubkey`, which took `duration` and ended with `status`.
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

    /// Number of Homeservers whose Run completed ([`TurnStatus::Ok`]).
    pub fn count_ok(&self) -> usize {
        self.count(TurnStatus::Ok)
    }

    /// Number of Homeservers whose Run failed with an error ([`TurnStatus::Error`]).
    pub fn count_error(&self) -> usize {
        self.count(TurnStatus::Error)
    }

    /// Number of Homeservers whose Run panicked ([`TurnStatus::Panic`]).
    pub fn count_panic(&self) -> usize {
        self.count(TurnStatus::Panic)
    }

    /// Number of Homeservers whose Run reached its Budget
    /// ([`TurnStatus::BudgetSpent`]).
    pub fn count_budget_spent(&self) -> usize {
        self.count(TurnStatus::BudgetSpent)
    }

    /// Number of Homeservers whose Run could not be built
    /// ([`TurnStatus::FailedToBuild`]).
    pub fn count_failed_to_build(&self) -> usize {
        self.count(TurnStatus::FailedToBuild)
    }

    /// Number of Homeservers skipped, e.g. because they were in backoff
    /// ([`TurnStatus::Skipped`]).
    pub fn count_skipped(&self) -> usize {
        self.count(TurnStatus::Skipped)
    }
}

/// A Cycle's [`CycleStats`] once [`Cycle::after_cycle`] has been through them.
/// [`Cycle::run`] returns it.
pub struct CycleReport(pub CycleStats);

/// The [`TurnStatus`] a Run that ended with `result` is recorded with.
pub fn turn_status_from_run<Err>(result: Result<(), RunError<Err>>) -> TurnStatus {
    match result {
        Ok(_) => TurnStatus::Ok,
        Err(RunError::Internal(_)) => TurnStatus::Error,
        Err(RunError::Panicked) => TurnStatus::Panic,
        Err(RunError::BudgetSpent) => TurnStatus::BudgetSpent,
    }
}

/// A Cycle: one round over the Homeservers an Indexer follows, one Turn each, one after the
/// other, so that Runs never overlap.
///
/// Generic over the Runs' Event type `E` and error type `Err`. Which Homeservers get a Turn,
/// in what order, how many Runs a Cycle makes and which Homeservers are skipped are hooks:
/// [`Self::homeservers`], [`Self::run_limit`] and [`Self::should_skip`].
///
/// # Implementation Notes
/// - [`Self::build_run`] should return a fully configured [`Run`], ready to be run at once
/// - Implementors should ensure that the Runs they build are isolated and share no mutable
///   state unless that is intended
#[async_trait::async_trait]
pub trait Cycle<E, Err>: Send + Sync
where
    E: Send + Sync + 'static,
    Err: Display + Debug + Send + Sync + 'static,
{
    /// Why this Cycle could not list its Homeservers ([`Self::homeservers`]) or build a
    /// Run ([`Self::build_run`]). These are the implementor's own lookups, so the
    /// implementor picks the type.
    type Error: Display + Debug + Send + Sync + 'static;

    /// The shutdown signal: once it is `true`, [`Self::run`] stops before the next Turn.
    fn shutdown_rx(&self) -> Receiver<bool>;

    /// Builds the [`Run`] of one Homeserver's Turn.
    ///
    /// # Parameters
    /// * `hs_pubkey` - The Homeserver pubkey, as z-base-32 text: the Homeserver the Run reads
    ///   and handles Events from.
    ///
    /// # Returns
    /// The Run, ready to be run with its [`Run::run`] method.
    ///
    /// # Errors
    /// Returns an error if the Run couldn't be built.
    async fn build_run(&self, hs_pubkey: &str) -> Result<Arc<dyn Run<E, Err>>, Self::Error>;

    /// The Homeservers this Cycle goes round, as Homeserver pubkeys in z-base-32 text,
    /// ordered by priority. Called once, at the start of [`Self::run`].
    ///
    /// The list is not truncated here: [`Self::run_limit`] is applied in [`Self::run`], after
    /// the Homeservers [`Self::should_skip`] names are skipped.
    async fn homeservers(&self) -> Result<Vec<String>, Self::Error>;

    /// Maximum number of Homeservers whose Run is built and run per Cycle. Homeservers
    /// skipped by [`Self::should_skip`] don't count.
    ///
    /// No limit by default. A Cycle that caps its Runs should overwrite this.
    fn run_limit(&self) -> usize {
        usize::MAX
    }

    /// What the Cycle does with its [`CycleStats`], at the end of [`Self::run`]: log them,
    /// record metrics.
    ///
    /// No-op default implementation. A Cycle that reports its Turns should overwrite this.
    async fn after_cycle(&self, stats: CycleStats) -> CycleReport {
        CycleReport(stats)
    }

    /// One Cycle: gives each Homeserver its Turn, building and running its Run.
    ///
    /// # Returns
    /// How every Turn ended, as a [`CycleReport`].
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

    /// Whether the Homeserver `hs_pubkey` has no Run this Cycle, e.g. because it is in
    /// backoff. Called before its Run is built. A skipped Homeserver does not count against
    /// [`Self::run_limit`].
    ///
    /// Skips none by default. A Cycle that uses backoff should overwrite as needed.
    async fn should_skip(&self, _hs_pubkey: &str) -> bool {
        false
    }

    /// Called by [`Self::run`] after a Homeserver's Turn (build + run), with its
    /// [`TurnStatus`], to update the backoff that [`Self::should_skip`] checks.
    ///
    /// No-op default implementation. A Cycle that uses backoff should overwrite as needed.
    async fn record_turn(&self, _hs_pubkey: &str, _status: &TurnStatus) {}
}
