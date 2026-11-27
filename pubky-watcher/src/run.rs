//! The [`Run`]: one pass over one Homeserver, in which Events are read from a Cursor and each
//! is handed to the Handler, on its own task ([`run_on_own_task`]), with opt-in Retry through
//! [`dispatch_retryable_error`].
//!
//! Implement it when the Consumers ([`Watcher`](crate::Watcher)) are not enough: your own
//! Cursor store, your own Event type, Retry, or many Homeservers under a
//! [`Cycle`](crate::Cycle).

use std::fmt::{Debug, Display};
use std::{future::Future, sync::Arc, time::Duration};

use tracing::{debug, error, trace, warn, Instrument};

use super::hooks::{EventLineOutcome, FromEventLine, RetryQueue, RetryableError};

/// How long a Run may take before it is logged as slow, unless it sets its own
/// ([`Run::slow_run_threshold`]).
// TODO(#1014): read it, and the run budget, from the config file
pub const SLOW_RUN_THRESHOLD: Duration = Duration::from_secs(3_600);

/// How a Run ended when it did not fail.
/// Returned by [`Run::run_internal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The Run ended without failing or reaching its Budget: it did the work it found, or
    /// stopped early for shutdown.
    /// [`Run::run`] then returns `Ok`.
    Completed,
    /// The Run reached its Budget and ended with work left, keeping the Cursors it reached.
    /// It has not failed: no Event failed, and nothing goes to a Retry.
    /// [`Run::run`] then returns [`RunError::BudgetSpent`].
    BudgetSpent,
}

/// Why a Run did not complete.
/// Returned by [`Run::run`] and [`run_on_own_task`].
#[derive(Debug, thiserror::Error)]
pub enum RunError<Err> {
    /// [`Run::run_internal`] failed with the Run's own error.
    #[error("Internal error: {0}")]
    Internal(Err),
    /// The Run's task panicked: see [`run_on_own_task`].
    #[error("Execution panicked")]
    Panicked,
    /// The Run reached its Budget: see [`RunOutcome::BudgetSpent`].
    #[error("Run budget spent")]
    BudgetSpent,
}

impl<Err> RunError<Err> {
    /// Whether the Run panicked ([`RunError::Panicked`]).
    pub fn is_panic(&self) -> bool {
        matches!(self, RunError::Panicked)
    }
}

/// Sorts `error`, a Handler's failure on `event`, with [`RetryableError`], and hands the
/// Event to `queue` when it is worth a Retry.
///
/// - An error that should not be retried now is returned, so the caller stops its Batch.
/// - One not worth a Retry is dropped.
/// - Otherwise the Event is queued on `queue`, as a Missing dependency or as a Transient
///   failure. Without a queue it is dropped.
///
/// Opt-in: call it from an overridden [`Run::handle_error`]. The default
/// `handle_error` fails fast without requiring [`RetryableError`].
pub async fn dispatch_retryable_error<E, Err>(
    event: &E,
    error: Err,
    queue: Option<&dyn RetryQueue<E, Err>>,
) -> Result<(), Err>
where
    E: Sync,
    Err: RetryableError,
{
    if error.should_not_retry_now() {
        warn!(error = %error, "Got should-not-retry-now error, stopping batch");
        return Err(error);
    }

    if !error.should_enqueue_for_retry() {
        debug!(error = %error, "Error not worth retrying, skipping event");
        return Ok(());
    }

    let Some(queue) = queue else {
        return Ok(());
    };

    if error.is_missing_dependency() {
        queue.queue_missing_dependency(event).await
    } else {
        warn!(error = %error, "Transient error, queuing event for retry");
        queue.queue_transient_failure(event).await
    }
}

/// A Run: one pass over one Homeserver, in which Events are read from a Cursor and each is
/// handed to the Handler. It can be stopped gracefully through a shutdown signal the
/// implementation watches.
///
/// An implementation is one Run: a Consumer makes one per call, and a
/// [`Cycle`](crate::Cycle) builds one per Turn and uses it once. Generic over the Indexer's
/// Event type `E` and error type `Err`.
#[async_trait::async_trait]
pub trait Run<E, Err>: Send + Sync + 'static
where
    E: Send + Sync + 'static,
    Err: Display + Debug + Send + Sync + 'static,
{
    /// A short service label (e.g. `FeedConsumer`), named in the span and logs of
    /// [`Self::run`].
    fn instance_name(&self) -> &'static str;

    /// The Homeserver pubkey this Run reads Events from, as z-base-32 text, named in the span
    /// and logs of [`Self::run`].
    fn hs_pubkey(&self) -> &str;

    /// Runs [`Self::run_internal`] on its own task, and waits for it to end: see
    /// [`run_on_own_task`].
    async fn run(self: Arc<Self>) -> Result<(), RunError<Err>> {
        let slow_run_threshold = self.slow_run_threshold().unwrap_or(SLOW_RUN_THRESHOLD);
        let instance_name = self.instance_name();
        let hs_pubkey = self.hs_pubkey().to_string();
        run_on_own_task(
            instance_name,
            Some(&hs_pubkey),
            slow_run_threshold,
            self.run_internal(),
        )
        .await
    }

    /// The pass itself; [`Self::run`] calls it on its own task.
    ///
    /// Returns how the Run ended ([`RunOutcome`]), or the error it failed with.
    async fn run_internal(self: Arc<Self>) -> Result<RunOutcome, Err>;

    /// How long this Run may take before it is logged as slow.
    ///
    /// If not set, the [`SLOW_RUN_THRESHOLD`] is applied.
    fn slow_run_threshold(&self) -> Option<Duration> {
        None
    }

    /// Hands `event` to the Indexer's Handler, from [`Self::handle_event`] once
    /// [`Self::should_handle_event`] accepted it.
    async fn handle(&self, event: &E) -> Result<(), Err>;

    /// Parses one Event line and hands its Event to [`Self::handle_event`]. A line that does
    /// not parse, or names a Resource the Indexer does not recognise, is logged and
    /// skipped: Event lines come from untrusted Homeservers.
    async fn handle_event_line(&self, line: &str) -> Result<(), Err>
    where
        E: FromEventLine + Debug,
    {
        let outcome = match E::from_event_line(line) {
            Ok(outcome) => outcome,
            // Invalid Event lines come from untrusted Homeservers; treat as bad peer data, not
            // the Indexer's errors.
            Err(e) => {
                warn!(error = %e, "Invalid event line");
                return Ok(());
            }
        };
        match outcome {
            EventLineOutcome::Skipped => {}
            EventLineOutcome::Unrecognized { reason } => {
                warn!(%reason, "Unrecognized event URI");
            }
            EventLineOutcome::Parsed(event) => {
                trace!(?event, "Processing event");
                self.handle_event(&event).await?;
            }
        }

        Ok(())
    }

    /// Handles the failure of handling `event`.
    ///
    /// Called by [`Self::handle_event`]. Returns:
    /// - `Ok(())` to go on with the Batch
    /// - `Err(e)` to stop the Run and return the error
    ///
    /// The default fails fast. A Run that hands failed Events back for a Retry overrides it
    /// and calls [`dispatch_retryable_error`].
    async fn handle_error(&self, _event: &E, error: Err) -> Result<(), Err> {
        Err(error)
    }

    /// Decides whether this Run hands `event` to the Handler.
    ///
    /// Called at the start of [`Self::handle_event`], before the Event is handed to
    /// [`Self::handle`]. The default always handles the Event.
    async fn should_handle_event(&self, _event: &E) -> Result<bool, Err> {
        Ok(true)
    }

    /// The span [`Self::handle_event`] runs in. It must declare the empty fields
    /// `otel.status_code` and `otel.status_message`, which record the Event's outcome.
    fn event_span(&self, _event: &E) -> tracing::Span {
        tracing::info_span!(
            "event.process",
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        )
    }

    /// Handles `event` in its span, and hands a failure to [`Self::handle_error`].
    async fn handle_event(&self, event: &E) -> Result<(), Err> {
        let span = self.event_span(event);
        async {
            let span = tracing::Span::current();

            match self.should_handle_event(event).await {
                Ok(true) => {}
                Ok(false) => {
                    span.record("otel.status_code", "UNSET");
                    span.record("otel.status_message", "SKIPPED");
                    return Ok(());
                }
                Err(e) => {
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_message", tracing::field::display(&e));
                    return self.handle_error(event, e).await;
                }
            }

            if let Err(e) = self.handle(event).await {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_message", tracing::field::display(&e));

                self.handle_error(event, e).await?;
            } else {
                span.record("otel.status_code", "OK");
            }

            Ok(())
        }
        .instrument(span)
        .await
    }
}

/// Runs `run_internal`, the Run labelled `instance_name`, on its own task, and waits for it
/// to end.
///
/// A Run that takes longer than `slow_run_threshold` is logged as slow, again after each
/// further threshold, and still waited for: it is not stopped, so the next Run cannot start
/// next to it. A Run that never returns therefore holds up its caller for good.
///
/// `hs_pubkey` is the Homeserver the Run is bound to, named in its span. A Run that is not
/// bound to one Homeserver names none.
/// It fails with [`RunError::Panicked`] or [`RunError::Internal`], or with
/// [`RunError::BudgetSpent`] when the Run returns [`RunOutcome::BudgetSpent`].
pub async fn run_on_own_task<Err>(
    instance_name: &'static str,
    hs_pubkey: Option<&str>,
    slow_run_threshold: Duration,
    run_internal: impl Future<Output = Result<RunOutcome, Err>> + Send + 'static,
) -> Result<(), RunError<Err>>
where
    Err: Debug + Send + 'static,
{
    match wait_on_own_task(instance_name, hs_pubkey, slow_run_threshold, run_internal).await {
        Ok(Ok(RunOutcome::Completed)) => Ok(()),
        Ok(Ok(RunOutcome::BudgetSpent)) => Err(RunError::BudgetSpent),
        Ok(Err(e)) => Err(RunError::Internal(e)),
        Err(Panicked) => Err(RunError::Panicked),
    }
}

/// The task of a Run ended without returning: it panicked.
pub(crate) struct Panicked;

/// Runs `run` on its own task as [`run_on_own_task`] does, and returns what it returned, or
/// [`Panicked`]. A Run that fails is logged.
pub(crate) async fn wait_on_own_task<T, Err>(
    instance_name: &'static str,
    hs_pubkey: Option<&str>,
    slow_run_threshold: Duration,
    run: impl Future<Output = Result<T, Err>> + Send + 'static,
) -> Result<Result<T, Err>, Panicked>
where
    T: Send + 'static,
    Err: Debug + Send + 'static,
{
    let homeserver = hs_pubkey.map(tracing::field::display);
    let span = tracing::info_span!(
        "event_processor.run",
        service = %instance_name,
        homeserver,
    );
    let started = tokio::time::Instant::now();
    let mut handle = tokio::spawn(run.instrument(span));

    let join_result = loop {
        match tokio::time::timeout(slow_run_threshold, &mut handle).await {
            Ok(join_result) => break join_result,
            Err(_) => warn!(
                service = %instance_name,
                homeserver,
                running_for_secs = started.elapsed().as_secs(),
                "Event processor run is slow; waiting for it to finish"
            ),
        }
    };

    // The JoinError can be:
    // - join_error.is_panic() => panic by the inner future
    // - join_error.is_cancelled() => inner future was abruptly interrupted, for example
    //   - JoinHandle::abort() is called on the handle
    //   - the Tokio runtime is shut down
    // In our model, we don't trigger such interruptions. Instead we use the shutdown signal
    // to gracefully stop a Run. Therefore we consider all JoinErrors as panics.
    let result = join_result
        .inspect_err(|je| {
            error!(
                service = %instance_name,
                homeserver,
                error = ?je,
                "Event processor JoinError"
            )
        })
        .map_err(|_| Panicked)?;

    if let Err(e) = &result {
        error!(
            service = %instance_name,
            homeserver,
            error = ?e,
            "Event processor failed"
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::time::Duration;

    use super::{run_on_own_task, RunOutcome};

    /// The warning is the only sign of a Run that never returns, so it repeats after every
    /// threshold, with how long the Run has been going, and the Run is still waited for.
    #[tokio::test(start_paused = true)]
    #[tracing_test::traced_test]
    async fn a_slow_run_is_logged_at_every_threshold_and_still_waited_for() {
        let result = run_on_own_task::<Infallible>(
            "SlowRun",
            Some("homeserver"),
            Duration::from_secs(60),
            async {
                tokio::time::sleep(Duration::from_secs(250)).await;
                Ok(RunOutcome::Completed)
            },
        )
        .await;

        assert!(result.is_ok(), "the run is waited for, not stopped");
        logs_assert(|lines| {
            let warned: Vec<_> = lines
                .iter()
                .filter(|line| line.contains(" WARN ") && line.contains("run is slow"))
                .collect();
            for (line, secs) in warned.iter().zip([60, 120, 180, 240]) {
                if !line.contains(&format!("running_for_secs={secs}")) {
                    return Err(format!("expected running_for_secs={secs} in {line}"));
                }
            }
            match warned.len() {
                4 => Ok(()),
                n => Err(format!("expected 4 slow-run warnings, got {n}")),
            }
        });
    }
}
