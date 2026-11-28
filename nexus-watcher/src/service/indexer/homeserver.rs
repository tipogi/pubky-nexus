use super::{event_span, handle_error, homeserver_public_key, Run, RunOutcome};
use crate::errors::EventProcessorError;
use crate::events::retry::RetryScheduler;
use crate::events::{Event, EventHandler};
use nexus_common::db::kv::RedisError;
use nexus_common::db::{fetch_row_from_graph, queries, GraphResult};
use nexus_common::models::error::ModelError;
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::user::PkdnsHomeserverResolver;
use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};
use pubky::EventCursor;
use pubky_social_specs::PubkyId;
use pubky_watcher::{FeedBatch, FeedReader, HsErrorKind, HsOp, ProtocolViolation};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Counter for cursor lines the Primary HS sent that could not be applied.
///
/// A non-empty count means the HS returned a cursor we cannot parse, that would
/// move the stored cursor backward, or that advances without delivering events.
/// The cursor is not advanced and the same position is fetched on the next poll.
/// A sustained non-zero rate for one HS indicates it needs operator attention.
static INVALID_CURSOR_PRIMARY_HS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter(crate::METER_NAME)
        .u64_counter("watcher.primary_hs.cursor.invalid")
        .with_description("Cursor lines from the Primary HS that could not be applied")
        .build()
});

/// Counter for Primary HS batches that carried events but did not advance the cursor.
///
/// Distinct from [`INVALID_CURSOR_PRIMARY_HS`]: the cursor parses and does not
/// rewind, it simply holds at the position the batch was fetched with. Applying
/// it would make the next poll re-request and re-process the identical batch, so
/// the batch is skipped instead. Any non-zero count means the HS stream cannot
/// make progress and needs operator attention.
///
/// Only successful (2xx) responses reach the batch checks: the client fails a
/// non-2xx poll with its status and body instead, so an HS outage is recorded
/// as a failed poll, not as a stalled cursor.
static STALLED_CURSOR_PRIMARY_HS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter(crate::METER_NAME)
        .u64_counter("watcher.primary_hs.cursor.stalled")
        .with_description("Primary HS batches whose cursor did not advance past the requested one")
        .build()
});

/// Counter for events permanently rejected for exceeding a fetch size limit: a primary
/// homeserver `/events/` body over the client's events cap.
static REJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter(crate::METER_NAME)
        .u64_counter("watcher.fetch.rejected")
        .with_description("Event fetches rejected for exceeding a size limit")
        .build()
});

/// A user's homeserver mapping, classified relative to a processor's HS: the
/// `HOSTED_BY` edge, or the homeserver the user publishes while they have no edge.
///
/// The `stale` flag is only carried where it is meaningful: a stale mapping means
/// the user's published HS has diverged from the stored one (see
/// [`set_user_homeserver_stale`](nexus_common::db::queries::put::set_user_homeserver_stale)).
#[derive(Clone, Debug, PartialEq)]
pub enum HsMapping {
    /// The user has no `HOSTED_BY` edge and the lookup found no published homeserver.
    /// On pubky 0.9.3 a DHT or relay failure also surfaces as no homeserver, and a failed
    /// lookup or an answer signed by another key counts the same, so this does not prove
    /// the user publishes none.
    Unresolved,
    /// The user is mapped to this processor's HS.
    Current { stale: bool },
    /// The user is mapped to a different HS.
    Other { hs_id: String },
}

impl HsMapping {
    /// Reads a user's `HOSTED_BY` mapping relative to the homeserver `hs_id` from the graph,
    /// or `None` if the user has no edge.
    pub(crate) async fn stored(user_id: &str, hs_id: &str) -> GraphResult<Option<Self>> {
        let query = queries::get::get_user_homeserver(user_id);
        let Some(row) = fetch_row_from_graph(query).await? else {
            return Ok(None);
        };
        let stored_hs_id: String = row.get("homeserver_id")?;
        let stale: bool = row.get("stale")?;

        Ok(Some(Self::relative_to(stored_hs_id, stale, hs_id)))
    }

    /// Classifies a stored mapping to `stored_hs_id` relative to the homeserver `hs_id`.
    fn relative_to(stored_hs_id: String, stale: bool, hs_id: &str) -> Self {
        if stored_hs_id == hs_id {
            Self::Current { stale }
        } else {
            Self::Other {
                hs_id: stored_hs_id,
            }
        }
    }

    /// Whether a per-user read of the homeserver this mapping is relative to applies the
    /// user's events: only when the stored mapping points there and is not stale, as its
    /// user listing requires. A user with no stored mapping is on no per-user read's list;
    /// the bulk read classifies them by the homeserver they publish instead.
    pub(super) fn reads_here(mapping: Option<&Self>) -> bool {
        matches!(mapping, Some(Self::Current { stale: false }))
    }

    /// Whether a queued retry of an event read from the homeserver this mapping is relative
    /// to is still handled: unless the stored mapping points to another homeserver. That user
    /// is read there, and what they hold arrives through that homeserver's stream. A stale
    /// mapping still points here and a record naming it again resumes the read past the
    /// event, and a user with no stored mapping is the bulk read's, so both are retried.
    ///
    /// Known limit: a dropped DEL of something the user did not take along is lost, and the
    /// item stays indexed, like anything else left on the homeserver they moved from.
    pub(crate) fn retries_here(mapping: Option<&Self>) -> bool {
        !matches!(mapping, Some(Self::Other { .. }))
    }
}

/// Why a batch's closing cursor was not applied. Each variant has its own metric.
enum CursorRejection {
    /// Unparseable, would rewind the stored cursor, or advances without events.
    Invalid,
    /// The batch carried events, yet the checkpoint cannot move past them: the
    /// cursor holds at the position the batch was fetched with, or the batch
    /// carries no cursor line at all.
    Stalled,
}

impl CursorRejection {
    /// The rejection, and the reason the warning gives, for a Protocol violation that
    /// [`FeedBatch::closing_cursor`] found in a Batch whose cursor line is `raw_cursor`.
    fn for_violation(violation: &ProtocolViolation, raw_cursor: Option<&str>) -> (Self, String) {
        match (violation, raw_cursor) {
            (ProtocolViolation::MissingCursor, _) => (
                Self::Stalled,
                "batch carried events but no cursor line to advance the checkpoint".to_string(),
            ),
            (ProtocolViolation::CursorNotAdvanced { requested, .. }, Some(raw_cursor)) => (
                Self::Stalled,
                format!(
                    "batch carried events but cursor '{raw_cursor}' does not advance past the requested {requested}"
                ),
            ),
            (ProtocolViolation::CursorMovedWithoutEvents { requested, .. }, Some(raw_cursor)) => (
                Self::Invalid,
                format!(
                    "batch carried no events but cursor '{raw_cursor}' differs from the requested {requested}"
                ),
            ),
            // Never reached: Nexus's own check rejects a cursor line that is no `u64` first.
            (ProtocolViolation::MalformedCursor { .. }, _) => (Self::Invalid, violation.to_string()),
            // Never reached: `closing_cursor` returns none of the other violations, and the two
            // above only for a Batch with a cursor line.
            _ => (Self::Invalid, violation.to_string()),
        }
    }
}

/// Event processor for the primary homeserver
pub struct HsEventProcessor {
    /// The primary HS endpoint this processor fetches events from
    pub homeserver: Homeserver,

    /// See [WatcherConfig::events_limit]
    pub limit: u16,
    pub event_handler: Arc<dyn EventHandler>,
    /// Client the homeserver's events are polled through
    pub client: Arc<dyn FeedReader>,
    /// PKDNS lookup for the homeserver of a user without a `HOSTED_BY` edge
    pub resolver: Arc<dyn PkdnsHomeserverResolver>,
    pub shutdown_rx: Receiver<bool>,

    /// Scheduler used to enqueue failed events onto the retry queue
    pub retry_scheduler: Arc<RetryScheduler>,

    /// Per-run cache of users' homeserver mappings. For a given user's events in the events
    /// list, only the 1st one results in a graph lookup (and, for a user without a `HOSTED_BY`
    /// edge, a homeserver lookup), the rest read from this cache.
    ///
    /// Entries are deliberately never refreshed within a run: once a user's mapping
    /// is resolved, the same decision is reused for the rest of the batch even if the
    /// resolver realigns the underlying edge mid-run. The cache is dropped when the run ends.
    pub hs_mapping_cache: Mutex<HashMap<String, HsMapping>>,
}

#[async_trait::async_trait]
impl Run<Event, EventProcessorError> for HsEventProcessor {
    fn instance_name(&self) -> &'static str {
        "HsEventProcessor"
    }

    fn hs_pubkey(&self) -> &str {
        self.homeserver.id.as_ref()
    }

    /// The handler stores the line with the key of the homeserver the event was read from.
    async fn handle(&self, event: &Event) -> Result<(), EventProcessorError> {
        self.event_handler.handle(event, &self.homeserver.id).await
    }

    async fn handle_error(
        &self,
        event: &Event,
        error: EventProcessorError,
    ) -> Result<(), EventProcessorError> {
        handle_error(
            event,
            error,
            Some(&self.retry_scheduler),
            &self.homeserver.id,
        )
        .await
    }

    fn event_span(&self, event: &Event) -> tracing::Span {
        event_span(event)
    }

    /// Skips events from users that are not actively bound to this homeserver.
    ///
    /// Before an event is processed we inspect the user's `HOSTED_BY` edge:
    /// - A non-stale edge to this processor's homeserver: process.
    /// - No edge: the homeserver the user publishes is looked up instead. An event
    ///   is skipped only when the user publishes a different homeserver; when the
    ///   lookup finds none it is processed, since a DHT miss looks the same as no
    ///   record. A failed lookup counts as none: any DHT node can answer with another
    ///   key's packet, so stopping the batch on it would let one node stall this
    ///   homeserver.
    /// - A stale edge to this homeserver (the user's published homeserver has
    ///   diverged): log a warning and skip until the resolver realigns it.
    /// - An edge to a different homeserver: log a warning and skip.
    async fn should_handle_event(&self, event: &Event) -> Result<bool, EventProcessorError> {
        let user_id = event.target.user_id();

        match self.user_hs_mapping(user_id).await? {
            HsMapping::Unresolved | HsMapping::Current { stale: false } => Ok(true),

            // Bound here but the mapping is stale: skip until the resolver realigns it.
            HsMapping::Current { stale: true } => {
                warn!(
                    event.uri = %event.uri,
                    user_id = %user_id,
                    "User's homeserver mapping is stale; skipping event"
                );
                Ok(false)
            }

            // Bound to a different homeserver: skip.
            HsMapping::Other { hs_id } => {
                warn!(
                    event.uri = %event.uri,
                    user_id = %user_id,
                    user_homeserver = %hs_id,
                    "User is hosted on a different homeserver; skipping event"
                );
                Ok(false)
            }
        }
    }

    async fn run_internal(self: Arc<Self>) -> Result<RunOutcome, EventProcessorError> {
        let event_lines = self
            .poll_events()
            .await
            .inspect_err(|e| error!(error = ?e, "Error polling events"))?;

        if event_lines.is_empty() {
            debug!("No new events");
        } else {
            info!(event_lines = event_lines.len(), "Processing event lines");
            self.process_event_lines(event_lines).await?;
        }

        Ok(RunOutcome::Completed)
    }
}

impl HsEventProcessor {
    /// Resolves and caches a user's mapping relative to this processor's HS. A user without a
    /// `HOSTED_BY` edge is classified by the homeserver they publish.
    async fn user_hs_mapping(&self, user_id: &PubkyId) -> GraphResult<HsMapping> {
        if let Some(hs_mapping) = self.hs_mapping_cache.lock().await.get(user_id.as_ref()) {
            return Ok(hs_mapping.clone());
        }

        let mapping = match HsMapping::stored(user_id.as_ref(), &self.homeserver.id).await? {
            Some(stored) => stored,
            None => self.published_hs_mapping(user_id).await,
        };

        self.hs_mapping_cache
            .lock()
            .await
            .insert(user_id.as_ref().to_string(), mapping.clone());

        Ok(mapping)
    }

    /// Classifies the homeserver `user_id` publishes relative to this processor's HS.
    async fn published_hs_mapping(&self, user_id: &PubkyId) -> HsMapping {
        // A format-valid id need not be a curve point. No record can be signed by such a key,
        // so the user publishes no homeserver.
        let Ok(user_pk) = user_id.to_public_key() else {
            return HsMapping::Unresolved;
        };
        let published = match self.resolver.resolve_own_record(&user_pk).await {
            Ok(record) => record.homeserver(),
            Err(e) => {
                warn!(%user_id, error = %e, "PKDNS lookup failed; treating the user as publishing no homeserver");
                None
            }
        };
        match published {
            Some(hs_id) if hs_id.as_ref() == self.homeserver.id.as_ref() => {
                HsMapping::Current { stale: false }
            }
            Some(hs_id) => HsMapping::Other {
                hs_id: hs_id.to_string(),
            },
            None => HsMapping::Unresolved,
        }
    }

    /// Polls new events from the homeserver, from the current cursor on.
    ///
    /// Returns the event lines and the cursor line closing them, or no lines
    /// when there is nothing new.
    #[tracing::instrument(name = "events.poll", skip_all)]
    async fn poll_events(&self) -> Result<Vec<String>, EventProcessorError> {
        debug!(cursor = %self.homeserver.cursor, "Polling events");

        let hs = homeserver_public_key(&self.homeserver.id, HsOp::ReadFeed)?;
        let lines = self
            .client
            .read_feed(&hs, self.homeserver.cursor, self.limit)
            .await
            .inspect_err(|e| {
                if matches!(e.kind, HsErrorKind::TooLarge { .. }) {
                    REJECTED.add(1, &[KeyValue::new("reason", "size_exceeded")]);
                }
            })?;
        Ok(lines)
    }

    /// Processes a batch of event lines retrieved from the homeserver.
    ///
    /// The batch's cursor is checked *before* any handler runs and persisted
    /// only once the whole batch has been processed. A cursor the homeserver
    /// should not have sent — or failed to send at all — therefore costs
    /// nothing: the batch is skipped whole, the stored cursor stays put, and the
    /// next poll re-fetches from the same position instead of repeating side
    /// effects the cursor would never record.
    ///
    /// Per-event retry logic:
    /// - On error that should not be retried right now: stops the batch, cursor is not saved, next tick replays from same position
    /// - On MissingDependency: stores event in retry queue, continues processing
    /// - On 404 (blob not found): skips indexing, continues processing
    /// - On InvalidEventLine/SkipIndexing: logs and continues
    ///
    /// # Parameters
    /// - `lines`: A vector of strings representing event lines retrieved from the homeserver.
    #[tracing::instrument(name = "event_batch.process", skip_all, fields(batch.size = lines.len()))]
    pub async fn process_event_lines(&self, lines: Vec<String>) -> Result<(), EventProcessorError> {
        let batch = FeedBatch::split(&lines);

        // An idle answer: nothing to handle and no Cursor to store. `closing_cursor` would
        // return the requested Cursor, and storing it would only add a Redis write.
        if batch.cursor.is_none() && !batch.has_events() {
            return Ok(());
        }

        // Rejected: skip the batch entirely. Running the handlers would only queue up work
        // the next poll has to repeat, since the cursor stays where it is either way.
        let Some(next_cursor) = self.resolve_batch_cursor(&batch).await? else {
            return Ok(());
        };

        for line in &batch.event_lines {
            if *self.shutdown_rx.borrow() {
                debug!("Shutdown detected; exiting event processing loop");
                // The cursor is deliberately left unpersisted: the batch is only
                // partly processed, so the next run has to replay it.
                return Ok(());
            }

            self.handle_event_line(line).await?;
        }

        next_cursor.put_to_index().await?;

        Ok(())
    }

    /// Checks the Cursor closing `batch` and returns the [`Homeserver`] that stores it once
    /// the Batch is handled.
    ///
    /// A cursor line first goes through Nexus's own check against the stored Cursor, which
    /// refuses one that does not parse or would rewind it. Then the crate's Protocol check,
    /// [`FeedBatch::closing_cursor`], compares the Batch with the Cursor it was read from:
    /// `self.homeserver.cursor`, read from Redis when `self` was built and unchanged since,
    /// as a Run reads one Batch.
    ///
    /// Returns `Ok(None)` when the Cursor must not be stored, so the caller skips the Batch.
    /// Redis failures propagate instead of being charged to the Homeserver as bad input, so
    /// the Run is recorded as failed and retried on the next scheduled execution.
    async fn resolve_batch_cursor(
        &self,
        batch: &FeedBatch<'_>,
    ) -> Result<Option<Homeserver>, EventProcessorError> {
        if let Some(raw_cursor) = batch.cursor {
            info!("Received cursor for the next request: {raw_cursor}");

            match Homeserver::try_from_cursor(self.homeserver.id.clone(), raw_cursor).await {
                // The Cursor to store is the one the Protocol check below returns.
                Ok(_) => {}
                // Only the monotonicity guard raises `InvalidInput`; every other
                // Redis error is our own infrastructure failing, not bad HS input,
                // and must fail the run rather than silently skip a batch.
                Err(ModelError::KvOperationFailed(e))
                    if !matches!(e, RedisError::InvalidInput(_)) =>
                {
                    return Err(ModelError::KvOperationFailed(e).into());
                }
                // Unparseable, or it would rewind the stored cursor.
                Err(e) => {
                    let reason = format!("cursor '{raw_cursor}' cannot be applied: {e}");
                    self.reject_batch(&CursorRejection::Invalid, &reason);
                    return Ok(None);
                }
            }
        }

        match batch.closing_cursor(EventCursor::new(self.homeserver.cursor)) {
            Ok(cursor) => Ok(Some(Homeserver {
                id: self.homeserver.id.clone(),
                cursor: cursor.id(),
            })),
            Err(violation) => {
                let (rejection, reason) = CursorRejection::for_violation(&violation, batch.cursor);
                self.reject_batch(&rejection, &reason);
                Ok(None)
            }
        }
    }

    /// Records a skipped batch on the metric matching `rejection`, and warns.
    fn reject_batch(&self, rejection: &CursorRejection, reason: &str) {
        let counter = match rejection {
            CursorRejection::Invalid => &INVALID_CURSOR_PRIMARY_HS,
            CursorRejection::Stalled => &STALLED_CURSOR_PRIMARY_HS,
        };
        counter.add(1, &[KeyValue::new("hs_id", self.homeserver.id.to_string())]);

        warn!(hs_id = %self.homeserver.id, "Skipping batch: {reason}");
    }
}

#[cfg(test)]
mod tests {
    use super::HsMapping;

    #[test]
    fn a_stored_mapping_is_current_only_for_its_own_homeserver() {
        assert_eq!(
            HsMapping::relative_to("hs-a".into(), false, "hs-a"),
            HsMapping::Current { stale: false }
        );
        assert_eq!(
            HsMapping::relative_to("hs-a".into(), true, "hs-a"),
            HsMapping::Current { stale: true }
        );
        assert_eq!(
            HsMapping::relative_to("hs-b".into(), false, "hs-a"),
            HsMapping::Other {
                hs_id: "hs-b".into()
            }
        );
    }

    #[test]
    fn a_per_user_read_applies_only_a_current_non_stale_mapping() {
        let other = HsMapping::Other {
            hs_id: "hs-b".into(),
        };
        let cases = [
            (Some(HsMapping::Current { stale: false }), true),
            (Some(HsMapping::Current { stale: true }), false),
            (Some(other), false),
            (Some(HsMapping::Unresolved), false),
            (None, false),
        ];

        for (mapping, read) in cases {
            assert_eq!(HsMapping::reads_here(mapping.as_ref()), read, "{mapping:?}");
        }
    }

    #[test]
    fn a_retry_is_dropped_only_for_a_mapping_to_another_homeserver() {
        let other = HsMapping::Other {
            hs_id: "hs-b".into(),
        };
        let cases = [
            (Some(HsMapping::Current { stale: false }), true),
            (Some(HsMapping::Current { stale: true }), true),
            (Some(HsMapping::Unresolved), true),
            (None, true),
            (Some(other), false),
        ];

        for (mapping, retried) in cases {
            assert_eq!(
                HsMapping::retries_here(mapping.as_ref()),
                retried,
                "{mapping:?}"
            );
        }
    }
}
