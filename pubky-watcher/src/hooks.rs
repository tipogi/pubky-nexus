//! The traits an Indexer implements to plug into the crate.
//!
//! Implement [`EventHandler`] to give a Consumer ([`Watcher`](crate::Watcher)) its Handler.
//! Implement [`FromEventLine`] when your own [`Run`](crate::Run) reads Event lines into your
//! own Event type, and [`RetryableError`] with [`RetryQueue`] when it should hand a failed
//! Event back for a Retry instead of failing the Run.

/// How one Event line parsed.
/// Returned by [`FromEventLine::from_event_line`].
#[derive(Debug)]
pub enum EventLineOutcome<E> {
    /// A line the Indexer handles.
    /// Handed to [`Run::handle_event`](crate::Run::handle_event).
    Parsed(E),
    /// A well-formed line the Indexer ignores.
    /// Dropped silently by
    /// [`Run::handle_event_line`](crate::Run::handle_event_line).
    Skipped,
    /// A line naming a Resource the Indexer does not recognise.
    /// Logged and skipped by
    /// [`Run::handle_event_line`](crate::Run::handle_event_line).
    Unrecognized {
        /// Why the Resource was not recognised, for the log.
        /// Logged by
        /// [`Run::handle_event_line`](crate::Run::handle_event_line).
        reason: String,
    },
}

/// Parses an Event line (`PUT <uri>` / `DEL <uri>`) into the Indexer's Event type. A line that
/// cannot be parsed at all is an error; the Run logs it and moves on, since Event lines come
/// from untrusted Homeservers.
/// [`FeedEvent`](crate::FeedEvent) implements it.
pub trait FromEventLine: Sized {
    /// Why a line could not be parsed at all. Logged by
    /// [`Run::handle_event_line`](crate::Run::handle_event_line),
    /// never returned to the caller.
    type Error: std::fmt::Display;

    /// Parses `line`, one of a Batch's [`FeedBatch::event_lines`](crate::FeedBatch).
    fn from_event_line(line: &str) -> Result<EventLineOutcome<Self>, Self::Error>;
}

/// The Handler: the Indexer's code that gives an Event its meaning, e.g. indexing or storing
/// it.
///
/// A Consumer ([`Watcher`](crate::Watcher)) is given one. A failure fails a Feed consumer's
/// Run, so the Indexer starts the next one from the Checkpoint it last stored. On a Stream
/// consumer it ends that User pubkey's Batch only, at the Checkpoint of the last Event
/// handled.
#[async_trait::async_trait]
pub trait EventHandler<E, Err>: Send + Sync {
    /// Handles `event`. An error is given back as
    /// [`WatcherError::Handler`](crate::WatcherError::Handler): by a
    /// [`FeedConsumer`](crate::FeedConsumer) as its Run's error, by a
    /// [`StreamConsumer`](crate::StreamConsumer) in
    /// [`StreamOutcome::failures`](crate::StreamOutcome::failures).
    async fn handle(&self, event: &E) -> Result<(), Err>;
}

/// Sorts a Handler's failure for [`dispatch_retryable_error`](super::dispatch_retryable_error).
///
/// Opt-in: [`Run`](super::Run) does not require it. A Run that hands failed Events back for
/// a Retry implements it for its error type and calls `dispatch_retryable_error` from an
/// overridden [`handle_error`](super::Run::handle_error).
pub trait RetryableError: std::fmt::Display + Send + Sync {
    /// Whether the Run should stop, and the error be returned to the caller, instead of
    /// being handled as one Event's failure.
    /// [`dispatch_retryable_error`](crate::dispatch_retryable_error) checks it first.
    fn should_not_retry_now(&self) -> bool;

    /// Whether the failure is a Missing dependency: something the Event refers to has not
    /// been handled yet. Such an Event goes to [`RetryQueue::queue_missing_dependency`], any
    /// other to [`RetryQueue::queue_transient_failure`].
    fn is_missing_dependency(&self) -> bool;

    /// Whether the Event is worth a Retry. One that is not is dropped instead of reaching
    /// the [`RetryQueue`].
    fn should_enqueue_for_retry(&self) -> bool;
}

/// The Indexer's queue of Events to Retry. The implementation knows where an Event came
/// from, so it can tie the Retry to that Homeserver.
///
/// Used by [`dispatch_retryable_error`](super::dispatch_retryable_error) once a failure is
/// sorted as worth a Retry. The two methods let an implementation schedule a Missing
/// dependency and a Transient failure differently. The queue, its timing and its limits are
/// the Indexer's.
#[async_trait::async_trait]
pub trait RetryQueue<E, Err>: Send + Sync {
    /// Queues an Event whose failure is a Missing dependency
    /// ([`RetryableError::is_missing_dependency`]).
    async fn queue_missing_dependency(&self, event: &E) -> Result<(), Err>;

    /// Queues an Event whose failure is a Transient failure: any other one worth a Retry
    /// ([`RetryableError::should_enqueue_for_retry`]).
    async fn queue_transient_failure(&self, event: &E) -> Result<(), Err>;
}
