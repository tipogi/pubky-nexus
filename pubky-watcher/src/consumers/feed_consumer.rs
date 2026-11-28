use std::fmt::{Debug, Display};
use std::sync::Arc;

use pubky::{EventCursor, PublicKey};
use tokio::sync::watch::Receiver;
use tracing::{debug, warn};

use super::{
    run_consumer, validate_limit, ConfigError, FeedOutcome, Missing, WatcherError,
    DEFAULT_EVENTS_LIMIT,
};
use crate::feed::{FeedBatch, FeedEvent};
use crate::hooks::{EventHandler, EventLineOutcome, FromEventLine};
use crate::run::{Run, RunOutcome};
use crate::FeedReader;

/// Builds a [`FeedConsumer`]: see [`Watcher::feed`](super::Watcher::feed).
pub struct FeedConsumerBuilder<H> {
    hs_pubkey: PublicKey,
    handler: H,
    events_limit: u16,
    reader: Arc<dyn FeedReader>,
}

impl FeedConsumerBuilder<Missing> {
    pub(super) fn new<C>(client: C, hs_pubkey: PublicKey) -> Self
    where
        C: FeedReader,
    {
        Self {
            hs_pubkey,
            handler: Missing,
            events_limit: DEFAULT_EVENTS_LIMIT,
            reader: Arc::new(client),
        }
    }
}

impl<H> FeedConsumerBuilder<H> {
    /// The most Events a Run asks for ([`DEFAULT_EVENTS_LIMIT`] by default). Zero is
    /// rejected by [`Self::build`].
    pub fn events_limit(mut self, limit: u16) -> Self {
        self.events_limit = limit;
        self
    }

    /// The Handler each Event goes to: an [`EventHandler`] of [`FeedEvent`]s, whose
    /// error a failed Run gives back as [`WatcherError::Handler`].
    pub fn handler<NewH>(self, handler: NewH) -> FeedConsumerBuilder<NewH> {
        FeedConsumerBuilder {
            hs_pubkey: self.hs_pubkey,
            handler,
            events_limit: self.events_limit,
            reader: self.reader,
        }
    }

    /// Builds the Feed consumer, which stops reading once `shutdown_rx` turns `true`. Fails
    /// on an events limit of zero.
    /// The error is a [`ConfigError`].
    pub fn build<Err>(self, shutdown_rx: Receiver<bool>) -> Result<FeedConsumer<Err>, ConfigError>
    where
        H: EventHandler<FeedEvent, Err> + 'static,
        Err: Display + Debug + Send + Sync + 'static,
    {
        validate_limit(self.events_limit)?;
        Ok(FeedConsumer {
            config: Arc::new(Config {
                hs_pubkey_z32: self.hs_pubkey.z32(),
                hs_pubkey: self.hs_pubkey,
                handler: Arc::new(self.handler),
                events_limit: self.events_limit,
                reader: self.reader,
                shutdown_rx,
            }),
        })
    }
}

struct Config<Err> {
    hs_pubkey: PublicKey,
    hs_pubkey_z32: String,
    handler: Arc<dyn EventHandler<FeedEvent, Err>>,
    events_limit: u16,
    reader: Arc<dyn FeedReader>,
    shutdown_rx: Receiver<bool>,
}

/// A Feed consumer: a Consumer that reads a Homeserver's Events feed, one Batch per
/// [`Self::run`]. `Err` is its Handler's error.
pub struct FeedConsumer<Err> {
    config: Arc<Config<Err>>,
}

impl<Err> FeedConsumer<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    /// Makes one Run: reads the Batch after `cursor`, hands each Event to the Handler in
    /// order, and returns the Checkpoint to start the next Run from.
    ///
    /// The Batch's Cursor is checked before any Event is handled, with the Protocol check
    /// [`FeedBatch::closing_cursor`]: it must be a number, and move past `cursor` when the
    /// Batch has Events. A Protocol violation fails the Run
    /// with [`WatcherError::Protocol`]. An idle Homeserver answers no Events, and the Cursor
    /// stays where it was. A Handler error fails the Run.
    pub async fn run(&self, cursor: EventCursor) -> Result<FeedOutcome, WatcherError<Err>> {
        let run = Arc::new(FeedRun {
            config: self.config.clone(),
            cursor,
        });
        let read_run = run.clone();
        run_consumer(run.instance_name(), run.hs_pubkey(), async move {
            read_run.read().await
        })
        .await
    }
}

/// One Run of a [`FeedConsumer`], as a [`Run`], so each Event is handled in its
/// [`Run::event_span`].
struct FeedRun<Err> {
    config: Arc<Config<Err>>,
    cursor: EventCursor,
}

#[async_trait::async_trait]
impl<Err> Run<FeedEvent, WatcherError<Err>> for FeedRun<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    fn instance_name(&self) -> &'static str {
        "FeedConsumer"
    }

    fn hs_pubkey(&self) -> &str {
        &self.config.hs_pubkey_z32
    }

    async fn handle(&self, event: &FeedEvent) -> Result<(), WatcherError<Err>> {
        self.config
            .handler
            .handle(event)
            .await
            .map_err(WatcherError::Handler)
    }

    async fn run_internal(self: Arc<Self>) -> Result<RunOutcome, WatcherError<Err>> {
        self.read().await?;
        Ok(RunOutcome::Completed)
    }
}

impl<Err> FeedRun<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    async fn read(&self) -> Result<FeedOutcome, WatcherError<Err>> {
        let config = &self.config;
        if *config.shutdown_rx.borrow() {
            return Ok(FeedOutcome::interrupted(self.cursor, 0));
        }

        debug!(
            cursor = self.cursor.id(),
            limit = config.events_limit,
            "Polling events"
        );
        let lines = config
            .reader
            .read_feed(&config.hs_pubkey, self.cursor.id(), config.events_limit)
            .await?;
        if lines.is_empty() {
            debug!("No new events");
            return Ok(FeedOutcome::completed(self.cursor, 0));
        }

        let batch = FeedBatch::split(&lines);
        let closing_cursor = batch.closing_cursor(self.cursor)?;

        let mut handled_events = 0;
        for line in &batch.event_lines {
            if *config.shutdown_rx.borrow() {
                debug!(
                    processed_events = handled_events,
                    "Shutdown detected; the batch is read again next run"
                );
                return Ok(FeedOutcome::interrupted(self.cursor, handled_events));
            }

            match FeedEvent::from_event_line(line) {
                Ok(EventLineOutcome::Parsed(event)) => {
                    self.handle_event(&event).await?;
                    handled_events += 1;
                }
                Ok(EventLineOutcome::Skipped) => {}
                Ok(EventLineOutcome::Unrecognized { reason }) => {
                    warn!(%reason, "Skipping an unrecognized event line");
                }
                Err(never) => match never {},
            }
        }

        Ok(FeedOutcome::completed(closing_cursor, handled_events))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use pubky::{EventCursor, Keypair, PublicKey};
    use tokio::sync::watch;

    use crate::{
        ConfigError, EventHandler, EventType, FeedEvent, FeedReader, HsError, HsErrorKind, HsOp,
        ProtocolViolation, Watcher, WatcherError,
    };

    /// The Handler's own error: the index of the Event it failed on.
    #[derive(Debug, PartialEq, Eq)]
    struct HandlerFailed(usize);

    impl std::fmt::Display for HandlerFailed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "handler failed on event {}", self.0)
        }
    }

    /// Answers every read with `lines`, recording the Cursors and limits asked for.
    struct Lines {
        lines: Vec<String>,
        reads: Mutex<Vec<(u64, u16)>>,
    }

    impl Lines {
        fn new(lines: &[&str]) -> &'static Self {
            leak(Self {
                lines: lines.iter().map(|line| line.to_string()).collect(),
                reads: Mutex::default(),
            })
        }
    }

    #[async_trait::async_trait]
    impl FeedReader for &'static Lines {
        async fn read_feed(
            &self,
            _hs_pubkey: &PublicKey,
            cursor: u64,
            limit: u16,
        ) -> Result<Vec<String>, HsError> {
            self.reads.lock().unwrap().push((cursor, limit));
            Ok(self.lines.clone())
        }
    }

    /// Fails every read, as a Homeserver that is down.
    struct Down;

    #[async_trait::async_trait]
    impl FeedReader for Down {
        async fn read_feed(
            &self,
            hs_pubkey: &PublicKey,
            _cursor: u64,
            _limit: u16,
        ) -> Result<Vec<String>, HsError> {
            Err(HsError::new(
                hs_pubkey,
                HsOp::ReadFeed,
                HsErrorKind::ServerError(503, "down".into()),
            ))
        }
    }

    /// Records the Events it gets. Shuts the Consumer down after `shutdown_after` of them, or
    /// fails the one numbered `fail_at`.
    #[derive(Default)]
    struct Recorder {
        events: Mutex<Vec<FeedEvent>>,
        shutdown_after: Option<(usize, watch::Sender<bool>)>,
        fail_at: Option<usize>,
    }

    #[async_trait::async_trait]
    impl EventHandler<FeedEvent, HandlerFailed> for &'static Recorder {
        async fn handle(&self, event: &FeedEvent) -> Result<(), HandlerFailed> {
            let mut events = self.events.lock().unwrap();
            if self.fail_at == Some(events.len()) {
                return Err(HandlerFailed(events.len()));
            }
            events.push(event.clone());
            if let Some((after, shutdown)) = &self.shutdown_after {
                if events.len() == *after {
                    shutdown.send(true).unwrap();
                }
            }
            Ok(())
        }
    }

    fn leak<T>(value: T) -> &'static T {
        Box::leak(Box::new(value))
    }

    fn hs_pubkey() -> PublicKey {
        Keypair::random().public_key()
    }

    async fn run(
        reader: impl FeedReader,
        handler: &'static Recorder,
        cursor: u64,
    ) -> Result<crate::FeedOutcome, WatcherError<HandlerFailed>> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        Watcher::feed(reader, hs_pubkey())
            .handler(handler)
            .build(shutdown_rx)
            .expect("a valid configuration")
            .run(EventCursor::new(cursor))
            .await
    }

    /// The Events are handed over in order, and the Checkpoint moves to the Batch's Cursor.
    #[tokio::test]
    async fn a_batch_is_handled_and_moves_the_checkpoint() {
        let handler = leak(Recorder::default());
        let reader = Lines::new(&["PUT pubky://a/pub/x", "DEL pubky://a/pub/y", "cursor: 12"]);

        let outcome = run(reader, handler, 10).await.unwrap();

        assert_eq!(outcome.checkpoint, EventCursor::new(12));
        assert_eq!(outcome.handled_events, 2);
        assert!(outcome.completed);
        let events = handler.events.lock().unwrap();
        assert_eq!(events[0].event_type, EventType::Put);
        assert_eq!(events[1].uri, "pubky://a/pub/y");
    }

    /// Unrecognized lines are skipped, and still let the Checkpoint move.
    #[tokio::test]
    async fn unrecognized_lines_are_skipped() {
        let handler = leak(Recorder::default());
        let reader = Lines::new(&["garbage", "PUT pubky://a/pub/x", "cursor: 11"]);

        let outcome = run(reader, handler, 10).await.unwrap();

        assert_eq!(outcome.checkpoint, EventCursor::new(11));
        assert_eq!(outcome.handled_events, 1);
    }

    /// An idle Homeserver answers no lines, or only the Cursor it was read from: no Events,
    /// and the Checkpoint stays.
    #[tokio::test]
    async fn an_idle_homeserver_keeps_the_checkpoint() {
        for lines in [vec![], vec!["cursor: 7"]] {
            let handler = leak(Recorder::default());

            let outcome = run(Lines::new(&lines), handler, 7).await.unwrap();

            assert_eq!(outcome.checkpoint, EventCursor::new(7), "{lines:?}");
            assert_eq!(outcome.handled_events, 0);
            assert!(outcome.completed);
        }
    }

    /// A Batch whose Cursor is missing, no number, does not move past the one read from, or
    /// moves without Events fails the Run before any Event is handled.
    #[tokio::test]
    async fn a_batch_with_a_bad_cursor_fails_before_any_event() {
        let not_advanced = |got| ProtocolViolation::CursorNotAdvanced {
            user_pubkey: None,
            requested: 10,
            got,
        };
        for (lines, expected) in [
            (
                vec!["PUT pubky://a/pub/x"],
                ProtocolViolation::MissingCursor,
            ),
            (
                vec!["PUT pubky://a/pub/x", "cursor: nope"],
                ProtocolViolation::MalformedCursor { raw: "nope".into() },
            ),
            (vec!["PUT pubky://a/pub/x", "cursor: 10"], not_advanced(10)),
            (vec!["PUT pubky://a/pub/x", "cursor: 9"], not_advanced(9)),
            (
                vec!["cursor: 11"],
                ProtocolViolation::CursorMovedWithoutEvents {
                    requested: 10,
                    got: 11,
                },
            ),
        ] {
            let handler = leak(Recorder::default());

            let error = run(Lines::new(&lines), handler, 10).await.unwrap_err();

            assert!(
                matches!(&error, WatcherError::Protocol(got) if *got == expected),
                "{lines:?}: {error:?}"
            );
            assert!(handler.events.lock().unwrap().is_empty(), "{lines:?}");
        }
    }

    /// A Run stopped by shutdown is not an error. Its Checkpoint is the Cursor it started
    /// from, so the next Run reads the Batch again and no Event is skipped.
    #[tokio::test]
    async fn a_run_stopped_by_shutdown_keeps_the_checkpoint() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler = leak(Recorder {
            shutdown_after: Some((1, shutdown_tx)),
            ..Recorder::default()
        });
        let reader = Lines::new(&["PUT pubky://a/pub/x", "PUT pubky://a/pub/y", "cursor: 12"]);

        let outcome = Watcher::feed(reader, hs_pubkey())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(EventCursor::new(10))
            .await
            .unwrap();

        assert!(!outcome.completed);
        assert_eq!(outcome.checkpoint, EventCursor::new(10));
        assert_eq!(outcome.handled_events, 1);
    }

    /// A Consumer shut down before the Run does not read.
    #[tokio::test]
    async fn a_run_after_shutdown_does_not_read() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();
        let reader = Lines::new(&["PUT pubky://a/pub/x", "cursor: 12"]);

        let outcome = Watcher::feed(reader, hs_pubkey())
            .handler(leak(Recorder::default()))
            .build(shutdown_rx)
            .unwrap()
            .run(EventCursor::new(10))
            .await
            .unwrap();

        assert!(!outcome.completed);
        assert_eq!(outcome.checkpoint, EventCursor::new(10));
        assert!(reader.reads.lock().unwrap().is_empty());
    }

    /// A failed Read or a failed Handler fails the Run.
    #[tokio::test]
    async fn a_failed_read_or_handler_fails_the_run() {
        let error = run(Down, leak(Recorder::default()), 10).await.unwrap_err();
        assert!(
            matches!(&error, WatcherError::Homeserver(e) if e.op == HsOp::ReadFeed),
            "{error:?}"
        );

        let failing = leak(Recorder {
            fail_at: Some(1),
            ..Recorder::default()
        });
        let reader = Lines::new(&["PUT pubky://a/pub/x", "PUT pubky://a/pub/y", "cursor: 11"]);
        let error = run(reader, failing, 10).await.unwrap_err();
        assert!(
            matches!(error, WatcherError::Handler(HandlerFailed(1))),
            "{error:?}"
        );
        assert_eq!(error.to_string(), "handler failed on event 1");
    }

    /// The limit is passed to the Homeserver, and zero is rejected at build time.
    #[tokio::test]
    async fn the_events_limit_is_requested_and_must_not_be_zero() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let reader = Lines::new(&[]);

        Watcher::feed(reader, hs_pubkey())
            .handler(leak(Recorder::default()))
            .events_limit(7)
            .build(shutdown_rx.clone())
            .unwrap()
            .run(EventCursor::new(3))
            .await
            .unwrap();
        assert_eq!(*reader.reads.lock().unwrap(), [(3, 7)]);

        let zero = Watcher::feed(Lines::new(&[]), hs_pubkey())
            .handler(leak(Recorder::default()))
            .events_limit(0)
            .build::<HandlerFailed>(shutdown_rx);
        assert_eq!(zero.err(), Some(ConfigError::ZeroEventsLimit));
    }
}
