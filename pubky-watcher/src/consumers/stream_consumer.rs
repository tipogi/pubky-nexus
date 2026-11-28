use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use pubky::{EventCursor, PublicKey};
use tokio::sync::watch::Receiver;
use tracing::{debug, warn};

use super::{
    run_consumer, validate_limit, ConfigError, Missing, WatcherError, DEFAULT_EVENTS_LIMIT,
};
use crate::hooks::EventHandler;
use crate::run::{Run, RunOutcome};
use crate::{
    check_event_user_pubkey, check_stream_cursors, check_stream_limit, HsError, HsErrorKind,
    ProtocolViolation, StreamEvent, StreamReader,
};

/// The path an Event stream is read under when the builder sets none: everything its User
/// pubkey publishes.
/// Set another with [`StreamConsumerBuilder::path`].
pub const DEFAULT_STREAM_PATH: &str = "/pub/";

/// Builds a [`StreamConsumer`]: see [`Watcher::stream`](super::Watcher::stream). It sets the
/// Handler ([`Self::handler`]), the events limit ([`Self::events_limit`]) and the path
/// ([`Self::path`]); the User pubkeys are given to each [`StreamConsumer::run`].
pub struct StreamConsumerBuilder<H> {
    hs_pubkey: PublicKey,
    handler: H,
    events_limit: u16,
    path: String,
    reader: Arc<dyn StreamReader>,
}

impl StreamConsumerBuilder<Missing> {
    pub(super) fn new<C>(client: C, hs_pubkey: PublicKey) -> Self
    where
        C: StreamReader,
    {
        Self {
            hs_pubkey,
            handler: Missing,
            events_limit: DEFAULT_EVENTS_LIMIT,
            path: DEFAULT_STREAM_PATH.to_string(),
            reader: Arc::new(client),
        }
    }
}

impl<H> StreamConsumerBuilder<H> {
    /// The most Events a Run asks for per User pubkey ([`DEFAULT_EVENTS_LIMIT`] by default).
    /// Zero is rejected by [`Self::build`].
    pub fn events_limit(mut self, limit: u16) -> Self {
        self.events_limit = limit;
        self
    }

    /// Reads only the Events whose Resource path starts with `path`
    /// ([`DEFAULT_STREAM_PATH`] by default). A path that does not start with `/` is
    /// rejected by [`Self::build`].
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// The Handler each Event goes to: an [`EventHandler`] of [`StreamEvent`]s. Its error
    /// ends that User pubkey's Batch, and the Run gives it back as [`WatcherError::Handler`]
    /// in [`StreamOutcome::failures`].
    pub fn handler<NewH>(self, handler: NewH) -> StreamConsumerBuilder<NewH> {
        StreamConsumerBuilder {
            hs_pubkey: self.hs_pubkey,
            handler,
            events_limit: self.events_limit,
            path: self.path,
            reader: self.reader,
        }
    }
}

impl<H> StreamConsumerBuilder<H> {
    /// Builds the Stream consumer, which stops reading once `shutdown_rx` turns `true`. Fails
    /// on an events limit of zero, or a path that does not start with `/`.
    /// The error is a [`ConfigError`].
    pub fn build<Err>(self, shutdown_rx: Receiver<bool>) -> Result<StreamConsumer<Err>, ConfigError>
    where
        H: EventHandler<StreamEvent, Err> + 'static,
        Err: Display + Debug + Send + Sync + 'static,
    {
        validate_limit(self.events_limit)?;
        if !self.path.starts_with('/') {
            return Err(ConfigError::InvalidPath { path: self.path });
        }

        Ok(StreamConsumer {
            config: Arc::new(Config {
                hs_pubkey_z32: self.hs_pubkey.z32(),
                hs_pubkey: self.hs_pubkey,
                handler: Arc::new(self.handler),
                events_limit: self.events_limit,
                path: self.path,
                reader: self.reader,
                shutdown_rx,
            }),
        })
    }
}

struct Config<Err> {
    hs_pubkey: PublicKey,
    hs_pubkey_z32: String,
    handler: Arc<dyn EventHandler<StreamEvent, Err>>,
    events_limit: u16,
    path: String,
    reader: Arc<dyn StreamReader>,
    shutdown_rx: Receiver<bool>,
}

/// How a Run of a [`StreamConsumer`] ended. [`StreamConsumer::run`] returns it whatever went
/// wrong with a user or with the Homeserver, so the Checkpoints are never lost: what went
/// wrong is in [`Self::failures`] and [`Self::stopped`]. `Err` is the Handler's error.
#[derive(Debug)]
pub struct StreamOutcome<Err> {
    /// One Checkpoint per User pubkey the Run was given, in the order given: the Cursor with
    /// every Event of that User pubkey before it handled. They are the ones an Indexer may
    /// store. They have the shape of the Run's input: pass them to the next
    /// [`StreamConsumer::run`] as they are.
    ///
    /// A Checkpoint moves after each Event the Handler handled, and never past an Event it
    /// did not handle. A user listed in [`Self::failures`] for a Handler error stays at the
    /// last Event handled. A user whose Read failed, whose answer was a Protocol violation,
    /// or who was not read because the Run stopped before them ([`Self::stopped`]), keeps the
    /// Cursor they were given.
    pub checkpoints: Vec<(PublicKey, EventCursor)>,
    /// Each User pubkey whose Batch ended early while the Run went on to the next user, with
    /// the error that ended it, in the order read. Only two variants of [`WatcherError`]
    /// appear here:
    ///
    /// - [`WatcherError::Homeserver`]: the Homeserver refused the Read of that user's Event
    ///   stream, with an [`HsErrorKind`] of `NotFound`, `Rejected` (every status, `408`
    ///   included), `TooLarge`, `Malformed` or `Sdk`.
    /// - [`WatcherError::Handler`]: the Handler failed on one of that user's Events, with the
    ///   Handler's own error.
    ///
    /// The failure that stops a Run is in [`Self::stopped`] only, never here.
    pub failures: Vec<(PublicKey, WatcherError<Err>)>,
    /// The Events the Handler handled, each through [`EventHandler::handle`]. It counts those
    /// of a user listed in [`Self::failures`] too: the ones handled before their Batch ended.
    pub handled_events: usize,
    /// Why the Run stopped before reading every User pubkey's Event stream, as a
    /// [`StopReason`]; `None` when it read them all, and for a Run given no users. The users
    /// after the one it stopped at were not read, and every Checkpoint in
    /// [`Self::checkpoints`] may still be stored.
    pub stopped: Option<StopReason>,
}

/// Why a Run of a [`StreamConsumer`] stopped before reading every User pubkey's Event stream:
/// see [`StreamOutcome::stopped`]. A stopped Run has not failed: it returned `Ok`, with
/// every Checkpoint in [`StreamOutcome::checkpoints`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum StopReason {
    /// The shutdown signal given to [`StreamConsumerBuilder::build`] turned `true`. It is the
    /// Indexer's own stop, and says nothing about the Homeserver.
    Shutdown,
    /// The Homeserver could not be read, with the [`HsError`] of the Read that failed. Its
    /// [`HsErrorKind`] is `Timeout`, `Transport`, `ServerError` or `RateLimited`; the Client
    /// has already retried a rate-limited Read before it returns that kind. Any other kind
    /// ends one user's Batch and is listed in [`StreamOutcome::failures`].
    Homeserver(HsError),
    /// The Homeserver's answer was a Protocol violation, as the [`ProtocolViolation`] says:
    /// none of its Events is handled, and the Run reads nothing more from that Homeserver.
    /// It names the User pubkey whose Event stream broke the rules.
    Protocol(ProtocolViolation),
}

/// A Stream consumer: a Consumer that reads the Event streams of chosen User pubkeys on one
/// Homeserver, one after the other, one Batch each per [`Self::run`]. It is built once, by
/// [`Watcher::stream`](super::Watcher::stream), and makes any number of Runs: each
/// [`Self::run`] is given the User pubkeys to read. `Err` is its Handler's error.
pub struct StreamConsumer<Err> {
    config: Arc<Config<Err>>,
}

impl<Err> StreamConsumer<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    /// Makes one Run over `users`, pairs of a User pubkey and the Cursor to read it from:
    /// reads each User pubkey's Events after its Cursor, hands them to the Handler in order,
    /// and returns each one's Checkpoint in [`StreamOutcome::checkpoints`]. The Checkpoints
    /// have the shape and the order of `users`, so they are the next Run's `users` as they
    /// are; an Indexer whose list of users changed gives the new list.
    ///
    /// The Run returns `Ok` whenever it has Checkpoints to give, so a plain `?` keeps them.
    /// What went wrong is in the [`StreamOutcome`]:
    ///
    /// - **A failure that belongs to one user ends that user's Batch only**, and the next
    ///   user is read: a Read the Homeserver refused, or a Handler error. Each such user is
    ///   listed in [`StreamOutcome::failures`].
    /// - **A failure that belongs to the Homeserver stops the Run**, and the users after it
    ///   are not read: a Homeserver that cannot be read, or a Protocol violation.
    ///   [`StreamOutcome::stopped`] says why, as a [`StopReason`], as it does for the
    ///   shutdown signal given to [`StreamConsumerBuilder::build`].
    ///
    /// An Event stream's Batch is checked whole before any of its Events is handled, by the
    /// Protocol checks in this order: [`check_stream_limit`] (no more Events than asked for),
    /// [`check_stream_cursors`] (Cursors that only move forward), then
    /// [`check_event_user_pubkey`] on every Event (each its own User pubkey's). The first
    /// Protocol violation found ([`ProtocolViolation`]) is the one reported: none of the
    /// Batch's Events is handled, and the Run reads nothing more from that Homeserver.
    ///
    /// The Run's own error means only that the Run could not be made: a User pubkey listed
    /// twice, rejected before any Read with [`WatcherError::DuplicateUser`], or a task that
    /// panicked ([`WatcherError::Panicked`]). A Run with no users is a normal Run: it returns
    /// `Ok` with nothing read, nothing handled and no [`StreamOutcome::stopped`].
    pub async fn run(
        &self,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> Result<StreamOutcome<Err>, WatcherError<Err>> {
        let run = Arc::new(StreamRun {
            config: self.config.clone(),
            users,
        });
        let read_run = run.clone();
        run_consumer(run.instance_name(), run.hs_pubkey(), async move {
            read_run.read().await
        })
        .await
    }
}

/// One Run of a [`StreamConsumer`], as a [`Run`], so each Event is handled in its
/// [`Run::event_span`].
struct StreamRun<Err> {
    config: Arc<Config<Err>>,
    /// The User pubkeys to read, each with the Cursor to read it from.
    users: Vec<(PublicKey, EventCursor)>,
}

#[async_trait::async_trait]
impl<Err> Run<StreamEvent, WatcherError<Err>> for StreamRun<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    fn instance_name(&self) -> &'static str {
        "StreamConsumer"
    }

    fn hs_pubkey(&self) -> &str {
        &self.config.hs_pubkey_z32
    }

    async fn handle(&self, event: &StreamEvent) -> Result<(), WatcherError<Err>> {
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

impl<Err> StreamRun<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    async fn read(&self) -> Result<StreamOutcome<Err>, WatcherError<Err>> {
        if let Some(user_pubkey) = listed_twice(&self.users).cloned() {
            return Err(WatcherError::DuplicateUser { user_pubkey });
        }

        let hs_pubkey = &self.config.hs_pubkey_z32;
        let mut checkpoints = self.users.clone();
        let mut failures = Vec::new();
        let mut handled_events = 0;
        let mut stopped = None;

        for (user_pubkey, checkpoint) in checkpoints.iter_mut() {
            match self
                .read_user(user_pubkey, checkpoint, &mut handled_events)
                .await
            {
                Ok(()) => {}
                Err(BatchEnd::Failed(error)) => {
                    warn!(
                        homeserver = %hs_pubkey,
                        user = %user_pubkey.z32(),
                        %error,
                        "The user's batch ended early; reading the next user"
                    );
                    failures.push((user_pubkey.clone(), error));
                }
                Err(BatchEnd::Stopped(reason)) => {
                    log_stop(hs_pubkey, user_pubkey, &reason);
                    stopped = Some(reason);
                    break;
                }
            }
        }

        Ok(StreamOutcome {
            checkpoints,
            failures,
            handled_events,
            stopped,
        })
    }

    /// Reads one Batch of `user_pubkey`'s Event stream after `checkpoint`, checks it whole,
    /// and hands its Events to the Handler, moving `checkpoint` and counting in
    /// `handled_events` after each Event handled, and only then.
    async fn read_user(
        &self,
        user_pubkey: &PublicKey,
        checkpoint: &mut EventCursor,
        handled_events: &mut usize,
    ) -> Result<(), BatchEnd<Err>> {
        let config = &self.config;
        if *config.shutdown_rx.borrow() {
            return Err(BatchEnd::Stopped(StopReason::Shutdown));
        }

        let cursor = *checkpoint;
        debug!(user = %user_pubkey.z32(), cursor = cursor.id(), "Reading the user's events");
        let events = config
            .reader
            .read_stream(
                &config.hs_pubkey,
                user_pubkey,
                cursor.id(),
                config.events_limit,
                &config.path,
            )
            .await
            .map_err(sort_failed_read)?;
        let stopped = |violation| BatchEnd::Stopped(StopReason::Protocol(violation));
        check_stream_limit(user_pubkey, config.events_limit, &events).map_err(stopped)?;
        check_stream_cursors(user_pubkey, cursor, &events).map_err(stopped)?;
        for event in &events {
            check_event_user_pubkey(user_pubkey, event).map_err(stopped)?;
        }

        for event in &events {
            if *config.shutdown_rx.borrow() {
                return Err(BatchEnd::Stopped(StopReason::Shutdown));
            }

            self.handle_event(event).await.map_err(BatchEnd::Failed)?;
            *checkpoint = EventCursor::new(event.cursor);
            *handled_events += 1;
        }

        Ok(())
    }
}

/// Why not every Event of one User pubkey's Batch was handled.
enum BatchEnd<Err> {
    /// The Batch ended early, and the Run goes on with the next User pubkey.
    Failed(WatcherError<Err>),
    /// The Run stops: no further User pubkey is read.
    Stopped(StopReason),
}

/// Sorts a failed Read of a User pubkey's Event stream by its [`HsErrorKind`]: a Read the
/// Homeserver refused is that user's failure and ends their Batch, and a Homeserver that
/// cannot be read stops the Run. This is the one place the kinds are sorted, and it has no
/// catch-all arm: a kind added later does not compile until it is sorted here.
fn sort_failed_read<Err>(error: HsError) -> BatchEnd<Err> {
    match &error.kind {
        HsErrorKind::NotFound(_)
        | HsErrorKind::Rejected(..)
        | HsErrorKind::TooLarge { .. }
        | HsErrorKind::Malformed(_)
        | HsErrorKind::Sdk(_) => BatchEnd::Failed(WatcherError::Homeserver(error)),
        // The Client has already retried a rate-limited Read before it returns that kind.
        HsErrorKind::Timeout
        | HsErrorKind::Transport(_)
        | HsErrorKind::ServerError(..)
        | HsErrorKind::RateLimited(_) => BatchEnd::Stopped(StopReason::Homeserver(error)),
    }
}

/// Logs that the Run on the Homeserver `hs_pubkey` stopped at `user_pubkey`, for `reason`.
fn log_stop(hs_pubkey: &str, user_pubkey: &PublicKey, reason: &StopReason) {
    let user = user_pubkey.z32();
    match reason {
        StopReason::Shutdown => debug!(
            homeserver = %hs_pubkey,
            %user,
            "Shutdown detected; the run stops"
        ),
        StopReason::Homeserver(error) => warn!(
            homeserver = %hs_pubkey,
            %user,
            %error,
            "The homeserver could not be read; the run stops"
        ),
        StopReason::Protocol(violation) => warn!(
            homeserver = %hs_pubkey,
            %user,
            error = %violation,
            "Protocol violation; the run stops"
        ),
    }
}

/// The first User pubkey `users` lists a second time, if any: read from two Cursors, its
/// Events would be handled twice.
fn listed_twice(users: &[(PublicKey, EventCursor)]) -> Option<&PublicKey> {
    let mut seen = HashSet::with_capacity(users.len());
    users
        .iter()
        .map(|(user_pubkey, _)| user_pubkey)
        .find(|user_pubkey| !seen.insert(*user_pubkey))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use pubky::{EventCursor, PublicKey};
    use tokio::sync::watch;

    use crate::fixtures::{event, key};
    use crate::{
        ConfigError, EventHandler, HsError, HsErrorKind, HsOp, ProtocolViolation, StopReason,
        StreamEvent, StreamOutcome, StreamReader, Watcher, WatcherError,
    };

    /// What a scripted Read of a User pubkey's Event stream answers: a Batch, or why the
    /// Read failed.
    type Answer = Result<Vec<StreamEvent>, HsErrorKind>;

    /// Answers each Read of a User pubkey's Event stream with that user's next scripted
    /// answer, and with no Events once they are used up, recording the requests.
    #[derive(Default)]
    struct Streams {
        answers: Mutex<HashMap<PublicKey, VecDeque<Answer>>>,
        requests: Mutex<Vec<(PublicKey, u64, u16, String)>>,
    }

    impl Streams {
        /// One Batch per User pubkey.
        fn new(events: Vec<(&PublicKey, Vec<StreamEvent>)>) -> &'static Self {
            Self::in_turn(
                events
                    .into_iter()
                    .map(|(user, events)| (user, vec![events]))
                    .collect(),
            )
        }

        /// Each User pubkey's Batches, one per Read, in order.
        fn in_turn(batches: Vec<(&PublicKey, Vec<Vec<StreamEvent>>)>) -> &'static Self {
            Self::answering(
                batches
                    .into_iter()
                    .map(|(user, batches)| (user, batches.into_iter().map(Ok).collect()))
                    .collect(),
            )
        }

        /// Each User pubkey's answers, one per Read, in order: a Batch, or the kind of
        /// failure the Read ends with.
        fn answering(answers: Vec<(&PublicKey, Vec<Answer>)>) -> &'static Self {
            Box::leak(Box::new(Self {
                answers: Mutex::new(
                    answers
                        .into_iter()
                        .map(|(user, answers)| (user.clone(), answers.into()))
                        .collect(),
                ),
                requests: Mutex::default(),
            }))
        }

        /// The Reads asked for so far: whose Event stream, and after which Cursor.
        fn reads(&self) -> Vec<(PublicKey, u64)> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(user, cursor, _, _)| (user.clone(), *cursor))
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl StreamReader for &'static Streams {
        async fn read_stream(
            &self,
            hs_pubkey: &PublicKey,
            user_pubkey: &PublicKey,
            cursor: u64,
            limit: u16,
            path: &str,
        ) -> Result<Vec<StreamEvent>, HsError> {
            self.requests.lock().unwrap().push((
                user_pubkey.clone(),
                cursor,
                limit,
                path.to_string(),
            ));
            let mut answers = self.answers.lock().unwrap();
            answers
                .get_mut(user_pubkey)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| Ok(Vec::new()))
                .map_err(|kind| HsError::new(hs_pubkey, HsOp::ReadStream, kind))
        }
    }

    /// Records the Cursors of the Events it gets, and shuts the Consumer down after
    /// `shutdown_after` of them.
    #[derive(Default)]
    struct Recorder {
        cursors: Mutex<Vec<u64>>,
        shutdown_after: Option<(usize, watch::Sender<bool>)>,
    }

    #[async_trait::async_trait]
    impl EventHandler<StreamEvent, String> for &'static Recorder {
        async fn handle(&self, event: &StreamEvent) -> Result<(), String> {
            let mut cursors = self.cursors.lock().unwrap();
            cursors.push(event.cursor);
            if let Some((after, shutdown)) = &self.shutdown_after {
                if cursors.len() == *after {
                    shutdown.send(true).unwrap();
                }
            }
            Ok(())
        }
    }

    fn recorder() -> &'static Recorder {
        Box::leak(Box::default())
    }

    /// Each User pubkey's Events are handled, and each moves to its own Checkpoint.
    #[tokio::test]
    async fn each_user_moves_to_their_own_checkpoint() {
        let (alice, bob, carol) = (key(), key(), key());
        let reader = Streams::new(vec![
            (&alice, vec![event(&alice, 11), event(&alice, 12)]),
            (&bob, vec![event(&bob, 31)]),
        ]);
        let handler = recorder();
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
            (carol.clone(), EventCursor::new(50)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await
            .unwrap();

        assert!(outcome.stopped.is_none());
        assert_eq!(outcome.handled_events, 3);
        assert_eq!(
            outcome.checkpoints,
            vec![
                (alice, EventCursor::new(12)),
                (bob, EventCursor::new(31)),
                (carol, EventCursor::new(50)),
            ]
        );
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 12, 31]);
    }

    /// One Consumer makes any number of Runs: the Checkpoints a Run returns are the next
    /// Run's users as they are, and each user is read from where they stopped.
    #[tokio::test]
    async fn the_next_run_reads_each_user_from_the_last_checkpoints() {
        let (alice, bob, carol) = (key(), key(), key());
        let reader = Streams::in_turn(vec![
            (
                &alice,
                vec![
                    vec![event(&alice, 11), event(&alice, 12)],
                    vec![event(&alice, 13)],
                ],
            ),
            (&bob, vec![vec![event(&bob, 31)], vec![event(&bob, 32)]]),
        ]);
        let handler = recorder();
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
            (carol.clone(), EventCursor::new(50)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let consumer = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap();

        let first = consumer.run(users).await.unwrap();
        let second = consumer.run(first.checkpoints).await.unwrap();

        assert!(second.stopped.is_none());
        assert_eq!(second.handled_events, 2);
        assert_eq!(
            second.checkpoints,
            vec![
                (alice.clone(), EventCursor::new(13)),
                (bob.clone(), EventCursor::new(32)),
                (carol.clone(), EventCursor::new(50)),
            ]
        );
        assert_eq!(
            reader.reads(),
            [
                (alice.clone(), 10),
                (bob.clone(), 30),
                (carol.clone(), 50),
                (alice, 12),
                (bob, 31),
                (carol, 50),
            ]
        );
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 12, 31, 13, 32]);
    }

    /// A later Run checks each Batch against the Cursor that Run was given: a Homeserver
    /// that goes back and sends Events at or before a user's last Checkpoint breaks the
    /// protocol. The Run stops, none of that Batch reaches the Handler a second time, and
    /// the user keeps their Checkpoint.
    #[tokio::test]
    async fn a_later_run_stops_on_a_stream_that_goes_back_before_its_checkpoint() {
        let alice = key();
        let reader = Streams::in_turn(vec![(
            &alice,
            vec![
                vec![event(&alice, 11), event(&alice, 12)],
                vec![event(&alice, 12), event(&alice, 13)],
            ],
        )]);
        let handler = recorder();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let consumer = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap();

        let first = consumer
            .run(vec![(alice.clone(), EventCursor::new(10))])
            .await
            .unwrap();
        let second = consumer.run(first.checkpoints).await.unwrap();

        let expected = ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(alice.clone()),
            requested: 12,
            got: 12,
        };
        assert!(
            matches!(&second.stopped, Some(StopReason::Protocol(got)) if *got == expected),
            "{:?}",
            second.stopped
        );
        assert_eq!(second.checkpoints, vec![(alice, EventCursor::new(12))]);
        assert_eq!(second.handled_events, 0);
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 12]);
    }

    /// A Run with no users is a normal Run: nothing is read, nothing is handled, and it is
    /// not reported as stopped early.
    #[tokio::test]
    async fn a_run_with_no_users_reads_nothing() {
        let reader = Streams::new(vec![]);
        let handler = recorder();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(vec![])
            .await
            .unwrap();

        assert!(outcome.stopped.is_none());
        assert_eq!(outcome.handled_events, 0);
        assert!(outcome.checkpoints.is_empty());
        assert!(reader.reads().is_empty());
        assert!(handler.cursors.lock().unwrap().is_empty());
    }

    /// A User pubkey listed twice fails the Run before any Read, even of the users listed
    /// before it: its two Cursors would read the same Events twice.
    #[tokio::test]
    async fn a_user_listed_twice_fails_the_run_before_any_read() {
        let (alice, bob) = (key(), key());
        let reader = Streams::new(vec![
            (&alice, vec![event(&alice, 21)]),
            (&bob, vec![event(&bob, 31)]),
        ]);
        let handler = recorder();
        let users = vec![
            (bob, EventCursor::new(30)),
            (alice.clone(), EventCursor::new(10)),
            (alice.clone(), EventCursor::new(20)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await;

        assert!(
            matches!(&result, Err(WatcherError::DuplicateUser { user_pubkey }) if *user_pubkey == alice),
            "{result:?}"
        );
        assert!(reader.reads().is_empty());
        assert!(handler.cursors.lock().unwrap().is_empty());
    }

    /// Each request asks for the User pubkey's Cursor, the limit and the path: `/pub/` unless
    /// set.
    #[tokio::test]
    async fn requests_carry_the_cursor_limit_and_path() {
        let alice = key();
        for (path, expected) in [(None, "/pub/"), (Some("/pub/my.app/"), "/pub/my.app/")] {
            let reader = Streams::new(vec![]);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut builder = Watcher::stream(reader, key())
                .handler(recorder())
                .events_limit(9);
            if let Some(path) = path {
                builder = builder.path(path);
            }

            builder
                .build(shutdown_rx)
                .unwrap()
                .run(vec![(alice.clone(), EventCursor::new(4))])
                .await
                .unwrap();

            assert_eq!(
                *reader.requests.lock().unwrap(),
                [(alice.clone(), 4, 9, expected.to_string())]
            );
        }
    }

    /// A Protocol violation stops the Run: a Batch with more Events than asked for, an Event
    /// of another User pubkey, or a Cursor that does not move forward hands none of that
    /// answer's Events to the Handler. The stop reason names the User pubkey whose Event
    /// stream broke the rules and is not also listed under `failures`, later users are not
    /// read, and the earlier users keep the Checkpoints they reached.
    #[tokio::test]
    async fn a_protocol_violation_stops_the_run_and_hands_none_of_its_events_to_the_handler() {
        let (alice, bob, carol, mallory) = (key(), key(), key(), key());
        let not_advanced = |requested, got| ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(bob.clone()),
            requested,
            got,
        };
        for (events, expected) in [
            (
                vec![event(&bob, 31), event(&bob, 32), event(&bob, 33)],
                ProtocolViolation::TooManyEvents {
                    user_pubkey: bob.clone(),
                    got: 3,
                    limit: 2,
                },
            ),
            (
                vec![event(&bob, 31), event(&mallory, 32)],
                ProtocolViolation::ForeignEvent {
                    user_pubkey: bob.clone(),
                    event_user_pubkey: mallory.clone(),
                },
            ),
            (vec![event(&bob, 30)], not_advanced(30, 30)),
            (vec![event(&bob, 32), event(&bob, 31)], not_advanced(32, 31)),
        ] {
            let reader = Streams::new(vec![
                (&alice, vec![event(&alice, 11), event(&alice, 12)]),
                (&bob, events),
                (&carol, vec![event(&carol, 51)]),
            ]);
            let handler = recorder();
            let users = vec![
                (alice.clone(), EventCursor::new(10)),
                (bob.clone(), EventCursor::new(30)),
                (carol.clone(), EventCursor::new(50)),
            ];
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            let outcome = Watcher::stream(reader, key())
                .handler(handler)
                .events_limit(2)
                .build(shutdown_rx)
                .unwrap()
                .run(users)
                .await
                .unwrap();

            assert!(
                matches!(&outcome.stopped, Some(StopReason::Protocol(got)) if *got == expected),
                "{:?}",
                outcome.stopped
            );
            assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
            assert_eq!(
                outcome.checkpoints,
                vec![
                    (alice.clone(), EventCursor::new(12)),
                    (bob.clone(), EventCursor::new(30)),
                    (carol.clone(), EventCursor::new(50)),
                ]
            );
            assert_eq!(outcome.handled_events, 2);
            assert_eq!(reader.reads(), [(alice.clone(), 10), (bob.clone(), 30)]);
            assert_eq!(*handler.cursors.lock().unwrap(), [11, 12]);
        }
    }

    /// A Batch that breaks two rules reports the one checked first: its Cursors are checked
    /// before whose Events they are, so an Event of another User pubkey followed by an
    /// out-of-order Cursor stops the Run on the Cursor. None of the Batch is handled.
    #[tokio::test]
    async fn a_batch_with_a_foreign_event_and_an_out_of_order_cursor_stops_on_the_cursor() {
        let (bob, carol, mallory) = (key(), key(), key());
        let reader = Streams::new(vec![
            (&bob, vec![event(&mallory, 31), event(&bob, 30)]),
            (&carol, vec![event(&carol, 51)]),
        ]);
        let handler = recorder();
        let users = vec![
            (bob.clone(), EventCursor::new(30)),
            (carol.clone(), EventCursor::new(50)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users.clone())
            .await
            .unwrap();

        let expected = ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(bob.clone()),
            requested: 31,
            got: 30,
        };
        assert!(
            matches!(&outcome.stopped, Some(StopReason::Protocol(got)) if *got == expected),
            "{:?}",
            outcome.stopped
        );
        assert_eq!(outcome.checkpoints, users);
        assert_eq!(outcome.handled_events, 0);
        assert!(handler.cursors.lock().unwrap().is_empty());
        assert_eq!(reader.reads(), [(bob.clone(), 30)]);
    }

    /// A Run stopped by shutdown is not an error: the Checkpoint of the User pubkey in
    /// progress is the Cursor of the last Event handled, the ones after it are not read and
    /// keep theirs, and the stop reason is the shutdown, not a Homeserver's failure.
    #[tokio::test]
    async fn a_run_stopped_by_shutdown_keeps_its_checkpoints() {
        let (alice, bob) = (key(), key());
        let reader = Streams::new(vec![
            (&alice, vec![event(&alice, 11), event(&alice, 12)]),
            (&bob, vec![event(&bob, 31)]),
        ]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler: &'static Recorder = Box::leak(Box::new(Recorder {
            shutdown_after: Some((1, shutdown_tx)),
            ..Recorder::default()
        }));
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
        ];

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await
            .unwrap();

        assert!(
            matches!(outcome.stopped, Some(StopReason::Shutdown)),
            "{:?}",
            outcome.stopped
        );
        assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
        assert_eq!(outcome.handled_events, 1);
        assert_eq!(
            outcome.checkpoints,
            vec![
                (alice.clone(), EventCursor::new(11)),
                (bob, EventCursor::new(30))
            ]
        );
        assert_eq!(reader.reads(), [(alice, 10)]);
        assert_eq!(*handler.cursors.lock().unwrap(), [11]);
    }

    /// Fails every Read of an Event stream, as a Homeserver that is down, counting the Reads
    /// it was asked for.
    #[derive(Default)]
    struct Down {
        reads: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl StreamReader for &'static Down {
        async fn read_stream(
            &self,
            hs_pubkey: &PublicKey,
            _user_pubkey: &PublicKey,
            _cursor: u64,
            _limit: u16,
            _path: &str,
        ) -> Result<Vec<StreamEvent>, HsError> {
            *self.reads.lock().unwrap() += 1;
            Err(HsError::new(
                hs_pubkey,
                HsOp::ReadStream,
                HsErrorKind::ServerError(503, "down".into()),
            ))
        }
    }

    /// The Handler's own error: the Cursor of the Event it failed on.
    #[derive(Debug, PartialEq, Eq)]
    struct FailedAt(u64);

    impl std::fmt::Display for FailedAt {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "handler failed at cursor {}", self.0)
        }
    }

    /// Records the Cursors of the Events it gets, and fails on the Event at `fail_at`.
    struct FailingAt {
        fail_at: u64,
        cursors: Mutex<Vec<u64>>,
    }

    #[async_trait::async_trait]
    impl EventHandler<StreamEvent, FailedAt> for &'static FailingAt {
        async fn handle(&self, event: &StreamEvent) -> Result<(), FailedAt> {
            if event.cursor == self.fail_at {
                return Err(FailedAt(event.cursor));
            }
            self.cursors.lock().unwrap().push(event.cursor);
            Ok(())
        }
    }

    fn failing_at(fail_at: u64) -> &'static FailingAt {
        Box::leak(Box::new(FailingAt {
            fail_at,
            cursors: Mutex::default(),
        }))
    }

    /// A Handler error ends that user's Batch only: the Run lists the user with the Handler's
    /// own error, typed, their Checkpoint is the last Event handled, and the next user is
    /// read.
    #[tokio::test]
    async fn a_handler_error_ends_that_users_batch_and_the_next_user_is_read() {
        let (alice, bob) = (key(), key());
        let reader = Streams::new(vec![
            (
                &alice,
                vec![event(&alice, 11), event(&alice, 12), event(&alice, 13)],
            ),
            (&bob, vec![event(&bob, 31)]),
        ]);
        let handler = failing_at(12);
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await
            .unwrap();

        assert!(
            matches!(
                outcome.failures.as_slice(),
                [(user, WatcherError::Handler(FailedAt(12)))] if *user == alice
            ),
            "{:?}",
            outcome.failures
        );
        assert_eq!(
            outcome.checkpoints,
            vec![
                (alice.clone(), EventCursor::new(11)),
                (bob.clone(), EventCursor::new(31)),
            ]
        );
        assert_eq!(reader.reads(), [(alice, 10), (bob, 30)]);
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 31]);
    }

    /// `handled_events` counts the Events of a user whose Batch ended early: the ones the
    /// Handler handled before the Event it failed on.
    #[tokio::test]
    async fn handled_events_counts_the_events_of_a_user_whose_batch_ended_early() {
        let (alice, bob) = (key(), key());
        let reader = Streams::new(vec![
            (
                &alice,
                vec![event(&alice, 11), event(&alice, 12), event(&alice, 13)],
            ),
            (&bob, vec![event(&bob, 31)]),
        ]);
        let handler = failing_at(13);
        let users = vec![(alice, EventCursor::new(10)), (bob, EventCursor::new(30))];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await
            .unwrap();

        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.handled_events, 3);
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 12, 31]);
    }

    /// A user stuck on a failing Event appears in `failures` on every Run and does not move:
    /// each Run reads them from the same Checkpoint, and never past the Event.
    #[tokio::test]
    async fn a_user_stuck_on_a_failing_event_is_listed_on_every_run_and_does_not_move() {
        let (alice, bob) = (key(), key());
        let reader = Streams::in_turn(vec![
            (
                &alice,
                vec![
                    vec![event(&alice, 11), event(&alice, 12), event(&alice, 13)],
                    vec![event(&alice, 12), event(&alice, 13)],
                    vec![event(&alice, 12), event(&alice, 13)],
                ],
            ),
            (&bob, vec![vec![event(&bob, 31)], vec![event(&bob, 32)]]),
        ]);
        let handler = failing_at(12);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let consumer = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap();

        let mut users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
        ];
        for _ in 0..3 {
            let outcome = consumer.run(users).await.unwrap();
            assert!(
                matches!(
                    outcome.failures.as_slice(),
                    [(user, WatcherError::Handler(FailedAt(12)))] if *user == alice
                ),
                "{:?}",
                outcome.failures
            );
            assert_eq!(
                outcome.checkpoints[0],
                (alice.clone(), EventCursor::new(11))
            );
            users = outcome.checkpoints;
        }

        assert_eq!(users[1], (bob.clone(), EventCursor::new(32)));
        assert_eq!(
            reader.reads(),
            [
                (alice.clone(), 10),
                (bob.clone(), 30),
                (alice.clone(), 11),
                (bob.clone(), 31),
                (alice, 11),
                (bob, 32),
            ]
        );
        assert_eq!(*handler.cursors.lock().unwrap(), [11, 31, 32]);
    }

    /// A Homeserver that is down stops the Run at the first user: the Run still returns `Ok`
    /// with every user's Checkpoint where it was, the stop reason is the Homeserver's error,
    /// and the Homeserver is asked once, not once per user.
    #[tokio::test]
    async fn a_failed_stream_request_stops_the_run_with_every_checkpoint() {
        let (alice, bob) = (key(), key());
        let reader: &'static Down = Box::leak(Box::default());
        let handler = recorder();
        let users = vec![(alice, EventCursor::new(10)), (bob, EventCursor::new(30))];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users.clone())
            .await
            .unwrap();

        assert!(
            matches!(
                &outcome.stopped,
                Some(StopReason::Homeserver(e))
                    if e.op == HsOp::ReadStream && matches!(e.kind, HsErrorKind::ServerError(503, _))
            ),
            "{:?}",
            outcome.stopped
        );
        assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
        assert_eq!(outcome.checkpoints, users);
        assert_eq!(outcome.handled_events, 0);
        assert_eq!(*reader.reads.lock().unwrap(), 1);
        assert!(handler.cursors.lock().unwrap().is_empty());
    }

    /// Makes one Run over Alice, Bob and Carol, in that order, where the Read of Bob's Event
    /// stream fails with `kind` and the two others have Events.
    async fn run_where_the_second_read_fails(kind: HsErrorKind) -> SecondReadFailed {
        let (alice, bob, carol) = (key(), key(), key());
        let reader = Streams::answering(vec![
            (&alice, vec![Ok(vec![event(&alice, 11), event(&alice, 12)])]),
            (&bob, vec![Err(kind)]),
            (&carol, vec![Ok(vec![event(&carol, 51)])]),
        ]);
        let handler = recorder();
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
            (carol.clone(), EventCursor::new(50)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users)
            .await
            .unwrap();

        SecondReadFailed {
            alice,
            bob,
            carol,
            outcome,
            reads: reader.reads(),
            handled: handler.cursors.lock().unwrap().clone(),
        }
    }

    /// What [`run_where_the_second_read_fails`] saw from outside.
    struct SecondReadFailed {
        alice: PublicKey,
        bob: PublicKey,
        carol: PublicKey,
        outcome: StreamOutcome<String>,
        reads: Vec<(PublicKey, u64)>,
        handled: Vec<u64>,
    }

    /// A refused Read ends that user's Batch only: the user is listed under `failures` with
    /// the Homeserver's error and keeps the Cursor they were given, the next user is read,
    /// and the Run did not stop.
    async fn assert_a_refused_read_ends_that_users_batch(kind: HsErrorKind) {
        let expected = kind.to_string();
        let run = run_where_the_second_read_fails(kind).await;
        let SecondReadFailed {
            alice, bob, carol, ..
        } = run;

        assert!(
            matches!(
                run.outcome.failures.as_slice(),
                [(user, WatcherError::Homeserver(error))]
                    if *user == bob
                        && error.op == HsOp::ReadStream
                        && error.kind.to_string() == expected
            ),
            "{:?}",
            run.outcome.failures
        );
        assert!(run.outcome.stopped.is_none(), "{:?}", run.outcome.stopped);
        assert_eq!(
            run.outcome.checkpoints,
            vec![
                (alice.clone(), EventCursor::new(12)),
                (bob.clone(), EventCursor::new(30)),
                (carol.clone(), EventCursor::new(51)),
            ]
        );
        assert_eq!(run.outcome.handled_events, 3);
        assert_eq!(run.reads, [(alice, 10), (bob, 30), (carol, 50)]);
        assert_eq!(run.handled, [11, 12, 51]);
    }

    /// A Read refused as not found ends that user's Batch, and the next user is read.
    #[tokio::test]
    async fn a_read_refused_as_not_found_ends_that_users_batch_and_the_next_user_is_read() {
        assert_a_refused_read_ends_that_users_batch(HsErrorKind::NotFound("gone".into())).await;
    }

    /// A Read rejected with any other status, `408` included, ends that user's Batch, and
    /// the next user is read.
    #[tokio::test]
    async fn a_rejected_read_ends_that_users_batch_and_the_next_user_is_read() {
        for status in [400, 403, 408] {
            assert_a_refused_read_ends_that_users_batch(HsErrorKind::Rejected(status, "no".into()))
                .await;
        }
    }

    /// An answer one byte past its size cap ends that user's Batch, and the next user is
    /// read.
    #[tokio::test]
    async fn a_read_over_the_size_cap_ends_that_users_batch_and_the_next_user_is_read() {
        assert_a_refused_read_ends_that_users_batch(HsErrorKind::TooLarge {
            got: 1_048_577,
            max: 1_048_576,
        })
        .await;
    }

    /// A malformed Read ends that user's Batch, and the next user is read.
    #[tokio::test]
    async fn a_malformed_read_ends_that_users_batch_and_the_next_user_is_read() {
        assert_a_refused_read_ends_that_users_batch(HsErrorKind::malformed("bad url")).await;
    }

    /// A Read the SDK refused ends that user's Batch, and the next user is read.
    #[tokio::test]
    async fn a_read_the_sdk_refused_ends_that_users_batch_and_the_next_user_is_read() {
        assert_a_refused_read_ends_that_users_batch(HsErrorKind::Sdk("no record".into())).await;
    }

    /// A Homeserver failure stops the Run: the stop reason carries the Homeserver's error,
    /// which is not also listed under `failures`, later users are not read, and the earlier
    /// users keep the Checkpoints they reached.
    async fn assert_a_homeserver_failure_stops_the_run(kind: HsErrorKind) {
        let expected = kind.to_string();
        let run = run_where_the_second_read_fails(kind).await;
        let SecondReadFailed {
            alice, bob, carol, ..
        } = run;

        assert!(
            matches!(
                &run.outcome.stopped,
                Some(StopReason::Homeserver(error))
                    if error.op == HsOp::ReadStream && error.kind.to_string() == expected
            ),
            "{:?}",
            run.outcome.stopped
        );
        assert!(
            run.outcome.failures.is_empty(),
            "{:?}",
            run.outcome.failures
        );
        assert_eq!(
            run.outcome.checkpoints,
            vec![
                (alice.clone(), EventCursor::new(12)),
                (bob.clone(), EventCursor::new(30)),
                (carol, EventCursor::new(50)),
            ]
        );
        assert_eq!(run.outcome.handled_events, 2);
        assert_eq!(run.reads, [(alice, 10), (bob, 30)]);
        assert_eq!(run.handled, [11, 12]);
    }

    /// A Read that timed out stops the Run, with later users unread and earlier Checkpoints
    /// kept.
    #[tokio::test]
    async fn a_timed_out_read_stops_the_run_and_keeps_the_earlier_checkpoints() {
        assert_a_homeserver_failure_stops_the_run(HsErrorKind::Timeout).await;
    }

    /// A transport failure stops the Run, with later users unread and earlier Checkpoints
    /// kept.
    #[tokio::test]
    async fn a_transport_failure_stops_the_run_and_keeps_the_earlier_checkpoints() {
        assert_a_homeserver_failure_stops_the_run(HsErrorKind::transport("connection reset")).await;
    }

    /// A `5xx` answer stops the Run, with later users unread and earlier Checkpoints kept.
    #[tokio::test]
    async fn a_server_error_stops_the_run_and_keeps_the_earlier_checkpoints() {
        assert_a_homeserver_failure_stops_the_run(HsErrorKind::ServerError(503, "down".into()))
            .await;
    }

    /// A Read still rate limited after the Client's retries stops the Run, with later users
    /// unread and earlier Checkpoints kept.
    #[tokio::test]
    async fn a_rate_limited_read_stops_the_run_and_keeps_the_earlier_checkpoints() {
        assert_a_homeserver_failure_stops_the_run(HsErrorKind::RateLimited("slow down".into()))
            .await;
    }

    /// The failure that stops a Run is its stop reason only: a user whose Batch ended before
    /// the stop is still listed under `failures`, and the user the Run stopped at is not.
    #[tokio::test]
    async fn the_failure_that_stops_a_run_is_not_listed_under_failures() {
        let (alice, bob, carol) = (key(), key(), key());
        let reader = Streams::answering(vec![
            (&alice, vec![Err(HsErrorKind::NotFound("gone".into()))]),
            (&bob, vec![Err(HsErrorKind::Timeout)]),
            (&carol, vec![Ok(vec![event(&carol, 51)])]),
        ]);
        let handler = recorder();
        let users = vec![
            (alice.clone(), EventCursor::new(10)),
            (bob.clone(), EventCursor::new(30)),
            (carol, EventCursor::new(50)),
        ];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let outcome = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(users.clone())
            .await
            .unwrap();

        assert!(
            matches!(
                outcome.failures.as_slice(),
                [(user, WatcherError::Homeserver(error))]
                    if *user == alice && matches!(error.kind, HsErrorKind::NotFound(_))
            ),
            "{:?}",
            outcome.failures
        );
        assert!(
            matches!(
                &outcome.stopped,
                Some(StopReason::Homeserver(error)) if matches!(error.kind, HsErrorKind::Timeout)
            ),
            "{:?}",
            outcome.stopped
        );
        assert_eq!(outcome.checkpoints, users);
        assert_eq!(reader.reads(), [(alice, 10), (bob, 30)]);
        assert!(handler.cursors.lock().unwrap().is_empty());
    }

    /// An events limit of zero, or a path that does not start with `/`, is rejected at build
    /// time.
    #[test]
    fn a_misconfigured_consumer_is_rejected_at_build_time() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let build = |limit, path: &str| {
            Watcher::stream(Streams::new(vec![]), key())
                .handler(recorder())
                .events_limit(limit)
                .path(path)
                .build(shutdown_rx.clone())
        };

        assert!(build(1, "/pub/").is_ok());
        assert_eq!(build(0, "/pub/").err(), Some(ConfigError::ZeroEventsLimit));
        assert_eq!(
            build(1, "pub/").err(),
            Some(ConfigError::InvalidPath {
                path: "pub/".into()
            })
        );
    }
}
