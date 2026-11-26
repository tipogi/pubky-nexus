use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use pubky::{EventCursor, PublicKey};
use tokio::sync::watch::Receiver;
use tracing::debug;

use super::{
    run_consumer, validate_limit, ConfigError, FeedOutcome, Missing, ProtocolViolation,
    WatcherError, DEFAULT_EVENTS_LIMIT,
};
use crate::hooks::EventHandler;
use crate::run::{Run, RunOutcome};
use crate::{StreamEvent, StreamReader};

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

    /// The Handler each Event goes to: an [`EventHandler`] of [`StreamEvent`]s, whose
    /// error a failed Run gives back as [`WatcherError::Handler`].
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

/// How a Run of a [`StreamConsumer`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOutcome {
    /// One Checkpoint per User pubkey the Run was given, in the order given: the Cursor with
    /// every Event of that User pubkey before it handled. They are the ones an Indexer may
    /// store. They have the shape of the Run's input: pass them to the next
    /// [`StreamConsumer::run`] as they are.
    pub checkpoints: Vec<(PublicKey, EventCursor)>,
    /// The Events handed to the Handler.
    /// Each went to the Handler's [`EventHandler::handle`].
    pub handled_events: usize,
    /// Whether the Run read every User pubkey's Event stream; `true` for a Run given no
    /// users. A Run stopped by the shutdown signal given to [`StreamConsumerBuilder::build`]
    /// returns `false`; each Checkpoint may still be stored.
    pub completed: bool,
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
    /// A User pubkey listed twice fails the Run before any Read, with
    /// [`WatcherError::DuplicateUser`]. A Run with no users is a normal Run: it returns `Ok`
    /// with nothing read, nothing handled and [`StreamOutcome::completed`] set.
    ///
    /// An Event stream's Batch is checked before any of its Events is handled: no more Events
    /// than asked for, every Event its own User pubkey's, and Cursors that only move forward
    /// ([`ProtocolViolation`] otherwise). A Batch that breaks this, a failed Read or a Handler
    /// error fails the Run.
    pub async fn run(
        &self,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> Result<StreamOutcome, WatcherError<Err>> {
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
    async fn read(&self) -> Result<StreamOutcome, WatcherError<Err>> {
        if let Some(user_pubkey) = listed_twice(&self.users).cloned() {
            return Err(WatcherError::DuplicateUser { user_pubkey });
        }

        let mut checkpoints = self.users.clone();
        let mut handled_events = 0;

        for (user_pubkey, checkpoint) in checkpoints.iter_mut() {
            let read = self.read_user(user_pubkey, *checkpoint).await?;
            *checkpoint = read.checkpoint;
            handled_events += read.handled_events;
            if !read.completed {
                return Ok(StreamOutcome {
                    checkpoints,
                    handled_events,
                    completed: false,
                });
            }
        }

        Ok(StreamOutcome {
            checkpoints,
            handled_events,
            completed: true,
        })
    }

    async fn read_user(
        &self,
        user_pubkey: &PublicKey,
        cursor: EventCursor,
    ) -> Result<FeedOutcome, WatcherError<Err>> {
        let config = &self.config;
        if *config.shutdown_rx.borrow() {
            return Ok(FeedOutcome::interrupted(cursor, 0));
        }

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
            .await?;
        validate_events(user_pubkey, cursor, config.events_limit, &events)?;

        let mut checkpoint = cursor;
        let mut handled_events = 0;
        for event in &events {
            if *config.shutdown_rx.borrow() {
                debug!(user = %user_pubkey.z32(), processed_events = handled_events, "Shutdown detected");
                return Ok(FeedOutcome::interrupted(checkpoint, handled_events));
            }

            self.handle_event(event).await?;
            checkpoint = EventCursor::new(event.cursor);
            handled_events += 1;
        }

        Ok(FeedOutcome::completed(checkpoint, handled_events))
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

/// Checks the Batch of `user_pubkey`'s Event stream, read after `requested` with `limit`: no
/// more Events than asked for, every Event `user_pubkey`'s own, so that one user's Event
/// stream cannot inject another's Events, and Cursors that only move forward.
fn validate_events(
    user_pubkey: &PublicKey,
    requested: EventCursor,
    limit: u16,
    events: &[StreamEvent],
) -> Result<(), ProtocolViolation> {
    if events.len() > limit as usize {
        return Err(ProtocolViolation::TooManyEvents {
            user_pubkey: user_pubkey.clone(),
            got: events.len(),
            limit,
        });
    }

    let mut floor = requested.id();
    for event in events {
        if event.user_pubkey() != user_pubkey {
            return Err(ProtocolViolation::ForeignEvent {
                user_pubkey: user_pubkey.clone(),
                event_user_pubkey: event.user_pubkey().clone(),
            });
        }
        if event.cursor <= floor {
            return Err(ProtocolViolation::CursorNotAdvanced {
                user_pubkey: Some(user_pubkey.clone()),
                requested: floor,
                got: event.cursor,
            });
        }
        floor = event.cursor;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use pubky::{EventCursor, Keypair, PubkyResource, PublicKey};
    use tokio::sync::watch;

    use crate::{
        ConfigError, EventHandler, EventType, HsError, HsErrorKind, HsOp, ProtocolViolation,
        StreamEvent, StreamReader, Watcher, WatcherError,
    };

    fn key() -> PublicKey {
        Keypair::random().public_key()
    }

    /// An Event of `user_pubkey`'s at `cursor`.
    fn event(user_pubkey: &PublicKey, cursor: u64) -> StreamEvent {
        let resource =
            PubkyResource::new(user_pubkey.clone(), "/pub/app/item").expect("valid path");
        StreamEvent::new(EventType::Put, &resource, cursor)
    }

    /// Answers each Read of a User pubkey's Event stream with that user's next scripted
    /// Batch, and with no Events once they are used up, recording the requests.
    #[derive(Default)]
    struct Streams {
        batches: Mutex<HashMap<PublicKey, VecDeque<Vec<StreamEvent>>>>,
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
            Box::leak(Box::new(Self {
                batches: Mutex::new(
                    batches
                        .into_iter()
                        .map(|(user, batches)| (user.clone(), batches.into()))
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
            _hs_pubkey: &PublicKey,
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
            let mut batches = self.batches.lock().unwrap();
            Ok(batches
                .get_mut(user_pubkey)
                .and_then(VecDeque::pop_front)
                .unwrap_or_default())
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

        assert!(outcome.completed);
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

        assert!(second.completed);
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
    /// protocol, and none of that Batch reaches the Handler a second time.
    #[tokio::test]
    async fn a_later_run_rejects_a_stream_that_goes_back_before_its_checkpoint() {
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
        let second = consumer.run(first.checkpoints).await;

        let expected = ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(alice),
            requested: 12,
            got: 12,
        };
        assert!(
            matches!(&second, Err(WatcherError::Protocol(got)) if *got == expected),
            "{second:?}"
        );
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

        assert!(outcome.completed);
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

    /// A Batch with more Events than asked for, an Event of another User pubkey, or a Cursor
    /// that does not move forward fails the Run before any of its Events is handled.
    #[tokio::test]
    async fn a_stream_that_breaks_the_protocol_fails_before_any_event() {
        let (alice, mallory) = (key(), key());
        let not_advanced = |requested, got| ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(alice.clone()),
            requested,
            got,
        };
        for (events, expected) in [
            (
                vec![event(&alice, 11), event(&alice, 12), event(&alice, 13)],
                ProtocolViolation::TooManyEvents {
                    user_pubkey: alice.clone(),
                    got: 3,
                    limit: 2,
                },
            ),
            (
                vec![event(&alice, 11), event(&mallory, 12)],
                ProtocolViolation::ForeignEvent {
                    user_pubkey: alice.clone(),
                    event_user_pubkey: mallory.clone(),
                },
            ),
            (vec![event(&alice, 10)], not_advanced(10, 10)),
            (
                vec![event(&alice, 12), event(&alice, 11)],
                not_advanced(12, 11),
            ),
        ] {
            let reader = Streams::new(vec![(&alice, events)]);
            let handler = recorder();
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);

            let result = Watcher::stream(reader, key())
                .handler(handler)
                .events_limit(2)
                .build(shutdown_rx)
                .unwrap()
                .run(vec![(alice.clone(), EventCursor::new(10))])
                .await;

            assert!(
                matches!(&result, Err(WatcherError::Protocol(got)) if *got == expected),
                "{result:?}"
            );
            assert!(handler.cursors.lock().unwrap().is_empty());
        }
    }

    /// A Run stopped by shutdown is not an error: the Checkpoint of the User pubkey in
    /// progress is the Cursor of the last Event handled, and the ones after it keep theirs.
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

        assert!(!outcome.completed);
        assert_eq!(outcome.handled_events, 1);
        assert_eq!(
            outcome.checkpoints,
            vec![(alice, EventCursor::new(11)), (bob, EventCursor::new(30))]
        );
    }

    /// Fails every Read of an Event stream, as a Homeserver that is down.
    struct Down;

    #[async_trait::async_trait]
    impl StreamReader for Down {
        async fn read_stream(
            &self,
            hs_pubkey: &PublicKey,
            _user_pubkey: &PublicKey,
            _cursor: u64,
            _limit: u16,
            _path: &str,
        ) -> Result<Vec<StreamEvent>, HsError> {
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

    /// A Handler error fails the Run with the Handler's own error, typed; the Events before
    /// it were handled.
    #[tokio::test]
    async fn a_handler_failure_fails_the_run_with_its_own_error() {
        let alice = key();
        let reader = Streams::new(vec![(
            &alice,
            vec![event(&alice, 11), event(&alice, 12), event(&alice, 13)],
        )]);
        let handler: &'static FailingAt = Box::leak(Box::new(FailingAt {
            fail_at: 12,
            cursors: Mutex::default(),
        }));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = Watcher::stream(reader, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(vec![(alice, EventCursor::new(10))])
            .await
            .unwrap_err();

        assert!(
            matches!(error, WatcherError::Handler(FailedAt(12))),
            "{error:?}"
        );
        assert_eq!(*handler.cursors.lock().unwrap(), [11]);
    }

    /// A failed Read of an Event stream fails the Run as the Homeserver's error, before any
    /// Event.
    #[tokio::test]
    async fn a_failed_stream_request_fails_the_run() {
        let handler = recorder();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = Watcher::stream(Down, key())
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run(vec![(key(), EventCursor::new(10))])
            .await
            .unwrap_err();

        assert!(
            matches!(&error, WatcherError::Homeserver(e) if e.op == HsOp::ReadStream),
            "{error:?}"
        );
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
