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

/// The paths a user's stream is read under when the builder sets none: everything they
/// publish.
/// Set another with [`StreamConsumerBuilder::path`].
pub const DEFAULT_STREAM_PATH: &str = "/pub/";

/// Builds a [`StreamConsumer`]: see [`Watcher::stream`](super::Watcher::stream).
pub struct StreamConsumerBuilder<H> {
    hs_pubkey: PublicKey,
    users: Vec<(PublicKey, EventCursor)>,
    handler: H,
    events_limit: u16,
    path: String,
    reader: Arc<dyn StreamReader>,
}

impl StreamConsumerBuilder<Missing> {
    pub(super) fn new<C>(
        client: C,
        hs_pubkey: PublicKey,
        users: Vec<(PublicKey, EventCursor)>,
    ) -> Self
    where
        C: StreamReader,
    {
        Self {
            hs_pubkey,
            users,
            handler: Missing,
            events_limit: DEFAULT_EVENTS_LIMIT,
            path: DEFAULT_STREAM_PATH.to_string(),
            reader: Arc::new(client),
        }
    }
}

impl<H> StreamConsumerBuilder<H> {
    /// The most events a run requests per user ([`DEFAULT_EVENTS_LIMIT`] by default). Zero is
    /// rejected by [`Self::build`].
    pub fn events_limit(mut self, limit: u16) -> Self {
        self.events_limit = limit;
        self
    }

    /// Reads only the events whose resource path starts with `path`
    /// ([`DEFAULT_STREAM_PATH`] by default). A path that does not start with `/` is
    /// rejected by [`Self::build`].
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// The handler each event goes to: an [`EventHandler`] of [`StreamEvent`]s, whose
    /// error a failed run gives back as [`WatcherError::Handler`].
    pub fn handler<NewH>(self, handler: NewH) -> StreamConsumerBuilder<NewH> {
        StreamConsumerBuilder {
            hs_pubkey: self.hs_pubkey,
            users: self.users,
            handler,
            events_limit: self.events_limit,
            path: self.path,
            reader: self.reader,
        }
    }
}

impl<H> StreamConsumerBuilder<H> {
    /// Builds the watcher, which stops reading once `shutdown_rx` turns `true`. Fails on an
    /// events limit of zero, an empty or duplicated user list, or a path that does not start
    /// with `/`.
    /// The error is a [`ConfigError`].
    pub fn build<Err>(self, shutdown_rx: Receiver<bool>) -> Result<StreamConsumer<Err>, ConfigError>
    where
        H: EventHandler<StreamEvent, Err> + 'static,
        Err: Display + Debug + Send + Sync + 'static,
    {
        validate_limit(self.events_limit)?;
        validate_users(&self.users)?;
        if !self.path.starts_with('/') {
            return Err(ConfigError::InvalidPath { path: self.path });
        }

        Ok(StreamConsumer {
            config: Arc::new(Config {
                hs_pubkey_z32: self.hs_pubkey.z32(),
                hs_pubkey: self.hs_pubkey,
                users: self.users,
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
    users: Vec<(PublicKey, EventCursor)>,
    handler: Arc<dyn EventHandler<StreamEvent, Err>>,
    events_limit: u16,
    path: String,
    reader: Arc<dyn StreamReader>,
    shutdown_rx: Receiver<bool>,
}

/// How a run of a [`StreamConsumer`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOutcome {
    /// The cursor to start each user's next run from, in the order the users were given,
    /// safe to persist: every event of the user before it was handled.
    /// Pass them to the next [`Watcher::stream`](super::Watcher::stream).
    pub checkpoints: Vec<(PublicKey, EventCursor)>,
    /// The events handed to the handler.
    /// Each went to the handler's [`EventHandler::handle`].
    pub handled_events: usize,
    /// Whether the run read every user. A run stopped by the shutdown signal given to
    /// [`StreamConsumerBuilder::build`] returns `false`; each user's cursor is still safe
    /// to persist.
    pub completed: bool,
}

/// Reads a known set of users' `/events-stream`s on one homeserver, one user after the other,
/// one batch each per [`Self::run`]. `Err` is its handler's error.
pub struct StreamConsumer<Err> {
    config: Arc<Config<Err>>,
}

impl<Err> StreamConsumer<Err>
where
    Err: Display + Debug + Send + Sync + 'static,
{
    /// Reads each user's events after their cursor, hands them to the handler in order, and
    /// returns each user's cursor to start the next run from.
    ///
    /// A user's stream is checked before any of its events is handled: no more events than
    /// requested, every event the user's own, and cursors that only move forward
    /// ([`ProtocolViolation`] otherwise). A stream that breaks this, or a handler error, fails
    /// the run.
    pub async fn run(&self) -> Result<StreamOutcome, WatcherError<Err>> {
        let run = Arc::new(StreamRun {
            config: self.config.clone(),
        });
        let read_run = run.clone();
        run_consumer(run.instance_name(), run.hs_pubkey(), async move {
            read_run.read().await
        })
        .await
    }
}

/// One run of a [`StreamConsumer`], as a processor, so each event is handled in its
/// [`Run::event_span`].
struct StreamRun<Err> {
    config: Arc<Config<Err>>,
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
        let mut checkpoints = self.config.users.clone();
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

fn validate_users(users: &[(PublicKey, EventCursor)]) -> Result<(), ConfigError> {
    if users.is_empty() {
        return Err(ConfigError::NoUsers);
    }
    for (index, (user_pubkey, _)) in users.iter().enumerate() {
        if users[..index]
            .iter()
            .any(|(earlier, _)| earlier == user_pubkey)
        {
            return Err(ConfigError::DuplicateUser {
                user_pubkey: user_pubkey.clone(),
            });
        }
    }
    Ok(())
}

/// Checks `user`'s stream, read after `requested` with `limit`: no more events than
/// requested, every event owned by `user`, so that one user's stream cannot inject another's
/// events, and cursors that only move forward.
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
    use std::collections::HashMap;
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

    /// An event of `user_pubkey`'s at `cursor`.
    fn event(user_pubkey: &PublicKey, cursor: u64) -> StreamEvent {
        let resource =
            PubkyResource::new(user_pubkey.clone(), "/pub/app/item").expect("valid path");
        StreamEvent::new(EventType::Put, &resource, cursor)
    }

    /// Answers each user with their scripted events, recording the requests.
    #[derive(Default)]
    struct Streams {
        events: HashMap<PublicKey, Vec<StreamEvent>>,
        requests: Mutex<Vec<(PublicKey, u64, u16, String)>>,
    }

    impl Streams {
        fn new(events: Vec<(&PublicKey, Vec<StreamEvent>)>) -> &'static Self {
            Box::leak(Box::new(Self {
                events: events
                    .into_iter()
                    .map(|(user, events)| (user.clone(), events))
                    .collect(),
                requests: Mutex::default(),
            }))
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
            Ok(self.events.get(user_pubkey).cloned().unwrap_or_default())
        }
    }

    /// Records the cursors of the events it gets, and shuts the watcher down after
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

    /// Each user's events are handled, and each user moves to their own last cursor.
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

        let outcome = Watcher::stream(reader, key(), users)
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run()
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

    /// Each request asks for the user's cursor, the limit and the path: `/pub/` unless set.
    #[tokio::test]
    async fn requests_carry_the_cursor_limit_and_path() {
        let alice = key();
        for (path, expected) in [(None, "/pub/"), (Some("/pub/my.app/"), "/pub/my.app/")] {
            let reader = Streams::new(vec![]);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut builder =
                Watcher::stream(reader, key(), vec![(alice.clone(), EventCursor::new(4))])
                    .handler(recorder())
                    .events_limit(9);
            if let Some(path) = path {
                builder = builder.path(path);
            }

            builder.build(shutdown_rx).unwrap().run().await.unwrap();

            assert_eq!(
                *reader.requests.lock().unwrap(),
                [(alice.clone(), 4, 9, expected.to_string())]
            );
        }
    }

    /// A stream with more events than requested, an event of another user, or a cursor that
    /// does not move forward fails the run before any of its events is handled.
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

            let result =
                Watcher::stream(reader, key(), vec![(alice.clone(), EventCursor::new(10))])
                    .handler(handler)
                    .events_limit(2)
                    .build(shutdown_rx)
                    .unwrap()
                    .run()
                    .await;

            assert!(
                matches!(&result, Err(WatcherError::Protocol(got)) if *got == expected),
                "{result:?}"
            );
            assert!(handler.cursors.lock().unwrap().is_empty());
        }
    }

    /// A run stopped by shutdown is not an error: the user in progress keeps the cursor of
    /// the last event handled, and the users after them keep theirs.
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

        let outcome = Watcher::stream(reader, key(), users)
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run()
            .await
            .unwrap();

        assert!(!outcome.completed);
        assert_eq!(outcome.handled_events, 1);
        assert_eq!(
            outcome.checkpoints,
            vec![(alice, EventCursor::new(11)), (bob, EventCursor::new(30))]
        );
    }

    /// Fails every stream request, as a homeserver that is down.
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

    /// The handler's own error: the cursor of the event it failed on.
    #[derive(Debug, PartialEq, Eq)]
    struct FailedAt(u64);

    impl std::fmt::Display for FailedAt {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "handler failed at cursor {}", self.0)
        }
    }

    /// Records the cursors of the events it gets, and fails on the event at `fail_at`.
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

    /// A handler error fails the run with the handler's own error, typed; the events before
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

        let error = Watcher::stream(reader, key(), vec![(alice, EventCursor::new(10))])
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run()
            .await
            .unwrap_err();

        assert!(
            matches!(error, WatcherError::Handler(FailedAt(12))),
            "{error:?}"
        );
        assert_eq!(*handler.cursors.lock().unwrap(), [11]);
    }

    /// A failed stream request fails the run as the homeserver's error, before any event.
    #[tokio::test]
    async fn a_failed_stream_request_fails_the_run() {
        let handler = recorder();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = Watcher::stream(Down, key(), vec![(key(), EventCursor::new(10))])
            .handler(handler)
            .build(shutdown_rx)
            .unwrap()
            .run()
            .await
            .unwrap_err();

        assert!(
            matches!(&error, WatcherError::Homeserver(e) if e.op == HsOp::ReadStream),
            "{error:?}"
        );
        assert!(handler.cursors.lock().unwrap().is_empty());
    }

    /// An events limit of zero, no users, a user listed twice, or a path that does not start
    /// with `/` is rejected at build time.
    #[test]
    fn a_misconfigured_consumer_is_rejected_at_build_time() {
        let alice = key();
        let one = || vec![(alice.clone(), EventCursor::new(0))];
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let build = |users, limit, path: &str| {
            Watcher::stream(Streams::new(vec![]), key(), users)
                .handler(recorder())
                .events_limit(limit)
                .path(path)
                .build(shutdown_rx.clone())
        };

        assert!(build(one(), 1, "/pub/").is_ok());
        assert_eq!(
            build(one(), 0, "/pub/").err(),
            Some(ConfigError::ZeroEventsLimit)
        );
        assert_eq!(build(vec![], 1, "/pub/").err(), Some(ConfigError::NoUsers));
        assert_eq!(
            build([one(), one()].concat(), 1, "/pub/").err(),
            Some(ConfigError::DuplicateUser {
                user_pubkey: alice.clone()
            })
        );
        assert_eq!(
            build(one(), 1, "pub/").err(),
            Some(ConfigError::InvalidPath {
                path: "pub/".into()
            })
        );
    }
}
