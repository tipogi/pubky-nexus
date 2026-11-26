# pubky-watcher

`pubky-watcher` consumes [Pubky](https://pubky.org) Homeservers' Events for an Indexer. It
brings the Client that reads them, the Consumers that hand each Event to the Indexer's
Handler, and the Cycle that goes round every Homeserver the Indexer follows. The Indexer
brings the meaning: what an Event does, where Cursors are kept, and which Homeservers to
follow.

An Indexer is a service that reads Events from Homeservers to build its own queryable view of
their data: a search engine, a moderation tool, an archive, an app-specific index.

A Homeserver reports each write or delete of a Resource as an Event,
`PUT pubky://<user>/<path>` or `DEL pubky://<user>/<path>`. An Event names its Resource and
never carries its data: for a `PUT`, the Indexer reads the Resource itself, and it may be gone
by then.

Status: `0.1.0`, not published. The boundary and the names are still under discussion.

## Quick start: a Feed consumer

```rust,ignore
use pubky::EventCursor;
use pubky_watcher::{EventHandler, FeedEvent, Watcher, WatcherClient};

struct Print;

/// The Handler's own error type: a failed Run gives it back as `WatcherError::Handler`.
#[derive(Debug, thiserror::Error)]
#[error("could not print the event")]
struct PrintError;

#[async_trait::async_trait]
impl EventHandler<FeedEvent, PrintError> for Print {
    async fn handle(&self, event: &FeedEvent) -> Result<(), PrintError> {
        println!("{} {}", event.event_type, event.uri);
        Ok(())
    }
}

let consumer = Watcher::feed(WatcherClient::mainnet()?, hs_pubkey)
    .handler(Print)
    .build(shutdown_rx)?;

let outcome = consumer.run(EventCursor::new(stored_cursor)).await?;
// Store `outcome.checkpoint`, and start the next Run from it.
```

`Watcher::feed` builds a Feed consumer, which reads a Homeserver's Events feed (`/events/`):
the Events of every user it hosts, under one Cursor. `Watcher::stream(client, hs_pubkey)`
builds a Stream consumer instead: it reads the Event streams (`/events-stream`) of chosen User
pubkeys, with one Cursor per User pubkey and an optional path prefix (`/pub/` by default). It
is built once, and each Run is given its users, as pairs of User pubkey and Cursor:

```rust,ignore
let consumer = Watcher::stream(WatcherClient::mainnet()?, hs_pubkey)
    .handler(Print) // an `EventHandler<StreamEvent, PrintError>` here
    .build(shutdown_rx)?;

// `users` is a `Vec<(PublicKey, EventCursor)>`, from your Cursor store.
let outcome = consumer.run(users).await?;
// Store `outcome.checkpoints`: one pair per user given, in the order given. They are the
// next Run's users as they are. `outcome.failures` lists the users whose Batch ended early,
// and `outcome.stopped` says why the Run stopped before reading every user, if it did.
let outcome = consumer.run(outcome.checkpoints).await?;
```

A user listed twice fails the Run before any Read, with `WatcherError::DuplicateUser`. A Run
with no users is a normal one: it returns `Ok`, with nothing read and nothing handled.

Each call of a Consumer makes one Run. What a Run guarantees:

- **It returns a Checkpoint.** Every Event before it was handled, so it is the Cursor to
  store. A Stream consumer returns one per User pubkey.
- **Shutdown loses nothing.** A Run stopped by its shutdown signal returns `Ok` and a
  Checkpoint that may still be stored: a Feed consumer's with `completed = false`, a Stream
  consumer's with `stopped` set to `StopReason::Shutdown`.
- **An idle Homeserver is cheap.** It answers no Events, and the Checkpoint is the Cursor the
  Run started from.
- **A hostile or broken Homeserver cannot move you backwards.** The Batch of an Events feed
  must be a success, within the size cap, and carry a numeric Cursor that moves forward when
  it has Events. The Batch of an Event stream must hold no more Events than asked for, only
  those of the User pubkey asked for, with Cursors that only move forward. A Batch that breaks
  this is a Protocol violation: none of its Events reaches your Handler, and the Run reads
  nothing more from that Homeserver.
- **Misconfiguration fails at build time:** an events limit of zero, or a path that does not
  start with `/`. `build` fails with a `ConfigError`.

A Feed consumer's Run fails as a whole, and the next Run reads its Batch again. It fails with
a `WatcherError<E>`, where `E` is your Handler's error: `Homeserver` (an `HsError`), `Protocol`
(a `ProtocolViolation`: which rule the Homeserver broke, with the Cursors involved),
`Handler(E)` or `Panicked`.

### A Stream consumer's Run

A Stream consumer reads its users one after the other, so its Run reports what happened to
each of them:

- **It always returns every Checkpoint.** Whatever went wrong with a user or with the
  Homeserver, the Run returns `Ok` with a `StreamOutcome<E>`, whose `checkpoints` hold one
  pair of User pubkey and Cursor for every user given, in the order given. A plain `?` on a
  Run keeps them. A Checkpoint moves after each Event your Handler handled, and never past
  one it did not handle.
- **A failure that belongs to one user ends that user's Batch only.** The next user is still
  read. `failures` lists each such user with the error, a `WatcherError<E>`: `Handler(E)`,
  your Handler's own error, or `Homeserver`, the `HsError` of the Read the Homeserver refused.
- **A failure that belongs to the Homeserver stops the Run.** The users after it are not
  read. `stopped` says why, as a `StopReason`: `Homeserver` (an `HsError`), `Protocol` (a
  `ProtocolViolation`, which names the User pubkey whose Event stream broke the rules) or
  `Shutdown`, your own signal. It is `None` when every user was read. What stops a Run is not
  also listed in `failures`.
- **A Run's own error means the Run could not be made.** It fails with a `WatcherError<E>`
  only as `DuplicateUser` (a User pubkey given twice) or `Panicked` (its task panicked).

Which failure belongs to whom:

| Situation | Belongs to | What the Run does | Where that user's Checkpoint is |
|---|---|---|---|
| The Homeserver refuses the user's Read: `HsErrorKind` `NotFound`, `Rejected` (every status, `408` included), `TooLarge`, `Malformed`, `Sdk` | the user | Ends that user's Batch, reads the next user | The Cursor they were given |
| Your Handler fails on one of the user's Events | the user | Ends that user's Batch, reads the next user | The last Event handled |
| The Homeserver cannot be read: `HsErrorKind` `Timeout`, `Transport`, `ServerError`, `RateLimited` (the Client has already retried a rate-limited Read) | the Homeserver | Stops | The Cursor they were given |
| A Protocol violation, of any kind | the Homeserver | Stops; none of that answer's Events is handled | The Cursor they were given |
| The shutdown signal | the Indexer | Stops | The last Event handled |

A user the Run stopped before keeps the Cursor they were given. `handled_events` counts every
Event your Handler handled, those of a user whose Batch ended early included.

## The Client

`WatcherClient` is the crate's read-only access to the Pubky network. Build it with
`WatcherClient::mainnet()` or `WatcherClient::testnet(host)`, or set the request timeout and
the blob cap yourself:

```rust
let client = WatcherClient::builder()
    .testnet("localhost") // leave out for the public network
    .request_timeout(Duration::from_secs(30))
    .max_file_size(10 * (1 << 20))
    .build()?;
```

All three fail with a `ClientError`, whose `source()` is the SDK's error. A Client can also
wrap an SDK you already set up, with `from_pubky` or `from_shared`. `sdk()` returns the SDK
underneath, for what the Client does not read.

The Client makes four Reads and never writes. Each Read is a trait of its own, so that your
tests fake only the one they exercise:

| Trait | What it reads |
|---|---|
| `FeedReader` | a Homeserver's Events feed, `/events/` |
| `StreamReader` | a User pubkey's Event stream, `/events-stream` (a `429` is retried inside the Client) |
| `ResourceReader` | the Resource a `PUT` Event names, or a blob, under a size cap |
| `PkdnsLookup` | the Homeserver record a User pubkey publishes; `published_homeserver` reads the Homeserver pubkey it names |

The PKDNS lookup returns the Homeserver record as it was signed, so that you can keep it,
check its timestamp, and refuse an older answer that is replayed or lagging. A failed PKDNS
lookup is a `PkdnsLookupError`, whose `source()` is pkarr's error.

A failed Read of a Homeserver is an `HsError`: which Read (`ReadFeed`, `ReadStream`,
`ReadResource`, `ReadBlob`) and a kind (not found, rate limited, server error, rejected,
timeout, transport, too large, SDK, malformed), so that you can choose your own Retry policy.

## Advanced: your own `Run` and `Cycle`

The Consumers are built on lower-level traits, which an Indexer with its own Cursor store,
Retry queue or backoff implements directly:

- `FromEventLine` parses an Event line into your Event type.
- `Run<E, Err>` is one pass over one Homeserver: Events are read from a Cursor and each is
  handed to the Handler. Each Run is waited for on its own task, logged as slow after a
  threshold, and reported as failed if it panics. Its `handle_error` fails fast by default.
- `Cycle<E, Err>` is one round over the Homeservers you follow: one Turn each, one after the
  other, reported in `CycleStats` (Runs that completed, failed, panicked or reached their
  Budget, and Homeservers skipped). Its hooks decide which Homeservers it goes round
  (`homeservers`), which it skips, e.g. those in your backoff (`should_skip`), and how many
  Runs it makes (`run_limit`).
- Retry is opt-in: implement `RetryableError` for your error type and `RetryQueue` for your
  queue, and call `dispatch_retryable_error` from `handle_error`. The crate sorts a Handler's
  failure into a Missing dependency or a Transient failure; the queue and its limits are
  yours.

## Examples

See [`examples/`](examples/README.md): one per Consumer and one for your own `Run` under a
`Cycle`.

## Versioning

Until the crate is published, its API changes in breaking ways without a version bump. The
first published release starts the semver promise. Breaking changes so far, against the API
as it was first extracted:

- Every public name follows one glossary, written for the crate on its own. The event types
  are `FeedEvent`, `StreamEvent` and `EventType`. The Client's Reads are
  `FeedReader::read_feed`, `StreamReader::read_stream`, `ResourceReader::read_resource` and
  `read_blob`, and `PkdnsLookup::lookup_homeserver_record`. The hooks are `FromEventLine` and
  `RetryQueue`. The lower-level traits are `Run` and `Cycle`, with `Turn`, `TurnStatus`,
  `CycleStats` and `CycleReport`. The entry points are `Watcher::feed` and `Watcher::stream`,
  which build a `FeedConsumer` and a `StreamConsumer` whose outcomes carry a `checkpoint`.
  A parameter or field that holds a Homeserver pubkey is `hs_pubkey`, and one that holds a
  User pubkey is `user_pubkey`; only the `hs` field of `HsError`, and the parameter of
  `HsError::new` that fills it, keep their name. No old name is kept as an alias. Behaviour is
  unchanged. Text changes only where it prints a renamed name: a failed Read prints its `HsOp`
  variant, each Consumer labels its Run's span with its type name, and a `Debug` print shows
  the new type and field names.
- `WatcherError` is generic over the Handler's error: `WatcherError<E>` with `Homeserver`,
  `Protocol`, `Handler(E)` and `Panicked`, instead of a boxed error. A Handler returns its own
  error type, and the Consumers are `FeedConsumer<E>` and `StreamConsumer<E>`.
- `build()` fails with the new `ConfigError`, and a Homeserver that breaks the rules of its
  Events feed or Event stream is reported with the new `ProtocolViolation`.
- A Stream consumer is given its users on each Run, not when it is built.
  `Watcher::stream(client, hs_pubkey)` takes no users, and `StreamConsumer::run(users)` takes
  them as pairs of User pubkey and Cursor, the shape and order of the `checkpoints` it
  returns. A user listed twice fails the Run with the new `WatcherError::DuplicateUser`, and a
  Run with no users returns `Ok`. `ConfigError::NoUsers` and `ConfigError::DuplicateUser` are
  removed.
- A Stream consumer's Run reports its failures per user and always returns every Checkpoint.
  `StreamConsumer::run` returns `Ok` where it failed with `WatcherError::Homeserver`,
  `Protocol` or `Handler`: a refused Read or a Handler error ends that user's Batch and the
  next user is read, and a Homeserver that cannot be read or a Protocol violation stops the
  Run. It fails only with `DuplicateUser` or `Panicked`. `StreamOutcome` is
  `StreamOutcome<E>`, generic over the Handler's error: it gains `failures` and `stopped`,
  loses `completed`, and no longer implements `Clone`, `PartialEq` and `Eq`. The new
  `StopReason` says why a Run stopped.
- `BuildError` is removed: `Cycle` has an associated `type Error`, returned by `build_run`,
  `homeservers` and `run`.
- `HsErrorKind::from_sdk` and `HsErrorKind::from_reqwest` are no longer public.
- `WatcherClient::mainnet()` and `testnet()` fail with `ClientError` instead of
  `pubky::Error`, and `PkdnsLookup::lookup_homeserver_record` with the crate's
  `PkdnsLookupError` instead of pkarr's. Neither names an SDK type: the SDK's error is
  reachable only as their `source()`. `PkdnsLookupError`'s message still keeps the SDK's
  wording and includes the source's text ("Failed to resolve the DHT record: …"), so logs that
  print it read as before.

## Dependencies

The Client takes a Homeserver pubkey and a User pubkey as a `pubky::PublicKey`. The crate
depends on the Pubky SDK, `reqwest`, `tokio`, `futures` and `tracing`, and brings no database,
no metrics and no configuration with it.
