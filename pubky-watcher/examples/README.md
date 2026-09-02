# pubky-watcher examples

The examples cover the single-homeserver `/events/` feed, one user's
`/events-stream` feed, and the lower-level processor traits.

### Staging homeserver

```text
ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy
```

## `poll_homeserver_builder`

Convenience path: [`Watcher::homeserver`](../src/watcher.rs) with an
[`EventHandler`](../src/traits.rs). It polls one homeserver and returns the
cursor to pass into the next run or persist in application storage.

```bash
cargo run -p pubky-watcher --example poll_homeserver_builder
```

## `poll_key_stream_builder`

Per-key path: [`Watcher::key_stream`](../src/watcher.rs) polls one user's finite
event stream on one homeserver and returns that stream's next cursor.

```bash
PUBKY_USER=<user-public-key> \
  cargo run -p pubky-watcher --example poll_key_stream_builder
```

## `poll_homeserver`

Advanced path: implement [`TEventProcessor`](../src/processor.rs) and
[`TEventProcessorRunner`](../src/runner.rs) yourself (closer to how Nexus wires
the crate).

```bash
cargo run -p pubky-watcher --example poll_homeserver
```
