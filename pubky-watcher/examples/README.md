# pubky-watcher examples

Each example builds a `WatcherClient` for mainnet and reads the public staging Homeserver:

```text
ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy
```

## `feed_consumer`

The quick path: a Feed consumer, built with `Watcher::feed` and given an `EventHandler`. It
makes five Runs over the Homeserver's Events feed (`/events/`), starting each from the
Checkpoint the one before returned, and reads each `PUT`'s Resource through `ResourceReader`.

```bash
cargo run -p pubky-watcher --example feed_consumer
```

## `stream_consumer`

Per User pubkey: a Stream consumer, built once with `Watcher::stream`. It makes three Runs
over the Event streams (`/events-stream`) of three User pubkeys on the Homeserver. Each Run
returns one Checkpoint per User pubkey, and they are the next Run's users as they are.

```bash
cargo run -p pubky-watcher --example stream_consumer
```

## `run_under_cycle`

The low-level path: an Indexer's own `Run`, built and run by a `Cycle` once per Turn, the way
Nexus uses the crate.

```bash
cargo run -p pubky-watcher --example run_under_cycle
```
