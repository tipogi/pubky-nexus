# pubky-watcher examples

## `poll_homeserver`

Minimal processor + runner: one `GET /events/` poll on the public staging
homeserver, split with [`EventBatch`](../src/events.rs), printed via
[`EventHandler`](../src/traits.rs).

### Staging homeserver

The example connects directly to the following public staging homeserver:

```text
ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy
```

### Run

`pubky-watcher` logs with `tracing`. This example prints with `println!` and
does not install `tracing-subscriber`.

```bash
cargo run -p pubky-watcher --example poll_homeserver
```
